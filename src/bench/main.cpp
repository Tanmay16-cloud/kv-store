#include <chrono>
#include <cstdint>
#include <iostream>
#include <limits>
#include <optional>
#include <string>

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#endif

namespace {

#ifdef _WIN32

class WinsockSession {
public:
    WinsockSession() {
        ok_ = WSAStartup(MAKEWORD(2, 2), &data_) == 0;
    }

    ~WinsockSession() {
        if (ok_) {
            WSACleanup();
        }
    }

    bool Ok() const {
        return ok_;
    }

private:
    WSADATA data_{};
    bool ok_{false};
};

std::optional<std::uint16_t> ParsePort(const char* text) {
    try {
        const unsigned long value = std::stoul(text);
        if (value > std::numeric_limits<std::uint16_t>::max()) {
            return std::nullopt;
        }
        return static_cast<std::uint16_t>(value);
    } catch (...) {
        return std::nullopt;
    }
}

std::optional<std::size_t> ParseCount(const char* text) {
    try {
        std::size_t parsed_chars = 0;
        const auto value = std::stoull(text, &parsed_chars);
        if (parsed_chars != std::string(text).size()) {
            return std::nullopt;
        }
        return static_cast<std::size_t>(value);
    } catch (...) {
        return std::nullopt;
    }
}

bool SendLine(SOCKET socket, const std::string& line) {
    std::string message = line + "\n";
    const char* data = message.data();
    int remaining = static_cast<int>(message.size());

    while (remaining > 0) {
        const int sent = send(socket, data, remaining, 0);
        if (sent == SOCKET_ERROR) {
            return false;
        }

        data += sent;
        remaining -= sent;
    }

    return true;
}

bool ReceiveLine(SOCKET socket, std::string& pending, std::string& line) {
    while (true) {
        const std::size_t newline = pending.find('\n');
        if (newline != std::string::npos) {
            line = pending.substr(0, newline);
            pending.erase(0, newline + 1);
            if (!line.empty() && line.back() == '\r') {
                line.pop_back();
            }
            return true;
        }

        char buffer[4096]{};
        const int received = recv(socket, buffer, static_cast<int>(sizeof(buffer)), 0);
        if (received <= 0) {
            return false;
        }

        pending.append(buffer, static_cast<std::size_t>(received));
    }
}

SOCKET ConnectToServer(const char* host, std::uint16_t port) {
    addrinfo hints{};
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;

    addrinfo* resolved = nullptr;
    const std::string port_text = std::to_string(port);
    if (getaddrinfo(host, port_text.c_str(), &hints, &resolved) != 0) {
        return INVALID_SOCKET;
    }

    SOCKET server = INVALID_SOCKET;
    for (addrinfo* candidate = resolved; candidate != nullptr; candidate = candidate->ai_next) {
        server = socket(candidate->ai_family, candidate->ai_socktype, candidate->ai_protocol);
        if (server == INVALID_SOCKET) {
            continue;
        }

        if (connect(server, candidate->ai_addr, static_cast<int>(candidate->ai_addrlen)) == 0) {
            break;
        }

        closesocket(server);
        server = INVALID_SOCKET;
    }

    freeaddrinfo(resolved);
    return server;
}

bool RoundTrip(SOCKET socket, std::string& pending, const std::string& request, std::string& response) {
    return SendLine(socket, request) && ReceiveLine(socket, pending, response);
}

void PrintUsage() {
    std::cerr << "Usage: kv-bench <host> <port> <mode> <operations>\n";
    std::cerr << "Modes: set | get | mixed\n";
}

#endif

}  // namespace

int main(int argc, char* argv[]) {
#ifndef _WIN32
    std::cerr << "kv-bench is currently implemented for Windows using WinSock.\n";
    return 1;
#else
    if (argc != 5) {
        PrintUsage();
        return 1;
    }

    const auto port = ParsePort(argv[2]);
    const auto operations = ParseCount(argv[4]);
    const std::string mode = argv[3];
    if (!port.has_value() || !operations.has_value() || operations.value() == 0 ||
        (mode != "set" && mode != "get" && mode != "mixed")) {
        PrintUsage();
        return 1;
    }

    WinsockSession winsock;
    if (!winsock.Ok()) {
        std::cerr << "Failed to initialize WinSock.\n";
        return 1;
    }

    SOCKET server = ConnectToServer(argv[1], port.value());
    if (server == INVALID_SOCKET) {
        std::cerr << "Failed to connect to " << argv[1] << ':' << port.value() << '\n';
        return 1;
    }

    std::string pending;
    std::string response;
    if (!ReceiveLine(server, pending, response)) {
        std::cerr << "Failed to read server greeting.\n";
        closesocket(server);
        return 1;
    }

    if (mode == "get" || mode == "mixed") {
        for (std::size_t i = 0; i < operations.value(); ++i) {
            if (!RoundTrip(server,
                           pending,
                           "SET bench-" + std::to_string(i) + " value-" + std::to_string(i),
                           response) ||
                response.rfind("OK", 0) != 0) {
                std::cerr << "Failed to preload benchmark keys.\n";
                closesocket(server);
                return 1;
            }
        }
    }

    const auto started_at = std::chrono::steady_clock::now();
    for (std::size_t i = 0; i < operations.value(); ++i) {
        std::string request;
        if (mode == "set") {
            request = "SET bench-" + std::to_string(i) + " value-" + std::to_string(i);
        } else if (mode == "get") {
            request = "GET bench-" + std::to_string(i);
        } else {
            if (i % 2 == 0) {
                request = "SET bench-mixed-" + std::to_string(i) + " value-" + std::to_string(i);
            } else {
                request = "GET bench-" + std::to_string(i);
            }
        }

        if (!RoundTrip(server, pending, request, response)) {
            std::cerr << "Benchmark request failed at operation " << i << ".\n";
            closesocket(server);
            return 1;
        }

        if (response.rfind("ERR", 0) == 0 || response.rfind("MOVED", 0) == 0) {
            std::cerr << "Benchmark stopped after response: " << response << '\n';
            closesocket(server);
            return 1;
        }
    }
    const auto ended_at = std::chrono::steady_clock::now();

    const auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(ended_at - started_at);
    const double elapsed_seconds = static_cast<double>(elapsed.count()) / 1'000'000.0;
    const double ops_per_second = static_cast<double>(operations.value()) / elapsed_seconds;
    const double avg_latency_ms =
        static_cast<double>(elapsed.count()) / 1000.0 / static_cast<double>(operations.value());

    std::cout << "mode=" << mode << '\n';
    std::cout << "operations=" << operations.value() << '\n';
    std::cout << "elapsed_ms=" << (static_cast<double>(elapsed.count()) / 1000.0) << '\n';
    std::cout << "ops_per_second=" << ops_per_second << '\n';
    std::cout << "avg_latency_ms=" << avg_latency_ms << '\n';

    SendLine(server, "QUIT");
    closesocket(server);
    return 0;
#endif
}
