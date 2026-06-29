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

#endif

}  // namespace

int main(int argc, char* argv[]) {
#ifndef _WIN32
    std::cerr << "kv-cli is currently implemented for Windows using WinSock.\n";
    return 1;
#else
    if (argc != 3) {
        std::cerr << "Usage: kv-cli <host> <port>\n";
        return 1;
    }

    const auto port = ParsePort(argv[2]);
    if (!port.has_value()) {
        std::cerr << "Invalid port: " << argv[2] << '\n';
        return 1;
    }

    WinsockSession winsock;
    if (!winsock.Ok()) {
        std::cerr << "Failed to initialize WinSock.\n";
        return 1;
    }

    addrinfo hints{};
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;

    addrinfo* resolved = nullptr;
    const std::string port_text = std::to_string(port.value());
    if (getaddrinfo(argv[1], port_text.c_str(), &hints, &resolved) != 0) {
        std::cerr << "Failed to resolve host: " << argv[1] << '\n';
        return 1;
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

    if (server == INVALID_SOCKET) {
        std::cerr << "Failed to connect to " << argv[1] << ':' << port.value() << '\n';
        return 1;
    }

    std::string pending;
    std::string response;
    if (ReceiveLine(server, pending, response)) {
        std::cout << response << '\n';
    }

    std::string line;
    while (std::cout << "kv-cli> " && std::getline(std::cin, line)) {
        if (!SendLine(server, line)) {
            std::cerr << "Failed to send command.\n";
            closesocket(server);
            return 1;
        }

        if (!ReceiveLine(server, pending, response)) {
            std::cerr << "Server closed the connection.\n";
            closesocket(server);
            return 1;
        }

        std::cout << response << '\n';
        if (response == "BYE") {
            break;
        }
    }

    closesocket(server);
    return 0;
#endif
}
