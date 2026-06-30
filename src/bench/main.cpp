#include <chrono>
#include <cstdint>
#include <iostream>
#include <limits>
#include <optional>
#include <sstream>
#include <string>
#include <utility>

#include "kvstore/net/socket.h"

namespace {

constexpr std::chrono::milliseconds kClientIoTimeout{5000};

struct Endpoint {
    std::string host;
    std::uint16_t port{};
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

std::optional<Endpoint> ParseEndpoint(const std::string& text) {
    const auto colon = text.rfind(':');
    if (colon == std::string::npos || colon == 0 || colon + 1 >= text.size()) {
        return std::nullopt;
    }

    const auto port = ParsePort(text.substr(colon + 1).c_str());
    if (!port.has_value()) {
        return std::nullopt;
    }

    return Endpoint{text.substr(0, colon), port.value()};
}

std::optional<Endpoint> ParseRedirectResponse(const std::string& response) {
    std::istringstream input(response);
    std::string kind;
    std::string shard;
    std::string endpoint_text;
    std::string trailing;

    input >> kind;
    if (kind == "MOVED") {
        input >> shard >> endpoint_text;
        if (shard.empty() || endpoint_text.empty() || (input >> trailing)) {
            return std::nullopt;
        }
        return ParseEndpoint(endpoint_text);
    }

    if (kind == "NOT_LEADER") {
        input >> endpoint_text;
        if (endpoint_text.empty() || (input >> trailing)) {
            return std::nullopt;
        }
        return ParseEndpoint(endpoint_text);
    }

    return std::nullopt;
}

bool IsRedirectToCurrentServer(const Endpoint& redirect, const Endpoint& current_endpoint) {
    return redirect.host == current_endpoint.host && redirect.port == current_endpoint.port;
}

bool ConnectTo(Endpoint endpoint,
               kvstore::net::SocketHandle& server,
               std::string& pending,
               Endpoint& current_endpoint) {
    const kvstore::net::SocketHandle next =
        kvstore::net::ConnectTcp(endpoint.host, endpoint.port, kClientIoTimeout);
    if (next == kvstore::net::kInvalidSocket) {
        return false;
    }

    std::string next_pending;
    std::string greeting;
    if (!kvstore::net::ReceiveLine(next, next_pending, greeting)) {
        kvstore::net::CloseSocket(next);
        return false;
    }

    if (server != kvstore::net::kInvalidSocket) {
        kvstore::net::CloseSocket(server);
    }

    server = next;
    pending = std::move(next_pending);
    current_endpoint = std::move(endpoint);
    return true;
}

bool RoundTrip(kvstore::net::SocketHandle& server,
               std::string& pending,
               Endpoint& current_endpoint,
               const std::string& request,
               std::string& response) {
    if (!kvstore::net::SendLine(server, request) ||
        !kvstore::net::ReceiveLine(server, pending, response)) {
        return false;
    }

    const auto redirect = ParseRedirectResponse(response);
    if (!redirect.has_value()) {
        return true;
    }

    if (IsRedirectToCurrentServer(redirect.value(), current_endpoint)) {
        return true;
    }

    if (!ConnectTo(redirect.value(), server, pending, current_endpoint)) {
        return false;
    }

    return kvstore::net::SendLine(server, request) &&
           kvstore::net::ReceiveLine(server, pending, response);
}

void PrintUsage() {
    std::cerr << "Usage: kv-bench <host> <port> <mode> <operations>\n";
    std::cerr << "Modes: set | get | mixed\n";
}

}  // namespace

int main(int argc, char* argv[]) {
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

    kvstore::net::SocketRuntime socket_runtime;
    if (!socket_runtime.Ok()) {
        std::cerr << "Failed to initialize socket runtime.\n";
        return 1;
    }

    kvstore::net::SocketHandle server = kvstore::net::kInvalidSocket;
    Endpoint current_endpoint{argv[1], port.value()};
    std::string pending;
    if (!ConnectTo(current_endpoint, server, pending, current_endpoint)) {
        std::cerr << "Failed to connect to " << argv[1] << ':' << port.value() << '\n';
        return 1;
    }

    std::string response;
    if (mode == "get" || mode == "mixed") {
        for (std::size_t i = 0; i < operations.value(); ++i) {
            if (!RoundTrip(server,
                           pending,
                           current_endpoint,
                           "SET bench-" + std::to_string(i) + " value-" + std::to_string(i),
                           response) ||
                response.rfind("OK", 0) != 0) {
                std::cerr << "Failed to preload benchmark keys.\n";
                kvstore::net::CloseSocket(server);
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

        if (!RoundTrip(server, pending, current_endpoint, request, response)) {
            std::cerr << "Benchmark request failed at operation " << i << ".\n";
            kvstore::net::CloseSocket(server);
            return 1;
        }

        if (response.rfind("ERR", 0) == 0 || response.rfind("MOVED", 0) == 0 ||
            response.rfind("NOT_LEADER", 0) == 0) {
            std::cerr << "Benchmark stopped after response: " << response << '\n';
            kvstore::net::CloseSocket(server);
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

    kvstore::net::SendLine(server, "QUIT");
    kvstore::net::CloseSocket(server);
    return 0;
}
