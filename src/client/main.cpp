#include <cstdint>
#include <chrono>
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

std::string RedirectKind(const std::string& response) {
    std::istringstream input(response);
    std::string kind;
    input >> kind;
    if (kind == "MOVED" || kind == "NOT_LEADER") {
        return kind;
    }
    return {};
}

bool IsRedirectToCurrentServer(const Endpoint& redirect, const Endpoint& current_endpoint) {
    return redirect.host == current_endpoint.host && redirect.port == current_endpoint.port;
}

bool ConnectTo(Endpoint endpoint,
               kvstore::net::SocketHandle& server,
               std::string& pending,
               Endpoint& current_endpoint,
               std::string* greeting = nullptr) {
    const kvstore::net::SocketHandle next =
        kvstore::net::ConnectTcp(endpoint.host, endpoint.port, kClientIoTimeout);
    if (next == kvstore::net::kInvalidSocket) {
        return false;
    }

    std::string next_pending;
    std::string next_greeting;
    if (!kvstore::net::ReceiveLine(next, next_pending, next_greeting)) {
        kvstore::net::CloseSocket(next);
        return false;
    }

    if (server != kvstore::net::kInvalidSocket) {
        kvstore::net::CloseSocket(server);
    }

    server = next;
    pending = std::move(next_pending);
    current_endpoint = std::move(endpoint);
    if (greeting != nullptr) {
        *greeting = std::move(next_greeting);
    }
    return true;
}

bool RoundTripWithRedirect(kvstore::net::SocketHandle& server,
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

    const auto kind = RedirectKind(response);
    std::cerr << (kind == "NOT_LEADER" ? "Leader is " : "Redirected to ")
              << redirect->host << ':' << redirect->port << '\n';
    if (!ConnectTo(redirect.value(), server, pending, current_endpoint)) {
        return false;
    }

    return kvstore::net::SendLine(server, request) &&
           kvstore::net::ReceiveLine(server, pending, response);
}

}  // namespace

int main(int argc, char* argv[]) {
    if (argc != 3) {
        std::cerr << "Usage: kv-cli <host> <port>\n";
        return 1;
    }

    const auto port = ParsePort(argv[2]);
    if (!port.has_value()) {
        std::cerr << "Invalid port: " << argv[2] << '\n';
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
    std::string greeting;
    if (!ConnectTo(current_endpoint, server, pending, current_endpoint, &greeting)) {
        std::cerr << "Failed to connect to " << argv[1] << ':' << port.value() << '\n';
        return 1;
    }
    std::cout << greeting << '\n';

    std::string line;
    std::string response;
    while (std::cout << "kv-cli> " && std::getline(std::cin, line)) {
        if (!RoundTripWithRedirect(server, pending, current_endpoint, line, response)) {
            std::cerr << "Command failed or server closed the connection.\n";
            kvstore::net::CloseSocket(server);
            return 1;
        }

        std::cout << response << '\n';
        if (response == "BYE") {
            break;
        }
    }

    kvstore::net::CloseSocket(server);
    return 0;
}
