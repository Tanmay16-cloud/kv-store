#include <cstdint>
#include <iostream>
#include <limits>
#include <optional>
#include <sstream>
#include <string>
#include <utility>

#include "kvstore/net/socket.h"

namespace {

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

std::optional<Endpoint> ParseMovedResponse(const std::string& response) {
    std::istringstream input(response);
    std::string kind;
    std::string shard;
    std::string endpoint_text;
    std::string trailing;

    input >> kind >> shard >> endpoint_text;
    if (kind != "MOVED" || shard.empty() || endpoint_text.empty() || (input >> trailing)) {
        return std::nullopt;
    }

    return ParseEndpoint(endpoint_text);
}

bool ConnectTo(Endpoint endpoint,
               kvstore::net::SocketHandle& server,
               std::string& pending,
               std::string* greeting = nullptr) {
    const kvstore::net::SocketHandle next = kvstore::net::ConnectTcp(endpoint.host, endpoint.port);
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
    if (greeting != nullptr) {
        *greeting = std::move(next_greeting);
    }
    return true;
}

bool RoundTripWithRedirect(kvstore::net::SocketHandle& server,
                           std::string& pending,
                           const std::string& request,
                           std::string& response) {
    if (!kvstore::net::SendLine(server, request) ||
        !kvstore::net::ReceiveLine(server, pending, response)) {
        return false;
    }

    const auto redirect = ParseMovedResponse(response);
    if (!redirect.has_value()) {
        return true;
    }

    std::cerr << "Redirected to " << redirect->host << ':' << redirect->port << '\n';
    if (!ConnectTo(redirect.value(), server, pending)) {
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
    std::string pending;
    std::string greeting;
    if (!ConnectTo(Endpoint{argv[1], port.value()}, server, pending, &greeting)) {
        std::cerr << "Failed to connect to " << argv[1] << ':' << port.value() << '\n';
        return 1;
    }
    std::cout << greeting << '\n';

    std::string line;
    std::string response;
    while (std::cout << "kv-cli> " && std::getline(std::cin, line)) {
        if (!RoundTripWithRedirect(server, pending, line, response)) {
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
