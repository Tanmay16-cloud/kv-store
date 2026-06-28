#include "kvstore/server/tcp_server.h"

#include <array>
#include <functional>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "kvstore/protocol/command_executor.h"
#include "kvstore/protocol/command_parser.h"

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#endif

namespace kvstore::server {
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

bool SendLine(SOCKET client, const std::string& line) {
    std::string response = line + "\n";
    const char* data = response.data();
    int remaining = static_cast<int>(response.size());

    while (remaining > 0) {
        const int sent = send(client, data, remaining, 0);
        if (sent == SOCKET_ERROR) {
            return false;
        }

        data += sent;
        remaining -= sent;
    }

    return true;
}

void HandleClient(SOCKET client, storage::KeyValueStore& store) {
    std::array<char, 4096> buffer{};
    std::string pending;

    SendLine(client, "OK connected to kvstore");

    while (true) {
        const int bytes_received = recv(client, buffer.data(), static_cast<int>(buffer.size()), 0);
        if (bytes_received <= 0) {
            break;
        }

        pending.append(buffer.data(), static_cast<std::size_t>(bytes_received));

        std::size_t newline = pending.find('\n');
        while (newline != std::string::npos) {
            const std::string line = pending.substr(0, newline);
            pending.erase(0, newline + 1);

            const auto parsed = protocol::CommandParser::Parse(line);
            if (!parsed.has_value()) {
                if (!SendLine(client, "ERR invalid command")) {
                    closesocket(client);
                    return;
                }
            } else {
                const auto result = protocol::ExecuteCommand(store, parsed.value());
                if (!SendLine(client, result.response)) {
                    closesocket(client);
                    return;
                }
                if (result.should_close) {
                    closesocket(client);
                    return;
                }
            }

            newline = pending.find('\n');
        }
    }

    closesocket(client);
}

#endif

}  // namespace

TcpServer::TcpServer(std::uint16_t port) : port_(port) {}

int TcpServer::Run() {
#ifndef _WIN32
    std::cerr << "TCP server is currently implemented for Windows using WinSock.\n";
    return 1;
#else
    WinsockSession winsock;
    if (!winsock.Ok()) {
        std::cerr << "Failed to initialize WinSock.\n";
        return 1;
    }

    const SOCKET listener = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (listener == INVALID_SOCKET) {
        std::cerr << "Failed to create server socket.\n";
        return 1;
    }

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_ANY);
    address.sin_port = htons(port_);

    if (bind(listener, reinterpret_cast<sockaddr*>(&address), sizeof(address)) == SOCKET_ERROR) {
        std::cerr << "Failed to bind TCP server to port " << port_ << ".\n";
        closesocket(listener);
        return 1;
    }

    if (listen(listener, SOMAXCONN) == SOCKET_ERROR) {
        std::cerr << "Failed to listen for TCP clients.\n";
        closesocket(listener);
        return 1;
    }

    std::cout << "KV TCP server listening on port " << port_ << ".\n";

    while (true) {
        SOCKET client = accept(listener, nullptr, nullptr);
        if (client == INVALID_SOCKET) {
            std::cerr << "Failed to accept client connection.\n";
            continue;
        }

        std::thread client_thread(HandleClient, client, std::ref(store_));
        client_thread.detach();
    }

    closesocket(listener);
    return 0;
#endif
}

}  // namespace kvstore::server
