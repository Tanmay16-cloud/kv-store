#include "kvstore/net/socket.h"

#include <algorithm>
#include <array>
#include <cstddef>
#include <limits>
#include <string>

#ifdef _WIN32
#include <ws2tcpip.h>
#else
#include <csignal>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

namespace kvstore::net {
namespace {

int SendBytes(SocketHandle socket, const char* data, std::size_t size) {
#ifdef _WIN32
    const auto bounded_size =
        static_cast<int>(std::min(size, static_cast<std::size_t>(std::numeric_limits<int>::max())));
    const int sent = send(socket, data, bounded_size, 0);
    return sent == SOCKET_ERROR ? -1 : sent;
#else
    const auto sent = send(socket, data, size, 0);
    if (sent <= 0 || sent > static_cast<decltype(sent)>(std::numeric_limits<int>::max())) {
        return -1;
    }
    return static_cast<int>(sent);
#endif
}

int ReceiveBytes(SocketHandle socket, char* data, std::size_t size) {
#ifdef _WIN32
    const auto bounded_size =
        static_cast<int>(std::min(size, static_cast<std::size_t>(std::numeric_limits<int>::max())));
    const int received = recv(socket, data, bounded_size, 0);
    return received == SOCKET_ERROR ? -1 : received;
#else
    const auto received = recv(socket, data, size, 0);
    if (received <= 0 || received > static_cast<decltype(received)>(std::numeric_limits<int>::max())) {
        return -1;
    }
    return static_cast<int>(received);
#endif
}

bool SocketCallFailed(int result) {
#ifdef _WIN32
    return result == SOCKET_ERROR;
#else
    return result != 0;
#endif
}

int ConnectSocket(SocketHandle socket, const addrinfo* address) {
#ifdef _WIN32
    return connect(socket, address->ai_addr, static_cast<int>(address->ai_addrlen));
#else
    return connect(socket, address->ai_addr, address->ai_addrlen);
#endif
}

}  // namespace

SocketRuntime::SocketRuntime() {
#ifdef _WIN32
    ok_ = WSAStartup(MAKEWORD(2, 2), &data_) == 0;
#else
    std::signal(SIGPIPE, SIG_IGN);
    ok_ = true;
#endif
}

SocketRuntime::~SocketRuntime() {
#ifdef _WIN32
    if (ok_) {
        WSACleanup();
    }
#endif
}

bool SocketRuntime::Ok() const {
    return ok_;
}

void CloseSocket(SocketHandle socket) {
    if (socket == kInvalidSocket) {
        return;
    }

#ifdef _WIN32
    closesocket(socket);
#else
    close(socket);
#endif
}

bool SendLine(SocketHandle socket, std::string_view line) {
    std::string message(line);
    message.push_back('\n');

    const char* data = message.data();
    std::size_t remaining = message.size();

    while (remaining > 0) {
        const int sent = SendBytes(socket, data, remaining);
        if (sent <= 0) {
            return false;
        }

        data += sent;
        remaining -= static_cast<std::size_t>(sent);
    }

    return true;
}

bool ReceiveLine(SocketHandle socket, std::string& pending, std::string& line) {
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

        std::array<char, 4096> buffer{};
        const int received = ReceiveBytes(socket, buffer.data(), buffer.size());
        if (received <= 0) {
            return false;
        }
        pending.append(buffer.data(), static_cast<std::size_t>(received));
    }
}

SocketHandle ConnectTcp(std::string_view host, std::uint16_t port) {
    addrinfo hints{};
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;

    addrinfo* resolved = nullptr;
    const std::string host_text(host);
    const std::string port_text = std::to_string(port);
    if (getaddrinfo(host_text.c_str(), port_text.c_str(), &hints, &resolved) != 0) {
        return kInvalidSocket;
    }

    SocketHandle connected_socket = kInvalidSocket;
    for (addrinfo* candidate = resolved; candidate != nullptr; candidate = candidate->ai_next) {
        connected_socket =
            socket(candidate->ai_family, candidate->ai_socktype, candidate->ai_protocol);
        if (connected_socket == kInvalidSocket) {
            continue;
        }

        if (ConnectSocket(connected_socket, candidate) == 0) {
            break;
        }

        CloseSocket(connected_socket);
        connected_socket = kInvalidSocket;
    }

    freeaddrinfo(resolved);
    return connected_socket;
}

SocketHandle ListenTcp(std::uint16_t port) {
    const SocketHandle listener = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (listener == kInvalidSocket) {
        return kInvalidSocket;
    }

    const int reuse_address = 1;
#ifdef _WIN32
    setsockopt(listener,
               SOL_SOCKET,
               SO_REUSEADDR,
               reinterpret_cast<const char*>(&reuse_address),
               sizeof(reuse_address));
#else
    setsockopt(listener, SOL_SOCKET, SO_REUSEADDR, &reuse_address, sizeof(reuse_address));
#endif

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_ANY);
    address.sin_port = htons(port);

    if (SocketCallFailed(bind(listener, reinterpret_cast<sockaddr*>(&address), sizeof(address)))) {
        CloseSocket(listener);
        return kInvalidSocket;
    }

    if (SocketCallFailed(listen(listener, SOMAXCONN))) {
        CloseSocket(listener);
        return kInvalidSocket;
    }

    return listener;
}

SocketHandle AcceptTcp(SocketHandle listener) {
    return accept(listener, nullptr, nullptr);
}

}  // namespace kvstore::net
