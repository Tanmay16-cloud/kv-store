#include "kvstore/net/socket.h"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstddef>
#include <limits>
#include <string>

#ifdef _WIN32
#include <ws2tcpip.h>
#else
#include <cerrno>
#include <csignal>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/select.h>
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

bool SetSocketBlocking(SocketHandle socket, bool blocking) {
#ifdef _WIN32
    u_long mode = blocking ? 0 : 1;
    return ioctlsocket(socket, FIONBIO, &mode) == 0;
#else
    const int flags = fcntl(socket, F_GETFL, 0);
    if (flags < 0) {
        return false;
    }

    const int next_flags = blocking ? (flags & ~O_NONBLOCK) : (flags | O_NONBLOCK);
    return fcntl(socket, F_SETFL, next_flags) == 0;
#endif
}

bool SocketConnectInProgress() {
#ifdef _WIN32
    const int error = WSAGetLastError();
    return error == WSAEWOULDBLOCK || error == WSAEINPROGRESS || error == WSAEINVAL;
#else
    return errno == EINPROGRESS;
#endif
}

int GetSocketError(SocketHandle socket) {
    int socket_error = 0;
#ifdef _WIN32
    int length = sizeof(socket_error);
    if (getsockopt(socket, SOL_SOCKET, SO_ERROR, reinterpret_cast<char*>(&socket_error), &length) != 0) {
        return WSAGetLastError();
    }
#else
    socklen_t length = sizeof(socket_error);
    if (getsockopt(socket, SOL_SOCKET, SO_ERROR, &socket_error, &length) != 0) {
        return errno;
    }
#endif
    return socket_error;
}

timeval ToTimeval(std::chrono::milliseconds timeout) {
    timeval value{};
    value.tv_sec = static_cast<long>(timeout.count() / 1000);
    value.tv_usec = static_cast<long>((timeout.count() % 1000) * 1000);
    return value;
}

void ApplyIoTimeouts(SocketHandle socket, std::chrono::milliseconds timeout) {
#ifdef _WIN32
    const DWORD timeout_ms = static_cast<DWORD>(timeout.count());
    setsockopt(socket, SOL_SOCKET, SO_RCVTIMEO, reinterpret_cast<const char*>(&timeout_ms),
               sizeof(timeout_ms));
    setsockopt(socket, SOL_SOCKET, SO_SNDTIMEO, reinterpret_cast<const char*>(&timeout_ms),
               sizeof(timeout_ms));
#else
    timeval value = ToTimeval(timeout);
    setsockopt(socket, SOL_SOCKET, SO_RCVTIMEO, &value, sizeof(value));
    setsockopt(socket, SOL_SOCKET, SO_SNDTIMEO, &value, sizeof(value));
#endif
}

bool ConnectSocket(SocketHandle socket,
                   const addrinfo* address,
                   std::chrono::milliseconds timeout) {
    if (!SetSocketBlocking(socket, false)) {
        return false;
    }

#ifdef _WIN32
    const int result = connect(socket, address->ai_addr, static_cast<int>(address->ai_addrlen));
    if (result == 0) {
        SetSocketBlocking(socket, true);
        ApplyIoTimeouts(socket, timeout);
        return true;
    }
#else
    const int result = connect(socket, address->ai_addr, address->ai_addrlen);
    if (result == 0) {
        SetSocketBlocking(socket, true);
        ApplyIoTimeouts(socket, timeout);
        return true;
    }
#endif

    if (!SocketConnectInProgress()) {
        SetSocketBlocking(socket, true);
        return false;
    }

    fd_set write_set;
    FD_ZERO(&write_set);
    FD_SET(socket, &write_set);

    timeval wait_time = ToTimeval(timeout);
    const int ready = select(static_cast<int>(socket + 1), nullptr, &write_set, nullptr, &wait_time);
    if (ready <= 0 || !FD_ISSET(socket, &write_set) || GetSocketError(socket) != 0) {
        SetSocketBlocking(socket, true);
        return false;
    }

    SetSocketBlocking(socket, true);
    ApplyIoTimeouts(socket, timeout);
    return true;
}

bool IsInvalidAcceptedSocket(SocketHandle socket) {
#ifdef _WIN32
    return socket == INVALID_SOCKET;
#else
    return socket < 0;
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

SocketHandle ConnectTcp(std::string_view host,
                        std::uint16_t port,
                        std::chrono::milliseconds timeout) {
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

        if (ConnectSocket(connected_socket, candidate, timeout)) {
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
    const SocketHandle accepted = accept(listener, nullptr, nullptr);
    return IsInvalidAcceptedSocket(accepted) ? kInvalidSocket : accepted;
}

}  // namespace kvstore::net
