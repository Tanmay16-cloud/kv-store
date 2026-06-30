#pragma once

#include <cstdint>
#include <string>
#include <string_view>

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <winsock2.h>
#endif

namespace kvstore::net {

#ifdef _WIN32
using SocketHandle = SOCKET;
inline constexpr SocketHandle kInvalidSocket = INVALID_SOCKET;
#else
using SocketHandle = int;
inline constexpr SocketHandle kInvalidSocket = -1;
#endif

class SocketRuntime {
public:
    SocketRuntime();
    ~SocketRuntime();

    SocketRuntime(const SocketRuntime&) = delete;
    SocketRuntime& operator=(const SocketRuntime&) = delete;

    bool Ok() const;

private:
#ifdef _WIN32
    WSADATA data_{};
#endif
    bool ok_{false};
};

void CloseSocket(SocketHandle socket);
bool SendLine(SocketHandle socket, std::string_view line);
bool ReceiveLine(SocketHandle socket, std::string& pending, std::string& line);

SocketHandle ConnectTcp(std::string_view host, std::uint16_t port);
SocketHandle ListenTcp(std::uint16_t port);
SocketHandle AcceptTcp(SocketHandle listener);

}  // namespace kvstore::net
