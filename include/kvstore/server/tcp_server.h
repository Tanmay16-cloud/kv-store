#pragma once

#include <cstdint>

#include "kvstore/storage/kv_store.h"

namespace kvstore::server {

class TcpServer {
public:
    explicit TcpServer(std::uint16_t port);

    int Run();

private:
    std::uint16_t port_;
    storage::KeyValueStore store_;
};

}  // namespace kvstore::server
