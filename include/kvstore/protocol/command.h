#pragma once

#include <string>

namespace kvstore::protocol {

enum class CommandType {
    Set,
    Get,
    Del,
    Exists,
    Ping,
    Metrics,
    ClusterInfo,
    ClusterNodes,
    ClusterKey,
    Quit,
};

struct Command {
    CommandType type{};
    std::string key;
    std::string value;
    bool is_replicated{false};
};

}  // namespace kvstore::protocol
