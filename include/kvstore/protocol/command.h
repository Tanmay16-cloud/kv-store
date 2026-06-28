#pragma once

#include <string>

namespace kvstore::protocol {

enum class CommandType {
    Set,
    Get,
    Del,
    Exists,
    Quit,
};

struct Command {
    CommandType type{};
    std::string key;
    std::string value;
};

}  // namespace kvstore::protocol
