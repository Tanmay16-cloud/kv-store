#pragma once

#include <optional>
#include <string_view>

#include "kvstore/protocol/command.h"

namespace kvstore::protocol {

class CommandParser {
public:
    static std::optional<Command> Parse(std::string_view input);
};

}  // namespace kvstore::protocol
