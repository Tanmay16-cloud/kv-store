#pragma once

#include <string>

#include "kvstore/protocol/command.h"
#include "kvstore/storage/kv_store.h"

namespace kvstore::protocol {

struct CommandResult {
    std::string response;
    bool should_close{false};
};

CommandResult ExecuteCommand(storage::KeyValueStore& store, const Command& command);

}  // namespace kvstore::protocol
