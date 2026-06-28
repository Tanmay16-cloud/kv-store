#include "kvstore/protocol/command_executor.h"

namespace kvstore::protocol {

CommandResult ExecuteCommand(storage::KeyValueStore& store, const Command& command) {
    switch (command.type) {
        case CommandType::Set:
            return {store.Set(command.key, command.value) ? "OK" : "ERR invalid key", false};
        case CommandType::Get: {
            const auto value = store.Get(command.key);
            return {value.value_or("NOT_FOUND"), false};
        }
        case CommandType::Del:
            return {store.Delete(command.key) ? "1" : "0", false};
        case CommandType::Exists:
            return {store.Exists(command.key) ? "1" : "0", false};
        case CommandType::Quit:
            return {"BYE", true};
    }

    return {"ERR invalid command", false};
}

}  // namespace kvstore::protocol
