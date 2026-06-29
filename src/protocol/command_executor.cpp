#include "kvstore/protocol/command_executor.h"

#include "kvstore/persistence/write_ahead_log.h"

namespace kvstore::protocol {

CommandResult ExecuteCommand(storage::KeyValueStore& store,
                             const Command& command,
                             persistence::WriteAheadLog* wal,
                             const cluster::ClusterMetadata* cluster_metadata,
                             const metrics::ServerMetrics* server_metrics) {
    switch (command.type) {
        case CommandType::Set:
            if (wal != nullptr && !wal->Append(command)) {
                return {"ERR failed to persist command", false};
            }
            return {store.Set(command.key, command.value) ? "OK" : "ERR invalid key", false};
        case CommandType::Get: {
            const auto value = store.Get(command.key);
            return {value.value_or("NOT_FOUND"), false};
        }
        case CommandType::Del:
            if (wal != nullptr && !wal->Append(command)) {
                return {"ERR failed to persist command", false};
            }
            return {store.Delete(command.key) ? "1" : "0", false};
        case CommandType::Exists:
            return {store.Exists(command.key) ? "1" : "0", false};
        case CommandType::Metrics:
            if (server_metrics == nullptr) {
                return {"ERR metrics unavailable", false};
            }
            return {server_metrics->Describe(), false};
        case CommandType::ClusterInfo:
            if (cluster_metadata == nullptr) {
                return {"ERR cluster metadata unavailable", false};
            }
            return {cluster_metadata->DescribeInfo(), false};
        case CommandType::ClusterNodes:
            if (cluster_metadata == nullptr) {
                return {"ERR cluster metadata unavailable", false};
            }
            return {cluster_metadata->DescribeNodes(), false};
        case CommandType::ClusterKey:
            if (cluster_metadata == nullptr) {
                return {"ERR cluster metadata unavailable", false};
            }
            return {cluster_metadata->DescribeKey(command.key), false};
        case CommandType::Quit:
            return {"BYE", true};
    }

    return {"ERR invalid command", false};
}

}  // namespace kvstore::protocol
