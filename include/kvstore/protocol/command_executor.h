#pragma once

#include <string>

#include "kvstore/cluster/cluster_metadata.h"
#include "kvstore/metrics/server_metrics.h"
#include "kvstore/protocol/command.h"
#include "kvstore/storage/kv_store.h"

namespace kvstore::persistence {
class WriteAheadLog;
}

namespace kvstore::protocol {

struct CommandResult {
    std::string response;
    bool should_close{false};
};

CommandResult ExecuteCommand(storage::KeyValueStore& store,
                             const Command& command,
                             persistence::WriteAheadLog* wal = nullptr,
                             const cluster::ClusterMetadata* cluster_metadata = nullptr,
                             const metrics::ServerMetrics* server_metrics = nullptr);

}  // namespace kvstore::protocol
