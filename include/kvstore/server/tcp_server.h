#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "kvstore/cluster/cluster_metadata.h"
#include "kvstore/metrics/server_metrics.h"
#include "kvstore/persistence/raft_metadata_store.h"
#include "kvstore/persistence/write_ahead_log.h"
#include "kvstore/raft/raft_state.h"
#include "kvstore/storage/kv_store.h"

namespace kvstore::server {

enum class ServerRole {
    Standalone,
    Leader,
    Follower,
};

struct FollowerEndpoint {
    std::string host;
    std::uint16_t port{};
};

class TcpServer {
public:
    explicit TcpServer(std::uint16_t port);
    TcpServer(std::uint16_t port,
              ServerRole role,
              std::vector<FollowerEndpoint> followers,
              std::optional<cluster::ClusterMetadata> cluster_metadata = std::nullopt);

    int Run();

private:
    static constexpr std::size_t kCompactionThreshold = 5;

    std::uint16_t port_;
    ServerRole role_;
    std::vector<FollowerEndpoint> followers_;
    raft::RaftState raft_state_;
    storage::KeyValueStore store_;
    std::optional<cluster::ClusterMetadata> cluster_metadata_;
    metrics::ServerMetrics metrics_;
    persistence::WriteAheadLog wal_;
    persistence::RaftMetadataStore raft_metadata_;
    std::mutex execution_mutex_;
    std::chrono::steady_clock::time_point last_leader_contact_;
    std::size_t writes_since_compaction_{0};
};

}  // namespace kvstore::server
