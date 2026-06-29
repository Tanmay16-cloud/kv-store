#include "kvstore/cluster/cluster_metadata.h"

#include <functional>
#include <sstream>
#include <utility>

namespace kvstore::cluster {

ClusterMetadata::ClusterMetadata() = default;

ClusterMetadata::ClusterMetadata(std::string local_node_id,
                                 std::size_t shard_count,
                                 std::size_t local_shard_id,
                                 std::vector<ClusterNode> nodes)
    : local_node_id_(std::move(local_node_id)),
      shard_count_(shard_count),
      local_shard_id_(local_shard_id),
      nodes_(std::move(nodes)) {
    for (std::size_t i = 0; i < nodes_.size(); ++i) {
        shard_to_node_index_.emplace(nodes_[i].shard_id, i);
    }
}

bool ClusterMetadata::Enabled() const {
    return shard_count_ > 0 && shard_to_node_index_.contains(local_shard_id_);
}

std::size_t ClusterMetadata::ShardCount() const {
    return shard_count_;
}

std::size_t ClusterMetadata::LocalShardId() const {
    return local_shard_id_;
}

const std::string& ClusterMetadata::LocalNodeId() const {
    return local_node_id_;
}

std::size_t ClusterMetadata::ShardForKey(const std::string& key) const {
    if (shard_count_ == 0) {
        return 0;
    }

    return std::hash<std::string>{}(key) % shard_count_;
}

bool ClusterMetadata::OwnsKey(const std::string& key) const {
    return !Enabled() || ShardForKey(key) == local_shard_id_;
}

const ClusterNode* ClusterMetadata::OwnerForKey(const std::string& key) const {
    return NodeForShard(ShardForKey(key));
}

const ClusterNode* ClusterMetadata::NodeForShard(std::size_t shard_id) const {
    const auto found = shard_to_node_index_.find(shard_id);
    if (found == shard_to_node_index_.end()) {
        return nullptr;
    }

    return &nodes_[found->second];
}

const std::vector<ClusterNode>& ClusterMetadata::Nodes() const {
    return nodes_;
}

std::string ClusterMetadata::DescribeInfo() const {
    if (!Enabled()) {
        return "CLUSTER disabled";
    }

    std::ostringstream output;
    output << "CLUSTER node=" << local_node_id_ << " shard=" << local_shard_id_
           << " shard_count=" << shard_count_ << " nodes=" << nodes_.size();
    return output.str();
}

std::string ClusterMetadata::DescribeNodes() const {
    if (!Enabled()) {
        return "CLUSTER disabled";
    }

    std::ostringstream output;
    for (std::size_t i = 0; i < nodes_.size(); ++i) {
        const auto& node = nodes_[i];
        if (i != 0) {
            output << ';';
        }
        output << node.shard_id << ':' << node.node_id << '@' << node.host << ':' << node.port;
        if (node.is_local) {
            output << ":local";
        }
    }
    return output.str();
}

std::string ClusterMetadata::DescribeKey(const std::string& key) const {
    if (!Enabled()) {
        return "CLUSTER disabled";
    }

    const auto shard_id = ShardForKey(key);
    const auto* owner = NodeForShard(shard_id);
    std::ostringstream output;
    output << "KEY " << key << " SHARD " << shard_id;
    if (owner == nullptr) {
        output << " OWNER unknown";
    } else {
        output << " OWNER " << owner->node_id << ' ' << owner->host << ':' << owner->port;
    }
    return output.str();
}

}  // namespace kvstore::cluster
