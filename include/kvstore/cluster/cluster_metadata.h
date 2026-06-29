#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace kvstore::cluster {

struct ClusterNode {
    std::string node_id;
    std::string host;
    std::uint16_t port{};
    std::size_t shard_id{};
    bool is_local{false};
};

class ClusterMetadata {
public:
    ClusterMetadata();
    ClusterMetadata(std::string local_node_id,
                    std::size_t shard_count,
                    std::size_t local_shard_id,
                    std::vector<ClusterNode> nodes);

    bool Enabled() const;
    std::size_t ShardCount() const;
    std::size_t LocalShardId() const;
    const std::string& LocalNodeId() const;
    std::size_t ShardForKey(const std::string& key) const;
    bool OwnsKey(const std::string& key) const;
    const ClusterNode* OwnerForKey(const std::string& key) const;
    const ClusterNode* NodeForShard(std::size_t shard_id) const;
    const std::vector<ClusterNode>& Nodes() const;

    std::string DescribeInfo() const;
    std::string DescribeNodes() const;
    std::string DescribeKey(const std::string& key) const;

private:
    std::string local_node_id_;
    std::size_t shard_count_{0};
    std::size_t local_shard_id_{0};
    std::vector<ClusterNode> nodes_;
    std::unordered_map<std::size_t, std::size_t> shard_to_node_index_;
};

}  // namespace kvstore::cluster
