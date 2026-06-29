#pragma once

#include <filesystem>
#include <mutex>

#include "kvstore/raft/raft_state.h"

namespace kvstore::persistence {

class RaftMetadataStore {
public:
    explicit RaftMetadataStore(std::filesystem::path path);

    bool Load(raft::RaftState& raft_state) const;
    bool Save(const raft::PersistentState& state);

private:
    std::filesystem::path path_;
    mutable std::mutex mutex_;
};

}  // namespace kvstore::persistence
