#pragma once

#include <filesystem>
#include <mutex>

#include "kvstore/protocol/command.h"
#include "kvstore/storage/kv_store.h"

namespace kvstore::persistence {

class WriteAheadLog {
public:
    WriteAheadLog(std::filesystem::path wal_path, std::filesystem::path snapshot_path);

    bool Replay(storage::KeyValueStore& store) const;
    bool Append(const protocol::Command& command);
    bool Compact(const storage::KeyValueStore& store);

private:
    bool LoadSnapshot(storage::KeyValueStore& store) const;
    bool SaveSnapshot(const storage::KeyValueStore& store) const;

    std::filesystem::path wal_path_;
    std::filesystem::path snapshot_path_;
    mutable std::mutex mutex_;
};

}  // namespace kvstore::persistence
