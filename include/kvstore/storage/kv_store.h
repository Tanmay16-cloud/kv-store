#pragma once

#include <cstddef>
#include <optional>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <vector>

namespace kvstore::storage {

class KeyValueStore {
public:
    bool Set(std::string key, std::string value);
    std::optional<std::string> Get(const std::string& key) const;
    bool Delete(const std::string& key);
    bool Exists(const std::string& key) const;
    std::size_t Size() const;
    std::vector<std::pair<std::string, std::string>> Snapshot() const;
    void ReplaceAll(std::vector<std::pair<std::string, std::string>> entries);

private:
    mutable std::shared_mutex mutex_;
    std::unordered_map<std::string, std::string> data_;
};

}  // namespace kvstore::storage
