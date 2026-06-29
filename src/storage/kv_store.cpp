#include "kvstore/storage/kv_store.h"

#include <mutex>
#include <utility>

namespace kvstore::storage {

bool KeyValueStore::Set(std::string key, std::string value) {
    if (key.empty()) {
        return false;
    }

    std::unique_lock lock(mutex_);
    data_[std::move(key)] = std::move(value);
    return true;
}

std::optional<std::string> KeyValueStore::Get(const std::string& key) const {
    std::shared_lock lock(mutex_);
    const auto iter = data_.find(key);
    if (iter == data_.end()) {
        return std::nullopt;
    }

    return iter->second;
}

bool KeyValueStore::Delete(const std::string& key) {
    std::unique_lock lock(mutex_);
    return data_.erase(key) > 0;
}

bool KeyValueStore::Exists(const std::string& key) const {
    std::shared_lock lock(mutex_);
    return data_.contains(key);
}

std::size_t KeyValueStore::Size() const {
    std::shared_lock lock(mutex_);
    return data_.size();
}

std::vector<std::pair<std::string, std::string>> KeyValueStore::Snapshot() const {
    std::shared_lock lock(mutex_);
    std::vector<std::pair<std::string, std::string>> entries;
    entries.reserve(data_.size());
    for (const auto& [key, value] : data_) {
        entries.emplace_back(key, value);
    }
    return entries;
}

void KeyValueStore::ReplaceAll(std::vector<std::pair<std::string, std::string>> entries) {
    std::unique_lock lock(mutex_);
    data_.clear();
    for (auto& [key, value] : entries) {
        data_.emplace(std::move(key), std::move(value));
    }
}

}  // namespace kvstore::storage
