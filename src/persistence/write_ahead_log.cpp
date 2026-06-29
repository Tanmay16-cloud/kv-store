#include "kvstore/persistence/write_ahead_log.h"

#include <fstream>
#include <string>
#include <utility>
#include <vector>

#include "kvstore/protocol/command_parser.h"

namespace kvstore::persistence {
namespace {

bool ApplyCommand(storage::KeyValueStore& store, const protocol::Command& command) {
    switch (command.type) {
        case protocol::CommandType::Set:
            return store.Set(command.key, command.value);
        case protocol::CommandType::Del:
            store.Delete(command.key);
            return true;
        case protocol::CommandType::Get:
        case protocol::CommandType::Exists:
        case protocol::CommandType::Quit:
            return false;
    }

    return false;
}

}  // namespace

WriteAheadLog::WriteAheadLog(std::filesystem::path wal_path, std::filesystem::path snapshot_path)
    : wal_path_(std::move(wal_path)), snapshot_path_(std::move(snapshot_path)) {}

bool WriteAheadLog::Replay(storage::KeyValueStore& store) const {
    std::lock_guard lock(mutex_);

    if (!LoadSnapshot(store)) {
        return false;
    }

    std::ifstream input(wal_path_);
    if (!input.is_open()) {
        return true;
    }

    std::string line;
    while (std::getline(input, line)) {
        const auto parsed = protocol::CommandParser::Parse(line);
        if (!parsed.has_value()) {
            return false;
        }

        if (!ApplyCommand(store, parsed.value())) {
            return false;
        }
    }

    return input.eof();
}

bool WriteAheadLog::Append(const protocol::Command& command) {
    std::lock_guard lock(mutex_);

    std::ofstream output(wal_path_, std::ios::app);
    if (!output.is_open()) {
        return false;
    }

    switch (command.type) {
        case protocol::CommandType::Set:
            output << "SET " << command.key << ' ' << command.value << '\n';
            break;
        case protocol::CommandType::Del:
            output << "DEL " << command.key << '\n';
            break;
        case protocol::CommandType::Get:
        case protocol::CommandType::Exists:
        case protocol::CommandType::Quit:
            return true;
    }

    output.flush();
    return output.good();
}

bool WriteAheadLog::Compact(const storage::KeyValueStore& store) {
    std::lock_guard lock(mutex_);

    if (!SaveSnapshot(store)) {
        return false;
    }

    std::ofstream truncate_wal(wal_path_, std::ios::trunc);
    return truncate_wal.good();
}

bool WriteAheadLog::LoadSnapshot(storage::KeyValueStore& store) const {
    std::ifstream input(snapshot_path_);
    if (!input.is_open()) {
        return true;
    }

    std::vector<std::pair<std::string, std::string>> entries;
    std::string line;
    while (std::getline(input, line)) {
        const auto parsed = protocol::CommandParser::Parse(line);
        if (!parsed.has_value()) {
            return false;
        }

        const auto& command = parsed.value();
        if (command.type != protocol::CommandType::Set) {
            return false;
        }

        entries.emplace_back(command.key, command.value);
    }

    if (!input.eof()) {
        return false;
    }

    store.ReplaceAll(std::move(entries));
    return true;
}

bool WriteAheadLog::SaveSnapshot(const storage::KeyValueStore& store) const {
    std::ofstream output(snapshot_path_, std::ios::trunc);
    if (!output.is_open()) {
        return false;
    }

    for (const auto& [key, value] : store.Snapshot()) {
        output << "SET " << key << ' ' << value << '\n';
        if (!output.good()) {
            return false;
        }
    }

    output.flush();
    return output.good();
}

}  // namespace kvstore::persistence
