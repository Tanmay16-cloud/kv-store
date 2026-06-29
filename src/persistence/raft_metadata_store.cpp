#include "kvstore/persistence/raft_metadata_store.h"

#include <fstream>
#include <optional>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "kvstore/protocol/command_parser.h"

namespace kvstore::persistence {
namespace {

constexpr const char* kHeader = "RAFT_STATE_V1";

bool IsDurableCommand(protocol::CommandType type) {
    return type == protocol::CommandType::Set || type == protocol::CommandType::Del;
}

std::string TrimLeft(std::string text) {
    const auto first = text.find_first_not_of(" \t\r\n");
    if (first == std::string::npos) {
        return {};
    }

    text.erase(0, first);
    return text;
}

std::optional<std::uint64_t> ParseUint64(const std::string& text) {
    try {
        std::size_t parsed_chars = 0;
        const auto value = std::stoull(text, &parsed_chars);
        if (parsed_chars != text.size()) {
            return std::nullopt;
        }
        return static_cast<std::uint64_t>(value);
    } catch (...) {
        return std::nullopt;
    }
}

std::string ToCommandLine(const protocol::Command& command) {
    if (command.type == protocol::CommandType::Set) {
        return "SET " + command.key + " " + command.value;
    }
    if (command.type == protocol::CommandType::Del) {
        return "DEL " + command.key;
    }
    return {};
}

}  // namespace

RaftMetadataStore::RaftMetadataStore(std::filesystem::path path) : path_(std::move(path)) {}

bool RaftMetadataStore::Load(raft::RaftState& raft_state) const {
    std::lock_guard lock(mutex_);

    std::ifstream input(path_);
    if (!input.is_open()) {
        return true;
    }

    std::string line;
    if (!std::getline(input, line) || line != kHeader) {
        return false;
    }

    raft::PersistentState state;
    bool saw_term = false;
    bool saw_vote = false;

    while (std::getline(input, line)) {
        std::istringstream tokens(line);
        std::string kind;
        tokens >> kind;

        if (kind == "TERM") {
            std::string term_text;
            std::string trailing;
            tokens >> term_text;
            const auto term = ParseUint64(term_text);
            if (!term.has_value() || (tokens >> trailing)) {
                return false;
            }
            state.current_term = term.value();
            saw_term = true;
            continue;
        }

        if (kind == "VOTED_FOR") {
            std::string voted_for;
            std::string trailing;
            tokens >> voted_for;
            if (voted_for.empty() || (tokens >> trailing)) {
                return false;
            }
            if (voted_for != "-") {
                state.voted_for = voted_for;
            }
            saw_vote = true;
            continue;
        }

        if (kind == "LOG") {
            std::string index_text;
            std::string term_text;
            tokens >> index_text >> term_text;

            std::string command_text;
            std::getline(tokens, command_text);
            command_text = TrimLeft(std::move(command_text));

            const auto index = ParseUint64(index_text);
            const auto term = ParseUint64(term_text);
            const auto command = protocol::CommandParser::Parse(command_text);
            if (!index.has_value() || !term.has_value() || !command.has_value() ||
                !IsDurableCommand(command->type)) {
                return false;
            }

            const auto expected_index = static_cast<std::uint64_t>(state.log.size() + 1);
            if (index.value() != expected_index) {
                return false;
            }

            state.log.push_back(raft::LogEntry{index.value(), term.value(), command.value()});
            continue;
        }

        return false;
    }

    if (!input.eof() || !saw_term || !saw_vote) {
        return false;
    }

    raft_state.RestorePersistentState(state);
    return true;
}

bool RaftMetadataStore::Save(const raft::PersistentState& state) {
    std::lock_guard lock(mutex_);

    std::ofstream output(path_, std::ios::trunc);
    if (!output.is_open()) {
        return false;
    }

    output << kHeader << '\n';
    output << "TERM " << state.current_term << '\n';
    output << "VOTED_FOR " << state.voted_for.value_or("-") << '\n';

    for (const auto& entry : state.log) {
        const auto command_line = ToCommandLine(entry.command);
        if (command_line.empty()) {
            return false;
        }
        output << "LOG " << entry.index << ' ' << entry.term << ' ' << command_line << '\n';
        if (!output.good()) {
            return false;
        }
    }

    output.flush();
    return output.good();
}

}  // namespace kvstore::persistence
