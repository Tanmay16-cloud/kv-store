#include "kvstore/server/tcp_server.h"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <functional>
#include <iostream>
#include <mutex>
#include <optional>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "kvstore/persistence/raft_metadata_store.h"
#include "kvstore/protocol/command.h"
#include "kvstore/protocol/command_executor.h"
#include "kvstore/protocol/command_parser.h"

#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#endif

namespace kvstore::server {
namespace {

#ifdef _WIN32

constexpr std::chrono::milliseconds kHeartbeatInterval{1000};

class WinsockSession {
public:
    WinsockSession() {
        ok_ = WSAStartup(MAKEWORD(2, 2), &data_) == 0;
    }

    ~WinsockSession() {
        if (ok_) {
            WSACleanup();
        }
    }

    bool Ok() const {
        return ok_;
    }

private:
    WSADATA data_{};
    bool ok_{false};
};

bool SendLine(SOCKET client, const std::string& line) {
    std::string response = line + "\n";
    const char* data = response.data();
    int remaining = static_cast<int>(response.size());

    while (remaining > 0) {
        const int sent = send(client, data, remaining, 0);
        if (sent == SOCKET_ERROR) {
            return false;
        }

        data += sent;
        remaining -= sent;
    }

    return true;
}

bool ReceiveLine(SOCKET socket, std::string& pending, std::string& line) {
    while (true) {
        const std::size_t newline = pending.find('\n');
        if (newline != std::string::npos) {
            line = pending.substr(0, newline);
            pending.erase(0, newline + 1);
            if (!line.empty() && line.back() == '\r') {
                line.pop_back();
            }
            return true;
        }

        std::array<char, 4096> buffer{};
        const int received = recv(socket, buffer.data(), static_cast<int>(buffer.size()), 0);
        if (received <= 0) {
            return false;
        }
        pending.append(buffer.data(), static_cast<std::size_t>(received));
    }
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
        return static_cast<std::uint64_t>(std::stoull(text));
    } catch (...) {
        return std::nullopt;
    }
}

bool IsMutatingCommand(protocol::CommandType type) {
    return type == protocol::CommandType::Set || type == protocol::CommandType::Del;
}

bool IsKeyCommand(protocol::CommandType type) {
    return type == protocol::CommandType::Set || type == protocol::CommandType::Get ||
           type == protocol::CommandType::Del || type == protocol::CommandType::Exists;
}

bool IsClusterCommand(protocol::CommandType type) {
    return type == protocol::CommandType::ClusterInfo ||
           type == protocol::CommandType::ClusterNodes ||
           type == protocol::CommandType::ClusterKey;
}

std::string BuildMovedResponse(const cluster::ClusterMetadata& cluster_metadata,
                               const std::string& key) {
    const auto shard_id = cluster_metadata.ShardForKey(key);
    const auto* owner = cluster_metadata.OwnerForKey(key);
    if (owner == nullptr) {
        return "ERR shard owner unknown";
    }

    return "MOVED " + std::to_string(shard_id) + " " + owner->host + ":" +
           std::to_string(owner->port);
}

bool SaveRaftMetadata(persistence::RaftMetadataStore& raft_metadata,
                      const raft::RaftState& raft_state) {
    return raft_metadata.Save(raft_state.DurableState());
}

bool ApplyCommittedCommands(storage::KeyValueStore& store,
                            persistence::WriteAheadLog& wal,
                            const std::vector<protocol::Command>& commands,
                            std::size_t& writes_since_compaction,
                            std::size_t compaction_threshold,
                            std::string* last_response = nullptr) {
    for (const auto& command : commands) {
        const auto result = protocol::ExecuteCommand(store, command, &wal);
        if (last_response != nullptr) {
            *last_response = result.response;
        }
        if (result.response == "ERR failed to persist command") {
            return false;
        }

        ++writes_since_compaction;
        if (writes_since_compaction >= compaction_threshold && wal.Compact(store)) {
            writes_since_compaction = 0;
        }
    }

    return true;
}

std::optional<std::string> TryHandleRaftMessage(raft::RaftState& raft_state,
                                                persistence::RaftMetadataStore& raft_metadata,
                                                metrics::ServerMetrics& metrics,
                                                storage::KeyValueStore& store,
                                                persistence::WriteAheadLog& wal,
                                                std::size_t& writes_since_compaction,
                                                std::size_t compaction_threshold,
                                                const std::string& line) {
    std::istringstream input(line);
    std::string prefix;
    std::string kind;
    std::string term_text;
    std::string node_id;
    std::string trailing;

    input >> prefix;
    if (prefix != "RAFT") {
        return std::nullopt;
    }

    metrics.RecordRaftMessage();

    input >> kind >> term_text >> node_id;
    if (kind.empty() || term_text.empty() || node_id.empty()) {
        return std::string("ERR invalid raft message");
    }

    const auto term = ParseUint64(term_text);
    if (!term.has_value()) {
        return std::string("ERR invalid raft term");
    }

    if (kind == "VOTE") {
        if (input >> trailing) {
            return std::string("ERR invalid raft message");
        }

        const auto response = raft_state.HandleVoteRequest(raft::VoteRequest{term.value(), node_id});
        if (!SaveRaftMetadata(raft_metadata, raft_state)) {
            return std::string("ERR failed to persist raft metadata");
        }

        return std::string("VOTE ") + std::to_string(response.term) +
               (response.vote_granted ? " GRANTED" : " DENIED");
    }

    if (kind == "HEARTBEAT") {
        if (input >> trailing) {
            return std::string("ERR invalid raft message");
        }

        const auto response = raft_state.HandleHeartbeat(raft::HeartbeatRequest{term.value(), node_id});
        if (!SaveRaftMetadata(raft_metadata, raft_state)) {
            return std::string("ERR failed to persist raft metadata");
        }

        return std::string("HEARTBEAT ") + std::to_string(response.term) +
               (response.accepted ? " OK" : " STALE");
    }

    if (kind == "APPEND") {
        std::string previous_index_text;
        std::string previous_term_text;
        std::string leader_commit_text;
        std::string entry_index_text;
        std::string entry_term_text;
        input >> previous_index_text >> previous_term_text >> leader_commit_text >>
            entry_index_text >> entry_term_text;

        std::string command_text;
        std::getline(input, command_text);
        command_text = TrimLeft(std::move(command_text));

        const auto previous_index = ParseUint64(previous_index_text);
        const auto previous_term = ParseUint64(previous_term_text);
        const auto leader_commit = ParseUint64(leader_commit_text);
        const auto entry_index = ParseUint64(entry_index_text);
        const auto entry_term = ParseUint64(entry_term_text);
        const auto parsed_command = protocol::CommandParser::Parse(command_text);

        if (!previous_index.has_value() || !previous_term.has_value() ||
            !leader_commit.has_value() || !entry_index.has_value() ||
            !entry_term.has_value() || !parsed_command.has_value() ||
            !IsMutatingCommand(parsed_command->type)) {
            return std::string("ERR invalid raft append");
        }

        const auto response = raft_state.HandleAppendEntries(raft::AppendEntriesRequest{
            term.value(),
            node_id,
            previous_index.value(),
            previous_term.value(),
            leader_commit.value(),
            {raft::LogEntry{entry_index.value(), entry_term.value(), parsed_command.value()}}});
        if (!SaveRaftMetadata(raft_metadata, raft_state)) {
            return std::string("ERR failed to persist raft metadata");
        }

        if (response.success) {
        const auto commands = raft_state.MarkCommitted(raft_state.CommitIndex());
        if (!ApplyCommittedCommands(store, wal, commands, writes_since_compaction,
                                    compaction_threshold)) {
            metrics.RecordError();
            return std::string("ERR failed to apply raft commit");
        }
        }

        return std::string("APPEND ") + std::to_string(response.term) + " " +
               std::to_string(response.match_index) + (response.success ? " OK" : " REJECTED");
    }

    if (kind == "COMMIT") {
        std::string commit_index_text;
        input >> commit_index_text;
        if (commit_index_text.empty() || (input >> trailing)) {
            return std::string("ERR invalid raft commit");
        }

        const auto commit_index = ParseUint64(commit_index_text);
        if (!commit_index.has_value()) {
            return std::string("ERR invalid raft commit");
        }

        const auto heartbeat = raft_state.HandleHeartbeat(raft::HeartbeatRequest{term.value(), node_id});
        if (!SaveRaftMetadata(raft_metadata, raft_state)) {
            return std::string("ERR failed to persist raft metadata");
        }

        if (!heartbeat.accepted) {
            return std::string("COMMIT ") + std::to_string(heartbeat.term) + " " +
                   std::to_string(raft_state.LastApplied()) + " STALE";
        }

        const auto commands = raft_state.MarkCommitted(commit_index.value());
        if (!ApplyCommittedCommands(store, wal, commands, writes_since_compaction,
                                    compaction_threshold)) {
            metrics.RecordError();
            return std::string("ERR failed to apply raft commit");
        }

        return std::string("COMMIT ") + std::to_string(heartbeat.term) + " " +
               std::to_string(raft_state.LastApplied()) + " OK";
    }

    return std::string("ERR invalid raft message");
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

SOCKET ConnectToEndpoint(const FollowerEndpoint& follower) {
    addrinfo hints{};
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;

    addrinfo* resolved = nullptr;
    const std::string port_text = std::to_string(follower.port);
    if (getaddrinfo(follower.host.c_str(), port_text.c_str(), &hints, &resolved) != 0) {
        return INVALID_SOCKET;
    }

    SOCKET connected_socket = INVALID_SOCKET;
    for (addrinfo* candidate = resolved; candidate != nullptr; candidate = candidate->ai_next) {
        connected_socket = socket(candidate->ai_family, candidate->ai_socktype, candidate->ai_protocol);
        if (connected_socket == INVALID_SOCKET) {
            continue;
        }

        if (connect(connected_socket, candidate->ai_addr, static_cast<int>(candidate->ai_addrlen)) == 0) {
            break;
        }

        closesocket(connected_socket);
        connected_socket = INVALID_SOCKET;
    }

    freeaddrinfo(resolved);

    return connected_socket;
}

bool SendRequestToEndpoint(const FollowerEndpoint& follower,
                           const std::string& request,
                           std::string& response) {
    const SOCKET socket_to_follower = ConnectToEndpoint(follower);
    if (socket_to_follower == INVALID_SOCKET) {
        return false;
    }

    std::string pending;
    const bool ok = ReceiveLine(socket_to_follower, pending, response) &&
                    SendLine(socket_to_follower, request) &&
                    ReceiveLine(socket_to_follower, pending, response);

    closesocket(socket_to_follower);
    return ok;
}

struct AppendFollowerResult {
    bool success{false};
    std::uint64_t match_index{0};
};

AppendFollowerResult ParseAppendFollowerResponse(const std::string& response,
                                                 std::uint64_t expected_term) {
    std::istringstream input(response);
    std::string kind;
    std::string term_text;
    std::string match_index_text;
    std::string status;
    std::string trailing;

    input >> kind >> term_text >> match_index_text >> status;
    const auto term = ParseUint64(term_text);
    const auto match_index = ParseUint64(match_index_text);
    if (kind != "APPEND" || !term.has_value() || term.value() != expected_term ||
        !match_index.has_value() || (input >> trailing)) {
        return {};
    }

    return {status == "OK", match_index.value()};
}

AppendFollowerResult AppendEntryToFollower(const FollowerEndpoint& follower,
                                           const std::string& leader_id,
                                           const raft::LogEntry& entry,
                                           std::uint64_t previous_log_index,
                                           std::uint64_t previous_log_term,
                                           std::uint64_t leader_commit) {
    std::string response;
    const std::string request =
        "RAFT APPEND " + std::to_string(entry.term) + " " + leader_id + " " +
        std::to_string(previous_log_index) + " " + std::to_string(previous_log_term) + " " +
        std::to_string(leader_commit) + " " + std::to_string(entry.index) + " " +
        std::to_string(entry.term) + " " + ToCommandLine(entry.command);

    if (!SendRequestToEndpoint(follower, request, response)) {
        return {};
    }

    return ParseAppendFollowerResponse(response, entry.term);
}

bool CommitEntryOnFollower(const FollowerEndpoint& follower,
                           const std::string& leader_id,
                           std::uint64_t term,
                           std::uint64_t commit_index) {
    std::string response;
    const std::string request =
        "RAFT COMMIT " + std::to_string(term) + " " + leader_id + " " +
        std::to_string(commit_index);
    const std::string expected =
        "COMMIT " + std::to_string(term) + " " + std::to_string(commit_index) + " OK";

    return SendRequestToEndpoint(follower, request, response) && response == expected;
}

bool ReplicateLogThroughEntry(const FollowerEndpoint& follower,
                              const std::string& leader_id,
                              const raft::RaftState& raft_state,
                              const raft::LogEntry& target_entry) {
    std::uint64_t next_index = target_entry.index;

    while (next_index <= target_entry.index) {
        const auto entry = raft_state.LogEntryAt(next_index);
        if (!entry.has_value()) {
            return false;
        }

        const auto previous_log_index = next_index - 1;
        const auto previous_log_term = raft_state.LogTermAt(previous_log_index).value_or(0);
        const auto result = AppendEntryToFollower(follower,
                                                  leader_id,
                                                  entry.value(),
                                                  previous_log_index,
                                                  previous_log_term,
                                                  raft_state.CommitIndex());
        if (result.success) {
            ++next_index;
            continue;
        }

        if (next_index == 1) {
            return false;
        }

        next_index = std::min(next_index - 1, result.match_index + 1);
        if (next_index == 0) {
            next_index = 1;
        }
    }

    return true;
}

std::size_t AppendEntryToFollowers(const std::vector<FollowerEndpoint>& followers,
                                   const std::string& leader_id,
                                   const raft::RaftState& raft_state,
                                   const raft::LogEntry& entry) {
    std::size_t acknowledgements = 1;
    for (const auto& follower : followers) {
        if (ReplicateLogThroughEntry(follower, leader_id, raft_state, entry)) {
            ++acknowledgements;
        }
    }

    return acknowledgements;
}

void CommitEntryOnFollowers(const std::vector<FollowerEndpoint>& followers,
                            const std::string& leader_id,
                            std::uint64_t term,
                            std::uint64_t commit_index) {
    for (const auto& follower : followers) {
        if (!CommitEntryOnFollower(follower, leader_id, term, commit_index)) {
            std::cerr << "Commit failed for follower " << follower.host << ":"
                      << follower.port << ".\n";
        }
    }
}

bool SendHeartbeatToFollower(const FollowerEndpoint& follower,
                             const std::string& leader_id,
                             std::uint64_t term) {
    std::string response;
    const std::string request =
        "RAFT HEARTBEAT " + std::to_string(term) + " " + leader_id;
    const std::string expected = "HEARTBEAT " + std::to_string(term) + " OK";

    return SendRequestToEndpoint(follower, request, response) && response == expected;
}

void HeartbeatLoop(const std::vector<FollowerEndpoint>& followers,
                   raft::RaftState& raft_state,
                   std::mutex& execution_mutex) {
    while (true) {
        std::this_thread::sleep_for(kHeartbeatInterval);

        std::string leader_id;
        std::uint64_t term = 0;
        {
            std::lock_guard lock(execution_mutex);
            if (raft_state.Role() != raft::NodeRole::Leader) {
                continue;
            }
            leader_id = raft_state.NodeId();
            term = raft_state.CurrentTerm();
        }

        for (const auto& follower : followers) {
            if (!SendHeartbeatToFollower(follower, leader_id, term)) {
                std::cerr << "Heartbeat failed for follower " << follower.host << ":"
                          << follower.port << ".\n";
            }
        }
    }
}

void HandleClient(SOCKET client,
                  ServerRole role,
                  const std::vector<FollowerEndpoint>& followers,
                  raft::RaftState& raft_state,
                  const std::optional<cluster::ClusterMetadata>& cluster_metadata,
                  metrics::ServerMetrics& metrics,
                  persistence::RaftMetadataStore& raft_metadata,
                  storage::KeyValueStore& store,
                  persistence::WriteAheadLog& wal,
                  std::mutex& execution_mutex,
                  std::size_t& writes_since_compaction,
                  std::size_t compaction_threshold) {
    const auto close_client = [&metrics](SOCKET socket) {
        closesocket(socket);
        metrics.RecordConnectionClosed();
    };

    std::array<char, 4096> buffer{};
    std::string pending;

    metrics.RecordConnectionOpened();
    SendLine(client, "OK connected to kvstore");

    while (true) {
        const int bytes_received = recv(client, buffer.data(), static_cast<int>(buffer.size()), 0);
        if (bytes_received <= 0) {
            break;
        }

        pending.append(buffer.data(), static_cast<std::size_t>(bytes_received));

        std::size_t newline = pending.find('\n');
        while (newline != std::string::npos) {
            const std::string line = pending.substr(0, newline);
            pending.erase(0, newline + 1);

            std::optional<std::string> raft_response;
            {
                std::lock_guard lock(execution_mutex);
                raft_response = TryHandleRaftMessage(raft_state,
                                                     raft_metadata,
                                                     metrics,
                                                     store,
                                                     wal,
                                                     writes_since_compaction,
                                                     compaction_threshold,
                                                     line);
            }
            if (raft_response.has_value()) {
                if (!SendLine(client, raft_response.value())) {
                    close_client(client);
                    return;
                }
                newline = pending.find('\n');
                continue;
            }

            const auto parsed = protocol::CommandParser::Parse(line);
            if (!parsed.has_value()) {
                metrics.RecordError();
                if (!SendLine(client, "ERR invalid command")) {
                    close_client(client);
                    return;
                }
            } else {
                protocol::CommandResult result;
                {
                    std::lock_guard lock(execution_mutex);
                    const bool is_write = IsMutatingCommand(parsed->type);
                    const bool is_key_command = IsKeyCommand(parsed->type);
                    metrics.RecordCommandHandled();
                    if (is_write) {
                        metrics.RecordWriteCommand();
                    } else if (is_key_command) {
                        metrics.RecordReadCommand();
                    }
                    if (IsClusterCommand(parsed->type)) {
                        result = protocol::ExecuteCommand(store,
                                                          parsed.value(),
                                                          nullptr,
                                                          cluster_metadata ? &cluster_metadata.value()
                                                                           : nullptr,
                                                          &metrics);
                    } else if (cluster_metadata.has_value() && is_key_command &&
                               !parsed->is_replicated &&
                               !cluster_metadata->OwnsKey(parsed->key)) {
                        metrics.RecordClusterRedirect();
                        result = {BuildMovedResponse(cluster_metadata.value(), parsed->key), false};
                    } else if (role == ServerRole::Follower && is_write && !parsed->is_replicated) {
                        result = {"ERR follower is read-only", false};
                    } else if (role == ServerRole::Leader && is_write && !parsed->is_replicated &&
                               raft_state.Role() != raft::NodeRole::Leader) {
                        result = {"ERR node is not raft leader", false};
                    } else if (role == ServerRole::Leader && is_write && !parsed->is_replicated) {
                        const auto entry = raft_state.AppendLeaderEntry(parsed.value());
                        if (!SaveRaftMetadata(raft_metadata, raft_state)) {
                            raft_state.TruncateUncommittedFrom(entry.index);
                            result = {"ERR failed to persist raft metadata", false};
                        } else {
                            const auto acknowledgements = AppendEntryToFollowers(followers,
                                                                                 raft_state.NodeId(),
                                                                                 raft_state,
                                                                                 entry);
                            const auto cluster_size = followers.size() + 1;
                            const auto majority = (cluster_size / 2) + 1;
                            if (acknowledgements < majority) {
                                raft_state.TruncateUncommittedFrom(entry.index);
                                SaveRaftMetadata(raft_metadata, raft_state);
                                result = {"ERR replication failed", false};
                            } else {
                                std::string applied_response;
                                const auto commands = raft_state.MarkCommitted(entry.index);
                                if (!ApplyCommittedCommands(store,
                                                            wal,
                                                            commands,
                                                            writes_since_compaction,
                                                            compaction_threshold,
                                                            &applied_response)) {
                                    result = {"ERR failed to persist command", false};
                                } else {
                                    CommitEntryOnFollowers(followers,
                                                           raft_state.NodeId(),
                                                           entry.term,
                                                           entry.index);
                                    result = {applied_response.empty() ? "OK" : applied_response,
                                              false};
                                }
                            }
                        }
                    } else {
                        result = protocol::ExecuteCommand(store,
                                                          parsed.value(),
                                                          &wal,
                                                          cluster_metadata ? &cluster_metadata.value()
                                                                           : nullptr,
                                                          &metrics);
                        if (result.response != "ERR failed to persist command" && is_write) {
                            ++writes_since_compaction;
                            if (writes_since_compaction >= compaction_threshold && wal.Compact(store)) {
                                writes_since_compaction = 0;
                            }
                        }
                    }

                    if (result.response.rfind("ERR", 0) == 0) {
                        metrics.RecordError();
                    }
                }

                if (!SendLine(client, result.response)) {
                    close_client(client);
                    return;
                }
                if (result.should_close) {
                    close_client(client);
                    return;
                }
            }

            newline = pending.find('\n');
        }
    }

    close_client(client);
}

#endif

}  // namespace

TcpServer::TcpServer(std::uint16_t port)
    : TcpServer(port, ServerRole::Standalone, {}) {}

TcpServer::TcpServer(std::uint16_t port,
                     ServerRole role,
                     std::vector<FollowerEndpoint> followers,
                     std::optional<cluster::ClusterMetadata> cluster_metadata)
    : port_(port),
      role_(role),
      followers_(std::move(followers)),
      raft_state_("node-" + std::to_string(port)),
      cluster_metadata_(std::move(cluster_metadata)),
      wal_("kvstore-" + std::to_string(port) + ".wal",
           "kvstore-" + std::to_string(port) + ".snapshot"),
      raft_metadata_("kvstore-" + std::to_string(port) + ".raft") {}

int TcpServer::Run() {
#ifndef _WIN32
    std::cerr << "TCP server is currently implemented for Windows using WinSock.\n";
    return 1;
#else
    if (!wal_.Replay(store_)) {
        std::cerr << "Failed to replay persistence files.\n";
        return 1;
    }

    if (!raft_metadata_.Load(raft_state_)) {
        std::cerr << "Failed to load Raft metadata file.\n";
        return 1;
    }

    WinsockSession winsock;
    if (!winsock.Ok()) {
        std::cerr << "Failed to initialize WinSock.\n";
        return 1;
    }

    const SOCKET listener = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (listener == INVALID_SOCKET) {
        std::cerr << "Failed to create server socket.\n";
        return 1;
    }

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_ANY);
    address.sin_port = htons(port_);

    if (bind(listener, reinterpret_cast<sockaddr*>(&address), sizeof(address)) == SOCKET_ERROR) {
        std::cerr << "Failed to bind TCP server to port " << port_ << ".\n";
        closesocket(listener);
        return 1;
    }

    if (listen(listener, SOMAXCONN) == SOCKET_ERROR) {
        std::cerr << "Failed to listen for TCP clients.\n";
        closesocket(listener);
        return 1;
    }

    if (role_ == ServerRole::Leader) {
        {
            std::lock_guard lock(execution_mutex_);
            raft_state_.StartElection();
            raft_state_.BecomeLeader();
            if (!SaveRaftMetadata(raft_metadata_, raft_state_)) {
                std::cerr << "Failed to persist Raft leader metadata.\n";
                return 1;
            }
        }

        std::thread heartbeat_thread(HeartbeatLoop,
                                     std::cref(followers_),
                                     std::ref(raft_state_),
                                     std::ref(execution_mutex_));
        heartbeat_thread.detach();
    }

    std::cout << "KV TCP server listening on port " << port_ << ".\n";

    while (true) {
        SOCKET client = accept(listener, nullptr, nullptr);
        if (client == INVALID_SOCKET) {
            std::cerr << "Failed to accept client connection.\n";
            continue;
        }

        std::thread client_thread(HandleClient,
                                  client,
                                  role_,
                                  std::cref(followers_),
                                  std::ref(raft_state_),
                                  std::cref(cluster_metadata_),
                                  std::ref(metrics_),
                                  std::ref(raft_metadata_),
                                  std::ref(store_),
                                  std::ref(wal_),
                                  std::ref(execution_mutex_),
                                  std::ref(writes_since_compaction_),
                                  kCompactionThreshold);
        client_thread.detach();
    }

    closesocket(listener);
    return 0;
#endif
}

}  // namespace kvstore::server
