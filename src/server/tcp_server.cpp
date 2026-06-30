#include "kvstore/server/tcp_server.h"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <mutex>
#include <optional>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "kvstore/net/socket.h"
#include "kvstore/persistence/raft_metadata_store.h"
#include "kvstore/protocol/command.h"
#include "kvstore/protocol/command_executor.h"
#include "kvstore/protocol/command_parser.h"

namespace kvstore::server {
namespace {

constexpr std::chrono::milliseconds kHeartbeatInterval{1000};
constexpr std::chrono::milliseconds kElectionPollInterval{200};
constexpr std::chrono::milliseconds kBaseElectionTimeout{2500};
constexpr std::chrono::milliseconds kPeerRpcTimeout{500};

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

std::string NodeIdForPort(std::uint16_t port) {
    return "node-" + std::to_string(port);
}

std::optional<FollowerEndpoint> EndpointForNodeId(const std::vector<FollowerEndpoint>& peers,
                                                  std::uint16_t local_port,
                                                  const std::string& node_id) {
    if (node_id == NodeIdForPort(local_port)) {
        return FollowerEndpoint{"127.0.0.1", local_port};
    }

    for (const auto& peer : peers) {
        if (node_id == NodeIdForPort(peer.port)) {
            return peer;
        }
    }

    return std::nullopt;
}

std::string BuildNotLeaderResponse(const std::vector<FollowerEndpoint>& peers,
                                   std::uint16_t local_port,
                                   const raft::RaftState& raft_state) {
    const auto leader_id = raft_state.KnownLeaderId();
    if (!leader_id.has_value()) {
        return "ERR node is not raft leader";
    }

    const auto endpoint = EndpointForNodeId(peers, local_port, leader_id.value());
    if (!endpoint.has_value()) {
        return "ERR node is not raft leader";
    }

    return "NOT_LEADER " + endpoint->host + ":" + std::to_string(endpoint->port);
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
                                                std::chrono::steady_clock::time_point& last_leader_contact,
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
        if (response.vote_granted) {
            last_leader_contact = std::chrono::steady_clock::now();
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
        if (response.accepted) {
            last_leader_contact = std::chrono::steady_clock::now();
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
            last_leader_contact = std::chrono::steady_clock::now();
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
        last_leader_contact = std::chrono::steady_clock::now();

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

net::SocketHandle ConnectToEndpoint(const FollowerEndpoint& follower) {
    return net::ConnectTcp(follower.host, follower.port, kPeerRpcTimeout);
}

bool SendRequestToEndpoint(const FollowerEndpoint& follower,
                           const std::string& request,
                           std::string& response) {
    const net::SocketHandle socket_to_follower = ConnectToEndpoint(follower);
    if (socket_to_follower == net::kInvalidSocket) {
        return false;
    }

    std::string pending;
    std::string greeting;
    response.clear();
    bool ok = net::ReceiveLine(socket_to_follower, pending, greeting) &&
              net::SendLine(socket_to_follower, request) &&
              net::ReceiveLine(socket_to_follower, pending, response);

    if (ok && response == "OK connected to kvstore") {
        ok = net::ReceiveLine(socket_to_follower, pending, response);
    }

    net::CloseSocket(socket_to_follower);
    return ok;
}

struct VotePeerResult {
    bool reachable{false};
    bool granted{false};
    std::uint64_t term{0};
};

VotePeerResult ParseVotePeerResponse(const std::string& response) {
    std::istringstream input(response);
    std::string kind;
    std::string term_text;
    std::string status;
    std::string trailing;

    input >> kind >> term_text >> status;
    const auto term = ParseUint64(term_text);
    if (kind != "VOTE" || !term.has_value() || (input >> trailing)) {
        return {};
    }

    return {true, status == "GRANTED", term.value()};
}

VotePeerResult RequestVoteFromPeer(const FollowerEndpoint& peer,
                                   const std::string& candidate_id,
                                   std::uint64_t term) {
    std::string response;
    const std::string request =
        "RAFT VOTE " + std::to_string(term) + " " + candidate_id;
    if (!SendRequestToEndpoint(peer, request, response)) {
        return {};
    }

    return ParseVotePeerResponse(response);
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
                                           std::uint64_t leader_term,
                                           const raft::LogEntry& entry,
                                           std::uint64_t previous_log_index,
                                           std::uint64_t previous_log_term,
                                           std::uint64_t leader_commit) {
    std::string response;
    const std::string request =
        "RAFT APPEND " + std::to_string(leader_term) + " " + leader_id + " " +
        std::to_string(previous_log_index) + " " + std::to_string(previous_log_term) + " " +
        std::to_string(leader_commit) + " " + std::to_string(entry.index) + " " +
        std::to_string(entry.term) + " " + ToCommandLine(entry.command);

    if (!SendRequestToEndpoint(follower, request, response)) {
        return {};
    }

    return ParseAppendFollowerResponse(response, leader_term);
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
                                                  raft_state.CurrentTerm(),
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

std::optional<std::uint64_t> LogTermAt(const std::vector<raft::LogEntry>& log,
                                       std::uint64_t index) {
    if (index == 0) {
        return 0;
    }
    if (index > log.size()) {
        return std::nullopt;
    }

    return log[index - 1].term;
}

std::optional<raft::LogEntry> LogEntryAt(const std::vector<raft::LogEntry>& log,
                                         std::uint64_t index) {
    if (index == 0 || index > log.size()) {
        return std::nullopt;
    }

    return log[index - 1];
}

bool ReplicateLogSnapshotThroughIndex(const FollowerEndpoint& follower,
                                      const std::string& leader_id,
                                      std::uint64_t leader_term,
                                      const std::vector<raft::LogEntry>& log,
                                      std::uint64_t leader_commit,
                                      std::uint64_t target_index) {
    std::uint64_t next_index = target_index;

    while (next_index <= target_index) {
        const auto entry = LogEntryAt(log, next_index);
        if (!entry.has_value()) {
            return false;
        }

        const auto previous_log_index = next_index - 1;
        const auto previous_log_term = LogTermAt(log, previous_log_index).value_or(0);
        const auto result = AppendEntryToFollower(follower,
                                                  leader_id,
                                                  leader_term,
                                                  entry.value(),
                                                  previous_log_index,
                                                  previous_log_term,
                                                  leader_commit);
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
    const auto cluster_size = followers.size() + 1;
    const auto majority = (cluster_size / 2) + 1;
    for (const auto& follower : followers) {
        if (ReplicateLogThroughEntry(follower, leader_id, raft_state, entry)) {
            ++acknowledgements;
            if (acknowledgements >= majority) {
                break;
            }
        }
    }

    return acknowledgements;
}

void CommitEntryOnFollowers(const std::vector<FollowerEndpoint>& followers,
                            const std::string& leader_id,
                            std::uint64_t term,
                            std::uint64_t commit_index) {
    std::size_t acknowledgements = 1;
    const auto cluster_size = followers.size() + 1;
    const auto majority = (cluster_size / 2) + 1;
    for (const auto& follower : followers) {
        if (CommitEntryOnFollower(follower, leader_id, term, commit_index)) {
            ++acknowledgements;
            if (acknowledgements >= majority) {
                break;
            }
        } else {
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
        std::uint64_t commit_index = 0;
        std::vector<raft::LogEntry> log_snapshot;
        {
            std::lock_guard lock(execution_mutex);
            if (raft_state.Role() != raft::NodeRole::Leader) {
                continue;
            }
            leader_id = raft_state.NodeId();
            term = raft_state.CurrentTerm();
            commit_index = raft_state.CommitIndex();
            log_snapshot = raft_state.DurableState().log;
        }

        for (const auto& follower : followers) {
            bool ok = false;
            if (commit_index > 0 && LogEntryAt(log_snapshot, commit_index).has_value()) {
                ok = ReplicateLogSnapshotThroughIndex(follower,
                                                      leader_id,
                                                      term,
                                                      log_snapshot,
                                                      commit_index,
                                                      commit_index);
            } else {
                ok = SendHeartbeatToFollower(follower, leader_id, term);
            }

            if (!ok) {
                std::cerr << "Replication heartbeat failed for follower " << follower.host
                          << ":" << follower.port << ".\n";
            }
        }
    }
}

void ElectionLoop(const std::vector<FollowerEndpoint>& peers,
                  raft::RaftState& raft_state,
                  persistence::RaftMetadataStore& raft_metadata,
                  std::mutex& execution_mutex,
                  std::chrono::steady_clock::time_point& last_leader_contact,
                  std::uint16_t local_port) {
    if (peers.empty()) {
        return;
    }

    const auto election_timeout =
        kBaseElectionTimeout + std::chrono::milliseconds((local_port % 10) * 1000);

    while (true) {
        std::this_thread::sleep_for(kElectionPollInterval);

        std::string candidate_id;
        std::uint64_t election_term = 0;
        {
            std::lock_guard lock(execution_mutex);
            if (raft_state.Role() == raft::NodeRole::Leader) {
                continue;
            }

            const auto now = std::chrono::steady_clock::now();
            if (now - last_leader_contact < election_timeout) {
                continue;
            }

            election_term = raft_state.StartElection();
            candidate_id = raft_state.NodeId();
            last_leader_contact = now;
            if (!SaveRaftMetadata(raft_metadata, raft_state)) {
                std::cerr << "Failed to persist Raft election metadata.\n";
                continue;
            }
        }

        std::size_t votes = 1;
        std::uint64_t highest_term = election_term;
        for (const auto& peer : peers) {
            const auto response = RequestVoteFromPeer(peer, candidate_id, election_term);
            if (!response.reachable) {
                continue;
            }
            highest_term = std::max(highest_term, response.term);
            if (response.term == election_term && response.granted) {
                ++votes;
            }
        }

        const auto cluster_size = peers.size() + 1;
        const auto majority = (cluster_size / 2) + 1;
        {
            std::lock_guard lock(execution_mutex);
            if (highest_term > raft_state.CurrentTerm()) {
                raft_state.ObserveTerm(highest_term);
                SaveRaftMetadata(raft_metadata, raft_state);
                last_leader_contact = std::chrono::steady_clock::now();
                continue;
            }

            if (raft_state.Role() == raft::NodeRole::Candidate &&
                raft_state.CurrentTerm() == election_term && votes >= majority) {
                raft_state.BecomeLeader();
                if (!SaveRaftMetadata(raft_metadata, raft_state)) {
                    std::cerr << "Failed to persist Raft leader metadata after election.\n";
                }
                std::cout << "Raft node " << candidate_id << " became leader for term "
                          << election_term << ".\n";
            }
        }
    }
}

void HandleClient(net::SocketHandle client,
                  ServerRole role,
                  const std::vector<FollowerEndpoint>& followers,
                  std::uint16_t local_port,
                  raft::RaftState& raft_state,
                  const std::optional<cluster::ClusterMetadata>& cluster_metadata,
                  metrics::ServerMetrics& metrics,
                  persistence::RaftMetadataStore& raft_metadata,
                  storage::KeyValueStore& store,
                  persistence::WriteAheadLog& wal,
                  std::mutex& execution_mutex,
                  std::chrono::steady_clock::time_point& last_leader_contact,
                  std::size_t& writes_since_compaction,
                  std::size_t compaction_threshold) {
    const auto close_client = [&metrics](net::SocketHandle socket) {
        net::CloseSocket(socket);
        metrics.RecordConnectionClosed();
    };

    std::string pending;

    metrics.RecordConnectionOpened();
    net::SendLine(client, "OK connected to kvstore");

    while (true) {
        std::string line;
        if (!net::ReceiveLine(client, pending, line)) {
            break;
        }

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
                                                 last_leader_contact,
                                                 line);
        }
        if (raft_response.has_value()) {
            if (!net::SendLine(client, raft_response.value())) {
                close_client(client);
                return;
            }
            continue;
        }

        const auto parsed = protocol::CommandParser::Parse(line);
        if (!parsed.has_value()) {
            metrics.RecordError();
            if (!net::SendLine(client, "ERR invalid command")) {
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
                } else if (role != ServerRole::Standalone && is_write && !parsed->is_replicated &&
                           raft_state.Role() != raft::NodeRole::Leader) {
                    result = {BuildNotLeaderResponse(followers, local_port, raft_state), false};
                } else if (role != ServerRole::Standalone && is_write && !parsed->is_replicated) {
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

            if (!net::SendLine(client, result.response)) {
                close_client(client);
                return;
            }
            if (result.should_close) {
                close_client(client);
                return;
            }
        }
    }

    close_client(client);
}

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
      raft_metadata_("kvstore-" + std::to_string(port) + ".raft"),
      last_leader_contact_(std::chrono::steady_clock::now()) {}

int TcpServer::Run() {
    if (!wal_.Replay(store_)) {
        std::cerr << "Failed to replay persistence files.\n";
        return 1;
    }

    if (!raft_metadata_.Load(raft_state_)) {
        std::cerr << "Failed to load Raft metadata file.\n";
        return 1;
    }

    net::SocketRuntime socket_runtime;
    if (!socket_runtime.Ok()) {
        std::cerr << "Failed to initialize socket runtime.\n";
        return 1;
    }

    const net::SocketHandle listener = net::ListenTcp(port_);
    if (listener == net::kInvalidSocket) {
        std::cerr << "Failed to listen for TCP clients on port " << port_ << ".\n";
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
    }

    if (role_ != ServerRole::Standalone) {
        std::thread heartbeat_thread(HeartbeatLoop,
                                     std::cref(followers_),
                                     std::ref(raft_state_),
                                     std::ref(execution_mutex_));
        heartbeat_thread.detach();

        std::thread election_thread(ElectionLoop,
                                    std::cref(followers_),
                                    std::ref(raft_state_),
                                    std::ref(raft_metadata_),
                                    std::ref(execution_mutex_),
                                    std::ref(last_leader_contact_),
                                    port_);
        election_thread.detach();
    }

    std::cout << "KV TCP server listening on port " << port_ << ".\n";

    while (true) {
        const net::SocketHandle client = net::AcceptTcp(listener);
        if (client == net::kInvalidSocket) {
            std::cerr << "Failed to accept client connection.\n";
            continue;
        }

        std::thread client_thread(HandleClient,
                                  client,
                                  role_,
                                  std::cref(followers_),
                                  port_,
                                  std::ref(raft_state_),
                                  std::cref(cluster_metadata_),
                                  std::ref(metrics_),
                                  std::ref(raft_metadata_),
                                  std::ref(store_),
                                  std::ref(wal_),
                                  std::ref(execution_mutex_),
                                  std::ref(last_leader_contact_),
                                  std::ref(writes_since_compaction_),
                                  kCompactionThreshold);
        client_thread.detach();
    }

    net::CloseSocket(listener);
    return 0;
}

}  // namespace kvstore::server
