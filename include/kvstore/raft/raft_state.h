#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "kvstore/protocol/command.h"

namespace kvstore::raft {

enum class NodeRole {
    Follower,
    Candidate,
    Leader,
};

struct VoteRequest {
    std::uint64_t term{};
    std::string candidate_id;
};

struct VoteResponse {
    std::uint64_t term{};
    bool vote_granted{false};
};

struct HeartbeatRequest {
    std::uint64_t term{};
    std::string leader_id;
};

struct HeartbeatResponse {
    std::uint64_t term{};
    bool accepted{false};
};

struct LogEntry {
    std::uint64_t index{};
    std::uint64_t term{};
    protocol::Command command;
};

struct AppendEntriesRequest {
    std::uint64_t term{};
    std::string leader_id;
    std::uint64_t previous_log_index{};
    std::uint64_t previous_log_term{};
    std::uint64_t leader_commit{};
    std::vector<LogEntry> entries;
};

struct AppendEntriesResponse {
    std::uint64_t term{};
    bool success{false};
    std::uint64_t match_index{};
};

struct PersistentState {
    std::uint64_t current_term{};
    std::optional<std::string> voted_for;
    std::vector<LogEntry> log;
};

class RaftState {
public:
    explicit RaftState(std::string node_id);

    const std::string& NodeId() const;
    std::uint64_t CurrentTerm() const;
    NodeRole Role() const;
    std::optional<std::string> VotedFor() const;
    std::uint64_t LastLogIndex() const;
    std::uint64_t LastLogTerm() const;
    std::optional<std::uint64_t> LogTermAt(std::uint64_t index) const;
    std::optional<LogEntry> LogEntryAt(std::uint64_t index) const;
    std::uint64_t CommitIndex() const;
    std::uint64_t LastApplied() const;
    PersistentState DurableState() const;

    std::uint64_t StartElection();
    void BecomeLeader();
    void RestorePersistentState(const PersistentState& state);
    LogEntry AppendLeaderEntry(const protocol::Command& command);
    bool TruncateUncommittedFrom(std::uint64_t index);
    VoteResponse HandleVoteRequest(const VoteRequest& request);
    HeartbeatResponse HandleHeartbeat(const HeartbeatRequest& request);
    AppendEntriesResponse HandleAppendEntries(const AppendEntriesRequest& request);
    std::vector<protocol::Command> MarkCommitted(std::uint64_t commit_index);

private:
    void StepDownToFollower(std::uint64_t term);

    std::string node_id_;
    std::uint64_t current_term_{0};
    NodeRole role_{NodeRole::Follower};
    std::optional<std::string> voted_for_;
    std::vector<LogEntry> log_;
    std::uint64_t commit_index_{0};
    std::uint64_t last_applied_{0};
};

}  // namespace kvstore::raft
