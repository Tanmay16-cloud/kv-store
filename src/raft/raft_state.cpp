#include "kvstore/raft/raft_state.h"

#include <algorithm>
#include <cstddef>
#include <utility>

namespace kvstore::raft {

RaftState::RaftState(std::string node_id) : node_id_(std::move(node_id)) {}

const std::string& RaftState::NodeId() const {
    return node_id_;
}

std::uint64_t RaftState::CurrentTerm() const {
    return current_term_;
}

NodeRole RaftState::Role() const {
    return role_;
}

std::optional<std::string> RaftState::VotedFor() const {
    return voted_for_;
}

std::uint64_t RaftState::LastLogIndex() const {
    if (log_.empty()) {
        return 0;
    }

    return log_.back().index;
}

std::uint64_t RaftState::LastLogTerm() const {
    if (log_.empty()) {
        return 0;
    }

    return log_.back().term;
}

std::optional<std::uint64_t> RaftState::LogTermAt(std::uint64_t index) const {
    if (index == 0) {
        return 0;
    }
    if (index > log_.size()) {
        return std::nullopt;
    }

    return log_[index - 1].term;
}

std::optional<LogEntry> RaftState::LogEntryAt(std::uint64_t index) const {
    if (index == 0 || index > log_.size()) {
        return std::nullopt;
    }

    return log_[index - 1];
}

std::uint64_t RaftState::CommitIndex() const {
    return commit_index_;
}

std::uint64_t RaftState::LastApplied() const {
    return last_applied_;
}

PersistentState RaftState::DurableState() const {
    return PersistentState{current_term_, voted_for_, log_};
}

std::uint64_t RaftState::StartElection() {
    ++current_term_;
    role_ = NodeRole::Candidate;
    voted_for_ = node_id_;
    return current_term_;
}

void RaftState::BecomeLeader() {
    role_ = NodeRole::Leader;
}

void RaftState::RestorePersistentState(const PersistentState& state) {
    current_term_ = state.current_term;
    voted_for_ = state.voted_for;
    log_ = state.log;
    role_ = NodeRole::Follower;
    commit_index_ = 0;
    last_applied_ = 0;
}

LogEntry RaftState::AppendLeaderEntry(const protocol::Command& command) {
    const LogEntry entry{LastLogIndex() + 1, current_term_, command};
    log_.push_back(entry);
    return entry;
}

bool RaftState::TruncateUncommittedFrom(std::uint64_t index) {
    if (index == 0 || index <= commit_index_ || index > LastLogIndex()) {
        return false;
    }

    log_.erase(log_.begin() + static_cast<std::ptrdiff_t>(index - 1), log_.end());
    return true;
}

VoteResponse RaftState::HandleVoteRequest(const VoteRequest& request) {
    if (request.term < current_term_) {
        return {current_term_, false};
    }

    if (request.term > current_term_) {
        StepDownToFollower(request.term);
    }

    const bool can_vote = !voted_for_.has_value() || voted_for_.value() == request.candidate_id;
    if (!can_vote) {
        return {current_term_, false};
    }

    voted_for_ = request.candidate_id;
    role_ = NodeRole::Follower;
    return {current_term_, true};
}

HeartbeatResponse RaftState::HandleHeartbeat(const HeartbeatRequest& request) {
    if (request.term < current_term_) {
        return {current_term_, false};
    }

    if (request.term > current_term_) {
        StepDownToFollower(request.term);
    } else {
        role_ = NodeRole::Follower;
    }

    return {current_term_, true};
}

AppendEntriesResponse RaftState::HandleAppendEntries(const AppendEntriesRequest& request) {
    if (request.term < current_term_) {
        return {current_term_, false, LastLogIndex()};
    }

    if (request.term > current_term_) {
        StepDownToFollower(request.term);
    } else {
        role_ = NodeRole::Follower;
    }

    if (request.previous_log_index > LastLogIndex()) {
        return {current_term_, false, LastLogIndex()};
    }

    if (request.previous_log_index > 0 &&
        log_[request.previous_log_index - 1].term != request.previous_log_term) {
        log_.erase(log_.begin() + static_cast<std::ptrdiff_t>(request.previous_log_index - 1),
                   log_.end());
        if (commit_index_ > LastLogIndex()) {
            commit_index_ = LastLogIndex();
        }
        if (last_applied_ > LastLogIndex()) {
            last_applied_ = LastLogIndex();
        }
        return {current_term_, false, LastLogIndex()};
    }

    std::uint64_t expected_index = request.previous_log_index + 1;
    for (const auto& entry : request.entries) {
        if (entry.index != expected_index) {
            return {current_term_, false, LastLogIndex()};
        }

        if (entry.index <= LastLogIndex()) {
            auto& existing = log_[entry.index - 1];
            if (existing.term != entry.term) {
                log_.erase(log_.begin() + static_cast<std::ptrdiff_t>(entry.index - 1), log_.end());
                log_.push_back(entry);
            }
        } else {
            log_.push_back(entry);
        }

        ++expected_index;
    }

    if (request.leader_commit > commit_index_) {
        commit_index_ = std::min(request.leader_commit, LastLogIndex());
    }

    return {current_term_, true, LastLogIndex()};
}

std::vector<protocol::Command> RaftState::MarkCommitted(std::uint64_t commit_index) {
    commit_index_ = std::min(commit_index, LastLogIndex());

    std::vector<protocol::Command> commands;
    while (last_applied_ < commit_index_) {
        commands.push_back(log_[last_applied_].command);
        ++last_applied_;
    }

    return commands;
}

void RaftState::StepDownToFollower(std::uint64_t term) {
    current_term_ = term;
    role_ = NodeRole::Follower;
    voted_for_.reset();
}

}  // namespace kvstore::raft
