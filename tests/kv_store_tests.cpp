#include "kvstore/cluster/cluster_metadata.h"
#include "kvstore/metrics/server_metrics.h"
#include "kvstore/persistence/write_ahead_log.h"
#include "kvstore/persistence/raft_metadata_store.h"
#include "kvstore/protocol/command.h"
#include "kvstore/protocol/command_executor.h"
#include "kvstore/protocol/command_parser.h"
#include "kvstore/raft/raft_state.h"
#include "kvstore/storage/kv_store.h"

#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

namespace {

int failures = 0;

void Expect(bool condition, const std::string& message) {
    if (!condition) {
        ++failures;
        std::cerr << "FAIL: " << message << '\n';
    }
}

template <typename T, typename U>
void ExpectEqual(const T& actual, const U& expected, const std::string& message) {
    if (!(actual == expected)) {
        ++failures;
        std::cerr << "FAIL: " << message << " expected=" << expected << " actual=" << actual
                  << '\n';
    }
}

std::string FindKeyForShard(const kvstore::cluster::ClusterMetadata& metadata, std::size_t shard_id) {
    for (int i = 0; i < 5000; ++i) {
        const std::string candidate = "key-" + std::to_string(shard_id) + "-" + std::to_string(i);
        if (metadata.ShardForKey(candidate) == shard_id) {
            return candidate;
        }
    }

    return {};
}

void TestSetAndGet() {
    kvstore::storage::KeyValueStore store;

    Expect(store.Set("name", "tanmay"), "set should accept a non-empty key");
    const auto value = store.Get("name");

    Expect(value.has_value(), "get should return a value for an existing key");
    ExpectEqual(value.value_or(""), std::string("tanmay"), "get should return stored value");
    ExpectEqual(store.Size(), static_cast<std::size_t>(1), "size should count stored keys");
}

void TestMissingAndDelete() {
    kvstore::storage::KeyValueStore store;

    Expect(!store.Get("missing").has_value(), "missing keys should return no value");
    Expect(!store.Delete("missing"), "delete should report false for missing keys");

    Expect(store.Set("session", "active"), "set should store key before delete");
    Expect(store.Exists("session"), "exists should report true for stored key");
    Expect(store.Delete("session"), "delete should report true for existing key");
    Expect(!store.Exists("session"), "exists should report false after delete");
}

void TestEmptyKeyRejected() {
    kvstore::storage::KeyValueStore store;

    Expect(!store.Set("", "value"), "empty keys should be rejected");
    ExpectEqual(store.Size(), static_cast<std::size_t>(0), "rejected keys should not change size");
}

void TestConcurrentWrites() {
    kvstore::storage::KeyValueStore store;
    std::vector<std::thread> workers;

    constexpr int thread_count = 8;
    constexpr int writes_per_thread = 250;

    for (int thread_id = 0; thread_id < thread_count; ++thread_id) {
        workers.emplace_back([&store, thread_id] {
            for (int i = 0; i < writes_per_thread; ++i) {
                store.Set("key-" + std::to_string(thread_id) + "-" + std::to_string(i),
                          std::to_string(i));
            }
        });
    }

    for (auto& worker : workers) {
        worker.join();
    }

    ExpectEqual(store.Size(), static_cast<std::size_t>(thread_count * writes_per_thread),
                "concurrent writes should store all unique keys");
}

void TestCommandParser() {
    using kvstore::protocol::CommandParser;
    using kvstore::protocol::CommandType;

    {
        const auto parsed = CommandParser::Parse("  SET   name   tanmay  ");
        Expect(parsed.has_value(), "set command should parse");
        Expect(parsed->type == CommandType::Set, "set command type should match");
        ExpectEqual(parsed->key, std::string("name"), "set key should parse");
        ExpectEqual(parsed->value, std::string("tanmay"), "set value should parse");
    }

    {
        const auto parsed = CommandParser::Parse("get user");
        Expect(parsed.has_value(), "get command should parse");
        Expect(parsed->type == CommandType::Get, "get command type should match");
        ExpectEqual(parsed->key, std::string("user"), "get key should parse");
    }

    {
        const auto parsed = CommandParser::Parse("DEL session");
        Expect(parsed.has_value(), "del command should parse");
        Expect(parsed->type == CommandType::Del, "del command type should match");
        ExpectEqual(parsed->key, std::string("session"), "del key should parse");
    }

    {
        const auto parsed = CommandParser::Parse("exists token");
        Expect(parsed.has_value(), "exists command should parse");
        Expect(parsed->type == CommandType::Exists, "exists command type should match");
        ExpectEqual(parsed->key, std::string("token"), "exists key should parse");
    }

    {
        const auto parsed = CommandParser::Parse("quit");
        Expect(parsed.has_value(), "quit command should parse");
        Expect(parsed->type == CommandType::Quit, "quit command type should match");
    }

    {
        const auto parsed = CommandParser::Parse("cluster info");
        Expect(parsed.has_value(), "cluster info command should parse");
        Expect(parsed->type == CommandType::ClusterInfo, "cluster info type should match");
    }

    {
        const auto parsed = CommandParser::Parse("metrics");
        Expect(parsed.has_value(), "metrics command should parse");
        Expect(parsed->type == CommandType::Metrics, "metrics command type should match");
    }

    {
        const auto parsed = CommandParser::Parse("ping");
        Expect(parsed.has_value(), "ping command should parse");
        Expect(parsed->type == CommandType::Ping, "ping command type should match");
    }

    {
        const auto parsed = CommandParser::Parse("ping now");
        Expect(!parsed.has_value(), "ping with trailing text should fail");
    }

    {
        const auto parsed = CommandParser::Parse("CLUSTER NODES");
        Expect(parsed.has_value(), "cluster nodes command should parse");
        Expect(parsed->type == CommandType::ClusterNodes, "cluster nodes type should match");
    }

    {
        const auto parsed = CommandParser::Parse("cluster key profile");
        Expect(parsed.has_value(), "cluster key command should parse");
        Expect(parsed->type == CommandType::ClusterKey, "cluster key type should match");
        ExpectEqual(parsed->key, std::string("profile"), "cluster key argument should parse");
    }

    {
        const auto parsed = CommandParser::Parse("REPL SET name tanmay");
        Expect(parsed.has_value(), "replicated set command should parse");
        Expect(parsed->type == CommandType::Set, "replicated set command type should match");
        Expect(parsed->is_replicated, "replicated set should be marked replicated");
        ExpectEqual(parsed->key, std::string("name"), "replicated set key should parse");
        ExpectEqual(parsed->value, std::string("tanmay"), "replicated set value should parse");
    }

    {
        const auto parsed = CommandParser::Parse("REPL GET name");
        Expect(!parsed.has_value(), "replicated read command should fail");
    }

    {
        const auto parsed = CommandParser::Parse("set only_key");
        Expect(!parsed.has_value(), "set without value should fail");
    }

    {
        const auto parsed = CommandParser::Parse("get key extra");
        Expect(!parsed.has_value(), "get with extra token should fail");
    }

    {
        const auto parsed = CommandParser::Parse("cluster key");
        Expect(!parsed.has_value(), "cluster key without key should fail");
    }
}

void TestCommandExecutor() {
    using kvstore::cluster::ClusterMetadata;
    using kvstore::cluster::ClusterNode;
    using kvstore::metrics::ServerMetrics;
    using kvstore::protocol::Command;
    using kvstore::protocol::CommandType;
    using kvstore::protocol::ExecuteCommand;

    kvstore::storage::KeyValueStore store;
    ServerMetrics metrics;
    const ClusterMetadata cluster_metadata(
        "node-5000",
        2,
        0,
        {ClusterNode{"node-5000", "127.0.0.1", 5000, 0, true},
         ClusterNode{"node-5001", "127.0.0.1", 5001, 1, false}});

    {
        metrics.RecordConnectionOpened();
        metrics.RecordCommandHandled();
        metrics.RecordReadCommand();
        metrics.RecordClusterRedirect();
        metrics.RecordRaftMessage();
        metrics.RecordError();
        metrics.RecordConnectionClosed();

        const auto result =
            ExecuteCommand(store, Command{CommandType::Metrics, {}, {}}, nullptr, nullptr, &metrics);
        Expect(result.response.rfind("METRICS uptime_ms=", 0) == 0,
               "metrics should return metrics description");
        Expect(result.response.find("total_connections=1") != std::string::npos,
               "metrics should include connection count");
        Expect(result.response.find("active_connections=0") != std::string::npos,
               "metrics should include active connection count");
        Expect(result.response.find("total_commands=1") != std::string::npos,
               "metrics should include command count");
        Expect(result.response.find("total_reads=1") != std::string::npos,
               "metrics should include read count");
        Expect(result.response.find("total_cluster_redirects=1") != std::string::npos,
               "metrics should include redirect count");
        Expect(result.response.find("total_raft_messages=1") != std::string::npos,
               "metrics should include raft message count");
        Expect(result.response.find("total_errors=1") != std::string::npos,
               "metrics should include error count");
    }

    {
        const auto result = ExecuteCommand(store, Command{CommandType::Metrics, {}, {}});
        ExpectEqual(result.response, std::string("ERR metrics unavailable"),
                    "metrics should fail without a metrics provider");
    }

    {
        const auto result = ExecuteCommand(store, Command{CommandType::Ping, {}, {}});
        ExpectEqual(result.response, std::string("PONG"), "ping should return PONG");
        Expect(!result.should_close, "ping should keep connection open");
    }

    {
        const auto result = ExecuteCommand(store, Command{CommandType::Set, "name", "tanmay"});
        ExpectEqual(result.response, std::string("OK"), "set should return OK");
        Expect(!result.should_close, "set should keep connection open");
    }

    {
        const auto result = ExecuteCommand(store, Command{CommandType::Get, "name", {}});
        ExpectEqual(result.response, std::string("tanmay"), "get should return stored value");
    }

    {
        const auto result = ExecuteCommand(store, Command{CommandType::Exists, "name", {}});
        ExpectEqual(result.response, std::string("1"), "exists should return 1 for present key");
    }

    {
        const auto result = ExecuteCommand(store, Command{CommandType::Del, "name", {}});
        ExpectEqual(result.response, std::string("1"), "del should return 1 for deleted key");
    }

    {
        const auto result = ExecuteCommand(store, Command{CommandType::Get, "name", {}});
        ExpectEqual(result.response, std::string("NOT_FOUND"), "get should return NOT_FOUND");
    }

    {
        const auto result = ExecuteCommand(store, Command{CommandType::Quit, {}, {}});
        ExpectEqual(result.response, std::string("BYE"), "quit should return BYE");
        Expect(result.should_close, "quit should close connection");
    }

    {
        const auto result =
            ExecuteCommand(store, Command{CommandType::ClusterInfo, {}, {}}, nullptr, &cluster_metadata);
        Expect(result.response.find("CLUSTER node=node-5000") != std::string::npos,
               "cluster info should describe local node");
    }

    {
        const auto result =
            ExecuteCommand(store, Command{CommandType::ClusterNodes, {}, {}}, nullptr, &cluster_metadata);
        Expect(result.response.find("0:node-5000@127.0.0.1:5000:local") != std::string::npos,
               "cluster nodes should include local shard");
        Expect(result.response.find("1:node-5001@127.0.0.1:5001") != std::string::npos,
               "cluster nodes should include remote shard");
    }

    {
        const auto result = ExecuteCommand(store,
                                           Command{CommandType::ClusterKey, "profile", {}},
                                           nullptr,
                                           &cluster_metadata);
        Expect(result.response.find("KEY profile SHARD ") != std::string::npos,
               "cluster key should describe shard ownership");
    }
}

void TestWriteAheadLogReplay() {
    using kvstore::persistence::WriteAheadLog;
    using kvstore::protocol::Command;
    using kvstore::protocol::CommandType;
    using kvstore::protocol::ExecuteCommand;

    const auto wal_path = std::filesystem::temp_directory_path() / "kvstore_wal_test.log";
    const auto snapshot_path = std::filesystem::temp_directory_path() / "kvstore_snapshot_test.snapshot";
    std::filesystem::remove(wal_path);
    std::filesystem::remove(snapshot_path);

    {
        kvstore::storage::KeyValueStore store;
        WriteAheadLog wal(wal_path, snapshot_path);

        ExpectEqual(ExecuteCommand(store, Command{CommandType::Set, "name", "tanmay"}, &wal).response,
                    std::string("OK"), "wal set should return OK");
        ExpectEqual(ExecuteCommand(store, Command{CommandType::Set, "city", "goa"}, &wal).response,
                    std::string("OK"), "wal second set should return OK");
        ExpectEqual(ExecuteCommand(store, Command{CommandType::Del, "city", {}}, &wal).response,
                    std::string("1"), "wal delete should return 1");
    }

    {
        kvstore::storage::KeyValueStore recovered;
        WriteAheadLog wal(wal_path, snapshot_path);

        Expect(wal.Replay(recovered), "wal replay should succeed");
        ExpectEqual(recovered.Get("name").value_or(""), std::string("tanmay"),
                    "wal replay should restore set key");
        Expect(!recovered.Exists("city"), "wal replay should apply delete");
    }

    std::filesystem::remove(wal_path);
    std::filesystem::remove(snapshot_path);
}

void TestRaftElectionState() {
    using kvstore::raft::HeartbeatRequest;
    using kvstore::raft::NodeRole;
    using kvstore::raft::RaftState;
    using kvstore::raft::VoteRequest;

    RaftState node("node-a");
    ExpectEqual(node.CurrentTerm(), static_cast<std::uint64_t>(0), "raft term should start at zero");
    Expect(node.Role() == NodeRole::Follower, "raft node should start as follower");

    const auto election_term = node.StartElection();
    ExpectEqual(election_term, static_cast<std::uint64_t>(1), "starting election should increment term");
    Expect(node.Role() == NodeRole::Candidate, "starting election should become candidate");
    Expect(node.VotedFor().value_or("") == "node-a", "candidate should vote for itself");

    const auto stale_vote = node.HandleVoteRequest(VoteRequest{0, "node-b"});
    Expect(!stale_vote.vote_granted, "stale vote request should be rejected");

    const auto newer_vote = node.HandleVoteRequest(VoteRequest{2, "node-b"});
    Expect(newer_vote.vote_granted, "newer term vote request should be granted");
    ExpectEqual(node.CurrentTerm(), static_cast<std::uint64_t>(2), "newer vote should update term");
    Expect(node.Role() == NodeRole::Follower, "newer vote should step down to follower");
    Expect(node.VotedFor().value_or("") == "node-b", "node should remember vote");

    const auto duplicate_vote = node.HandleVoteRequest(VoteRequest{2, "node-b"});
    Expect(duplicate_vote.vote_granted, "same candidate can receive duplicate vote");

    const auto second_candidate_vote = node.HandleVoteRequest(VoteRequest{2, "node-c"});
    Expect(!second_candidate_vote.vote_granted, "node should not vote twice in same term");

    node.BecomeLeader();
    node.ObserveTerm(3);
    ExpectEqual(node.CurrentTerm(), static_cast<std::uint64_t>(3),
                "observing a newer term should update term");
    Expect(node.Role() == NodeRole::Follower, "observing a newer term should step down");

    node.BecomeLeader();
    const auto heartbeat = node.HandleHeartbeat(HeartbeatRequest{4, "node-c"});
    Expect(heartbeat.accepted, "newer heartbeat should be accepted");
    ExpectEqual(node.CurrentTerm(), static_cast<std::uint64_t>(4), "heartbeat should update term");
    Expect(node.Role() == NodeRole::Follower, "newer heartbeat should step leader down");
    ExpectEqual(node.KnownLeaderId().value_or(""), std::string("node-c"),
                "heartbeat should remember known leader");
}

void TestRaftLogReplicationState() {
    using kvstore::protocol::Command;
    using kvstore::protocol::CommandType;
    using kvstore::raft::AppendEntriesRequest;
    using kvstore::raft::NodeRole;
    using kvstore::raft::RaftState;

    RaftState leader("node-a");
    RaftState follower("node-b");

    leader.StartElection();
    leader.BecomeLeader();

    const Command command{CommandType::Set, "name", "tanmay"};
    const auto previous_index = leader.LastLogIndex();
    const auto previous_term = leader.LogTermAt(previous_index).value_or(0);
    const auto entry = leader.AppendLeaderEntry(command);

    const auto append_response = follower.HandleAppendEntries(AppendEntriesRequest{
        leader.CurrentTerm(),
        leader.NodeId(),
        previous_index,
        previous_term,
        leader.CommitIndex(),
        {entry}});

    Expect(append_response.success, "follower should append a valid leader log entry");
    ExpectEqual(follower.LastLogIndex(), static_cast<std::uint64_t>(1),
                "follower log should contain appended entry");
    ExpectEqual(follower.CommitIndex(), static_cast<std::uint64_t>(0),
                "append alone should not commit the entry");
    Expect(follower.Role() == NodeRole::Follower, "append should keep receiver as follower");

    const auto commands = follower.MarkCommitted(entry.index);
    ExpectEqual(commands.size(), static_cast<std::size_t>(1),
                "committing the entry should return one command to apply");
    Expect(commands.front().type == CommandType::Set, "committed command type should match");
    ExpectEqual(commands.front().key, std::string("name"), "committed command key should match");
    ExpectEqual(commands.front().value, std::string("tanmay"), "committed command value should match");
    ExpectEqual(follower.LastApplied(), static_cast<std::uint64_t>(1),
                "committing should advance last applied");

    const auto stale_response = follower.HandleAppendEntries(AppendEntriesRequest{
        0,
        leader.NodeId(),
        follower.LastLogIndex(),
        follower.LastLogTerm(),
        follower.CommitIndex(),
        {}});
    Expect(!stale_response.success, "stale append entries request should be rejected");
}

void TestRaftLaggingFollowerCatchUp() {
    using kvstore::protocol::Command;
    using kvstore::protocol::CommandType;
    using kvstore::raft::AppendEntriesRequest;
    using kvstore::raft::RaftState;

    RaftState leader("node-a");
    RaftState follower("node-b");

    leader.StartElection();
    leader.BecomeLeader();

    const auto first_entry = leader.AppendLeaderEntry(Command{CommandType::Set, "a", "1"});
    const auto second_entry = leader.AppendLeaderEntry(Command{CommandType::Set, "b", "2"});

    const auto missing_previous = follower.HandleAppendEntries(AppendEntriesRequest{
        leader.CurrentTerm(),
        leader.NodeId(),
        second_entry.index - 1,
        leader.LogTermAt(second_entry.index - 1).value_or(0),
        leader.CommitIndex(),
        {second_entry}});
    Expect(!missing_previous.success, "lagging follower should reject append with missing previous entry");
    ExpectEqual(missing_previous.match_index, static_cast<std::uint64_t>(0),
                "empty follower should report last matching index as zero");

    const auto first_append = follower.HandleAppendEntries(AppendEntriesRequest{
        leader.CurrentTerm(),
        leader.NodeId(),
        0,
        0,
        leader.CommitIndex(),
        {first_entry}});
    Expect(first_append.success, "lagging follower should accept missing first entry");

    const auto second_append = follower.HandleAppendEntries(AppendEntriesRequest{
        leader.CurrentTerm(),
        leader.NodeId(),
        first_entry.index,
        first_entry.term,
        leader.CommitIndex(),
        {second_entry}});
    Expect(second_append.success, "lagging follower should accept next entry after catch-up");
    ExpectEqual(follower.LastLogIndex(), second_entry.index,
                "lagging follower should catch up to leader log index");
}

void TestRaftConflictingFollowerLogRecovery() {
    using kvstore::protocol::Command;
    using kvstore::protocol::CommandType;
    using kvstore::raft::AppendEntriesRequest;
    using kvstore::raft::RaftState;

    RaftState leader("node-a");
    RaftState follower("node-b");

    leader.StartElection();
    leader.BecomeLeader();

    const auto leader_first = leader.AppendLeaderEntry(Command{CommandType::Set, "a", "1"});
    const auto leader_second = leader.AppendLeaderEntry(Command{CommandType::Set, "b", "2"});

    follower.HandleAppendEntries(AppendEntriesRequest{
        leader.CurrentTerm(),
        leader.NodeId(),
        0,
        0,
        0,
        {leader_first}});

    follower.HandleAppendEntries(AppendEntriesRequest{
        leader.CurrentTerm() + 1,
        "node-c",
        leader_first.index,
        leader_first.term,
        0,
        {kvstore::raft::LogEntry{2, leader.CurrentTerm() + 1, Command{CommandType::Set, "b", "stale"}}}});

    const auto rejected = follower.HandleAppendEntries(AppendEntriesRequest{
        leader.CurrentTerm() + 1,
        leader.NodeId(),
        leader_second.index,
        leader_second.term,
        leader.CommitIndex(),
        {}});
    Expect(!rejected.success, "follower should reject previous-log term mismatch");
    ExpectEqual(follower.LastLogIndex(), leader_first.index,
                "conflicting follower should truncate divergent entries");

    const auto repaired = follower.HandleAppendEntries(AppendEntriesRequest{
        leader.CurrentTerm() + 1,
        leader.NodeId(),
        leader_first.index,
        leader_first.term,
        leader.CommitIndex(),
        {kvstore::raft::LogEntry{leader_second.index,
                                 leader.CurrentTerm() + 1,
                                 leader_second.command}}});
    Expect(repaired.success, "follower should accept corrected entry after truncation");
    ExpectEqual(follower.LastLogIndex(), leader_second.index,
                "repaired follower should end at leader log index");
}

void TestRaftMetadataStoreRoundTrip() {
    using kvstore::persistence::RaftMetadataStore;
    using kvstore::protocol::Command;
    using kvstore::protocol::CommandType;
    using kvstore::raft::NodeRole;
    using kvstore::raft::RaftState;

    const auto raft_path = std::filesystem::temp_directory_path() / "kvstore_raft_metadata_test.raft";
    std::filesystem::remove(raft_path);

    {
        RaftState original("node-a");
        original.StartElection();
        original.AppendLeaderEntry(Command{CommandType::Set, "name", "tanmay bits"});
        original.AppendLeaderEntry(Command{CommandType::Del, "old-key", {}});

        RaftMetadataStore store(raft_path);
        Expect(store.Save(original.DurableState()), "raft metadata save should succeed");
    }

    {
        RaftState recovered("node-a");
        RaftMetadataStore store(raft_path);

        Expect(store.Load(recovered), "raft metadata load should succeed");
        ExpectEqual(recovered.CurrentTerm(), static_cast<std::uint64_t>(1),
                    "raft metadata should restore current term");
        ExpectEqual(recovered.VotedFor().value_or(""), std::string("node-a"),
                    "raft metadata should restore voted-for candidate");
        Expect(recovered.Role() == NodeRole::Follower,
               "restored raft state should restart as follower");
        ExpectEqual(recovered.LastLogIndex(), static_cast<std::uint64_t>(2),
                    "raft metadata should restore log entries");

        const auto first_entry = recovered.LogEntryAt(1);
        Expect(first_entry.has_value(), "first restored log entry should exist");
        Expect(first_entry->command.type == CommandType::Set,
               "first restored log command type should match");
        ExpectEqual(first_entry->command.key, std::string("name"),
                    "first restored log key should match");
        ExpectEqual(first_entry->command.value, std::string("tanmay bits"),
                    "first restored log value should preserve spaces");

        const auto second_entry = recovered.LogEntryAt(2);
        Expect(second_entry.has_value(), "second restored log entry should exist");
        Expect(second_entry->command.type == CommandType::Del,
               "second restored log command type should match");
        ExpectEqual(second_entry->command.key, std::string("old-key"),
                    "second restored log key should match");
    }

    std::filesystem::remove(raft_path);
}

void TestClusterMetadataRouting() {
    using kvstore::cluster::ClusterMetadata;
    using kvstore::cluster::ClusterNode;

    const ClusterMetadata metadata(
        "node-5000",
        3,
        1,
        {ClusterNode{"node-5000", "127.0.0.1", 5000, 1, true},
         ClusterNode{"node-5001", "127.0.0.1", 5001, 0, false},
         ClusterNode{"node-5002", "127.0.0.1", 5002, 2, false}});

    Expect(metadata.Enabled(), "cluster metadata should be enabled with local shard");
    ExpectEqual(metadata.LocalShardId(), static_cast<std::size_t>(1),
                "cluster metadata should report local shard id");

    const auto local_key = FindKeyForShard(metadata, 1);
    const auto remote_key = FindKeyForShard(metadata, 2);
    Expect(!local_key.empty(), "should find a key for the local shard");
    Expect(!remote_key.empty(), "should find a key for a remote shard");
    Expect(metadata.OwnsKey(local_key), "local-shard key should belong to the local node");
    Expect(!metadata.OwnsKey(remote_key), "remote-shard key should not belong to the local node");

    const auto* owner = metadata.OwnerForKey(remote_key);
    Expect(owner != nullptr, "remote shard should have an owner");
    if (owner != nullptr) {
        ExpectEqual(owner->node_id, std::string("node-5002"),
                    "remote key should resolve to the shard owner");
    }

    const auto key_description = metadata.DescribeKey(remote_key);
    Expect(key_description.find("OWNER node-5002 127.0.0.1:5002") != std::string::npos,
           "key description should include the shard owner endpoint");
}

void TestSnapshotCompaction() {
    using kvstore::persistence::WriteAheadLog;
    using kvstore::protocol::Command;
    using kvstore::protocol::CommandType;
    using kvstore::protocol::ExecuteCommand;

    const auto wal_path = std::filesystem::temp_directory_path() / "kvstore_compact_test.log";
    const auto snapshot_path = std::filesystem::temp_directory_path() / "kvstore_compact_test.snapshot";
    std::filesystem::remove(wal_path);
    std::filesystem::remove(snapshot_path);

    kvstore::storage::KeyValueStore store;
    WriteAheadLog wal(wal_path, snapshot_path);

    ExpectEqual(ExecuteCommand(store, Command{CommandType::Set, "name", "tanmay"}, &wal).response,
                std::string("OK"), "compact setup set name");
    ExpectEqual(ExecuteCommand(store, Command{CommandType::Set, "role", "sde"}, &wal).response,
                std::string("OK"), "compact setup set role");
    Expect(wal.Compact(store), "compaction should succeed");

    {
        std::ifstream wal_contents(wal_path);
        std::string contents;
        std::getline(wal_contents, contents);
        Expect(contents.empty(), "wal should be empty after compaction");
    }

    ExpectEqual(ExecuteCommand(store, Command{CommandType::Del, "role", {}}, &wal).response,
                std::string("1"), "delete after compaction should succeed");

    kvstore::storage::KeyValueStore recovered;
    Expect(wal.Replay(recovered), "replay after compaction should succeed");
    ExpectEqual(recovered.Get("name").value_or(""), std::string("tanmay"),
                "snapshot should preserve surviving key");
    Expect(!recovered.Exists("role"), "wal after snapshot should still apply later delete");

    std::filesystem::remove(wal_path);
    std::filesystem::remove(snapshot_path);
}

}  // namespace

int main() {
    TestSetAndGet();
    TestMissingAndDelete();
    TestEmptyKeyRejected();
    TestConcurrentWrites();
    TestCommandParser();
    TestCommandExecutor();
    TestWriteAheadLogReplay();
    TestRaftElectionState();
    TestRaftLogReplicationState();
    TestRaftLaggingFollowerCatchUp();
    TestRaftConflictingFollowerLogRecovery();
    TestRaftMetadataStoreRoundTrip();
    TestClusterMetadataRouting();
    TestSnapshotCompaction();

    if (failures != 0) {
        std::cerr << failures << " test failure(s)\n";
        return EXIT_FAILURE;
    }

    std::cout << "All kv_store tests passed\n";
    return EXIT_SUCCESS;
}
