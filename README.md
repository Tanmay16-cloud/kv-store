# Distributed Key-Value Store

A production-inspired distributed key-value store built from scratch in C++.

The long-term goal is to build a small but serious system in the spirit of Redis
and etcd: a TCP-accessible key-value database with persistence, replication,
leader election, log replication, cluster metadata, and benchmarking.

## Current Milestone

Milestone 13 adds runtime metrics and a simple benchmark client on top of the
existing shard-aware Raft-backed store:

- CMake-based C++20 project
- Thread-safe in-memory key-value store
- Line-based command parser for `SET`, `GET`, `DEL`, `EXISTS`, `QUIT`, and legacy internal `REPL` writes
- Shared command executor used by console, TCP, and replication paths
- Windows TCP server and CLI client using WinSock
- WAL plus snapshot compaction per server port
- Raft metadata persistence per server port in `.raft` files
- Cluster metadata tracks shard ownership and node endpoints
- `CLUSTER INFO`, `CLUSTER NODES`, and `CLUSTER KEY <key>` expose the shard map to clients
- `serve-cluster` mode routes keys to local shards and returns `MOVED <shard> <host:port>` for non-local keys
- `METRICS` exposes server-side counters such as connections, commands, redirects, Raft messages, and errors
- `kv-bench` can measure basic `SET`, `GET`, and mixed workloads against a running server
- Leader mode stores writes as Raft log entries before applying them
- Leader mode sends internal `RAFT APPEND` and `RAFT COMMIT` messages to followers
- Follower mode rejects direct client writes but accepts committed leader log entries
- Lagging followers can reject a missing previous index and be caught up from earlier leader log entries
- Raft node state tracks current term, role, voted-for candidate, and durable log entries
- TCP server can handle internal `RAFT VOTE`, `RAFT HEARTBEAT`, `RAFT APPEND`, and `RAFT COMMIT` messages
- Leader mode sends periodic heartbeat messages to configured followers
- Lightweight unit tests cover WAL recovery, Raft log replication, lagging follower catch-up, conflict repair, Raft metadata reload, and shard routing metadata

## Build

```powershell
cmake -S . -B build
cmake --build build --config Debug
```

## Test

```powershell
ctest --test-dir build -C Debug --output-on-failure
```

## Run Single Node

```powershell
.\build\Debug\kvstore.exe serve 5000
.\build\Debug\kv-cli.exe 127.0.0.1 5000
```

From the client, you can inspect runtime counters:

```text
METRICS
QUIT
```

Example response:

```text
METRICS uptime_ms=12490 total_connections=3 active_connections=1 total_commands=9 total_reads=4 total_writes=3 total_cluster_redirects=1 total_raft_messages=2 total_errors=0
```

## Run Benchmark

Start any server first, for example:

```powershell
.\build\Debug\kvstore.exe serve 5000
```

Then run the benchmark client from another terminal:

```powershell
.\build\Debug\kv-bench.exe 127.0.0.1 5000 set 1000
.\build\Debug\kv-bench.exe 127.0.0.1 5000 get 1000
.\build\Debug\kv-bench.exe 127.0.0.1 5000 mixed 1000
```

Each run prints:

```text
mode=set
operations=1000
elapsed_ms=...
ops_per_second=...
avg_latency_ms=...
```

## Run Sharded Demo

Terminal 1:

```powershell
.\build\Debug\kvstore.exe serve-cluster 5000 2 0 1=127.0.0.1:5001
```

Terminal 2:

```powershell
.\build\Debug\kvstore.exe serve-cluster 5001 2 1 0=127.0.0.1:5000
```

Terminal 3:

```powershell
.\build\Debug\kv-cli.exe 127.0.0.1 5000
CLUSTER INFO
CLUSTER NODES
CLUSTER KEY profile
SET profile tanmay
GET profile
QUIT
```

If the key belongs to shard `1`, the node on port `5000` responds with something like:

```text
MOVED 1 127.0.0.1:5001
```

Then connect the client to the reported node and retry the command there.

## Run Leader-Follower Demo

Terminal 1:

```powershell
.\build\Debug\kvstore.exe serve-follower 5001
```

Terminal 2:

```powershell
.\build\Debug\kvstore.exe serve-leader 5000 127.0.0.1:5001
```

Terminal 3:

```powershell
.\build\Debug\kv-cli.exe 127.0.0.1 5000
SET name tanmay
GET name
QUIT
```

Then connect to the follower and read the replicated value:

```powershell
.\build\Debug\kv-cli.exe 127.0.0.1 5001
GET name
QUIT
```

Each server persists to port-specific files such as `kvstore-5000.wal`, `kvstore-5000.snapshot`, `kvstore-5000.raft`, `kvstore-5001.wal`, `kvstore-5001.snapshot`, and `kvstore-5001.raft`.

## Try Raft Election Messages

Start any server:

```powershell
.\build\Debug\kvstore.exe serve-follower 5001
```

Connect with the CLI:

```powershell
.\build\Debug\kv-cli.exe 127.0.0.1 5001
```

Then send internal Raft messages:

```text
RAFT VOTE 1 node-a
RAFT VOTE 1 node-b
RAFT HEARTBEAT 2 node-a
QUIT
```

Expected responses:

```text
VOTE 1 GRANTED
VOTE 1 DENIED
HEARTBEAT 2 OK
```

This is not full Raft yet. For simplicity, this milestone models election state transitions, manual vote messages, and leader heartbeats. A production implementation would add randomized election timeouts, automatic vote requests, majority counting, persistent Raft metadata, log freshness checks, and efficient long-lived peer connections.

## Try Leader Heartbeats

Terminal 1:

```powershell
.\build\Debug\kvstore.exe serve-follower 5001
```

Terminal 2:

```powershell
.\build\Debug\kvstore.exe serve-leader 5000 127.0.0.1:5001
```

The leader sends a heartbeat to the follower about once per second. If the follower is stopped or unreachable, the leader prints a heartbeat failure message.

## Try Follower Catch-Up

Use three server terminals so the leader can still reach a majority if one follower is offline.

Terminal 1:

```powershell
.\build\Debug\kvstore.exe serve-follower 5001
```

Terminal 2:

```powershell
.\build\Debug\kvstore.exe serve-follower 5002
```

Terminal 3:

```powershell
.\build\Debug\kvstore.exe serve-leader 5000 127.0.0.1:5001 127.0.0.1:5002
```

Stop the follower on port `5002`, then connect a client to the leader:

```powershell
.\build\Debug\kv-cli.exe 127.0.0.1 5000
SET missed while-down
QUIT
```

Restart the follower on port `5002`, then send another write to the leader:

```powershell
.\build\Debug\kv-cli.exe 127.0.0.1 5000
SET after restart
QUIT
```

The leader first backfills the missed Raft log entry, then replicates the new one. You can connect to follower `5002` and read both keys:

```powershell
.\build\Debug\kv-cli.exe 127.0.0.1 5002
GET missed
GET after
QUIT
```

This is still a learning-stage distributed store. Raft term, vote, and log entries are persisted, and the cluster layer now knows shard ownership and can redirect clients. A production system would combine this with replicated shard groups, richer membership changes, and automatic client-side rerouting.

## Roadmap

1. Project foundation and single-node storage
2. Text command protocol
3. TCP server
4. CLI client
5. Write-ahead log
6. Snapshot and log compaction
7. Leader-follower replication
8. Raft leader election
9. Raft log replication
10. Failure recovery tests
11. Persistent Raft metadata
12. Sharding and cluster metadata
13. Metrics and benchmarking
14. Docker-based demos
