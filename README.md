# Distributed Key-Value Store

A production-inspired distributed key-value store built from scratch in C++.

The long-term goal is to build a small but serious system in the spirit of Redis
and etcd: a TCP-accessible key-value database with persistence, replication,
leader election, log replication, cluster metadata, and benchmarking.

## Current Milestone

Milestone 15 completes the learning-project roadmap with health checks and
client-side shard rerouting on top of the Docker-capable distributed store:

- CMake-based C++20 project
- Thread-safe in-memory key-value store
- Line-based command parser for `SET`, `GET`, `DEL`, `EXISTS`, `QUIT`, and legacy internal `REPL` writes
- Shared command executor used by console, TCP, and replication paths
- Portable TCP server, CLI client, and benchmark client using WinSock on Windows
  and POSIX sockets on Linux/macOS
- `PING` returns `PONG` for readiness and health checks
- WAL plus snapshot compaction per server port
- Raft metadata persistence per server port in `.raft` files
- Cluster metadata tracks shard ownership and node endpoints
- `CLUSTER INFO`, `CLUSTER NODES`, and `CLUSTER KEY <key>` expose the shard map to clients
- `serve-cluster` mode routes keys to local shards and returns `MOVED <shard> <host:port>` for non-local keys
- `kv-cli` and `kv-bench` automatically follow one `MOVED` redirect per command
- `METRICS` exposes server-side counters such as connections, commands, redirects, Raft messages, and errors
- `kv-bench` can measure basic `SET`, `GET`, and mixed workloads against a running server
- Multi-stage Docker image packages `kvstore`, `kv-cli`, and `kv-bench`
- Docker Compose profiles launch standalone, sharded, and leader-follower demos with health checks
- Leader mode stores writes as Raft log entries before applying them
- Leader mode sends internal `RAFT APPEND` and `RAFT COMMIT` messages to followers
- Follower mode rejects direct client writes but accepts committed leader log entries
- Peer-aware followers can start an election and become leader after missed heartbeats
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

## Run with Docker

Build the image and start a single-node server:

```powershell
docker compose --profile standalone up --build
```

In another terminal, use the CLI or benchmark client inside the running
container:

```powershell
docker compose --profile standalone exec standalone kv-cli 127.0.0.1 5000
docker compose --profile standalone exec standalone kv-bench 127.0.0.1 5000 mixed 1000
```

Start the two-shard demo:

```powershell
docker compose --profile cluster up --build
docker compose --profile cluster exec shard-0 kv-cli 127.0.0.1 5000
```

The CLI follows `MOVED` redirects automatically, so commands can be sent to any
shard and retried against the owner by the client.

```powershell
docker compose --profile cluster exec shard-0 kv-bench 127.0.0.1 5000 mixed 1000
```

Start the leader-follower demo:

```powershell
docker compose --profile raft up --build
docker compose --profile raft exec raft-leader kv-cli 127.0.0.1 5000
docker compose --profile raft exec raft-follower-1 kv-cli 127.0.0.1 5001
```

Stop a demo with `docker compose --profile <profile> down`. Add `-v` if you
want to delete the demo volumes and reset WAL, snapshot, and Raft metadata
files.

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
PING
QUIT
```

If the key belongs to shard `1`, the node on port `5000` responds with something like:

```text
MOVED 1 127.0.0.1:5001
```

The CLI follows that redirect automatically and retries the command once against
the reported owner.

## Run Leader-Follower Demo

Terminal 1:

```powershell
.\build\Debug\kvstore.exe serve-follower 5001 127.0.0.1:5000
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

Manual vote messages are still useful for inspecting election state transitions.
Peer-aware followers also run a lightweight automatic election loop when they
stop hearing from a leader.

## Try Leader Heartbeats

Terminal 1:

```powershell
.\build\Debug\kvstore.exe serve-follower 5001 127.0.0.1:5000
```

Terminal 2:

```powershell
.\build\Debug\kvstore.exe serve-leader 5000 127.0.0.1:5001
```

The leader sends a heartbeat to the follower about once per second. If the follower is stopped or unreachable, the leader prints a heartbeat failure message.

## Try Automatic Leader Failover

Use three server terminals so the remaining followers can still form a majority
after the leader stops.

Terminal 1:

```powershell
.\build\Debug\kvstore.exe serve-follower 5001 127.0.0.1:5002 127.0.0.1:5000
```

Terminal 2:

```powershell
.\build\Debug\kvstore.exe serve-follower 5002 127.0.0.1:5001 127.0.0.1:5000
```

Terminal 3:

```powershell
.\build\Debug\kvstore.exe serve-leader 5000 127.0.0.1:5001 127.0.0.1:5002
```

Stop the leader on port `5000`. After a few seconds, one follower should print
that it became leader for a newer term. Connect to the elected follower and send
a write:

```powershell
.\build\Debug\kv-cli.exe 127.0.0.1 5001
SET after-failover yes
GET after-failover
QUIT
```

If port `5001` did not win the election, try port `5002`. This demo now covers
basic automatic failover, but it still does not include client-side leader
discovery.

## Try Follower Catch-Up

Use three server terminals so the leader can still reach a majority if one follower is offline.

Terminal 1:

```powershell
.\build\Debug\kvstore.exe serve-follower 5001 127.0.0.1:5002 127.0.0.1:5000
```

Terminal 2:

```powershell
.\build\Debug\kvstore.exe serve-follower 5002 127.0.0.1:5001 127.0.0.1:5000
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

This is a completed learning-stage distributed store. Raft term, vote, and log
entries are persisted, the cluster layer knows shard ownership, and bundled
clients can follow shard redirects. A production implementation would still need
production-grade Raft election hardening, client-side leader discovery, dynamic
replicated shard groups, richer membership changes, snapshot installation, and
deeper operational hardening.

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
15. Health checks and client-side rerouting
