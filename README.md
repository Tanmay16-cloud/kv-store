# Distributed Key-Value Store

A production-inspired distributed key-value store built from scratch in C++.

The long-term goal is to build a small but serious system in the spirit of Redis
and etcd: a TCP-accessible key-value database with persistence, replication,
leader election, log replication, cluster metadata, and benchmarking.

## Current Milestone

Milestone 3 adds a basic TCP server on top of the parser and storage layer:

- CMake-based C++20 project
- Thread-safe in-memory key-value store
- Line-based command parser for `SET`, `GET`, `DEL`, `EXISTS`, and `QUIT`
- Shared command executor used by both console mode and TCP mode
- Windows TCP server using WinSock
- Lightweight unit tests without external dependencies

## Build

```powershell
cmake -S . -B build
cmake --build build --config Debug
```

## Test

```powershell
ctest --test-dir build -C Debug --output-on-failure
```

## Run Console Mode

```powershell
.\build\Debug\kvstore.exe
```

## Run TCP Server

```powershell
.\build\Debug\kvstore.exe serve 5000
```

In another PowerShell window, you can connect with:

```powershell
$client = [System.Net.Sockets.TcpClient]::new("127.0.0.1", 5000)
$stream = $client.GetStream()
$writer = [System.IO.StreamWriter]::new($stream)
$reader = [System.IO.StreamReader]::new($stream)
$writer.AutoFlush = $true
$reader.ReadLine()
$writer.WriteLine("SET name tanmay")
$reader.ReadLine()
$writer.WriteLine("GET name")
$reader.ReadLine()
$writer.WriteLine("QUIT")
$reader.ReadLine()
$client.Close()
```

## Protocol

The console and TCP server currently understand commands like:

```text
SET name tanmay
GET name
DEL name
EXISTS name
QUIT
```

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
11. Sharding and cluster metadata
12. Metrics, benchmarking, and Docker-based demos
