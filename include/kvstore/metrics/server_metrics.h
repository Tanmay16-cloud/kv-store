#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <string>

namespace kvstore::metrics {

class ServerMetrics {
public:
    ServerMetrics();

    void RecordConnectionOpened();
    void RecordConnectionClosed();
    void RecordCommandHandled();
    void RecordReadCommand();
    void RecordWriteCommand();
    void RecordClusterRedirect();
    void RecordRaftMessage();
    void RecordError();

    std::string Describe() const;

private:
    std::chrono::steady_clock::time_point started_at_;
    std::atomic<std::uint64_t> total_connections_{0};
    std::atomic<std::uint64_t> active_connections_{0};
    std::atomic<std::uint64_t> total_commands_{0};
    std::atomic<std::uint64_t> total_reads_{0};
    std::atomic<std::uint64_t> total_writes_{0};
    std::atomic<std::uint64_t> total_cluster_redirects_{0};
    std::atomic<std::uint64_t> total_raft_messages_{0};
    std::atomic<std::uint64_t> total_errors_{0};
};

}  // namespace kvstore::metrics
