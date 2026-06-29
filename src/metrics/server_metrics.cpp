#include "kvstore/metrics/server_metrics.h"

#include <cstdint>
#include <sstream>

namespace kvstore::metrics {

ServerMetrics::ServerMetrics() : started_at_(std::chrono::steady_clock::now()) {}

void ServerMetrics::RecordConnectionOpened() {
    total_connections_.fetch_add(1, std::memory_order_relaxed);
    active_connections_.fetch_add(1, std::memory_order_relaxed);
}

void ServerMetrics::RecordConnectionClosed() {
    active_connections_.fetch_sub(1, std::memory_order_relaxed);
}

void ServerMetrics::RecordCommandHandled() {
    total_commands_.fetch_add(1, std::memory_order_relaxed);
}

void ServerMetrics::RecordReadCommand() {
    total_reads_.fetch_add(1, std::memory_order_relaxed);
}

void ServerMetrics::RecordWriteCommand() {
    total_writes_.fetch_add(1, std::memory_order_relaxed);
}

void ServerMetrics::RecordClusterRedirect() {
    total_cluster_redirects_.fetch_add(1, std::memory_order_relaxed);
}

void ServerMetrics::RecordRaftMessage() {
    total_raft_messages_.fetch_add(1, std::memory_order_relaxed);
}

void ServerMetrics::RecordError() {
    total_errors_.fetch_add(1, std::memory_order_relaxed);
}

std::string ServerMetrics::Describe() const {
    const auto uptime_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                               std::chrono::steady_clock::now() - started_at_)
                               .count();

    std::ostringstream output;
    output << "METRICS uptime_ms=" << uptime_ms
           << " total_connections=" << total_connections_.load(std::memory_order_relaxed)
           << " active_connections=" << active_connections_.load(std::memory_order_relaxed)
           << " total_commands=" << total_commands_.load(std::memory_order_relaxed)
           << " total_reads=" << total_reads_.load(std::memory_order_relaxed)
           << " total_writes=" << total_writes_.load(std::memory_order_relaxed)
           << " total_cluster_redirects="
           << total_cluster_redirects_.load(std::memory_order_relaxed)
           << " total_raft_messages=" << total_raft_messages_.load(std::memory_order_relaxed)
           << " total_errors=" << total_errors_.load(std::memory_order_relaxed);
    return output.str();
}

}  // namespace kvstore::metrics
