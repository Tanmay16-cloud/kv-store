#include <cstdint>
#include <iostream>
#include <limits>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "kvstore/cluster/cluster_metadata.h"
#include "kvstore/protocol/command.h"
#include "kvstore/protocol/command_executor.h"
#include "kvstore/protocol/command_parser.h"
#include "kvstore/server/tcp_server.h"
#include "kvstore/storage/kv_store.h"

namespace {

std::optional<std::uint16_t> ParsePort(const char* text) {
    try {
        const unsigned long value = std::stoul(text);
        if (value > std::numeric_limits<std::uint16_t>::max()) {
            return std::nullopt;
        }
        return static_cast<std::uint16_t>(value);
    } catch (...) {
        return std::nullopt;
    }
}

std::optional<kvstore::server::FollowerEndpoint> ParseFollowerEndpoint(const std::string& text) {
    const auto colon = text.rfind(':');
    if (colon == std::string::npos || colon == 0 || colon + 1 >= text.size()) {
        return std::nullopt;
    }

    const auto port = ParsePort(text.substr(colon + 1).c_str());
    if (!port.has_value()) {
        return std::nullopt;
    }

    return kvstore::server::FollowerEndpoint{text.substr(0, colon), port.value()};
}

std::optional<std::size_t> ParseShardId(const std::string& text) {
    try {
        std::size_t parsed_chars = 0;
        const auto value = std::stoull(text, &parsed_chars);
        if (parsed_chars != text.size()) {
            return std::nullopt;
        }
        return static_cast<std::size_t>(value);
    } catch (...) {
        return std::nullopt;
    }
}

std::optional<kvstore::cluster::ClusterNode> ParseShardNodeMapping(const std::string& text) {
    const auto equals = text.find('=');
    if (equals == std::string::npos || equals == 0 || equals + 1 >= text.size()) {
        return std::nullopt;
    }

    const auto shard_id = ParseShardId(text.substr(0, equals));
    const auto endpoint = ParseFollowerEndpoint(text.substr(equals + 1));
    if (!shard_id.has_value() || !endpoint.has_value()) {
        return std::nullopt;
    }

    return kvstore::cluster::ClusterNode{
        "node-" + std::to_string(endpoint->port),
        endpoint->host,
        endpoint->port,
        shard_id.value(),
        false};
}

void PrintUsage() {
    std::cerr << "Usage:\n";
    std::cerr << "  kvstore serve [port]\n";
    std::cerr << "  kvstore serve-follower <port> [peer_host:peer_port ...]\n";
    std::cerr << "  kvstore serve-leader <port> [follower_host:follower_port ...]\n";
    std::cerr << "  kvstore serve-cluster <port> <shard_count> <local_shard_id>"
                 " [shard_id=host:port ...]\n";
}

}  // namespace

int main(int argc, char* argv[]) {
    if (argc >= 2) {
        const std::string mode = argv[1];

        if (mode == "serve") {
            std::uint16_t port = 5000;
            if (argc >= 3) {
                const auto parsed_port = ParsePort(argv[2]);
                if (!parsed_port.has_value()) {
                    PrintUsage();
                    return 1;
                }
                port = parsed_port.value();
            }

            kvstore::server::TcpServer server(port);
            return server.Run();
        }

        if (mode == "serve-follower") {
            if (argc < 3) {
                PrintUsage();
                return 1;
            }
            const auto port = ParsePort(argv[2]);
            if (!port.has_value()) {
                PrintUsage();
                return 1;
            }

            std::vector<kvstore::server::FollowerEndpoint> peers;
            for (int i = 3; i < argc; ++i) {
                const auto peer = ParseFollowerEndpoint(argv[i]);
                if (!peer.has_value()) {
                    PrintUsage();
                    return 1;
                }
                peers.push_back(peer.value());
            }

            kvstore::server::TcpServer server(port.value(),
                                              kvstore::server::ServerRole::Follower,
                                              std::move(peers));
            return server.Run();
        }

        if (mode == "serve-leader") {
            if (argc < 3) {
                PrintUsage();
                return 1;
            }
            const auto port = ParsePort(argv[2]);
            if (!port.has_value()) {
                PrintUsage();
                return 1;
            }

            std::vector<kvstore::server::FollowerEndpoint> followers;
            for (int i = 3; i < argc; ++i) {
                const auto follower = ParseFollowerEndpoint(argv[i]);
                if (!follower.has_value()) {
                    PrintUsage();
                    return 1;
                }
                followers.push_back(follower.value());
            }

            kvstore::server::TcpServer server(port.value(),
                                          kvstore::server::ServerRole::Leader,
                                          std::move(followers));
            return server.Run();
        }

        if (mode == "serve-cluster") {
            if (argc < 5) {
                PrintUsage();
                return 1;
            }

            const auto port = ParsePort(argv[2]);
            const auto shard_count = ParseShardId(argv[3]);
            const auto local_shard_id = ParseShardId(argv[4]);
            if (!port.has_value() || !shard_count.has_value() || !local_shard_id.has_value() ||
                shard_count.value() == 0 || local_shard_id.value() >= shard_count.value()) {
                PrintUsage();
                return 1;
            }

            std::vector<kvstore::cluster::ClusterNode> nodes;
            nodes.push_back(kvstore::cluster::ClusterNode{
                "node-" + std::to_string(port.value()),
                "127.0.0.1",
                port.value(),
                local_shard_id.value(),
                true});

            for (int i = 5; i < argc; ++i) {
                const auto node = ParseShardNodeMapping(argv[i]);
                if (!node.has_value() || node->shard_id >= shard_count.value()) {
                    PrintUsage();
                    return 1;
                }
                nodes.push_back(node.value());
            }

            kvstore::cluster::ClusterMetadata cluster_metadata("node-" + std::to_string(port.value()),
                                                               shard_count.value(),
                                                               local_shard_id.value(),
                                                               std::move(nodes));
            kvstore::server::TcpServer server(port.value(),
                                              kvstore::server::ServerRole::Standalone,
                                              {},
                                              std::move(cluster_metadata));
            return server.Run();
        }
    }

    kvstore::storage::KeyValueStore store;

    std::cout << "Distributed KV Store ready. Type QUIT to exit.\n";

    std::string line;
    while (std::cout << "kvstore> " && std::getline(std::cin, line)) {
        const auto parsed = kvstore::protocol::CommandParser::Parse(line);
        if (!parsed.has_value()) {
            std::cout << "ERR invalid command\n";
            continue;
        }

        const auto result = kvstore::protocol::ExecuteCommand(store, parsed.value());
        std::cout << result.response << '\n';
        if (result.should_close) {
            return 0;
        }
    }

    return 0;
}
