#include <cstdint>
#include <iostream>
#include <limits>
#include <optional>
#include <string>

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

}  // namespace

int main(int argc, char* argv[]) {
    if (argc >= 2 && std::string(argv[1]) == "serve") {
        std::uint16_t port = 5000;
        if (argc >= 3) {
            const auto parsed_port = ParsePort(argv[2]);
            if (!parsed_port.has_value()) {
                std::cerr << "Usage: kvstore serve [port]\n";
                return 1;
            }
            port = parsed_port.value();
        }

        kvstore::server::TcpServer server(port);
        return server.Run();
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
