#include "kvstore/protocol/command.h"
#include "kvstore/protocol/command_executor.h"
#include "kvstore/protocol/command_parser.h"
#include "kvstore/storage/kv_store.h"

#include <cstdlib>
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
        const auto parsed = CommandParser::Parse("set only_key");
        Expect(!parsed.has_value(), "set without value should fail");
    }

    {
        const auto parsed = CommandParser::Parse("get key extra");
        Expect(!parsed.has_value(), "get with extra token should fail");
    }
}

void TestCommandExecutor() {
    using kvstore::protocol::Command;
    using kvstore::protocol::CommandType;
    using kvstore::protocol::ExecuteCommand;

    kvstore::storage::KeyValueStore store;

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
}

}  // namespace

int main() {
    TestSetAndGet();
    TestMissingAndDelete();
    TestEmptyKeyRejected();
    TestConcurrentWrites();
    TestCommandParser();
    TestCommandExecutor();

    if (failures != 0) {
        std::cerr << failures << " test failure(s)\n";
        return EXIT_FAILURE;
    }

    std::cout << "All kv_store tests passed\n";
    return EXIT_SUCCESS;
}
