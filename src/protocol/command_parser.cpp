#include "kvstore/protocol/command_parser.h"

#include <cctype>
#include <string>
#include <string_view>
#include <utility>

namespace kvstore::protocol {
namespace {

std::string_view TrimLeft(std::string_view text) {
    while (!text.empty() && std::isspace(static_cast<unsigned char>(text.front())) != 0) {
        text.remove_prefix(1);
    }
    return text;
}

std::string_view TrimRight(std::string_view text) {
    while (!text.empty() && std::isspace(static_cast<unsigned char>(text.back())) != 0) {
        text.remove_suffix(1);
    }
    return text;
}

std::string_view Trim(std::string_view text) {
    return TrimRight(TrimLeft(text));
}

std::string ToLowerAscii(std::string_view text) {
    std::string lowered;
    lowered.reserve(text.size());
    for (char ch : text) {
        lowered.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(ch))));
    }
    return lowered;
}

std::pair<std::string_view, std::string_view> SplitFirstToken(std::string_view text) {
    text = TrimLeft(text);
    const auto split = text.find_first_of(" \t\r\n");
    if (split == std::string_view::npos) {
        return {text, std::string_view{}};
    }

    return {text.substr(0, split), text.substr(split)};
}

bool HasUnexpectedTrailingText(std::string_view text) {
    return !Trim(text).empty();
}

}  // namespace

std::optional<Command> CommandParser::Parse(std::string_view input) {
    input = Trim(input);
    if (input.empty()) {
        return std::nullopt;
    }

    const auto [command_token, remainder] = SplitFirstToken(input);
    const auto command = ToLowerAscii(command_token);

    if (command == "quit" || command == "exit") {
        if (HasUnexpectedTrailingText(remainder)) {
            return std::nullopt;
        }

        return Command{CommandType::Quit, {}, {}};
    }

    if (command == "set") {
        const auto [key_token, value_remainder] = SplitFirstToken(remainder);
        if (key_token.empty()) {
            return std::nullopt;
        }

        const auto value = TrimLeft(value_remainder);
        if (value.empty()) {
            return std::nullopt;
        }

        return Command{CommandType::Set, std::string(key_token), std::string(value)};
    }

    if (command == "get" || command == "del" || command == "exists") {
        const auto [key_token, trailing] = SplitFirstToken(remainder);
        if (key_token.empty() || HasUnexpectedTrailingText(trailing)) {
            return std::nullopt;
        }

        if (command == "get") {
            return Command{CommandType::Get, std::string(key_token), {}};
        }
        if (command == "del") {
            return Command{CommandType::Del, std::string(key_token), {}};
        }
        return Command{CommandType::Exists, std::string(key_token), {}};
    }

    return std::nullopt;
}

}  // namespace kvstore::protocol
