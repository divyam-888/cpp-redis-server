#pragma once
#include <string>
#include <unordered_map>
#include "Command.hpp"


class CommandRegistry {
private:
    // lookup table, map<command_name, ptr to class which inherits from Command class>
    std::unordered_map<std::string, std::unique_ptr<Command>> command_map;

public:
    // Call this once in main() to load all commands
    void registerCommand(std::unique_ptr<Command> cmd) {
        // Store command name in uppercase for case-insensitive lookup
        std::string name = cmd->name(); 
        std::transform(name.begin(), name.end(), name.begin(), ::toupper);
        command_map[name] = std::move(cmd);
    }

    // The new "process_command" logic
    Command* getCommand(const std::string& commandName) {
        std::transform(commandName.begin(), commandName.end(), commandName.begin(), ::toupper);
        auto it = command_map.find(commandName);
        if (it != command_map.end()) {
            return it->second.get();
        }
        return nullptr; // Command not found
    }
};