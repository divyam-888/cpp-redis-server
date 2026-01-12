#pragma once
#include "Command.hpp"
#include "CommandRegistry.hpp"

struct QueuedCommand {
    Command* cmd;
    std::vector<std::string> args;
};

class ClientContext {
public:
    int client_fd;
    bool is_replica;
    bool in_transaction;
    bool transaction_failed;
    std::vector<QueuedCommand> commandQueue;

    ClientContext(int fd) : client_fd(fd) {
        in_transaction = false;
        transaction_failed = false;
        is_replica = false;
    }

    void reset_transaction() {
        in_transaction = false;
        commandQueue.clear();
    }
};