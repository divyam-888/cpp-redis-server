#pragma once
#include <mutex>
#include "Command.hpp"
#include "CommandRegistry.hpp"


struct QueuedCommand {
    Command* cmd;
    std::vector<std::string> args;
};

class ClientContext {
public:
    int client_fd;
    int replica_index;
    int num_channel;
    bool in_subscribe_mode;
    bool is_replica;
    bool in_transaction;
    bool transaction_failed;
    std::mutex client_mtx;
    std::vector<QueuedCommand> commandQueue;

    ClientContext(int fd) : client_fd(fd) {
        in_transaction = false;
        transaction_failed = false;
        is_replica = false;
        in_subscribe_mode = false;
        num_channel = 0;
    }

    void reset_transaction() {
        in_transaction = false;
        commandQueue.clear();
    }
};