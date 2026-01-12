#pragma once
#include <string>
#include <vector>
#include <mutex>

struct ServerConfig {
    int port = 6379;
    std::string role = "master";

    std::string master_host = "";
    int master_port = 0;

    std::string master_replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"; // generate a pseudo-random alphanumeric string of 40 characters
    int master_repl_offset = 0;

    std::vector<int> replicas;
    std::mutex replica_mutex;
};

ServerConfig parse_args(int argc, char** argv);

