#include "Config.hpp"
#include <stdexcept>
#include <vector>
#include <iostream>

std::shared_ptr<ServerConfig> parse_args(int argc, char** argv) {
    auto config = std::make_shared<ServerConfig>();
    std::vector<std::string> args(argv + 1, argv + argc);

    for(size_t i = 0; i < args.size(); i++) {
        if(args[i] == "--port" && i + 1 < args.size()) {
            config->port = std::stoi(args[++i]);  
        } else if(args[i] == "--replicaof" && i + 1 < args.size()) {
            config->role = "slave";
            int pos = args[++i].find(' ');
            config->master_host = args[i].substr(0, pos);
            config->master_port = std::stoi(args[i].substr(pos + 1, 4));
        }
    }

    return config;
}