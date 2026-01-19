#include "Config.hpp"
#include <stdexcept>
#include <vector>
#include <iostream>

std::shared_ptr<ServerConfig> parse_args(int argc, char** argv) {
    // just simply initialising a shared_ptr gives nullptr we need to use make_shared
    auto config = std::make_shared<ServerConfig>();
    std::vector<std::string> args(argv + 1, argv + argc);

    for(size_t i = 0; i < args.size(); i++) {
        if(args[i] == "--port" && i + 1 < args.size()) {
            config->port = std::stoi(args[++i]);  
        } else if (args[i] == "--replicaof" && i + 1 < args.size()) {
            config->role = "slave";
            std::string value = args[++i]; // "localhost 6379"
            size_t space_pos = value.find(' ');
            if (space_pos != std::string::npos) {
                config->master_host = value.substr(0, space_pos);
                config->master_port = std::stoi(value.substr(space_pos + 1));
            }
        }
    }

    return config;
}