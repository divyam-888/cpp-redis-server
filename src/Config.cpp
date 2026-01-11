#include "Config.hpp"
#include <stdexcept>
#include <vector>
#include <iostream>

ServerConfig parse_args(int argc, char** argv) {
    ServerConfig config;
    std::vector<std::string> args(argv + 1, argv + argc);

    for(size_t i = 0; i < args.size(); i++) {
        if(args[i] == "--port" && i + 1 < args.size()) {
            std::cout << "It not is a slave nigga: " << config.role << " " << config.master_host << " " << config.master_port << '\n';
            config.port = std::stoi(args[++i]);  
        } else if(args[i] == "--replicaof" && i + 2 < args.size()) {
            config.role = "slave";
            config.master_host = args[++i];
            config.master_port = std::stoi(args[++i]);
            std::cout << "It is a slave nigga: " << config.role << " " << config.master_host << " " << config.master_port << '\n';
        }
    }

    return config;
}