#pragma once
#include <sys/socket.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <cstring>
#include <string>
#include <iostream>
#include "RESPReader.hpp"
#include "KVStore.hpp" 
#include "RESPParser.hpp"
#include "Command.hpp"
#include "CommandRegistry.hpp"
#include "PingEchoCommand.hpp"
#include "GetSetCommand.hpp"
#include "ListCommands.hpp"
#include "ClientContext.hpp"
#include "Config.hpp"

std::vector<std::string> extractArgs(RESPValue &input) {
  if (input.type != RESPType::ARRAY || input.array.empty()) {
    return {};
  }
  std::vector<std::string> args;
  args.reserve(input.array.size()); // pre allocate memory for optimization

  for(const auto& item : input.array) {
    if (item.type == RESPType::ARRAY) {
      // throw exception if nested arrays are given
      throw std::runtime_error("Nested arrays are not supported in commands");
      // or return empty to signal failure
      // return {}; 
    }
    args.push_back(item.value);
  }
  return args;
}

class ReplicationManager
{
private:
    std::shared_ptr<ServerConfig> config;
    KeyValueDatabase &db;
    CommandRegistry &registry;
    RESPReader reader;

    int connectToMaster(const std::string &host, int port_number)
    {
        struct addrinfo hints, *res, *p;
        std::memset(&hints, 0, sizeof(hints));
        hints.ai_family = AF_INET;       // Use AF_UNSPEC to support IPv4/IPv6
        hints.ai_socktype = SOCK_STREAM; // TCP

        std::string port_str = std::to_string(port_number);

        // getaddrinfo resolves "localhost" to 127.0.0.1 automatically
        int status = getaddrinfo(host.c_str(), port_str.c_str(), &hints, &res);
        if (status != 0)
        {
            std::cerr << "getaddrinfo error: " << gai_strerror(status) << std::endl;
            return -1;
        }

        int sock = -1;
        // Iterate through the results and connect to the first one that works
        for (p = res; p != nullptr; p = p->ai_next)
        {
            sock = socket(p->ai_family, p->ai_socktype, p->ai_protocol);
            if (sock < 0)
                continue;

            if (connect(sock, p->ai_addr, p->ai_addrlen) == 0)
            {
                break; // Success!
            }

            close(sock);
            sock = -1;
        }

        freeaddrinfo(res); // Free the linked list allocated by getaddrinfo

        if (sock == -1)
        {
            std::cerr << "ReplicationManager: Failed to connect to " << host << ":" << port_number << std::endl;
        }
        return sock;
    }

    void sendAndExpect(int fd, const std::string &cmd, const std::string &expected_response)
    {
        if (send(fd, cmd.c_str(), cmd.length(), 0) < 0)
        {
            throw std::runtime_error("Failed to send command to master: " + cmd);
        }

        std::string response = reader.read_next_message();

        std::cout << "[REPL] Master replied: " << response << std::endl;
    }

    void processMasterStream(int master_fd)
    {
        std::string rdb_data = reader.read_rdb();
        std::cout << "[REPL] Received RDB of size: " << rdb_data.size() << std::endl;

        ClientContext dummy_context(-1);

        while (true) {
            std::string raw_cmd = reader.read_next_message();
            if (raw_cmd.empty()) break; 

            RESPParser parser(raw_cmd);
            RESPValue input = parser.parse();

            std::vector<std::string> args = extractArgs(input);
    
            std::string cmdName = args[0];
            Command* cmd = registry.getCommand(cmdName);

            std::string response;

            if (!cmd) {
              response = "-ERR unknown command\r\n";
            } else if (args.size() < cmd->min_args()) {
              response = "-ERR wrong number of arguments\r\n";
            } else {
              if(dummy_context.in_transaction && cmd->name() != "EXEC" && cmd->name() != "DISCARD") {
                response = "+QUEUED\r\n";
                dummy_context.commandQueue.push_back({cmd, std::move(args)});
              } else {
                response = cmd->execute(dummy_context, args, db, true);
              }
            }

            std::cout << "[REPL] Executed propagated command: " << cmdName << std::endl;
        }
    }

public:
    ReplicationManager(std::shared_ptr<ServerConfig> cfg, KeyValueDatabase &db_, CommandRegistry &registry_) : config(cfg), 
                                                                                                        db(db_), registry(registry_) {}

    void startHandshake()
    {
        int master_fd = connectToMaster(config->master_host, config->master_port);

        if (master_fd < 0)
        {
            std::cout << "Failed to get master's file descriptor\n";
            return;
        }

        reader.set_fd(master_fd);

        sendAndExpect(master_fd, "*1\r\n$4\r\nPING\r\n", "+PONG\r\n");

        std::string port_cmd = "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$" +
                               std::to_string(std::to_string(config->port).length()) +
                               "\r\n" + std::to_string(config->port) + "\r\n";
        sendAndExpect(master_fd, port_cmd, "+OK\r\n");

        sendAndExpect(master_fd, "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n", "+OK\r\n");

        // send PSYNC
        // initial sync request: PSYNC ? -1
        sendAndExpect(master_fd, "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n", "+FULLRESYNC\r\n");

        // After PSYNC, the master will send an RDB file and then a stream of commands.
        // This thread should now enter a loop to process those commands.
        processMasterStream(master_fd);
    }
};