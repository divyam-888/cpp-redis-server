#pragma once
#include <sys/socket.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <cstring>
#include <string>
#include <iostream>
#include "Config.hpp"
#include "KVStore.hpp"

class ReplicationManager
{
private:
    std::shared_ptr<ServerConfig> config;
    KeyValueDatabase &db;

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

        // wait for the response from master
        char buffer[1024];
        ssize_t bytes_received = recv(fd, buffer, sizeof(buffer) - 1, 0);

        if (bytes_received <= 0)
        {
            throw std::runtime_error("Connection closed by master during handshake");
        }

        buffer[bytes_received] = '\0';
        std::string response(buffer);

        // validate if the reponse is correct
        // if (!expected_response.empty() && response.find(expected_response) != 0)
        // {
        //     throw std::runtime_error("Unexpected master response. Expected prefix: " +
        //                              expected_response + ", Received: " + response);
        // }

        std::cout << "[REPL] Master replied: " << response << std::endl;
    }

    void processMasterStream(int fd)
    {
    }

public:
    ReplicationManager(std::shared_ptr<ServerConfig> cfg, KeyValueDatabase &db_) : config(cfg), db(db_) {}

    void startHandshake()
    {
        int master_fd = connectToMaster(config->master_host, config->master_port);

        if (master_fd < 0)
        {
            std::cout << "Failed to get master's file descriptor\n";
            return;
        }

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