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

class ReplicationManager {
private:
    std::shared_ptr<ServerConfig> config;
    KeyValueDatabase& db;

    int connectToMaster(const std::string& host, int port_number) {
        int sock = socket(AF_INET, SOCK_STREAM, 0);
        if (sock < 0) {
            return -1;
        }

        struct sockaddr_in master_addr;
        std::memset(&master_addr, 0, sizeof(master_addr));
        master_addr.sin_family = AF_INET;
        master_addr.sin_port = htons(port_number);

        // Convert IP address from text to binary
        if (inet_pton(AF_INET, host.c_str(), &master_addr.sin_addr) <= 0) {
            std::cout << "Tried to connect to " << host << " at port: " << port_number << std::endl;
            std::cerr << "Invalid master address/ Address not supported" << std::endl;
            close(sock);
            return -1;
        }

        // CONNECT is the active part where we reach out
        if (connect(sock, (struct sockaddr *)&master_addr, sizeof(master_addr)) < 0) {
            std::cerr << "Connection to Master Failed" << std::endl;
            close(sock);
            return -1;
        }

        return sock; 
    }

    void sendAndExpect(int fd, const std::string& cmd, const std::string& expected_response) {
        if (send(fd, cmd.c_str(), cmd.length(), 0) < 0) {
            throw std::runtime_error("Failed to send command to master: " + cmd);
        }

        // wait for the response from master
        char buffer[1024];
        ssize_t bytes_received = recv(fd, buffer, sizeof(buffer) - 1, 0);
    
        if (bytes_received <= 0) {
            throw std::runtime_error("Connection closed by master during handshake");
        }

        buffer[bytes_received] = '\0';
        std::string response(buffer);

        // validate if the reponse is correct
        if (!expected_response.empty() && response.find(expected_response) != 0) {
            throw std::runtime_error("Unexpected master response. Expected prefix: " + 
                                     expected_response + ", Received: " + response);
        }
    
        std::cout << "[REPL] Master replied: " << response << std::endl;

    }

    void processMasterStream(int fd) {

    }

public:
    ReplicationManager(std::shared_ptr<ServerConfig> cfg, KeyValueDatabase& db_) : config(cfg), db(db_) {}

    void startHandshake() {
        int master_fd = connectToMaster(config->master_host, config->master_port);

        if(master_fd < 0) {
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