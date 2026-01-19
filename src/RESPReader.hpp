#pragma once
#include <string>
#include <sys/socket.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <iostream>

class RESPReader {
private:
    int fd;
    std::string buffer;

    bool ensure_capacity(size_t n) {
        while (buffer.size() < n) {
            char temp[4096];
            ssize_t bytes = recv(fd, temp, sizeof(temp), 0);
            if (bytes <= 0) {
                std::cout << "Not enough bytes: <" << n << std::endl;
                return false;
            }
            buffer.append(temp, bytes);
        }
        return true;
    }

    std::string read_line() {
        size_t pos;
        while ((pos = buffer.find("\r\n")) == std::string::npos) {
            if (!ensure_capacity(buffer.size() + 1)) return "";
        }
        std::string line = buffer.substr(0, pos + 2);
        buffer.erase(0, pos + 2);
        return line;
    }

public:
    RESPReader() {}

    RESPReader(int file_descriptor) : fd(file_descriptor) {}

    void set_fd(int fd_) {
        fd = fd_;
    }

    std::string read_next_message() {
        if (!ensure_capacity(1)) return "";

        char type = buffer[0];
        if (type == '+' || type == '-' || type == ':') {
            return read_line(); // Simple types are just one line
        } 
        
        if (type == '*') {
            // It's an array. We need to parse the count to know how much to read.
            std::string header = read_line(); 
            int count = std::stoi(header.substr(1, header.size() - 3));
            
            std::string full_array = header;
            for (int i = 0; i < count; ++i) {
                full_array += read_next_message(); 
            }
            return full_array;
        }

        if (type == '$') {
            // Bulk string: $<len>\r\n<data>\r\n
            std::string header = read_line();
            int len = std::stoi(header.substr(1, header.size() - 3));
            if (len == -1) return header; // Null bulk string

            size_t total_needed = len + 2; // data + \r\n
            ensure_capacity(total_needed);
            std::string data = buffer.substr(0, total_needed);
            buffer.erase(0, total_needed);
            return header + data;
        }
        return "";
    }

    // SPECIAL CASE: The RDB File
    std::string read_rdb() {
        // RDB follows the format: $<len>\r\n<raw_bytes> (No trailing \r\n)
        if (!ensure_capacity(1) || buffer[0] != '$') return "";
        
        std::string header = read_line();
        int len = std::stoi(header.substr(1, header.size() - 3));
        
        ensure_capacity(len);
        std::string rdb_data = buffer.substr(0, len);
        buffer.erase(0, len);
        return rdb_data;
    }
};