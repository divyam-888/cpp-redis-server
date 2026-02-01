#pragma once
#include <sys/socket.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <cstring>
#include <string>
#include <map>
#include <vector>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>

struct Subscriber {
    int fd;
    std::mutex* mtx; 

    bool operator==(const Subscriber& other) const { return fd == other.fd; }
};

struct SubscriberHasher {
    size_t operator()(const Subscriber& s) const { return std::hash<int>{}(s.fd); }
};

class PubSubManager {
private:
    std::unordered_map<std::string, std::unordered_set<Subscriber, SubscriberHasher>> channels;
    std::shared_mutex channel_mtx;

    std::string format_pub_message(const std::string& channel, const std::string& message) {
        std::string format_message = "*3\r\n$7\r\nmessage\r\n" + (std::string)"$" + std::to_string(channel.length()) + "\r\n" + channel + "\r\n" + 
                                    "$" + std::to_string(message.length()) + "\r\n" + message + "\r\n"; 
        return format_message;
    }

public:
    void subscribe(const std::string& channel, int fd, std::mutex& client_mtx) {
        std::unique_lock<std::shared_mutex> lock(channel_mtx);
        channels[channel].insert({fd, &client_mtx});
    }

    void unsubscribe(const std::string& channel, int fd) {
        std::unique_lock<std::shared_mutex> lock(channel_mtx);
        if (channels.count(channel)) {
            channels[channel].erase({fd, nullptr}); 
            if (channels[channel].empty()) channels.erase(channel);
        }
    }

    int publish(const std::string& channel, const std::string& message) {
        std::shared_lock<std::shared_mutex> lock(channel_mtx);

        auto it = channels.find(channel);
        if (it == channels.end()) return 0;

        std::string formatted = format_pub_message(channel, message);
        int count = 0;
        
        for (const auto& sub : it->second) {
            std::unique_lock<std::mutex> write_lock(*(sub.mtx));
            send(sub.fd, formatted.data(), formatted.length(), MSG_NOSIGNAL);
            count++;
        }
        return count;
    }
};