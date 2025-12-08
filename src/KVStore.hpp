#pragma once
#include <string>
#include <unordered_map>
#include <shared_mutex>
#include <mutex>
#include <optional>
#include <chrono>

class KeyValueDatabase {
private:
    struct Entry {
        std::string value;
        long long expiry_at = -1;
    };

    std::unordered_map<std::string, Entry> map;
    std::shared_mutex rw_lock; // Unlike std::mutex, which can be acquired only by one user, shared_mutex can be acquired by multiple users TO READ, it has to be uniquely acquired to WRITE

    long long current_time_ms();

public:
    void set(const std::string& key, const std::string& value, long long px_duration = -1);
    std::optional<std::string> get(const std::string& key);
};

// Declare that a global instance named 'database' exists somewhere.
extern KeyValueDatabase database;