#pragma once
#include <iostream>
#include <string>
#include <unordered_map>
#include <shared_mutex>
#include <mutex>
#include <optional>
#include <chrono>
#include <variant>
#include <deque>
#include <vector>
#include <list> 
#include <condition_variable>
#include "Stream.hpp"

enum class ObjType {STRING, LIST, HASH, STREAM};


using RedisList = std::deque<std::string>;
using Value = std::variant<std::string, RedisList, Stream>;
class KeyValueDatabase {
private:
    struct Entry {
        Value value;
        ObjType type;
        long long expiry_at = -1;
    };

    //Store info about sleeping clients
    struct BlockingContext {
        std::condition_variable_any cv; //The conditional variable to wake/notify the client
        bool is_fulfilled = false; //Updated by RPUSH/LPUSH when the list l is being updated and it finds a sleeping client blocking context in the list associated with blocking_map[l]
        std::string list; //Stores the name of list from which the client pops the value
        std::string item; //Stores the popped item
    };

    std::unordered_map<std::string, std::list<BlockingContext*> > blocking_map; // stores for each list: Blocking Context of the clients waiting for it
    std::unordered_map<std::string, Entry> map;
    std::shared_mutex rw_lock; // Unlike std::mutex, which can be acquired only by one user, shared_mutex can be acquired by multiple users TO READ, it has to be uniquely acquired to WRITE

    long long current_time_ms();

public:
    void SET(const std::string& key, const std::string& value, long long px_duration = -1);
    std::optional<std::string> GET(const std::string& key);
    int RPUSH(std::string& list_key, std::vector<std::string>& items); // Appends 'items' in the RedisList list at the back and returns the size of 'list'
    int LPUSH(std::string& list_key, std::vector<std::string>& items); // Appends 'items' in the RedisList list at the front and returns the size of 'list'
    std::vector<std::string> LRANGE(std::string& list_key, int start, int end); 
    int LLEN(std::string& list_key);
    std::vector<std::string> LPOP(std::string& list_key, int num_remove_item);
    std::optional<std::pair<std::string, std::string> > BLPOP(std::vector<std::string>& list_keys, double wait_time);
    std::string TYPE(std::string& key);
    StreamId XADD(std::string& stream_key, std::string& stream_id, std::vector<std::pair<std::string, std::string> >& fields);
};

// Declare that a global instance named 'database' exists somewhere.
