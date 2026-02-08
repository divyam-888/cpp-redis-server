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
#include "ClientContext.hpp"
#include "SortedSet.hpp"

enum class ObjType {STRING, LIST, HASH, STREAM, ZSET};

using RedisList = std::deque<std::string>;
using Value = std::variant<std::string, RedisList, Stream, ZSet, long long>;
class KeyValueDatabase {
private:
    struct Entry {
        Value value;
        ObjType type;
        long long expiry_at = -1;
    };

    //Store info about sleeping clients waiting for list
    struct BlockingContextList {
        std::condition_variable_any cv; //The conditional variable to wake/notify the client
        bool is_fulfilled = false; //Updated by RPUSH/LPUSH when the list l is being updated and it finds a sleeping client blocking context in the list associated with blocking_map[l]
        std::string list; //Stores the name of list from which the client pops the value
        std::string item; //Stores the popped item
    };

    //Store info about sleeping clients waiting for stream
    struct BlockingStreamController {
        std::mutex lock;
        std::condition_variable_any cv;
        bool is_fulfilled = false;
    };

    struct BlockingStreamNode {
        StreamId threshold_id;
        BlockingStreamController* controller;
    };

    std::unordered_map<std::string, std::list<BlockingContextList*> > blocking_map; // stores for each list: Blocking Context of the clients waiting for it
    std::unordered_map<std::string, std::list<BlockingStreamNode> > blocking_stream_map; // stores for each stream key the blocking 
    std::unordered_map<std::string, Entry> map; // database which stores everything
    std::mutex stream_blocking_mutex; // mutex for blocking global stream map which contains list of waiters for each stream_key
    std::shared_mutex rw_lock; // Unlike std::mutex, which can be acquired only by one user, shared_mutex can be acquired by multiple users TO READ, it has to be uniquely acquired to WRITE

    long long current_time_ms();

public:
    void SET(const std::string& key, const std::string& value, bool acquire_lock, long long px_duration = -1);
    std::optional<std::string> GET(const std::string& key, bool acquire_lock);
    int RPUSH(std::string& list_key, std::vector<std::string>& items, bool acquire_lock); // Appends 'items' in the RedisList list at the back and returns the size of 'list'
    int LPUSH(std::string& list_key, std::vector<std::string>& items, bool acquire_lock); // Appends 'items' in the RedisList list at the front and returns the size of 'list'
    std::vector<std::string> LRANGE(std::string& list_key, int start, int end, bool acquire_lock); 
    int LLEN(std::string& list_key, bool acquire_lock);
    std::vector<std::string> LPOP(std::string& list_key, int num_remove_item, bool acquire_lock);
    std::optional<std::pair<std::string, std::string> > BLPOP(std::vector<std::string>& list_keys, double wait_time, bool acquire_lock);
    std::string TYPE(std::string& key, bool acquire_lock);
    StreamId XADD(std::string& stream_key, std::string& stream_id, std::vector<std::pair<std::string, std::string> >& fields, bool acquire_lock);
    std::vector<StreamEntry> XRANGE(std::string& stream_key, std::string& start, std::string& end, bool acquire_lock);
    std::vector<std::pair<std::string, std::vector<StreamEntry> > > XREAD(int count, bool block, int64_t ms, const std::vector<std::string>& keys, const std::vector<std::string>& ids_str, bool acquire_lock);
    std::optional<long long> INCR(std::string& key, bool acquire_lock);
    std::vector<std::string> EXEC(std::vector<QueuedCommand>& commandQueue, ClientContext& context, KeyValueDatabase& db, bool acquire_lock);
    std::vector<std::string> KEYS(std::string &pattern, bool acquire_lock);
    int ZADD(std::string& set_key, std::vector<std::string>& members, std::vector<double>& scores, bool acquire_lock);
    int ZRANK(std::string& set_key, std::string& member, bool acquire_lock);
    std::vector<std::string> ZRANGE(std::string& set_key, int start, int end, bool acquire_lock);
    int ZCARD(std::string& set_key, bool acquire_lock);
    double ZSCORE(std::string& set_key, std::string& member, bool acquire_lock);
    int ZREM(std::string& set_key, std::vector<std::string>& members, bool acquire_lock);
};

// Declare that a global instance named 'database' exists somewhere.
