#include "KVStore.hpp"
#include "Stream.hpp"
#include "Command.hpp"
#include "ClientContext.hpp"


KeyValueDatabase db;

long long KeyValueDatabase::current_time_ms()
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

void KeyValueDatabase::SET(const std::string &key, const std::string &value, bool acquire_lock, long long px_duration)
{
    std::unique_lock<std::shared_mutex> db_lock(rw_lock, std::defer_lock); // we use unique_lock to acquire the mutex EXCLUSIVELY as we are WRITING

    if(acquire_lock) {
        db_lock.lock();
    }
    
    long long expiry = -1; // -1 means it has expiry as INF
    if (px_duration > 0)
    {
        expiry = current_time_ms() + px_duration;
    }
    Value myVar = value;
    map[key] = {myVar, ObjType::STRING, expiry};
}

std::optional<std::string> KeyValueDatabase::GET(const std::string &key, bool acquire_lock)
{
    std::unique_lock<std::shared_mutex> db_lock(rw_lock, std::defer_lock); // we use unique lock because we might delete/modify if the key is expired.

    if(acquire_lock) {
        db_lock.lock();
    }

    auto it = map.find(key);
    if (it == map.end() || it->second.type != ObjType::STRING)
    {
        return std::nullopt; // key not found, so we return null optional
    }
    if (it->second.expiry_at != -1 && it->second.expiry_at < current_time_ms())
    {
        map.erase(it); // key exists but has expired
        return std::nullopt;
    }

    if(std::holds_alternative<long long>(it->second.value)) {
        long long& val = std::get<long long>(it->second.value);
        return std::to_string(val);
    }
    
    return get<std::string>(it->second.value);
}

int KeyValueDatabase::RPUSH(std::string &list_key, std::vector<std::string> &items, bool acquire_lock) {
    std::unique_lock<std::shared_mutex> db_lock(rw_lock, std::defer_lock); 

    if(acquire_lock) {
        db_lock.lock();
    }

    auto it = map.find(list_key);

    if(it != map.end() && it->second.type != ObjType::LIST) return -1;

    if(it == map.end()) {
        RedisList dq;
        map[list_key] = {Value(dq), ObjType::LIST, -1};
        it = map.find(list_key);
    } 

    RedisList& dq = get<RedisList>(it->second.value);
    std::list<BlockingContextList*>& waiters = blocking_map[list_key];

    //#items which were to be added but weren't as they were popped by blpop
    int handed_off_count = 0;

    for(auto item : items) {
        if(!waiters.empty()) {
            BlockingContextList* ctx = waiters.front();
            waiters.pop_front();
            ctx->is_fulfilled = true;
            ctx->list = list_key;
            ctx->item = item;
            ctx->cv.notify_one();
            handed_off_count++;
        } else {
            dq.push_back(item);
        }
    }

    if (dq.empty()) {
        map.erase(it);
    }

    return dq.size() + handed_off_count;
}

int KeyValueDatabase::LPUSH(std::string &list_key, std::vector<std::string> &items, bool acquire_lock) {
    std::unique_lock<std::shared_mutex> db_lock(rw_lock, std::defer_lock); 

    if(acquire_lock) {
        db_lock.lock();
    }

    auto it = map.find(list_key);

    if(it != map.end() && it->second.type != ObjType::LIST) return -1;

    if(it == map.end()) {
        RedisList dq;
        map[list_key] = {Value(dq), ObjType::LIST, -1};
        it = map.find(list_key);
    } 

    RedisList& dq = get<RedisList>(it->second.value);
    std::list<BlockingContextList*>& waiters = blocking_map[list_key];

    int handed_off_count = 0;

    for(auto item : items) {
        if(!waiters.empty()) {
            BlockingContextList* ctx = waiters.front();
            waiters.pop_front();
            ctx->is_fulfilled = true;
            ctx->list = list_key;
            ctx->item = item;
            ctx->cv.notify_one();
            handed_off_count++;
        } else {
            dq.push_front(item);
        }
    }

    if (dq.empty()) {
        map.erase(it);
    }

    return dq.size() + handed_off_count;
}

std::vector<std::string> KeyValueDatabase::LRANGE(std::string &list_key, int start, int end, bool acquire_lock) {
    std::shared_lock<std::shared_mutex> db_lock(rw_lock, std::defer_lock); 

    if(acquire_lock) {
        db_lock.lock();
    }

    auto it = map.find(list_key);
    std::vector<std::string> items;

    if(it != map.end() && it->second.type == ObjType::LIST) {
        RedisList& dq = get<RedisList>(it->second.value);
        if(end < 0) end += dq.size();
        if(start < 0) start += dq.size();
        for(size_t i = std::max(0, start); i <= std::min(end, (int)(dq.size() - 1)); i++) items.push_back(dq[i]);
    }

    return items;
} 

int KeyValueDatabase::LLEN(std::string &list_key, bool acquire_lock) {
    std::shared_lock<std::shared_mutex> db_lock(rw_lock, std::defer_lock); 

    if(acquire_lock) {
        db_lock.lock();
    }

    auto it = map.find(list_key);
    int size = 0;
    if(it != map.end() && it->second.type == ObjType::LIST) {
        RedisList& dq = get<RedisList>(it->second.value);
        size = dq.size();
    }
    return size;
}

std::vector<std::string> KeyValueDatabase::LPOP(std::string &list_key, int num_remove_item, bool acquire_lock) {
    std::unique_lock<std::shared_mutex> db_lock(rw_lock, std::defer_lock); 

    if(acquire_lock) {
        db_lock.lock();
    }

    auto it = map.find(list_key);
    std::vector<std::string> removed_items;

    if(it != map.end() && it->second.type == ObjType::LIST) {
        RedisList& dq = get<RedisList>(it->second.value);
        num_remove_item = std::min(num_remove_item, (int)dq.size());
        for(int i = 0; i < num_remove_item; i++) {
            removed_items.push_back(dq.front());
            dq.pop_front();
        }

        if(dq.empty()) {
            map.erase(it);
        }
    }

    return removed_items;
}

std::optional<std::pair<std::string, std::string> > KeyValueDatabase::BLPOP(std::vector<std::string>& list_keys, double wait_time, bool acquire_lock) {
    std::unique_lock<std::shared_mutex> db_lock(rw_lock, std::defer_lock); 

    if(acquire_lock) {
        db_lock.lock();
    }
    
    // Check if any list is non-empty
    for(std::string& key : list_keys) {
        auto it = map.find(key);
        if(it == map.end() || it->second.type != ObjType::LIST) continue;
        RedisList& dq = std::get<RedisList>(it->second.value);
        if(!dq.empty()) {
            std::string item = dq.front();
            dq.pop_front();

            std::string key_list = key;
            if(dq.empty()) {
                map.erase(it);
            }

            return {{key_list, item}};
        }
    }

    //No non-empty list, block this thread and let it sleep
    BlockingContextList ctx;

    for(std::string& key : list_keys) {
        blocking_map[key].push_back(&ctx);
    }

    if(wait_time > 0.00001) {
        // wait till wait_time or till condition is satisfied
        ctx.cv.wait_for(db_lock, std::chrono::duration<double>(wait_time), [&]{return ctx.is_fulfilled;});
    } else {
        // wait forever till the condition is satisfied
        ctx.cv.wait(db_lock, [&]{ return ctx.is_fulfilled; });
    }

    // remove this blocking context from all the lists that we added it to earlier
    for(std::string& key : list_keys) {
        auto it = blocking_map.find(key);
        if (it != blocking_map.end()) {
            it->second.remove(&ctx);
            if(it->second.empty()) {
                blocking_map.erase(it);
            }
        }
    }

    if(ctx.is_fulfilled) {
        return make_pair(ctx.list, ctx.item);
    }

    return std::nullopt;
}

std::string KeyValueDatabase::TYPE(std::string& key, bool acquire_lock) {
    std::shared_lock<std::shared_mutex> db_lock(rw_lock, std::defer_lock); 

    if(acquire_lock) {
        db_lock.lock();
    }
    
    auto it = map.find(key);
    if(it == map.end()) {
        return "none";
    }

    switch(it->second.type) {
        case ObjType::STRING: return "string";
        case ObjType::HASH: return "hash";
        case ObjType::LIST: return "list";
        case ObjType::STREAM: return "stream";

        default: return "none";
    }
}

StreamId KeyValueDatabase::XADD(std::string& stream_key, std::string& stream_id, std::vector<std::pair<std::string, std::string> >& fields, bool acquire_lock) {
    std::unique_lock<std::shared_mutex> db_lock(rw_lock, std::defer_lock); 

    if(acquire_lock) {
        db_lock.lock();
    }

    auto it = map.find(stream_key);
    if(it == map.end()) {
        map[stream_key] = {Value(Stream()), ObjType::STREAM, -1};
        it = map.find(stream_key);
    } else if(it->second.type != ObjType::STREAM) {
        return {-1, 0};
    }
    
    Stream& stream = std::get<Stream>(it->second.value);
    
    StreamId id_param;
    
    if (stream_id == "*") {
        id_param = {-1, -1}; // auto generate time and sequence part
    } else if (stream_id.back() == '*') {
        // auto generate sequence part
        size_t dash = stream_id.find('-');
        if (dash == std::string::npos) return {0, -1}; // Format error
        id_param.ms = std::stoll(stream_id.substr(0, dash));
        id_param.seq = -1; 
    } else {
        size_t dash = stream_id.find('-');
        if (dash == std::string::npos) {
            try {
                id_param.ms = std::stoll(stream_id);
                id_param.seq = 0;
            } catch (...) { return {0, -1}; }
        } else {
            try {
                id_param.ms = std::stoll(stream_id.substr(0, dash));
                id_param.seq = std::stoll(stream_id.substr(dash + 1));
            } catch (...) { return {0, -1}; }
        }
    }

    StreamId new_id = stream.add(id_param, fields);

    /* As there are other stream as well which will try to update blocking stream context map we need to acquire lock for it. Though in 
    current implementation we have already acquired UNIQUE db_lock first to reach here (either on our own or through EXEC) and still hold it
    we have already assured concurrency but ideally we should have unlocked db and let others have it since we no longer want to update map 
    and acquire this lock instead */

    //check is some client is waiting for this stream entry
    std::lock_guard<std::mutex> stream_lock(stream_blocking_mutex);
    if(blocking_stream_map.count(stream_key)) {
        std::list<BlockingStreamNode>& waiters = blocking_stream_map[stream_key];
        for(auto& node : waiters) {
            if(node.threshold_id < new_id) {
                //There can be multiple streams waiting to notify this client so we first acquire lock to the clients block controller
                std::lock_guard<std::mutex> client_lock(node.controller->lock);
                node.controller->is_fulfilled = true;
                node.controller->cv.notify_one();
            }
        }
    }

    return new_id;
}

std::vector<StreamEntry> KeyValueDatabase::XRANGE(std::string& stream_key, std::string& start, std::string& end, bool acquire_lock) {
    StreamId startId, endId;
    try {
        startId = StreamId::parse(start, false); 
        endId = StreamId::parse(end, true);   
    } catch (...) {
        // rethrow so wrapper handles the error response
        throw; 
    }
    
    std::shared_lock<std::shared_mutex> db_lock(rw_lock, std::defer_lock); 

    if(acquire_lock) {
        db_lock.lock();
    }
    
    auto it = map.find(stream_key);
    if(it == map.end()) {
        return {}; // key doesn't exist so we return empty range
    } 
    if(it->second.type != ObjType::STREAM) {
        throw std::runtime_error("WRONGTYPE");
    }

    Stream& stream = std::get<Stream>(it->second.value);
    return stream.range(startId, endId);
}

std::vector<std::pair<std::string, std::vector<StreamEntry>>> KeyValueDatabase::XREAD(int count, bool block, int64_t ms, const std::vector<std::string>& keys, const std::vector<std::string>& ids_str, bool acquire_lock) { 
    // we need to resolve '$' and also store them incase for retry later 
    std::vector<std::string> resolved_ids_str = ids_str; 
    std::vector<StreamId> threshold_ids;

    //scope for read lock
    {
        std::shared_lock<std::shared_mutex> db_lock(rw_lock, std::defer_lock); 

        if(acquire_lock) {
            db_lock.lock();
        }
        
        for(size_t i = 0; i < keys.size(); i++) {
            // resolve $
            if (ids_str[i] == "$") {
                auto it = map.find(keys[i]);
                if (it != map.end() && it->second.type == ObjType::STREAM) {
                    Stream& stream = std::get<Stream>(it->second.value);
                    resolved_ids_str[i] = stream.last_id.toString(); 
                } else {
                    resolved_ids_str[i] = "0-0"; 
                }
            }
            threshold_ids.push_back(StreamId::parse(resolved_ids_str[i], false));
        }

        std::vector<std::pair<std::string, std::vector<StreamEntry>>> response;
        for(size_t i = 0; i < keys.size(); i++) {
            auto it = map.find(keys[i]);
            if(it == map.end() || it->second.type != ObjType::STREAM) continue;

            Stream& stream = std::get<Stream>(it->second.value);
            std::vector<StreamEntry> new_entries = stream.read(count, ms, threshold_ids[i]);

            if(!new_entries.empty()) {
                response.push_back({keys[i], std::move(new_entries)});
            }
        }

        if(!response.empty() || !block) {
            return response;
        }
        
    } // db locks goes out of scope, and is released
    

    /* Unlike BLPOP here for each key we have a unique parameter corresponding to each stream kay: threshold stream_id, so we need a different blocking context for each 
    instead of just creating a separate context with its own cv and fulfilled bool we create a controller which contains cv and bool as
    they are common for each key. For the unique parameter we create a new struct which hold the threshold id and a pointer to this controller struct
    (which contains common info). Since there are multiple streams which may try to update the controller we also need a mutex lock for it */
    
    BlockingStreamController controller;
    std::vector<std::pair<std::string, std::list<BlockingStreamNode>::iterator>> my_iterators;

    {
        std::lock_guard<std::mutex> map_lock(stream_blocking_mutex); // lock stream blocking map
        
        for(size_t i = 0; i < keys.size(); i++) {
            BlockingStreamNode node;
            node.threshold_id = threshold_ids[i]; // use resolved ids (removed $)
            node.controller = &controller;
            
            blocking_stream_map[keys[i]].push_back(node);

            //storing iterator to clean the blocking list later for this key
            auto it = std::prev(blocking_stream_map[keys[i]].end());
            my_iterators.push_back({keys[i], it});
        }
    }

    // sleep untill timeout or woke up by another thread. no DB locks are held here. safe to sleep.
    std::unique_lock<std::mutex> thread_lock(controller.lock);
    if(ms > 0) {
        controller.cv.wait_for(thread_lock, std::chrono::milliseconds(ms), 
                               [&]{ return controller.is_fulfilled; });
    } else {
        controller.cv.wait(thread_lock, [&]{ return controller.is_fulfilled; });
    }

    //clean the stream blocking map to remove the current thread from blocking list of all stream keys
    {
        std::lock_guard<std::mutex> map_lock(stream_blocking_mutex);
        for (auto& [key, it] : my_iterators) {
            if (blocking_stream_map.count(key)) {
                blocking_stream_map[key].erase(it);
                if (blocking_stream_map[key].empty()) {
                    blocking_stream_map.erase(key);
                }
            }
        }
    }

    // we need to pass resolved ids, if we pass ids_str (containing $), it will resolve to the new id and miss the data.
    return XREAD(count, false, 0, keys, resolved_ids_str, acquire_lock);
}

std::optional<long long> KeyValueDatabase::INCR(std::string& key, bool acquire_lock) {
    std::unique_lock<std::shared_mutex> db_lock(rw_lock, std::defer_lock);

    if(acquire_lock) {
        db_lock.lock();
    }

    auto it = map.find(key);
    
    if(it == map.end()) {
        map[key] = {1LL, ObjType::STRING, -1};
        return 1;
    } 

    Entry& obj = it->second;

    if(obj.type != ObjType::STRING) {
        return std::nullopt;
    }

    try {
        if(std::holds_alternative<long long>(obj.value)) {
            long long& val = std::get<long long>(obj.value);
            if (val == LLONG_MAX) throw std::out_of_range("overflow");
            return ++val;
        } else {
            std::string& str_val = std::get<std::string>(obj.value);
            long long val = std::stoll(str_val);
            val++;
            obj.value = val;
            return val;
        }
    } catch(...) {
        throw std::invalid_argument("value is not an integer");
    }
}

std::vector<std::string> KeyValueDatabase::EXEC(std::vector<QueuedCommand>& commandQueue, ClientContext& context, KeyValueDatabase& db, bool acquire_lock) {
    std::unique_lock<std::shared_mutex> db_lock(rw_lock);
    std::vector<std::string> results;

    for(auto& queued : commandQueue) {
        results.push_back(queued.cmd->execute(context, queued.args, db, false));
    }

    return results;
};

std::vector<std::string> KeyValueDatabase::KEYS(std::string& pattern, bool acquire_lock) {
    std::shared_lock<std::shared_mutex> db_lock(rw_lock, std::defer_lock);
    
    if(acquire_lock) {
        db_lock.lock();
    }

    std::vector<std::string> results;

    long long now = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count();

    for (auto it = map.begin(); it != map.end(); ) {
        if (it->second.expiry_at != -1 && it->second.expiry_at < now) {
            it = map.erase(it); 
            continue;
        }

        const std::string& key = it->first;

        if (pattern == "*") {
            results.push_back(key);
        } else if (pattern.back() == '*') {
            // avoid making a deep copy
            std::string_view prefix(pattern.data(), pattern.size() - 1);
            if (std::string_view(key).starts_with(prefix)) {
                results.push_back(key);
            }
        } else if (key == pattern) {
            // exact match
            results.push_back(key);
        }

        ++it;
    }
    return results;
}


int KeyValueDatabase::ZADD(std::string& set_key, std::vector<std::string>& members, std::vector<double>& scores, bool acquire_lock) {
    std::unique_lock<std::shared_mutex> db_lock(rw_lock, std::defer_lock);

    if(acquire_lock) {
        db_lock.lock();
    }

    auto it = map.find(set_key);

    if(it == map.end()) {
        map[set_key] = {Value(ZSet{}), ObjType::ZSET, -1};
        it = map.find(set_key);
    }

    ZSet& zset = std::get<ZSet>(it->second.value);

    int inserted = 0;

    for(int i = 0; i < members.size(); i++) {
        bool already_present = false;

        if(zset.score_map.find(members[i]) != zset.score_map.end()) {
            zset.score_set.erase({members[i], zset.score_map[members[i]]});
            already_present = true;
        }

        zset.score_map[members[i]] = scores[i];
        zset.score_set.insert({members[i], zset.score_map[members[i]]});

        if(!already_present) inserted++;
    }

    return inserted;
}

int KeyValueDatabase::ZRANK(std::string& set_key, std::string& member, bool acquire_lock) {
    std::shared_lock<std::shared_mutex> db_lock(rw_lock, std::defer_lock);

    if(acquire_lock) {
        db_lock.lock();
    }

    auto it = map.find(set_key);

    if(it == map.end()) {
        //sorted set does not exist
        return -1;
    }

    ZSet& zset = std::get<ZSet>(it->second.value);

    auto it_member = zset.score_map.find(member);

    if(it_member == zset.score_map.end()) {
        //given member does not exist inside the sorted set
        return -1;
    }

    double score = zset.score_map[member];

    auto it_set = zset.score_set.find({member, score});

    int rank = std::distance(zset.score_set.begin(), it_set);

    return rank;
}

std::vector<std::string> KeyValueDatabase::ZRANGE(std::string& set_key, int start, int end, bool acquire_lock) {
    std::shared_lock<std::shared_mutex> db_lock(rw_lock, std::defer_lock);

    if(acquire_lock) {
        db_lock.lock();
    }

    auto it = map.find(set_key);

    if(it == map.end()) {
        //sorted set does not exist
        return {};
    }

    ZSet& zset = std::get<ZSet>(it->second.value);

    int size = zset.score_map.size();

    start = (start < 0) ? (size + start) : start;
    end = (end < 0) ? (size + end) : std::min(end, size);

    if(start >= size || start > end) {
        return {};
    }

    std::vector<std::string> members;

    for(auto it = std::next(zset.score_set.begin(), start); it != std::next(zset.score_set.begin(), end + 1); it++) {
        members.push_back(it->member);
    }
    
    return members;
}

int KeyValueDatabase::ZCARD(std::string& set_key, bool acquire_lock) {
    std::shared_lock<std::shared_mutex> db_lock(rw_lock, std::defer_lock);

    if(acquire_lock) {
        db_lock.lock();
    }

    auto it = map.find(set_key);

    if(it == map.end()) {
        //sorted set does not exist
        return 0;
    }

    ZSet& zset = std::get<ZSet>(it->second.value);

    return zset.score_map.size();
}

double KeyValueDatabase::ZSCORE(std::string& set_key, std::string& member, bool acquire_lock) {
    std::shared_lock<std::shared_mutex> db_lock(rw_lock, std::defer_lock);

    if(acquire_lock) {
        db_lock.lock();
    }

    auto it = map.find(set_key);

    if(it == map.end()) {
        //sorted set does not exist
        return -1;
    }

    ZSet& zset = std::get<ZSet>(it->second.value);

    auto it_member = zset.score_map.find(member);

    if(it_member == zset.score_map.end()) {
        //given member does not exist inside the sorted set
        return -1;
    }

    double score = zset.score_map[member];

    return score;    
}

int KeyValueDatabase::ZREM(std::string& set_key, std::vector<std::string>& members, bool acquire_lock) {
    std::unique_lock<std::shared_mutex> db_lock(rw_lock, std::defer_lock);

    if(acquire_lock) {
        db_lock.lock();
    }

    auto it = map.find(set_key);

    if(it == map.end()) {
        //sorted set does not exist
        return 0;
    }

    ZSet& zset = std::get<ZSet>(it->second.value);

    int removed = 0;

    for(auto& member : members) {
        auto it_member = zset.score_map.find(member);

        if(it_member == zset.score_map.end()) {
            //given member does not exist inside the sorted set
            continue;
        }

        double score = zset.score_map[member];
        zset.score_set.erase({member, score});
        zset.score_map.erase(it_member);
        removed++;
    }
    
    return removed;
}
