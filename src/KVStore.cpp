#include "KVStore.hpp"

KeyValueDatabase db;

long long KeyValueDatabase::current_time_ms()
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

void KeyValueDatabase::SET(const std::string &key, const std::string &value, long long px_duration)
{
    std::unique_lock<std::shared_mutex> lock(rw_lock); // we use unique_lock to acquire the mutex EXCLUSIVELY as we are WRITING
    long long expiry = -1;                             // -1 means it has expiry as INF
    if (px_duration > 0)
    {
        expiry = current_time_ms() + px_duration;
    }
    std::variant<std::string, RedisList> myVar = value;
    map[key] = {myVar, ObjType::STRING, expiry};
}

std::optional<std::string> KeyValueDatabase::GET(const std::string &key)
{
    std::unique_lock<std::shared_mutex> lock(rw_lock); // we use unique lock because we might delete/modify if the key is expired.
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
    
    return get<std::string>(it->second.value);
}

int KeyValueDatabase::RPUSH(std::string &list_key, std::vector<std::string> &items) {
    std::unique_lock<std::shared_mutex> lock(rw_lock);
    auto it = map.find(list_key);

    if(it == map.end()) {
        RedisList dq;
        map[list_key] = {std::variant<std::string, RedisList>(dq), ObjType::LIST, -1};
        it = map.find(list_key);
    } else if(it != map.end() && it->second.type != ObjType::LIST) return -1;

    RedisList& dq = get<RedisList>(it->second.value);

    for(auto item : items) dq.push_back(item);

    return dq.size();
}

int KeyValueDatabase::LPUSH(std::string &list_key, std::vector<std::string> &items) {
    std::unique_lock<std::shared_mutex> lock(rw_lock);
    auto it = map.find(list_key);

    if(it == map.end()) {
        RedisList dq;
        map[list_key] = {std::variant<std::string, RedisList>(dq), ObjType::LIST, -1};
        it = map.find(list_key);
    } else if(it != map.end() && it->second.type != ObjType::LIST) return -1;

    RedisList& dq = get<RedisList>(it->second.value);

    for(auto item : items) dq.push_front(item);

    return dq.size();
}

std::vector<std::string> KeyValueDatabase::LRANGE(std::string &list_key, int start, int end) {
    std::shared_lock<std::shared_mutex> lock(rw_lock);
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

int KeyValueDatabase::LLEN(std::string &list_key) {
    std::shared_lock<std::shared_mutex> lock(rw_lock);
    auto it = map.find(list_key);
    int size = 0;
    if(it != map.end() && it->second.type == ObjType::LIST) {
        RedisList& dq = get<RedisList>(it->second.value);
        size = dq.size();
    }
    return size;
}

std::vector<std::string> KeyValueDatabase::LPOP(std::string &list_key, int num_remove_item) {
    std::unique_lock<std::shared_mutex> lock(rw_lock);
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