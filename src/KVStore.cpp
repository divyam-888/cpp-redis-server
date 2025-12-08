#include "KVStore.hpp"

KeyValueDatabase database;

long long KeyValueDatabase::current_time_ms()
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

void KeyValueDatabase::set(const std::string &key, const std::string &value, long long px_duration)
{
    std::unique_lock<std::shared_mutex> lock(rw_lock); // we use unique_lock to acquire the mutex EXCLUSIVELY as we are WRITING
    long long expiry = -1;                             // -1 means it has expiry as INF
    if (px_duration > 0)
    {
        expiry = current_time_ms() + px_duration;
    }
    map[key] = {value, expiry};
}

std::optional<std::string> KeyValueDatabase::get(const std::string &key)
{
    std::unique_lock<std::shared_mutex> lock(rw_lock); // we use unique lock because we might delete/modify if the key is expired.
    auto it = map.find(key);
    if (it == map.end())
    {
        return std::nullopt; // key not found, so we return null optional
    }
    if (it->second.expiry_at != -1 && it->second.expiry_at < current_time_ms())
    {
        map.erase(it); // key exists but has expired
        return std::nullopt;
    }
    return it->second.value;
}