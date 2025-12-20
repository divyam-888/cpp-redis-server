#pragma once
#include <string>
#include <unordered_map>
#include <shared_mutex>
#include <mutex>
#include <optional>
#include <chrono>
#include<variant>
#include<deque>
#include<vector>
#include<algorithm>
#include <string>
#include <unordered_map>
#include <optional>
#include "Command.hpp"
#include "KVStore.hpp"

class RPUSH : public Command
{
public:
    std::string name() const override { return "RPUSH"; }
    int min_args() const override { return 3; }

    std::string execute(const std::vector<std::string> &args, KeyValueDatabase &db) override
    {
        //args: RPUSH list_key item1 item2...
        std::string list_key = args[1];
        std::vector<std::string> items;
        items.reserve(args.size() - 2);

        for(size_t i = 2; i < args.size(); i++) items.push_back(args[i]);

        int size_list = db.RPUSH(list_key, items);

        if(size_list == -1) {
            return "-ERR WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
        }

        return ":" + std::to_string(size_list) + "\r\n";
    }  
};

class LPUSH : public Command
{
public:
    std::string name() const override { return "LPUSH"; }
    int min_args() const override { return 3; }

    std::string execute(const std::vector<std::string> &args, KeyValueDatabase &db) override
    {
        //args: LPUSH list_key item1 item2...
        std::string list_key = args[1];
        std::vector<std::string> items;
        items.reserve(args.size() - 2);

        for(size_t i = 2; i < args.size(); i++) items.push_back(args[i]);

        int size_list = db.LPUSH(list_key, items);

        if(size_list == -1) {
            return "-ERR WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
        }

        return ":" + std::to_string(size_list) + "\r\n";
    }  
};

class LRANGE : public Command
{
public:
    std::string name() const override { return "LRANGE"; }
    int min_args() const override { return 4; }

    std::string execute(const std::vector<std::string> &args, KeyValueDatabase &db) override
    {
        //args: LRANGE list_key st en
        std::string list_key = args[1];
        int start, end;
    
        try {
            start = std::stoi(args[2]);
            end = std::stoi(args[3]);
        } catch (...) {
            return "-ERR value is not an integer or out of range\r\n";
        }
        std::vector<std::string> items = db.LRANGE(list_key, start, end);
        
        std::string ans = "*" + std::to_string(items.size()) + "\r\n";
        for(const auto& str : items) {
            ans += "$" + std::to_string(str.length()) + "\r\n" + str + "\r\n";
        }
        return ans;
    }  
};

class LLEN : public Command
{
public:
    std::string name() const override { return "LLEN"; }
    int min_args() const override { return 2; }

    std::string execute(const std::vector<std::string> &args, KeyValueDatabase &db) override
    {
        //args: LLEN list_key
        std::string list_key = args[1];
        int length_list = db.LLEN(list_key);
        return ":" + std::to_string(length_list) + "\r\n";
    }  
};

class LPOP : public Command
{
public:
    std::string name() const override { return "LPOP"; }
    int min_args() const override { return 2; }

    std::string execute(const std::vector<std::string> &args, KeyValueDatabase &db) override
    {
        //args: LPOP list_key number
        std::string list_key = args[1];
        int num_remove_item = 1;

        if(args.size() >= 3) {
            try {
                num_remove_item = std::stoi(args[2]);
            } catch (...) {
                return "-ERR value is not an integer or out of range\r\n";
            }
        }

        std::vector<std::string> items = db.LPOP(list_key, num_remove_item);

        if(items.empty()) {
            return "$-1\r\n";
        } else if(items.size() == 1) {
            return "$" + std::to_string(items[0].length()) + "\r\n" + items[0] + "\r\n";
        }
        
        std::string ans = "*" + std::to_string(items.size()) + "\r\n";
        for(const auto& str : items) {
            ans += "$" + std::to_string(str.length()) + "\r\n" + str + "\r\n";
        }
        return ans;
    }  
};

class BLPOP : public Command {
public:
    std::string name() const override { return "BLPOP"; }
    int min_args() const override { return 3; }

    std::string execute(const std::vector<std::string>& args, KeyValueDatabase& db) {
        std::vector<std::string> list_keys;
        for(int i = 1; i < args.size() - 1; i++) {
            list_keys.push_back(args[i]);
        }

        double wait_time = 0;

        try {
            wait_time = std::stod(args.back());
        } catch (...) {
            return "-ERR timeout is not a float or out of range\r\n";
        }

        std::optional<std::pair<std::string, std::string> > result = db.BLPOP(list_keys, wait_time);

        if(result.has_value()) {
            std::string list = result.value().first;
            std::string item = result.value().second;

            std::string ans = "*2\r\n";

            ans += "$" + std::to_string(list.length()) + "\r\n" + list + "\r\n";
            ans += "$" + std::to_string(item.length()) + "\r\n" + item + "\r\n";

            return ans;
        } else {
            return "*-1\r\n"; //Null Bulk string
        }
    }
};

class TypeCommand : public Command {
public:
    std::string name() const override { return "TYPE"; }
    int min_args() const override { return 2; }

    std::string execute(const std::vector<std::string>& args, KeyValueDatabase& db) {
        std::string key = args[1];
        std::string type = db.TYPE(key);
        return "+" + type + "\r\n";
    }
};

class XADDCommand : public Command {
public:
    std::string name() const override { return "XADD"; }
    int min_args() const override { return 5; }

    std::string execute(const std::vector<std::string>& args, KeyValueDatabase& db) {
        std::string stream_key = args[1];
        std::string stream_id = args[2];
        std::vector<std::pair<std::string, std::string> > fields;
        for(int i = 3; i < args.size(); i += 2) {
            if(i + 1 == args.size()) {
                return "-wrong number of arguments for 'xadd' command\r\n";
            }
            fields.push_back({args[i], args[i + 1]});
        }

        StreamId result = db.XADD(stream_key, stream_id, fields);

        // handle errors using magic values
        if (result.ms == -1) { 
             return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
        }
        if (result.ms == 0 && result.seq == 0) {
             return "-ERR The ID specified in XADD must be greater than 0-0\r\n";
        }
        if (result.ms == 0 && result.seq == -1) {
             return "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n";
        }

        std::string ans = result.toString();
        return "$" + std::to_string(ans.length()) + "\r\n" + ans + "\r\n";
    }
};

class XRANGECommand : public Command {
public:
    std::string name() const override { return "XRANGE"; }
    int min_args() const override { return 4; }

    std::string execute(const std::vector<std::string>& args, KeyValueDatabase& db) {
        std::string stream_key = args[1];
        std::string start = args[2];
        std::string end = args[3];
        try {
            std::vector<StreamEntry> entries = db.XRANGE(stream_key, start, end);
            
            std::string ans = "*" + std::to_string(entries.size()) + "\r\n";
            
            for(const auto& entry : entries) {
                // for each entry it has 2 values: stream_id and the fields (key:value pairs)
                ans += "*2\r\n";
                
                std::string str_id = entry.id.toString();
                ans += "$" + std::to_string(str_id.length()) + "\r\n" + str_id + "\r\n";
                
                // size is fields.size() * 2 because key and value are separate elements
                ans += "*" + std::to_string(entry.fields.size() * 2) + "\r\n"; 
                
                for(const auto& field : entry.fields) {
                    ans += "$" + std::to_string(field.first.length()) + "\r\n" + field.first + "\r\n";
                    ans += "$" + std::to_string(field.second.length()) + "\r\n" + field.second + "\r\n";
                }
            }
            return ans;

        } catch (const std::invalid_argument&) {
            return "-ERR Invalid stream ID specified as stream command argument\r\n";
        } catch (const std::runtime_error& e) {
            return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
        } catch (...) {
            return "-ERR unknown error\r\n";
        }
    }
};

