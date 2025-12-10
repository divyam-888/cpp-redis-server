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

