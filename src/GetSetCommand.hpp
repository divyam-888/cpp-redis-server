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

class SetCommand : public Command
{
public:
    std::string name() const override { return "SET"; }
    int min_args() const override { return 3; } // SET key value
    bool isWriteCommand() const override { return true; }
    bool sendToMaster() const override { return false; }

    std::string execute(ClientContext& context, const std::vector<std::string>& args, KeyValueDatabase& db, bool acquire_lock) override
    {

        std::string key = args[1];
        std::string val = args[2];

        int PX = -1;

        for (size_t i = 3; i < args.size(); i++)
        {
            std::string arg = args[i];
            std::transform(arg.begin(), arg.end(), arg.begin(), ::toupper);

            if (arg == "PX" && i + 1 < args.size())
            {
                try
                {
                    PX = std::stoll(args[i + 1]);
                    i++;
                }
                catch (...)
                {
                    return "-ERR value is not an integer or out of range\r\n";
                }
            } else if (arg == "EX" && i + 1 < args.size())
            {
                try
                {
                    PX = std::stoll(args[i + 1]) * 1000;
                    i++;
                }
                catch (...)
                {
                    return "-ERR value is not an integer or out of range\r\n";
                }
            }
        }

        db.SET(key, val, acquire_lock, PX);
        return "+OK\r\n";
    }
};

class GetCommand : public Command
{
public:
    std::string name() const override { return "GET"; }
    int min_args() const override { return 2; }
    bool isWriteCommand() const override { return false; }
    bool sendToMaster() const override { return false; }

    std::string execute(ClientContext& context, const std::vector<std::string>& args, KeyValueDatabase& db, bool acquire_lock) override
    {
        std::string key = args[1];
        std::optional<std::string> result = db.GET(key, acquire_lock);

        if (result.has_value())
        {
          std::string value = result.value();
          return "$" + std::to_string(value.length()) + "\r\n" + value + "\r\n";
        }
        else
        {
          return "$-1\r\n";
        }
    }  
};