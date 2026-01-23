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
#include "ClientContext.hpp"
#include "Config.hpp"
#include <sstream>


class RPUSH : public Command
{
public:
    std::string name() const override { return "RPUSH"; }
    int min_args() const override { return 3; }
    bool isWriteCommand() const override { return true; }
    bool sendToMaster() const override { return false; }

    std::string execute(ClientContext& context, const std::vector<std::string> &args, KeyValueDatabase &db, bool acquire_lock) override
    {
        //args: RPUSH list_key item1 item2...
        std::string list_key = args[1];
        std::vector<std::string> items;
        items.reserve(args.size() - 2);

        for(size_t i = 2; i < args.size(); i++) items.push_back(args[i]);

        int size_list = db.RPUSH(list_key, items, acquire_lock);

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
    bool isWriteCommand() const override { return true; }
    bool sendToMaster() const override { return false; }

    std::string execute(ClientContext& context, const std::vector<std::string> &args, KeyValueDatabase &db, bool acquire_lock) override
    {
        //args: LPUSH list_key item1 item2...
        std::string list_key = args[1];
        std::vector<std::string> items;
        items.reserve(args.size() - 2);

        for(size_t i = 2; i < args.size(); i++) items.push_back(args[i]);

        int size_list = db.LPUSH(list_key, items, acquire_lock);

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
    bool isWriteCommand() const override { return false; }
    bool sendToMaster() const override { return false; }

    std::string execute(ClientContext& context, const std::vector<std::string> &args, KeyValueDatabase &db, bool acquire_lock) override
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
        std::vector<std::string> items = db.LRANGE(list_key, start, end, acquire_lock);
        
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
    bool isWriteCommand() const override { return false; }
    bool sendToMaster() const override { return false; }

    std::string execute(ClientContext& context, const std::vector<std::string> &args, KeyValueDatabase &db, bool acquire_lock) override
    {
        //args: LLEN list_key
        std::string list_key = args[1];
        int length_list = db.LLEN(list_key, acquire_lock);
        return ":" + std::to_string(length_list) + "\r\n";
    }  
};

class LPOP : public Command
{
public:
    std::string name() const override { return "LPOP"; }
    int min_args() const override { return 2; }
    bool isWriteCommand() const override { return true; }
    bool sendToMaster() const override { return false; }

    std::string execute(ClientContext& context, const std::vector<std::string> &args, KeyValueDatabase &db, bool acquire_lock) override
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

        std::vector<std::string> items = db.LPOP(list_key, num_remove_item, acquire_lock);

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
    bool isWriteCommand() const override { return true; }
    bool sendToMaster() const override { return false; }

    std::string execute(ClientContext& context, const std::vector<std::string> &args, KeyValueDatabase &db, bool acquire_lock) override
    {
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

        std::optional<std::pair<std::string, std::string> > result = db.BLPOP(list_keys, wait_time, acquire_lock);

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
    bool isWriteCommand() const override { return false; }
    bool sendToMaster() const override { return false; }

    std::string execute(ClientContext& context, const std::vector<std::string> &args, KeyValueDatabase &db, bool acquire_lock) override
    {
        std::string key = args[1];
        std::string type = db.TYPE(key, acquire_lock);
        return "+" + type + "\r\n";
    }
};

class XADDCommand : public Command {
public:
    std::string name() const override { return "XADD"; }
    int min_args() const override { return 5; }
    bool isWriteCommand() const override { return true; }
    bool sendToMaster() const override { return false; }

    std::string execute(ClientContext& context, const std::vector<std::string> &args, KeyValueDatabase &db, bool acquire_lock) override
    {
        std::string stream_key = args[1];
        std::string stream_id = args[2];
        std::vector<std::pair<std::string, std::string> > fields;
        for(int i = 3; i < args.size(); i += 2) {
            if(i + 1 == args.size()) {
                return "-wrong number of arguments for 'xadd' command\r\n";
            }
            fields.push_back({args[i], args[i + 1]});
        }

        StreamId result = db.XADD(stream_key, stream_id, fields, acquire_lock);

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
    bool isWriteCommand() const override { return false; }
    bool sendToMaster() const override { return false; }

    std::string execute(ClientContext& context, const std::vector<std::string> &args, KeyValueDatabase &db, bool acquire_lock) override
    {
        std::string stream_key = args[1];
        std::string start = args[2];
        std::string end = args[3];
        try {
            std::vector<StreamEntry> entries = db.XRANGE(stream_key, start, end, acquire_lock);
            
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

class XREADCommand : public Command {
    std::string name() const override { return "XREAD"; }
    int min_args() const override { return 4; }
    bool isWriteCommand() const override { return false; }
    bool sendToMaster() const override { return false; }

    std::string execute(ClientContext& context, const std::vector<std::string> &args, KeyValueDatabase &db, bool acquire_lock) override
    {
        int count = INT_MAX;
        bool block = false;
        int64_t ms;
        std::vector<std::string> key;
        std::vector<std::string> id;
        
        int pos = 1;
        if(args[pos] == "count") {
            count = stoll(args[pos + 1]);
            pos += 2;
        }

        if(args[pos] == "block") {
            block = true;
            ms = stoll(args[pos + 1]);
            pos += 2;
        }

        if (args[pos] != "streams") {
            return "-ERR syntax error\r\n"; 
        }

        pos++;

        int num_keys = (int)args.size() - pos;
        if(num_keys % 2 != 0) {
            return "-ERR wrong number of arguments for XREAD command\r\n";
        }

        for(int k = pos; k < pos + num_keys / 2; k++) {
            key.push_back(args[k]);
        }
        for(int k = pos + num_keys / 2; k < args.size(); k++) {
            id.push_back(args[k]);
        }

        try {
            std::cout << "Block = " << block << " for " << ms << '\n';
            std::vector<std::pair<std::string, std::vector<StreamEntry> > > entries = db.XREAD(count, block, ms, key, id, acquire_lock);
            //XREAD returns a vector of pair of stream_key and vector of streamEntry where each entry corresponds to a stream id and the key-value pairs added to this stream
            
            if(entries.empty()) {
                std::cout << "here\n";
                return "*-1\r\n";
            }

            std::string ans = "*" + std::to_string(entries.size()) + "\r\n";

            for(auto& [stream_key, stream_entry] : entries) {
                ans += "*2\r\n";
                ans += "$" + std::to_string(stream_key.length()) + "\r\n" + stream_key + "\r\n";

                ans += "*" + std::to_string(stream_entry.size()) + "\r\n";
            
                for(const auto& entry : stream_entry) {
                    // for each entry it has 2 values: stream_id and the fields (key:value pairs)
                    ans += "*2\r\n";
                
                    std::string stream_id = entry.id.toString();
                    ans += "$" + std::to_string(stream_id.length()) + "\r\n" + stream_id + "\r\n";
                
                    // size is fields.size() * 2 because key and value are separate elements
                    ans += "*" + std::to_string(entry.fields.size() * 2) + "\r\n"; 
                
                    for(const auto& field : entry.fields) {
                        ans += "$" + std::to_string(field.first.length()) + "\r\n" + field.first + "\r\n";
                        ans += "$" + std::to_string(field.second.length()) + "\r\n" + field.second + "\r\n";
                    }
                }
            }

            return ans;
        } catch (const std::invalid_argument&) {
            return "-ERR Invalid stream ID specified as stream command argument\r\n";
        } catch (const std::runtime_error& e) {
            return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
        }
    }
};

class IncrementCommand : public Command {
public:
    std::string name() const override { return "INCR"; }
    int min_args() const override { return 2; }
    bool isWriteCommand() const override { return true; }
    bool sendToMaster() const override { return false; }

    std::string execute(ClientContext& context, const std::vector<std::string> &args, KeyValueDatabase &db, bool acquire_lock) override
    {
        std::string key = args[1];
        try {
            std::optional<long long> opt = db.INCR(key, acquire_lock);
            if(opt.has_value()) {
                long long val = opt.value();
                std::string str_val = std::to_string(val);
                return ":" + str_val + "\r\n";
            } else {
                return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
            }
        } catch(const std::invalid_argument&) {
            return "-ERR value is not an integer or out of range\r\n";
        } catch (const std::out_of_range&) {
            return "-ERR value is not an integer or out of range\r\n";
        }
    }
};

class MultiCommand : public Command {
public:
    std::string name() const override { return "MULTI"; }
    int min_args() const override { return 1; }
    bool isWriteCommand() const override { return true; }
    bool sendToMaster() const override { return false; }

    std::string execute(ClientContext& context, const std::vector<std::string> &args, KeyValueDatabase &db, bool acquire_lock) override {
        if(context.in_transaction) {
            return "-ERR MULTI calls can not be nested\r\n";
        }
        context.in_transaction = true;
        return "+OK\r\n";
    }
};

class ExecCommand : public Command {
public:
    std::string name() const override { return "EXEC"; }
    int min_args() const override { return 1; }
    bool isWriteCommand() const override { return true; }
    bool sendToMaster() const override { return false; }

    std::string execute(ClientContext& context, const std::vector<std::string> &args, KeyValueDatabase &db, bool acquire_lock) override {
        if(!context.in_transaction) {
            return "-ERR EXEC without MULTI\r\n";
        }

        std::vector<std::string> results = db.EXEC(context.commandQueue, context, db, acquire_lock);
        
        context.reset_transaction();
        
        std::string resp = "*" + std::to_string(results.size()) + "\r\n";
        for(auto& result : results) resp += result;

        return resp;
    }
};

class DiscardCommand : public Command {
public:
    std::string name() const override { return "DISCARD"; }
    int min_args() const override { return 1; }
    bool isWriteCommand() const override { return true; }
    bool sendToMaster() const override { return false; }

    std::string execute(ClientContext& context, const std::vector<std::string> &args, KeyValueDatabase &db, bool acquire_lock) override {
        if(!context.in_transaction) {
            return "-ERR DISCARD without MULTI\r\n";
        }
        
        context.reset_transaction();

        return "+OK\r\n";
    }
};

class InfoCommand : public Command {
private:
    std::shared_ptr<ServerConfig> config;
public:
    InfoCommand(std::shared_ptr<ServerConfig> cfg) : config(cfg) {}
    std::string name() const override { return "INFO"; }
    int min_args() const override { return 0; }
    bool isWriteCommand() const override { return false; }
    bool sendToMaster() const override { return false; }

    std::string execute(ClientContext& context, const std::vector<std::string> &args, KeyValueDatabase &db, bool acquire_lock) override {
        std::ostringstream oss;
        oss << "# Replication\r\n";
        oss << "role:" << config->role << "\r\n";
        oss << "master_replid:" << config->master_replid << "\r\n";
        oss << "master_repl_offset:" << config->master_repl_offset << "\r\n";

        std::string info = oss.str();

        return "$" + std::to_string(info.length()) + "\r\n" + info + "\r\n";
    }
};

class REPLCONF : public Command {
private:
    std::shared_ptr<ServerConfig> config;
public:
    REPLCONF(std::shared_ptr<ServerConfig> cfg) : config(cfg) {}
    std::string name() const override { return "REPLCONF"; }
    int min_args() const override { return 0; }
    bool isWriteCommand() const override { return false; }
    bool sendToMaster() const override { return true; }

    std::string execute(ClientContext& context, const std::vector<std::string> &args, KeyValueDatabase &db, bool acquire_lock) override {
        if(args[1] == "GETACK") {
            std::string response = "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$" +
                                 std::to_string(std::to_string(config->master_repl_offset).length()) + "\r\n" 
                                + std::to_string(config->master_repl_offset) + "\r\n";
            return response;
        } else if(args[1] == "ACK") {
            size_t offset = std::stoull(args[2]);
            {
                std::lock_guard<std::mutex> lock(config->replica_mutex);
                if (context.replica_index < config->replicas.size()) {
                    config->replicas[context.replica_index].ack_offset = offset;
                }
            }
            // Wake up any threads waiting on a WAIT command
            config->replica_cv.notify_all();  
            return "";
        } 

        return "+OK\r\n";
    }    
};

class PSYNCCommand : public Command {
private:
    std::shared_ptr<ServerConfig> config;
public:
    PSYNCCommand(std::shared_ptr<ServerConfig> cfg) : config(cfg) {}
    std::string name() const override { return "PSYNC"; }
    int min_args() const override { return 3; }
    bool isWriteCommand() const override { return false; }
    bool sendToMaster() const override { return false; }

    std::string execute(ClientContext& context, const std::vector<std::string> &args, KeyValueDatabase &db, bool acquire_lock) override {
        context.is_replica = true;
        
        // as multiple replicas might try to connect at once, we need mutex
        {
            std::unique_lock<std::mutex> lock(config->replica_mutex);
            context.replica_index = (int)config->replicas.size();
            config->replicas.push_back({context.client_fd, 0});
        }

        std::string full_resync = "+FULLRESYNC " + config->master_replid + " 0\r\n";

        // we need to send empty RDB file as well
        std::string rdb_hex = "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";
        std::string rdb_content = hexToBinary(rdb_hex); 

        std::string rdb_transfer = "$" + std::to_string(rdb_content.length()) + "\r\n" + rdb_content;

        // no \r\n after the RDB content!!!
        return full_resync + rdb_transfer;
    }

    std::string hexToBinary(const std::string& hex) {
        std::string binary = "";
        for (size_t i = 0; i < hex.length(); i += 2) {
            std::string byteString = hex.substr(i, 2);
            char byte = (char) strtol(byteString.c_str(), NULL, 16);
            binary.push_back(byte);
        }

        return binary;
    }  
};

class WAITCommand : public Command{
private:
    std::shared_ptr<ServerConfig> config;
public:
    WAITCommand(std::shared_ptr<ServerConfig> cfg) : config(cfg) {}
    std::string name() const override { return "WAIT"; }
    int min_args() const override { return 3; }
    bool isWriteCommand() const override { return false; }
    bool sendToMaster() const override { return false; }

    std::string execute(ClientContext& context, const std::vector<std::string> &args, KeyValueDatabase &db, bool acquire_lock) override  {
        int target = std::stoi(args[1]);
        int timeout_ms = std::stoi(args[2]);
        size_t required_offset = config->master_repl_offset;

        {
            std::unique_lock<std::mutex> lock(config->replica_mutex);
            std::string getack = "*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n";
            for (auto& repl : config->replicas) {
                send(repl.fd, getack.data(), getack.length(), 0);
            }
        }

        std::unique_lock<std::mutex> lock(config->replica_mutex);

        auto predicate = [&]() {
            int count = 0;
            for (auto& repl : config->replicas) {
                if (repl.ack_offset >= required_offset) count++;
            }
            return count >= target;
        };

        // block the thread until predicate is true OR timeout reached
        config->replica_cv.wait_for(lock, std::chrono::milliseconds(timeout_ms), predicate);

        // final count after waking up
        int final_count = 0;
        for (auto& repl : config->replicas) {
            if (repl.ack_offset >= required_offset) final_count++;
        }

        return ":" + std::to_string(final_count) + "\r\n";
    }
};

class CONFIGCommand : public Command {
private:
    std::shared_ptr<ServerConfig> config;
public:
    CONFIGCommand(std::shared_ptr<ServerConfig> cfg) : config(cfg) {}
    std::string name() const override { return "CONFIG"; }
    int min_args() const override { return 3; }
    bool isWriteCommand() const override { return false; }
    bool sendToMaster() const override { return false; }

    std::string execute(ClientContext& context, const std::vector<std::string> &args, KeyValueDatabase &db, bool acquire_lock) override {
        std::string param;
        std::string value;
        if(args[2] == "dir") {
            param = "dir";
            value = config->rdb_file_dir;
        } else if(args[2] == "dbfilename") {
            param = "dbfilename";
            value = config->rdb_file_name;
        }

        return "*2\r\n$" + std::to_string(param.length()) + "\r\n" + param + "\r\n$" + std::to_string(value.length()) + "\r\n" + value + "\r\n";
    }
};

class KEYSCommand : public Command {
public:
    std::string name() const override { return "KEYS"; }
    int min_args() const override { return 2; }
    bool isWriteCommand() const override { return false; }
    bool sendToMaster() const override { return false; }

    std::string execute(ClientContext& context, const std::vector<std::string> &args, KeyValueDatabase &db, bool acquire_lock) override {
        std::string pattern = args[1];
        std::vector<std::string> keys = db.KEYS(pattern, acquire_lock);

        std::string response = "*" + std::to_string(keys.size()) + "\r\n";
        for(auto &key : keys) {
            response += "$" + std::to_string(key.length()) + "\r\n" + key + "\r\n";
        }

        return response;
    }
};