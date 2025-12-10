#include "Command.hpp"
#include "KVStore.hpp"

class PingCommand : public Command {
public:
    std::string name() const override { return "PING"; }
    
    std::string execute(const std::vector<std::string>& args, KeyValueDatabase& db) override {
        return "+PONG\r\n";
    }
};

class EchoCommand : public Command
{
public:
    std::string name() const override { return "ECHO"; }
    int min_args() const override { return 2; }

    std::string execute(const std::vector<std::string> &args, KeyValueDatabase &db) override
    {
        std::string arg = args[1];
        return "$" + std::to_string(arg.length()) + "\r\n" + arg + "\r\n"; 
    }
};