#include <optional>
#include "Command.hpp"
#include "KVStore.hpp"

class GetCommand : public Command
{
public:
    std::string name() const override { return "GET"; }
    int min_args() const override { return 2; }

    std::string execute(const std::vector<std::string> &args, KeyValueDatabase &db) override
    {
        std::string key = args[1];
        std::optional<std::string> result = db.GET(key);

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