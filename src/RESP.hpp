#pragma once
#include <string>
#include <vector>
#include <stdexcept>

enum class RESPType
{
    INTEGER,
    SIMPLE_STRING,
    BULK_STRING,
    ARRAY,
    ERROR
};

struct RESPValue
{
    RESPType type;
    std::string value;
    std::vector<RESPValue> array; // to store RESP commands as array
};

class RESPParser
{
public:
    RESPParser(const std::string &input);
    RESPValue parse();

private:
    const std::string &buffer;
    size_t pos; // Current cursor position

    std::string readLine();
    RESPValue parseInteger();
    RESPValue parseSimpleString();
    RESPValue parseBulkString();
    RESPValue parseArray();
};