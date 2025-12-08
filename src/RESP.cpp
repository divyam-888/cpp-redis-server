#include "RESP.hpp"

RESPParser::RESPParser(const std::string &input) : buffer(input), pos(0) {}

RESPValue RESPParser::parse()
{
    char type = buffer[pos];
    pos++; // Move past the type char
    switch (type)
    {
    case ':':
        return parseInteger();
    case '+':
        return parseSimpleString();
    case '$':
        return parseBulkString();
    case '*':
        return parseArray();
    default:
        throw std::runtime_error("Unknown type");
    }
}

// Read until \r\n and return that part
std::string RESPParser::readLine()
{
    size_t end = buffer.find("\r\n", pos);
    std::string line = buffer.substr(pos, end - pos);
    pos = end + 2; // Skip \r\n
    return line;
}

RESPValue RESPParser::parseInteger()
{
    return {RESPType::INTEGER, readLine(), {}};
}

RESPValue RESPParser::parseSimpleString()
{
    return {RESPType::SIMPLE_STRING, readLine(), {}};
}

RESPValue RESPParser::parseBulkString()
{
    int length = std::stoi(readLine());
    if (length == -1)
        return {RESPType::BULK_STRING, "", {}};

    std::string data = buffer.substr(pos, length);
    pos += length + 2;
    return {RESPType::BULK_STRING, data, {}};
}

RESPValue RESPParser::parseArray()
{
    int count = std::stoi(readLine());
    RESPValue result;
    result.type = RESPType::ARRAY;
    for (int i = 0; i < count; i++)
    {
        // Recursion: The array just calls parse() again, it sees each element as a command to be parsed again
        result.array.push_back(parse());
    }
    return result;
}