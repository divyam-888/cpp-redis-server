#include "RESPParser.hpp"

std::vector<std::string> RESPParser::extractArgs(RESPValue &input) {
  if (input.type != RESPType::ARRAY || input.array.empty()) {
    return {};
  }
  std::vector<std::string> args;
  args.reserve(input.array.size()); // pre allocate memory for optimization

  for(const auto& item : input.array) {
    if (item.type == RESPType::ARRAY) {
      // throw exception if nested arrays are given
      throw std::runtime_error("Nested arrays are not supported in commands");
      // or return empty to signal failure
      // return {}; 
    }
    args.push_back(item.value);
  }
  return args;
}

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