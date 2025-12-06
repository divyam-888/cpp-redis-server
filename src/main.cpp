#include <iostream>
#include <cstdlib>
#include <string>
#include <cstring>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <thread>
#include <chrono>
#include <vector>
#include <optional>
#include <algorithm>
#include <unordered_map>
#include <shared_mutex>
#include <mutex>
#include <chrono>
using namespace std;

class KeyValueDatabase {
  private:
    struct Entry {
      std::string value;
      long long expiry_at = -1;
    };

    std::unordered_map<std::string, Entry> map;
    std::shared_mutex rw_lock;

    long long current_time_ms() {
      return std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
    }
  public:
    void set(const std::string& key, const std::string& value, long long px_duration = -1) {
        
      std::unique_lock<std::shared_mutex> lock(rw_lock); // writer lock (exclusive)
        
      long long expiry = -1;
      if (px_duration > 0) {
          expiry = current_time_ms() + px_duration;
      }

      map[key] = {value, expiry};
    }

    std::optional<std::string> get(const std::string& key) {
      // acquire a write lock if we might need to delete 
        
      std::unique_lock<std::shared_mutex> lock(rw_lock); // we use unique lock because we might delete if the key expired.
        
      auto it = map.find(key);
      if (it == map.end()) {
          return std::nullopt; // key not found
      }

      // Check Expiry
      if (it->second.expiry_at != -1 && it->second.expiry_at < current_time_ms()) {
          map.erase(it); 
          return std::nullopt;
      }

      return it->second.value;
    }
};

KeyValueDatabase database;

enum class RESPType {INTEGER, SIMPLE_STRING, BULK_STRING, ARRAY, ERROR};

struct RESPValue {
    RESPType type;
    std::string value;              
    std::vector<RESPValue> array;    // to store RESP commands as array
};

class RESPParser {
public:
    RESPParser(const std::string& input) : buffer(input), pos(0) {}

    RESPValue parse() {
        char type = buffer[pos];
        pos++; // Move past the type char

        switch (type) {
            case ':': return parseInteger();
            case '+': return parseSimpleString();
            case '$': return parseBulkString();
            case '*': return parseArray();
            default: throw std::runtime_error("Unknown type");
        }
    }

private:
    const std::string& buffer; 
    size_t pos;                // Current cursor position

    // Read until \r\n and return that part
    std::string readLine() {
        size_t end = buffer.find("\r\n", pos);
        std::string line = buffer.substr(pos, end - pos);
        pos = end + 2; // Skip \r\n
        return line;
    }

    RESPValue parseInteger() {
      return {RESPType::INTEGER, readLine(), {}};
    }

    RESPValue parseSimpleString() {
        return {RESPType::SIMPLE_STRING, readLine(), {}};
    }

    RESPValue parseBulkString() {
        int length = std::stoi(readLine());
        if (length == -1) return {RESPType::BULK_STRING, "", {}}; 
        
        std::string data = buffer.substr(pos, length);
        pos += length + 2; 
        return {RESPType::BULK_STRING, data, {}};
    }
    
    RESPValue parseArray() {
        int count = std::stoi(readLine());
        RESPValue result;
        result.type = RESPType::ARRAY;
        
        for (int i = 0; i < count; i++) {
            // Recursion: The array just calls parse() again, it sees each element as a command to be parsed again
            result.array.push_back(parse());
        }
        return result;
    }
};

std::string process_command(RESPValue& input) {
  if(input.type != RESPType::ARRAY) {
    return "-ERR command must be an array\r\n";
  }

  if(input.array.empty()) {
    return "-ERR empty command\r\n";
  }

  std::string command = input.array[0].value;
    
  // normalize to uppercase 
  std::transform(command.begin(), command.end(), command.begin(), ::toupper);
  
  if (command == "PING") {
      return "+PONG\r\n";
  } 
  else if (command == "ECHO") {
      if (input.array.size() < 2) {
          return "-ERR wrong number of arguments for 'echo' command\r\n";
      }

      std::string argument = input.array[1].value;
      
      // returning it as RESP bulk string: $ + length + \r\n + content + \r\n
      return "$" + std::to_string(argument.length()) + "\r\n" + argument + "\r\n";
  } 
  else if(command == "SET") {
    if (input.array.size() < 3) {
        return "-ERR wrong number of arguments for 'SET' command\r\n";
    }

    std::string key = input.array[1].value;
    std::string value = input.array[2].value;
    int PX = -1;
    
    for (size_t i = 3; i < input.array.size(); i++) {
      std::string arg = input.array[i].value;
      std::transform(arg.begin(), arg.end(), arg.begin(), ::toupper);

      if (arg == "PX" && i + 1 < input.array.size()) {
          try {
              PX = std::stoll(input.array[i + 1].value); 
              i++; 
          } catch (...) {
              return "-ERR value is not an integer or out of range\r\n";
          }
      }
      else if (arg == "EX" && i + 1 < input.array.size()) {
          try {
            PX = std::stoll(input.array[i + 1].value) * 1000;
            i++;
          } catch (...) {
            return "-ERR value is not an integer or out of range\r\n";
          }
      }
  }

    database.set(key, value, PX);

    return "+OK\r\n";
  }
  else if(command == "GET") {
    if (input.array.size() < 2) {
        return "-ERR wrong number of arguments for 'GET' command\r\n";
    }

    std::string key = input.array[1].value;
    std::optional<std::string> result = database.get(key);

    if(result.has_value()) {
      std::string value = result.value();
      return "$" + std::to_string(value.length()) + "\r\n" + value + "\r\n";
    } else {
      return "$-1\r\n";
    }
  }
  return "-ERR unknown command\r\n";
}

void handleClient(int client_fd) {
    char buffer[1024];
    
    while (true) {
        ssize_t bytes_read = recv(client_fd, buffer, sizeof(buffer), 0);
        if (bytes_read <= 0) {
            std::cout << "Client disconnected\n";
            break;
        }
        string input(buffer, bytes_read);
        RESPParser parser(input);
        RESPValue command = parser.parse();

        std::string response = process_command(command);
        
        send(client_fd, response.data(), response.length(), 0);
    }
    
    close(client_fd);
}


int main(int argc, char **argv)
{
  // Flush after every std::cout / std::cerr
  std::cout << std::unitbuf;
  std::cerr << std::unitbuf;

  // Socket is used to create TCP server to accept inbound connections. It is like a telephone to accept incoming calls
  // Socket returns the file descriptor of the TCP server. It has parameters: Domain, Type, Protocol
  int server_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (server_fd < 0)
  {
    std::cerr << "Failed to create server socket\n";
    return 1;
  }

  // Since the tester restarts your program quite often, setting SO_REUSEADDR
  // ensures that we don't run into 'Address already in use' errors
  int reuse = 1;
  if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0)
  {
    std::cerr << "setsockopt failed\n";
    return 1;
  }

  /* Before socket/server can accept connections we need to bind it to an address. It's like setting up our 
  telephone on the wall and letting everyone know where to dial */

  // To bind a socket to a port, we need to set up a sockaddr_in structure with the address family, IP address, and port
  struct sockaddr_in server_addr;
  server_addr.sin_family = AF_INET;
  server_addr.sin_addr.s_addr = INADDR_ANY;
  server_addr.sin_port = htons(6379);

  if (bind(server_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) != 0)
  {
    std::cerr << "Failed to bind to port 6379\n";
    return 1;
  }

  // Now our socket/server is ready to take connections. We need to pass socket descriptor and backlog i.e, maximum # of connections our server can take to listen()
  // Backlog = 5 means If 5 people call at once, put 4 on hold. If a 6th calls, drop them.
  int connection_backlog = 5;
  if (listen(server_fd, connection_backlog) != 0)
  {
    std::cerr << "listen failed\n";
    return 1;
  }

  std::cout << "Logs from your program will appear here!\n";
  std::cout << "Server is listening...\n";

    while(true) {
        // Port for client, client is also a socket like our server, so it also requires to bind to a port
        struct sockaddr_in client_addr;
        int client_addr_len = sizeof(client_addr);
        
        // The Main Thread blocks here WAITING for a connection
        int client_fd = accept(server_fd, (struct sockaddr *)&client_addr, (socklen_t *)&client_addr_len);
        
        if (client_fd < 0) {
             std::cerr << "Failed to accept\n";
             break; 
        }

        std::cout << "New Client Connected! Spawning thread...\n";

        // New thread for this client; We use std::thread and pass the function + arguments
        std::thread client_thread(handleClient, client_fd);

        //Detaching the thread so main can continue running waiting for new clients
        client_thread.detach(); 
    }

  close(server_fd);
  return 0;
}
