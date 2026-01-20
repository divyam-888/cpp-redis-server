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
#include <algorithm>
#include "KVStore.hpp" 
#include "RESPParser.hpp"
#include "Command.hpp"
#include "CommandRegistry.hpp"
#include "PingEchoCommand.hpp"
#include "GetSetCommand.hpp"
#include "ListCommands.hpp"
#include "ClientContext.hpp"
#include "Config.hpp"
#include "ReplicationManager.hpp"

void handleClient(int client_fd, KeyValueDatabase &db, CommandRegistry &registry, std::shared_ptr<ServerConfig> config)
{
  char buffer[1024];
  ClientContext context(client_fd);

  while (true)
  {
    ssize_t bytes_read = recv(client_fd, buffer, sizeof(buffer), 0);
    if (bytes_read <= 0)
    {
      std::cout << "Client disconnected\n";
      break;
    }
    //Get the input from client in char* as RESP string
    std::string inputString(buffer, bytes_read);

    //Break the RESP concatenated string as RESPValue array
    RESPParser parser(inputString);
    RESPValue input = parser.parse();

    //From the RESPValue array, get the inputs as vector of string
    std::vector<std::string> args = parser.extractArgs(input);
    
    //Find the Command which we have to execute
    std::string cmdName = args[0];
    Command* cmd = registry.getCommand(cmdName);

    std::string response;
    bool should_propagate = false;

    if (!cmd) {
      response = "-ERR unknown command\r\n";
    } else if (args.size() < cmd->min_args()) {
      response = "-ERR wrong number of arguments\r\n";
    } else {
      if(context.in_transaction && cmd->name() != "EXEC" && cmd->name() != "DISCARD") {
        response = "+QUEUED\r\n";
        context.commandQueue.push_back({cmd, std::move(args)});
      } else {
        response = cmd->execute(context, args, db, true);

        if (cmd->isWriteCommand() && config->role == "master") {
          should_propagate = true;
        }
      }
    }

    if (should_propagate) {
      std::lock_guard<std::mutex> lock(config->replica_mutex);
      for (auto& [replica_fd, ack_offset] : config->replicas) {
        // send the ORIGINAL raw RESP string (inputString) to the replicas
        std::cout << "Propogating to replica: " << cmd->name() << std::endl;
        send(replica_fd, inputString.data(), inputString.length(), 0);
      }
      config->master_repl_offset += inputString.length();
    }

    send(client_fd, response.data(), response.length(), 0);
  }


  close(client_fd);
}

int main(int argc, char **argv)
{
  KeyValueDatabase db;
  CommandRegistry registry;
  std::shared_ptr<ServerConfig> config = parse_args(argc, argv);

  if (config->role == "slave") {
    // we use stack-allocated manager to start the handshake thread, so that in meantime we can start accepting client
    std::thread replication_thread([&config, &db, &registry]() {
        ReplicationManager manager(config, db, registry);
        manager.startHandshake();
    });
    replication_thread.detach(); 
  }

  registry.registerCommand(std::make_unique<PingCommand>());
  registry.registerCommand(std::make_unique<EchoCommand>());
  registry.registerCommand(std::make_unique<SetCommand>());
  registry.registerCommand(std::make_unique<GetCommand>());
  registry.registerCommand(std::make_unique<RPUSH>());
  registry.registerCommand(std::make_unique<LPUSH>());
  registry.registerCommand(std::make_unique<LRANGE>());
  registry.registerCommand(std::make_unique<LLEN>());
  registry.registerCommand(std::make_unique<LPOP>());
  registry.registerCommand(std::make_unique<BLPOP>());
  registry.registerCommand(std::make_unique<TypeCommand>());
  registry.registerCommand(std::make_unique<XADDCommand>());
  registry.registerCommand(std::make_unique<XRANGECommand>());
  registry.registerCommand(std::make_unique<XREADCommand>());
  registry.registerCommand(std::make_unique<IncrementCommand>());
  registry.registerCommand(std::make_unique<MultiCommand>());
  registry.registerCommand(std::make_unique<ExecCommand>());
  registry.registerCommand(std::make_unique<DiscardCommand>());
  registry.registerCommand(std::make_unique<InfoCommand>(config));
  registry.registerCommand(std::make_unique<REPLCONF>(config));
  registry.registerCommand(std::make_unique<PSYNCCommand>(config));
  registry.registerCommand(std::make_unique<WAITCommand>(config));
  
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
  server_addr.sin_port = htons(config->port);

  if (bind(server_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) != 0)
  {
    std::cerr << "Failed to bind to port " << config->port << '\n';
    return 1;
  } 

  std::cout << "Server started on port " << config->port << " as " << config->role << std::endl;

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

  while (true)
  {
    // Port for client, client is also a socket like our server, so it also requires to bind to a port
    struct sockaddr_in client_addr;
    int client_addr_len = sizeof(client_addr);

    // The Main Thread blocks here WAITING for a connection
    int client_fd = accept(server_fd, (struct sockaddr *)&client_addr, (socklen_t *)&client_addr_len);

    if (client_fd < 0)
    {
      std::cerr << "Failed to accept\n";
      break;
    }

    std::cout << "New Client Connected! Spawning thread...\n";

    // New thread for this client; We use std::thread and pass the routine + arguments
    std::thread client_thread(handleClient, client_fd, std::ref(db), std::ref(registry), config);

    // Detaching the thread so main can continue running waiting for new clients
    client_thread.detach();
  }

  close(server_fd);
  return 0;
}
