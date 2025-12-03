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
using namespace std;

void handleClient(int client_fd) {
    char buffer[1024];
    
    // Loop to keep talking to THIS client until they disconnect
    while (true) {
        ssize_t bytes_read = recv(client_fd, buffer, sizeof(buffer), 0);
        if (bytes_read <= 0) {
            std::cout << "Client disconnected\n";
            break; // Stop the loop if they hang up
        }

        const char *response = "+PONG\r\n";
        send(client_fd, response, strlen(response), 0);
    }
    
    close(client_fd); // Hang up the phone
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
        // 1. Prepare to accept a new connection
        struct sockaddr_in client_addr;
        int client_addr_len = sizeof(client_addr);
        
        // 2. The Main Thread blocks here WAITING for a connection
        int client_fd = accept(server_fd, (struct sockaddr *)&client_addr, (socklen_t *)&client_addr_len);
        
        if (client_fd < 0) {
             std::cerr << "Failed to accept\n";
             break; 
        }

        std::cout << "New Client Connected! Spawning thread...\n";

        // 3. Create a thread specifically for THIS client_fd
        // We use std::thread and pass the function + arguments
        std::thread client_thread(handleClient, client_fd);

        // 4. DETACH the thread
        // This is crucial. It tells C++: "Let this thread run on its own in the background. 
        // I (Main) am going back to the top of the loop to wait for the next person."
        client_thread.detach(); 
    }

  close(server_fd);
  return 0;
}
