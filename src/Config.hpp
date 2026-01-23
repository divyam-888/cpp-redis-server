#pragma once
#include <string>
#include <vector>
#include <mutex>
#include <memory>
#include <atomic>
#include <condition_variable>

struct ReplicaInfo {
    int fd;
    size_t ack_offset = 0;
};

struct ServerConfig {
    // by default it is localhost
    int port = 6379;
    std::string role = "master";

    std::string master_host = "";
    int master_port = 0;

    std::string master_replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"; // generate a pseudo-random alphanumeric string of 40 characters
    std::atomic<size_t> master_repl_offset{0};

    // store slaves' fd
    std::vector<ReplicaInfo> replicas;

    // std::mutex is neither copyable nor movable.
    std::mutex replica_mutex;

    //to notify the master if any replica responds the getack command
    std::condition_variable replica_cv;

    std::string rdb_file_dir = "";
    std::string rdb_file_name = "";
};

/* we need to return shared_ptr as during returing it will try to move/copy the ptr to the caller function 
which is prohibited as struct contains mutex which is not moveable/copyable */
std::shared_ptr<ServerConfig> parse_args(int argc, char** argv);


/* when you have a configuration object that needs to be shared across many threads (your main thread, 
your replication thread, and all client threads), you should not pass it by value or move it. 
Instead, you should place it on the Heap and share access to it using a Shared Pointer. By using std::shared_ptr<ServerConfig>, 
you aren't moving the struct itself; you are just passing around a small pointer to the same memory location. */
