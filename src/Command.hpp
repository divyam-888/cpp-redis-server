#pragma once
#include <string>
#include<vector>
#include "KVStore.hpp"

class ClientContext;

class Command {
public:
    virtual ~Command() = default;

    // 1. The Core Method
    // We pass 'args' (the user's data) and 'db' (the state)
    // It returns a std::string which is the RESP-formatted response
    virtual std::string execute(ClientContext& context, const std::vector<std::string>& args, KeyValueDatabase& db, bool acquire_lock) = 0;

    // 2. Metadata (Self-Documentation & Validation)
    // Each command knows its own name and requirements
    virtual std::string name() const = 0; 
    virtual int min_args() const { return 0; } // Default 0
};