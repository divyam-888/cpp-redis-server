#pragma once
#include <unordered_map>
#include <string>
#include <vector>
#include <shared_mutex>
#include <optional>
#include "CryptoHelper.hpp"

struct ACLUser {
    std::string username;
    bool nopass = true;
    std::vector<std::string> hashed_passwords;
};

class ACLManager {
private:
    std::unordered_map<std::string, ACLUser> users;
    std::shared_mutex acl_mutex;
public:
    ACLManager() {
        users["default"] = {"default", true, {}};
    }

    bool nopass() {
        return users["default"].nopass;
    }

    std::optional<ACLUser> getUser(const std::string& username) {
        std::shared_lock<std::shared_mutex> acl_lock(acl_mutex);
        
        if(users.find(username) == users.end()) {
            return std::nullopt;
        }

        return users[username];
    }

    void setUser(const std::string& username, const std::string& new_password) {
        std::unique_lock<std::shared_mutex> acl_lock(acl_mutex);

        std::string hash_password = CryptoHelper::sha256(new_password);

        if(users.find(username) == users.end()) {
            return;
        }

        users[username].nopass = false;
        users[username].hashed_passwords.push_back(hash_password);
    }

    bool authenticate(const std::string& username, const std::string& cleartext_password) {
        std::unique_lock<std::shared_mutex> acl_lock(acl_mutex);

        if(users.find(username) == users.end()) {
            return false;
        }

        std::string hashtext = CryptoHelper::sha256(cleartext_password);

        for(std::string& password : users[username].hashed_passwords) {
            if(hashtext == password) {
                return true;
            }
        }

        return false;
    }
};