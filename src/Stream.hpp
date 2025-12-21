#pragma once
#include <iostream>
#include <string>
#include<vector>
#include <unordered_map>
#include <chrono>
#include<map>

struct StreamId {
    int64_t ms;
    int64_t seq;
    bool operator<(const StreamId& other) const {
        if(ms != other.ms) {
            return ms < other.ms;
        }
        return seq < other.seq;
    }

    bool operator==(const StreamId& other) const {
        return (ms == other.ms && seq == other.seq);
    }

    std::string toString () const {
        return std::to_string(ms) + "-" + std::to_string(seq);
    }

    static StreamId parse(const std::string& str, bool is_end_boundary) {
        StreamId id;

        if (str == "-") return {0, 0};
        if (str == "+") return {INT64_MAX, INT64_MAX};

        size_t dash = str.find('-');
    
        try {
            if (dash == std::string::npos) {
                // no sequence given so we take boundary
                id.ms = std::stoll(str);
                id.seq = is_end_boundary ? INT64_MAX : 0;
            } else {
                id.ms = std::stoll(str.substr(0, dash));
                id.seq = std::stoll(str.substr(dash + 1));
            }
        } catch (const std::exception& e) {
            throw std::invalid_argument("Invalid Stream ID");
        }
        return id;
    } 
};

struct StreamEntry {
    StreamId id;
    std::vector<std::pair<std::string, std::string> > fields;
};

struct Stream {
    std::map<StreamId, StreamEntry> entries;
    StreamId last_id = {0, 0};

    StreamId add(StreamId& id, std::vector<std::pair<std::string, std::string> >& fields) {
        if (id.ms == -1 && id.seq == -1) {
            int64_t now = std::chrono::duration_cast<std::chrono::milliseconds>(
                              std::chrono::system_clock::now().time_since_epoch()).count();
            
            if (now < last_id.ms) {
                //System Clock is skewed, we will consider last_id, just increment the sequence number
                id.ms = last_id.ms;
                id.seq = last_id.seq + 1;
            } else if (now == last_id.ms) {
                // same millisecond collision
                id.ms = now;
                id.seq = last_id.seq + 1;
            } else {
                // time moved forward
                id.ms = now;
                id.seq = 0; // reset sequence
            }
        }
        // auto generate sequence number
        else if (id.seq == -1) {
            if (id.ms < last_id.ms) {
                return {0, -1}; // error: time is smaller than last top item
            }
            if (id.ms == last_id.ms) {
                id.seq = last_id.seq + 1;
            } else {
                id.seq = 0; // new ms start at 0
            }
        }
        
        // ID 0-0 is not allowed in Redis
        if (id.ms == 0 && id.seq == 0) return {0, 0}; 

        // must be strictly greater than last_id
        if (entries.size() > 0 && !(last_id < id)) {
            return {0, -1}; // error: ID is not greater
        }

        StreamEntry new_entry = {id, fields};
        entries[id] = new_entry;
        last_id = id;

        return id;
    }

    std::vector<StreamEntry> range(StreamId& start, StreamId& end) {
        std::vector<StreamEntry> query;
        auto it = entries.lower_bound(start);
        while(it != entries.end() && (it->first < end || it->first == end)) {
            query.push_back(it->second);
            it++;
        }
        return query;
    }

    std::vector<StreamEntry> read(int count, bool block, int64_t ms, StreamId& start) {
        std::cout << "entering steam.hpp\n";
        std::vector<StreamEntry> query;
        auto it = entries.upper_bound(start);
        int cnt = 0;
        if(it == entries.end()) {
            std::cout << "stream empty :P\n";
        }
        if(cnt < count) {
            std::cout << cnt << " < " << count << '\n';
        }
        while(it != entries.end() && cnt < count) {
            std::cout << cnt << "\n";
            query.push_back(it->second);
            it++;
            cnt++;
        }

        std::cout << query.size() << " exiting stream.hpp\n";
        return query;
    }

};

