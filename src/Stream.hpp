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
    bool operator<(const StreamId& other) {
        if(ms != other.ms) {
            return ms < other.ms;
        }
        return seq < other.seq;
    }

    bool operator==(const StreamId& other) {
        return (ms == other.ms && seq == other.seq);
    }

    std::string toString () const {
        return std::to_string(ms) + "-" + std::to_string(seq);
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
};

