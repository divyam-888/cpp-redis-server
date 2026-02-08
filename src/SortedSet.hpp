#pragma once
#include <set>
#include <unordered_map>
#include <string>

struct ZSetNode {
    std::string member;
    double score;

    bool operator<(const ZSetNode& other) const {
        if(score != other.score) return score < other.score;
        return member < other.member;
    }
};

struct ZSet {
    std::unordered_map<std::string, double> score_map;
    std::set<ZSetNode> score_set;
};