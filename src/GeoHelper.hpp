#pragma once
#include <iostream>
#include <cstdint>
#include <cmath>
#include <vector>
#include <string>

constexpr double MIN_LATITUDE = -85.05112878;
constexpr double MAX_LATITUDE = 85.05112878;
constexpr double MIN_LONGITUDE = -180.0;
constexpr double MAX_LONGITUDE = 180.0;

constexpr double LATITUDE_RANGE = MAX_LATITUDE - MIN_LATITUDE;
constexpr double LONGITUDE_RANGE = MAX_LONGITUDE - MIN_LONGITUDE;

struct Coordinates {
    double latitude;
    double longitude;
};

uint32_t compact_int64_to_int32(uint64_t v) {
    v = v & 0x5555555555555555ULL;
    v = (v | (v >> 1)) & 0x3333333333333333ULL;
    v = (v | (v >> 2)) & 0x0F0F0F0F0F0F0F0FULL;
    v = (v | (v >> 4)) & 0x00FF00FF00FF00FFULL;
    v = (v | (v >> 8)) & 0x0000FFFF0000FFFFULL;
    v = (v | (v >> 16)) & 0x00000000FFFFFFFFULL;
    return static_cast<uint32_t>(v);
}

Coordinates convert_grid_numbers_to_coordinates(uint32_t grid_latitude_number, uint32_t grid_longitude_number) {
    // Calculate the grid boundaries
    double grid_latitude_min = MIN_LATITUDE + LATITUDE_RANGE * (grid_latitude_number / pow(2, 26));
    double grid_latitude_max = MIN_LATITUDE + LATITUDE_RANGE * ((grid_latitude_number + 1) / pow(2, 26));
    double grid_longitude_min = MIN_LONGITUDE + LONGITUDE_RANGE * (grid_longitude_number / pow(2, 26));
    double grid_longitude_max = MIN_LONGITUDE + LONGITUDE_RANGE * ((grid_longitude_number + 1) / pow(2, 26));
    
    // Calculate the center point of the grid cell
    Coordinates result;
    result.latitude = (grid_latitude_min + grid_latitude_max) / 2;
    result.longitude = (grid_longitude_min + grid_longitude_max) / 2;
    
    return result;
}

Coordinates decode(uint64_t geo_code) {
    // Align bits of both latitude and longitude to take even-numbered position
    uint64_t y = geo_code >> 1;
    uint64_t x = geo_code;
    
    // Compact bits back to 32-bit ints
    uint32_t grid_latitude_number = compact_int64_to_int32(x);
    uint32_t grid_longitude_number = compact_int64_to_int32(y);
    
    return convert_grid_numbers_to_coordinates(grid_latitude_number, grid_longitude_number);
}

uint64_t spread_int32_to_int64(uint32_t v) {
    uint64_t result = v;
    result = (result | (result << 16)) & 0x0000FFFF0000FFFFULL;
    result = (result | (result << 8)) & 0x00FF00FF00FF00FFULL;
    result = (result | (result << 4)) & 0x0F0F0F0F0F0F0F0FULL;
    result = (result | (result << 2)) & 0x3333333333333333ULL;
    result = (result | (result << 1)) & 0x5555555555555555ULL;
    return result;
}

uint64_t interleave(uint32_t x, uint32_t y) {
    uint64_t x_spread = spread_int32_to_int64(x);
    uint64_t y_spread = spread_int32_to_int64(y);
    uint64_t y_shifted = y_spread << 1;
    return x_spread | y_shifted;
}

uint64_t encode(double latitude, double longitude) {
    // Normalize to the range 0-2^26
    double normalized_latitude = pow(2, 26) * (latitude - MIN_LATITUDE) / LATITUDE_RANGE;
    double normalized_longitude = pow(2, 26) * (longitude - MIN_LONGITUDE) / LONGITUDE_RANGE;

    // Truncate to integers
    uint32_t lat_int = static_cast<uint32_t>(normalized_latitude);
    uint32_t lon_int = static_cast<uint32_t>(normalized_longitude);

    return interleave(lat_int, lon_int);
}

static constexpr double EARTH_RADIUS_IN_METERS = 6372797.560856;

    // Convert degrees to radians
static double deg_to_rad(double deg) {
    return deg * M_PI / 180.0;
}

// The Haversine implementation
static double calculate_distance(double lon1, double lat1, double lon2, double lat2) {
    double dLat = deg_to_rad(lat2 - lat1);
    double dLon = deg_to_rad(lon2 - lon1);
    double lat1Rad = deg_to_rad(lat1);
    double lat2Rad = deg_to_rad(lat2);

    double a = std::sin(dLat / 2) * std::sin(dLat / 2) +
               std::sin(dLon / 2) * std::sin(dLon / 2) * std::cos(lat1Rad) * std::cos(lat2Rad);
    
    double c = 2 * std::atan2(std::sqrt(a), std::sqrt(1 - a));
    return EARTH_RADIUS_IN_METERS * c;
}

// Unit conversion utility
static double convert_from_meters(double distance_in_meters, const std::string& unit) {
    if (unit == "km") return distance_in_meters / 1000.0;
    if (unit == "mi") return distance_in_meters / 1609.34;
    if (unit == "ft") return distance_in_meters / 0.3048;
    return distance_in_meters; // default "m"
}
