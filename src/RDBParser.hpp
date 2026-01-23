#pragma once
#include <fstream>
#include <string>
#include <iostream>
#include <chrono>
#include "KVStore.hpp"

class RDBParser
{
public:
    static void load(const std::string &path, KeyValueDatabase &db)
    {
        std::ifstream file(path, std::ios::binary);

        if (!file.is_open()) {
            std::cout << "Unable to open the rdb file" << std::endl;
            return;
        }

        char header[6];
        file.read(header, 5);
        header[5] = '\0';
        if (std::string(header) != "REDIS") {
            return;
        }

        file.seekg(4, std::ios::cur);

        while (file)
        {
            unsigned char opcode;
            file.read(reinterpret_cast<char *>(&opcode), 1);

            // eof
            if (opcode == 0xFF) {
                std::cout << "Finished reading rdb file" << std::endl;
                break;
            }

            if (opcode == 0xFA) {
                // read metadata like redis ver
                readString(file);
                readString(file);
                continue;
            }

            if (opcode == 0xFE) {
                // meta data of db followed by index
                readLength(file);
                continue;
            }
            if (opcode == 0xFB) {
                // meta data of hash table: hashtable size + expiry hash table size
                readLength(file);
                readLength(file);
                continue;
            }

            long long expiry_ms = -1;
            unsigned char value_type;

            if (opcode == 0xFD) {
                // expiry in seconds, unsigned int of 4 bytes in little-endian
                uint32_t seconds;
                file.read(reinterpret_cast<char *>(&seconds), 4);
                expiry_ms = (long long)seconds * 1000;
            } else if (opcode == 0xFC) {
                // expiry in ms, unsigned long of 8 bytes in little-endian
                file.read(reinterpret_cast<char *>(&expiry_ms), 8);
            } else {
                value_type = opcode;
            }

            // value type opcode
            std::string key = readString(file);
            std::string value;
            if (value_type == 0) { 
                value = readString(file);
            } else {
                continue; 
            }

            long long current_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                       std::chrono::system_clock::now().time_since_epoch())
                                       .count();

            if (expiry_ms != -1) {
                std::cout << "extracted: " << key << " : " << value << " expiry: " << expiry_ms << " current: " << current_ms << std::endl;
                if (expiry_ms < 0) {
                    continue;
                }
                // convert absolute timestamp to relative duration for expiry logic
                expiry_ms = expiry_ms - current_ms;
            }

            db.SET(key, value, true, expiry_ms);
        }
    }

private:
    static uint32_t readLength(std::ifstream &file)
    {
        unsigned char first_byte;
        file.read(reinterpret_cast<char *>(&first_byte), 1);

        // mask of first 2 bits
        uint8_t type = (first_byte & 0xC0) >> 6;

        if (type == 0x00)
        {
            // 00: length is in the remaining 6 bits
            return first_byte & 0x3F;
        }
        else if (type == 0x01)
        {
            // 01: length is in 6 bits from this byte + 8 bits from the second byte
            unsigned char second_byte;
            file.read(reinterpret_cast<char *>(&second_byte), 1);
            return ((first_byte & 0x3F) << 8) | second_byte;
        }
        else if (type == 0x02)
        {
            // 10: 32-bit length. Read 4 more bytes.
            uint32_t length;
            // redis uses Big Endian for these lengths so we read it directly into memory.
            file.read(reinterpret_cast<char *>(&length), 4);
            // most modern systems are Little Endian but lengths are encoded in big endian so we need to swap bytes:
            length = __builtin_bswap32(length);
            return length;
        }

        // 11: special encoding (Integers converted to strings), handled in readString
        return first_byte & 0x3F;
    }
    static std::string readString(std::ifstream &file)
    {
        // if the first byte indicates a special integer encoding
        unsigned char first_byte = file.peek();
        uint8_t type = (first_byte & 0xC0) >> 6;

        if (type == 0x03) {   
            file.ignore(1); // consume the type byte
            uint8_t enc_type = first_byte & 0x3F;
            if (enc_type == 0)
            { // 8-bit integer
                int8_t val;
                file.read(reinterpret_cast<char *>(&val), 1);
                return std::to_string(val);
            }
            else if (enc_type == 1)
            { // 16-bit integer
                int16_t val;
                file.read(reinterpret_cast<char *>(&val), 2);
                return std::to_string(val);
            }
            else if (enc_type == 2)
            { // 32-bit integer
                int32_t val;
                file.read(reinterpret_cast<char *>(&val), 4);
                return std::to_string(val);
            }
        }

        // read length, then read data
        uint32_t len = readLength(file);
        std::string s;
        s.resize(len);
        file.read(&s[0], len);
        return s;
    }
};