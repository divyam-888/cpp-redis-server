#pragma once
#include <openssl/evp.h>
#include <iomanip>
#include <sstream>

class CryptoHelper {
public:
    static std::string sha256(const std::string& unhashed) {
        EVP_MD_CTX* context = EVP_MD_CTX_new();
        const EVP_MD* md = EVP_sha256();
        unsigned char hash[EVP_MAX_MD_SIZE];
        unsigned int lengthOfHash = 0;

        EVP_DigestInit_ex(context, md, nullptr);
        EVP_DigestUpdate(context, unhashed.c_str(), unhashed.length());
        EVP_DigestFinal_ex(context, hash, &lengthOfHash);
        EVP_MD_CTX_free(context);

        // Convert the binary hash to a hex string
        std::stringstream ss;
        for (unsigned int i = 0; i < lengthOfHash; ++i) {
            ss << std::hex << std::setw(2) << std::setfill('0') << (int)hash[i];
        }
        return ss.str();
    }
};