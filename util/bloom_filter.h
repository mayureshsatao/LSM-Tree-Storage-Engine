// util/bloom_filter.h
// Space-efficient Bloom filter with configurable false positive rate

#pragma once

#include "util/types.h"
#include <cmath>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

namespace lsm {

// MurmurHash3 - fast, high-quality hash function
class MurmurHash {
public:
    static uint64_t Hash64(const char* data, size_t len, uint64_t seed = 0) {
        const uint64_t m = 0xc6a4a7935bd1e995ULL;
        const int r = 47;

        uint64_t h = seed ^ (len * m);

        const uint64_t* blocks = reinterpret_cast<const uint64_t*>(data);
        const size_t nblocks = len / 8;

        for (size_t i = 0; i < nblocks; i++) {
            uint64_t k = blocks[i];

            k *= m;
            k ^= k >> r;
            k *= m;

            h ^= k;
            h *= m;
        }

        const uint8_t* tail = reinterpret_cast<const uint8_t*>(data + nblocks * 8);

        uint64_t k = 0;
        switch (len & 7) {
            case 7: k ^= static_cast<uint64_t>(tail[6]) << 48; [[fallthrough]];
            case 6: k ^= static_cast<uint64_t>(tail[5]) << 40; [[fallthrough]];
            case 5: k ^= static_cast<uint64_t>(tail[4]) << 32; [[fallthrough]];
            case 4: k ^= static_cast<uint64_t>(tail[3]) << 24; [[fallthrough]];
            case 3: k ^= static_cast<uint64_t>(tail[2]) << 16; [[fallthrough]];
            case 2: k ^= static_cast<uint64_t>(tail[1]) << 8;  [[fallthrough]];
            case 1: k ^= static_cast<uint64_t>(tail[0]);
                    k *= m;
                    h ^= k;
        }

        h ^= h >> r;
        h *= m;
        h ^= h >> r;

        return h;
    }

    // Generate two hashes that can be combined for k hash functions
    // h(i) = h1 + i*h2 (double hashing technique)
    static void Hash128(const char* data, size_t len, uint64_t* h1, uint64_t* h2) {
        *h1 = Hash64(data, len, 0);
        *h2 = Hash64(data, len, *h1);
    }
};

// Bloom filter configuration
struct BloomFilterPolicy {
    // Bits per key (higher = lower FPR, more space)
    // 10 bits/key ≈ 1% FPR
    // 15 bits/key ≈ 0.1% FPR
    // 20 bits/key ≈ 0.01% FPR
    int bits_per_key = 10;

    // Calculate optimal number of hash functions
    int OptimalNumHashes() const {
        // k = (m/n) * ln(2) = bits_per_key * ln(2)
        int k = static_cast<int>(bits_per_key * 0.69314718056);
        if (k < 1) k = 1;
        if (k > 30) k = 30;
        return k;
    }

    // Estimate false positive rate
    double EstimatedFPR() const {
        int k = OptimalNumHashes();
        // FPR ≈ (1 - e^(-k/bits_per_key))^k
        double exp_term = std::exp(-static_cast<double>(k) / bits_per_key);
        return std::pow(1.0 - exp_term, k);
    }
};

// Bloom filter builder - accumulates keys and builds filter
class BloomFilterBuilder {
public:
    explicit BloomFilterBuilder(const BloomFilterPolicy& policy = BloomFilterPolicy())
        : policy_(policy), num_keys_(0) {}

    // Add a key to the filter
    void AddKey(Slice key) {
        uint64_t h1, h2;
        MurmurHash::Hash128(key.data(), key.size(), &h1, &h2);
        hashes_.push_back({h1, h2});
        num_keys_++;
    }

    // Build the filter and return serialized data
    std::string Finish() {
        if (num_keys_ == 0) {
            // Empty filter: just store metadata
            return CreateFilter(0);
        }

        // Calculate filter size
        size_t bits = num_keys_ * policy_.bits_per_key;
        // Round up to multiple of 8
        bits = ((bits + 7) / 8) * 8;
        if (bits < 64) bits = 64;  // Minimum size

        return CreateFilter(bits);
    }

    // Reset for reuse
    void Reset() {
        hashes_.clear();
        num_keys_ = 0;
    }

    size_t NumKeys() const { return num_keys_; }

private:
    std::string CreateFilter(size_t num_bits) {
        size_t num_bytes = num_bits / 8;
        int num_hashes = policy_.OptimalNumHashes();

        // Allocate filter data + metadata
        // Format: [filter_data...][num_hashes (1 byte)]
        std::string result;
        result.resize(num_bytes + 1, 0);

        char* data = &result[0];

        // Set bits for each key
        for (const auto& [h1, h2] : hashes_) {
            for (int i = 0; i < num_hashes; i++) {
                uint64_t bit_pos = (h1 + i * h2) % num_bits;
                data[bit_pos / 8] |= (1 << (bit_pos % 8));
            }
        }

        // Store number of hash functions in last byte
        result[num_bytes] = static_cast<char>(num_hashes);

        return result;
    }

    BloomFilterPolicy policy_;
    std::vector<std::pair<uint64_t, uint64_t>> hashes_;
    size_t num_keys_;
};

// Bloom filter reader - checks if keys may exist
class BloomFilterReader {
public:
    BloomFilterReader() : data_(nullptr), num_bits_(0), num_hashes_(0) {}

    // Initialize from serialized filter data
    bool Init(Slice filter_data) {
        if (filter_data.size() < 1) {
            return false;
        }

        // Last byte is number of hash functions
        num_hashes_ = static_cast<uint8_t>(filter_data[filter_data.size() - 1]);
        if (num_hashes_ == 0 || num_hashes_ > 30) {
            return false;
        }

        data_ = filter_data.data();
        num_bits_ = (filter_data.size() - 1) * 8;

        return true;
    }

    // Check if key may exist (false = definitely not, true = maybe)
    bool MayContain(Slice key) const {
        if (num_bits_ == 0) {
            return true;  // Empty filter, assume key may exist
        }

        uint64_t h1, h2;
        MurmurHash::Hash128(key.data(), key.size(), &h1, &h2);

        for (int i = 0; i < num_hashes_; i++) {
            uint64_t bit_pos = (h1 + i * h2) % num_bits_;
            if ((data_[bit_pos / 8] & (1 << (bit_pos % 8))) == 0) {
                return false;  // Bit not set, key definitely doesn't exist
            }
        }

        return true;  // All bits set, key may exist
    }

    size_t NumBits() const { return num_bits_; }
    int NumHashes() const { return num_hashes_; }

    // Calculate memory usage
    size_t MemoryUsage() const {
        return (num_bits_ / 8) + 1;
    }

private:
    const char* data_;
    size_t num_bits_;
    int num_hashes_;
};

// Full bloom filter (owns its data)
class BloomFilter {
public:
    BloomFilter() = default;

    explicit BloomFilter(std::string data) : data_(std::move(data)) {
        reader_.Init(data_);
    }

    // Build from keys
    static BloomFilter Build(const std::vector<Slice>& keys,
                             const BloomFilterPolicy& policy = BloomFilterPolicy()) {
        BloomFilterBuilder builder(policy);
        for (const auto& key : keys) {
            builder.AddKey(key);
        }
        return BloomFilter(builder.Finish());
    }

    bool MayContain(Slice key) const {
        return reader_.MayContain(key);
    }

    // Serialization
    const std::string& Data() const { return data_; }

    size_t MemoryUsage() const { return data_.size(); }
    size_t NumBits() const { return reader_.NumBits(); }
    int NumHashes() const { return reader_.NumHashes(); }

    bool Empty() const { return data_.empty(); }

private:
    std::string data_;
    BloomFilterReader reader_;
};

// Utility to estimate filter size for capacity planning
class BloomFilterUtil {
public:
    // Calculate bits needed for target FPR
    static size_t BitsForFPR(size_t num_keys, double target_fpr) {
        if (num_keys == 0 || target_fpr >= 1.0) return 64;
        // m = -n * ln(p) / (ln(2)^2)
        double bits = -static_cast<double>(num_keys) * std::log(target_fpr) /
                      (std::log(2.0) * std::log(2.0));
        return std::max(static_cast<size_t>(bits), size_t(64));
    }

    // Calculate expected FPR for given parameters
    static double ExpectedFPR(size_t num_keys, size_t num_bits, int num_hashes) {
        if (num_keys == 0 || num_bits == 0) return 0.0;
        // FPR = (1 - e^(-kn/m))^k
        double exp_term = std::exp(-static_cast<double>(num_hashes * num_keys) / num_bits);
        return std::pow(1.0 - exp_term, num_hashes);
    }

    // Optimal number of hash functions
    static int OptimalNumHashes(size_t num_keys, size_t num_bits) {
        if (num_keys == 0) return 1;
        // k = (m/n) * ln(2)
        int k = static_cast<int>(static_cast<double>(num_bits) / num_keys * std::log(2.0));
        return std::max(1, std::min(30, k));
    }
};

}  // namespace lsm