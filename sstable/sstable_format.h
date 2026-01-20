// sstable/sstable_format.h
// SSTable file format definitions and encoding utilities

#pragma once

#include "util/types.h"
#include "util/bloom_filter.h"
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

namespace lsm {
namespace sstable {

// File format constants
constexpr uint64_t kSSTableMagic = 0x53535461626C6531ULL;  // "SSTable1"
constexpr size_t kFooterSize = 64;
constexpr size_t kBlockTrailerSize = 5;  // type (1) + crc (4)
constexpr int kDefaultBlockSize = 4096;
constexpr int kDefaultRestartInterval = 16;

// Block types
enum class BlockType : uint8_t {
    kData = 0x00,
    kIndex = 0x01,
};

// SSTable options
struct SSTableOptions {
    size_t block_size = kDefaultBlockSize;
    int restart_interval = kDefaultRestartInterval;
    bool verify_checksums = true;

    // Bloom filter settings
    bool use_bloom_filter = true;
    BloomFilterPolicy bloom_policy;  // Default: 10 bits/key, ~1% FPR
};

// Block handle: pointer to a block in the file
struct BlockHandle {
    uint64_t offset = 0;
    uint64_t size = 0;

    static constexpr size_t kMaxEncodedLength = 20;  // 2 * varint64

    std::string Encode() const {
        std::string result;
        PutVarint64(&result, offset);
        PutVarint64(&result, size);
        return result;
    }

    bool Decode(Slice* input) {
        return GetVarint64(input, &offset) && GetVarint64(input, &size);
    }

private:
    static void PutVarint64(std::string* dst, uint64_t v) {
        while (v >= 128) {
            dst->push_back(static_cast<char>(v | 128));
            v >>= 7;
        }
        dst->push_back(static_cast<char>(v));
    }

    static bool GetVarint64(Slice* input, uint64_t* value) {
        *value = 0;
        for (int shift = 0; shift <= 63 && !input->empty(); shift += 7) {
            uint8_t byte = static_cast<uint8_t>((*input)[0]);
            input->remove_prefix(1);
            if (byte & 128) {
                *value |= (static_cast<uint64_t>(byte & 127) << shift);
            } else {
                *value |= (static_cast<uint64_t>(byte) << shift);
                return true;
            }
        }
        return false;
    }
};

// Footer: stored at the end of the file
struct Footer {
    BlockHandle index_handle;
    BlockHandle bloom_handle;  // Bloom filter location
    uint64_t num_entries = 0;
    SequenceNumber min_sequence = 0;
    SequenceNumber max_sequence = 0;
    std::string min_key;
    std::string max_key;

    std::string Encode() const {
        std::string result;
        result.reserve(kFooterSize);

        // Index handle
        std::string handle_enc = index_handle.Encode();
        PutFixed32(&result, static_cast<uint32_t>(handle_enc.size()));
        result.append(handle_enc);

        // Bloom filter handle
        std::string bloom_enc = bloom_handle.Encode();
        PutFixed32(&result, static_cast<uint32_t>(bloom_enc.size()));
        result.append(bloom_enc);

        // Metadata
        PutFixed64(&result, num_entries);
        PutFixed64(&result, min_sequence);
        PutFixed64(&result, max_sequence);

        // Keys (length-prefixed)
        PutFixed32(&result, static_cast<uint32_t>(min_key.size()));
        result.append(min_key);
        PutFixed32(&result, static_cast<uint32_t>(max_key.size()));
        result.append(max_key);

        // Pad to fixed size minus magic
        while (result.size() < kFooterSize - 8) {
            result.push_back(0);
        }

        // Magic number
        PutFixed64(&result, kSSTableMagic);

        return result;
    }

    bool Decode(Slice input) {
        if (input.size() < kFooterSize) return false;

        // Start from the end to find magic
        const char* p = input.data() + input.size() - 8;
        uint64_t magic = DecodeFixed64(p);
        if (magic != kSSTableMagic) return false;

        // Decode from start
        p = input.data();

        // Index handle
        uint32_t handle_len = DecodeFixed32(p);
        p += 4;
        Slice handle_slice(p, handle_len);
        if (!index_handle.Decode(&handle_slice)) return false;
        p += handle_len;

        // Bloom filter handle
        uint32_t bloom_len = DecodeFixed32(p);
        p += 4;
        Slice bloom_slice(p, bloom_len);
        if (!bloom_handle.Decode(&bloom_slice)) return false;
        p += bloom_len;

        num_entries = DecodeFixed64(p); p += 8;
        min_sequence = DecodeFixed64(p); p += 8;
        max_sequence = DecodeFixed64(p); p += 8;

        uint32_t min_key_len = DecodeFixed32(p); p += 4;
        min_key.assign(p, min_key_len); p += min_key_len;

        uint32_t max_key_len = DecodeFixed32(p); p += 4;
        max_key.assign(p, max_key_len);

        return true;
    }

private:
    static void PutFixed32(std::string* dst, uint32_t val) {
        char buf[4];
        buf[0] = val & 0xff;
        buf[1] = (val >> 8) & 0xff;
        buf[2] = (val >> 16) & 0xff;
        buf[3] = (val >> 24) & 0xff;
        dst->append(buf, 4);
    }

    static void PutFixed64(std::string* dst, uint64_t val) {
        char buf[8];
        for (int i = 0; i < 8; i++) {
            buf[i] = (val >> (i * 8)) & 0xff;
        }
        dst->append(buf, 8);
    }

    static uint32_t DecodeFixed32(const char* p) {
        return static_cast<uint8_t>(p[0]) |
               (static_cast<uint8_t>(p[1]) << 8) |
               (static_cast<uint8_t>(p[2]) << 16) |
               (static_cast<uint8_t>(p[3]) << 24);
    }

    static uint64_t DecodeFixed64(const char* p) {
        uint64_t result = 0;
        for (int i = 0; i < 8; i++) {
            result |= static_cast<uint64_t>(static_cast<uint8_t>(p[i])) << (i * 8);
        }
        return result;
    }
};

// Varint encoding utilities
class Varint {
public:
    static void PutVarint32(std::string* dst, uint32_t v) {
        while (v >= 128) {
            dst->push_back(static_cast<char>(v | 128));
            v >>= 7;
        }
        dst->push_back(static_cast<char>(v));
    }

    static void PutVarint64(std::string* dst, uint64_t v) {
        while (v >= 128) {
            dst->push_back(static_cast<char>(v | 128));
            v >>= 7;
        }
        dst->push_back(static_cast<char>(v));
    }

    static bool GetVarint32(const char** p, const char* limit, uint32_t* value) {
        *value = 0;
        for (int shift = 0; shift <= 28 && *p < limit; shift += 7) {
            uint8_t byte = static_cast<uint8_t>(**p);
            (*p)++;
            if (byte & 128) {
                *value |= (static_cast<uint32_t>(byte & 127) << shift);
            } else {
                *value |= (static_cast<uint32_t>(byte) << shift);
                return true;
            }
        }
        return false;
    }

    static bool GetVarint64(const char** p, const char* limit, uint64_t* value) {
        *value = 0;
        for (int shift = 0; shift <= 63 && *p < limit; shift += 7) {
            uint8_t byte = static_cast<uint8_t>(**p);
            (*p)++;
            if (byte & 128) {
                *value |= (static_cast<uint64_t>(byte & 127) << shift);
            } else {
                *value |= (static_cast<uint64_t>(byte) << shift);
                return true;
            }
        }
        return false;
    }

    static int VarintLength(uint64_t v) {
        int len = 1;
        while (v >= 128) {
            v >>= 7;
            len++;
        }
        return len;
    }
};

// Fixed-width encoding utilities
class FixedEncode {
public:
    static void PutFixed32(std::string* dst, uint32_t val) {
        char buf[4];
        EncodeFixed32(buf, val);
        dst->append(buf, 4);
    }

    static void PutFixed64(std::string* dst, uint64_t val) {
        char buf[8];
        EncodeFixed64(buf, val);
        dst->append(buf, 8);
    }

    static void EncodeFixed32(char* buf, uint32_t val) {
        buf[0] = val & 0xff;
        buf[1] = (val >> 8) & 0xff;
        buf[2] = (val >> 16) & 0xff;
        buf[3] = (val >> 24) & 0xff;
    }

    static void EncodeFixed64(char* buf, uint64_t val) {
        for (int i = 0; i < 8; i++) {
            buf[i] = (val >> (i * 8)) & 0xff;
        }
    }

    static uint32_t DecodeFixed32(const char* p) {
        return static_cast<uint8_t>(p[0]) |
               (static_cast<uint8_t>(p[1]) << 8) |
               (static_cast<uint8_t>(p[2]) << 16) |
               (static_cast<uint8_t>(p[3]) << 24);
    }

    static uint64_t DecodeFixed64(const char* p) {
        uint64_t result = 0;
        for (int i = 0; i < 8; i++) {
            result |= static_cast<uint64_t>(static_cast<uint8_t>(p[i])) << (i * 8);
        }
        return result;
    }
};

}  // namespace sstable
}  // namespace lsm