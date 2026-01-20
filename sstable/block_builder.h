// sstable/block_builder.h
// Builds data blocks with prefix compression and restart points

#pragma once

#include "util/types.h"
#include "sstable/sstable_format.h"
#include "wal/wal_format.h"  // For CRC32

#include <cassert>
#include <string>
#include <vector>

namespace lsm {
namespace sstable {

// BlockBuilder generates blocks with the following format:
//
// Entry format:
//   shared_bytes (varint32)    - bytes shared with previous key
//   unshared_bytes (varint32)  - bytes in this key not shared
//   value_length (varint32)    - length of value
//   key_delta (char[])         - unshared portion of key
//   value (char[])             - value data
//
// Block trailer:
//   restarts (uint32[])        - array of restart point offsets
//   num_restarts (uint32)      - number of restart points
//
// Restart points allow binary search within the block by storing
// full keys at regular intervals (restart_interval).

class BlockBuilder {
public:
    explicit BlockBuilder(int restart_interval = kDefaultRestartInterval)
        : restart_interval_(restart_interval),
          counter_(0),
          finished_(false) {
        assert(restart_interval >= 1);
        restarts_.push_back(0);  // First restart at offset 0
    }

    // Reset the builder for reuse
    void Reset() {
        buffer_.clear();
        restarts_.clear();
        restarts_.push_back(0);
        last_key_.clear();
        counter_ = 0;
        finished_ = false;
    }

    // Add a key-value pair (keys must be added in sorted order)
    void Add(Slice key, Slice value) {
        assert(!finished_);
        assert(counter_ <= restart_interval_);
        assert(buffer_.empty() || key.compare(last_key_) > 0);

        size_t shared = 0;
        if (counter_ < restart_interval_) {
            // Calculate shared prefix with previous key
            const size_t min_len = std::min(last_key_.size(), key.size());
            while (shared < min_len && last_key_[shared] == key[shared]) {
                shared++;
            }
        } else {
            // Restart compression - store full key
            restarts_.push_back(static_cast<uint32_t>(buffer_.size()));
            counter_ = 0;
        }

        const size_t non_shared = key.size() - shared;

        // Encode entry
        Varint::PutVarint32(&buffer_, static_cast<uint32_t>(shared));
        Varint::PutVarint32(&buffer_, static_cast<uint32_t>(non_shared));
        Varint::PutVarint32(&buffer_, static_cast<uint32_t>(value.size()));

        // Append key delta and value
        buffer_.append(key.data() + shared, non_shared);
        buffer_.append(value.data(), value.size());

        // Update state
        last_key_.assign(key.data(), key.size());
        counter_++;
    }

    // Finish building the block, returns the block contents
    Slice Finish() {
        // Append restart points
        for (uint32_t restart : restarts_) {
            FixedEncode::PutFixed32(&buffer_, restart);
        }
        FixedEncode::PutFixed32(&buffer_, static_cast<uint32_t>(restarts_.size()));

        finished_ = true;
        return Slice(buffer_);
    }

    // Estimate current size (for deciding when to flush)
    size_t CurrentSizeEstimate() const {
        return buffer_.size() +                           // Current entries
               restarts_.size() * sizeof(uint32_t) +      // Restart array
               sizeof(uint32_t);                          // Num restarts
    }

    // Check if empty
    bool Empty() const {
        return buffer_.empty();
    }

    // Get last key added
    Slice LastKey() const {
        return last_key_;
    }

private:
    std::string buffer_;              // Destination buffer
    std::vector<uint32_t> restarts_;  // Restart points
    std::string last_key_;            // Last key added
    int restart_interval_;            // Keys between restarts
    int counter_;                     // Entries since last restart
    bool finished_;                   // Has Finish() been called?
};

// Builds the index block: maps last key of each data block to its location
class IndexBlockBuilder {
public:
    IndexBlockBuilder() : block_builder_(1) {}  // No prefix compression for index

    void AddEntry(Slice last_key, const BlockHandle& handle) {
        std::string handle_encoding = handle.Encode();
        block_builder_.Add(last_key, handle_encoding);
        entry_count_++;
    }

    Slice Finish() {
        return block_builder_.Finish();
    }

    size_t EntryCount() const { return entry_count_; }

    void Reset() {
        block_builder_.Reset();
        entry_count_ = 0;
    }

private:
    BlockBuilder block_builder_;
    size_t entry_count_ = 0;
};

// Wraps a finished block with type and CRC trailer
class BlockTrailer {
public:
    static std::string AddTrailer(Slice block_contents, BlockType type) {
        std::string result;
        result.reserve(block_contents.size() + kBlockTrailerSize);

        result.append(block_contents.data(), block_contents.size());
        result.push_back(static_cast<char>(type));

        // CRC of block contents + type
        uint32_t crc = wal::CRC32::Compute(result.data(), result.size());
        FixedEncode::PutFixed32(&result, crc);

        return result;
    }

    static bool VerifyTrailer(Slice block_with_trailer, BlockType expected_type) {
        if (block_with_trailer.size() < kBlockTrailerSize) {
            return false;
        }

        size_t contents_size = block_with_trailer.size() - kBlockTrailerSize;
        const char* trailer = block_with_trailer.data() + contents_size;

        BlockType type = static_cast<BlockType>(trailer[0]);
        if (type != expected_type) {
            return false;
        }

        uint32_t stored_crc = FixedEncode::DecodeFixed32(trailer + 1);
        uint32_t computed_crc = wal::CRC32::Compute(
            block_with_trailer.data(), contents_size + 1);

        return stored_crc == computed_crc;
    }
};

}  // namespace sstable
}  // namespace lsm