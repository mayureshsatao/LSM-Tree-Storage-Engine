// sstable/sstable_writer.h
// Writes memtables to SSTable files

#pragma once

#include "util/types.h"
#include "util/bloom_filter.h"
#include "sstable/sstable_format.h"
#include "sstable/block_builder.h"
#include "db/memtable.h"

#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

#include <string>
#include <memory>

namespace lsm {
namespace sstable {

// Statistics collected during SSTable creation
struct SSTableWriteStats {
    size_t data_size = 0;         // Total data block bytes
    size_t index_size = 0;        // Index block bytes
    size_t bloom_size = 0;        // Bloom filter bytes
    size_t num_entries = 0;       // Total key-value pairs
    size_t num_data_blocks = 0;   // Number of data blocks
    size_t raw_key_size = 0;      // Uncompressed key bytes
    size_t raw_value_size = 0;    // Uncompressed value bytes
    SequenceNumber min_seq = kMaxSequenceNumber;
    SequenceNumber max_seq = 0;
};

class SSTableWriter {
public:
    SSTableWriter(const std::string& path, const SSTableOptions& options = SSTableOptions())
        : path_(path),
          options_(options),
          fd_(-1),
          offset_(0),
          data_block_(options.restart_interval),
          bloom_builder_(options.bloom_policy),
          closed_(false),
          num_entries_(0),
          min_sequence_(kMaxSequenceNumber),
          max_sequence_(0) {}

    ~SSTableWriter() {
        if (!closed_) {
            Abandon();
        }
    }

    SSTableWriter(const SSTableWriter&) = delete;
    SSTableWriter& operator=(const SSTableWriter&) = delete;

    // Open the file for writing
    Status Open() {
        fd_ = ::open(path_.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (fd_ < 0) {
            return Status::IOError("Failed to create SSTable: " + path_);
        }
        return Status::OK();
    }

    // Add a key-value entry (must be called in sorted order)
    Status Add(Slice key, Slice value, SequenceNumber seq, ValueType type) {
        if (fd_ < 0) {
            return Status::IOError("SSTable not open");
        }

        // Encode internal key: user_key + sequence + type
        std::string internal_key = EncodeInternalKey(key, seq, type);

        // Track first and last keys
        if (num_entries_ == 0) {
            first_key_.assign(internal_key);
            stats_.min_seq = seq;
        }
        last_key_.assign(internal_key);

        // Update sequence range
        if (seq < min_sequence_) min_sequence_ = seq;
        if (seq > max_sequence_) max_sequence_ = seq;

        // Add to current data block
        data_block_.Add(internal_key, value);
        num_entries_++;

        // Add user key to bloom filter
        if (options_.use_bloom_filter) {
            bloom_builder_.AddKey(key);
        }

        stats_.raw_key_size += key.size();
        stats_.raw_value_size += value.size();

        // Flush block if it's large enough
        if (data_block_.CurrentSizeEstimate() >= options_.block_size) {
            Status s = FlushDataBlock();
            if (!s.ok()) return s;
        }

        return Status::OK();
    }

    // Convenience method: add from internal key struct
    Status Add(const InternalKey& ikey, Slice value) {
        return Add(ikey.user_key, value, ikey.sequence, ikey.type);
    }

    // Finish writing the SSTable
    Status Finish(SSTableWriteStats* stats = nullptr) {
        if (fd_ < 0) {
            return Status::IOError("SSTable not open");
        }

        // Flush any remaining data
        if (!data_block_.Empty()) {
            Status s = FlushDataBlock();
            if (!s.ok()) return s;
        }

        // Write index block
        BlockHandle index_handle;
        Status s = WriteIndexBlock(&index_handle);
        if (!s.ok()) return s;

        // Write bloom filter
        BlockHandle bloom_handle;
        s = WriteBloomFilter(&bloom_handle);
        if (!s.ok()) return s;

        // Write footer
        s = WriteFooter(index_handle, bloom_handle);
        if (!s.ok()) return s;

        // Sync and close
        if (::fsync(fd_) != 0) {
            return Status::IOError("Failed to sync SSTable");
        }
        if (::close(fd_) != 0) {
            return Status::IOError("Failed to close SSTable");
        }
        fd_ = -1;
        closed_ = true;

        // Return stats if requested
        if (stats) {
            *stats = stats_;
            stats->num_entries = num_entries_;
            stats->min_seq = min_sequence_;
            stats->max_seq = max_sequence_;
            stats->bloom_size = stats_.bloom_size;
        }

        return Status::OK();
    }

    // Abandon the file (delete partial writes)
    void Abandon() {
        if (fd_ >= 0) {
            ::close(fd_);
            fd_ = -1;
            ::unlink(path_.c_str());
        }
        closed_ = true;
    }

    // Flush a memtable to an SSTable file (convenience function)
    static Status FlushMemTable(const std::string& path,
                                 MemTable* memtable,
                                 const SSTableOptions& options = SSTableOptions(),
                                 SSTableWriteStats* stats = nullptr) {
        SSTableWriter writer(path, options);

        Status s = writer.Open();
        if (!s.ok()) return s;

        // Iterate memtable in sorted order
        std::unique_ptr<MemTable::Iterator> iter(memtable->NewIterator());
        for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
            s = writer.Add(iter->InternalKey(), iter->Value());
            if (!s.ok()) {
                writer.Abandon();
                return s;
            }
        }

        return writer.Finish(stats);
    }

    const std::string& Path() const { return path_; }
    size_t NumEntries() const { return num_entries_; }

private:
    std::string EncodeInternalKey(Slice user_key, SequenceNumber seq, ValueType type) {
        std::string result;
        result.reserve(user_key.size() + 8);
        result.append(user_key.data(), user_key.size());

        // Pack sequence and type: (seq << 8) | type
        uint64_t packed = (seq << 8) | static_cast<uint8_t>(type);
        char buf[8];
        FixedEncode::EncodeFixed64(buf, packed);
        result.append(buf, 8);

        return result;
    }

    Status FlushDataBlock() {
        if (data_block_.Empty()) {
            return Status::OK();
        }

        // Finish and get block contents
        Slice block_contents = data_block_.Finish();

        // Add trailer (type + CRC)
        std::string block_with_trailer = BlockTrailer::AddTrailer(
            block_contents, BlockType::kData);

        // Record block handle for index
        BlockHandle handle;
        handle.offset = offset_;
        handle.size = block_with_trailer.size();

        // Write to file
        Status s = WriteRaw(block_with_trailer);
        if (!s.ok()) return s;

        // Add to index
        index_builder_.AddEntry(data_block_.LastKey(), handle);

        stats_.data_size += block_with_trailer.size();
        stats_.num_data_blocks++;

        // Reset for next block
        data_block_.Reset();

        return Status::OK();
    }

    Status WriteIndexBlock(BlockHandle* handle) {
        Slice index_contents = index_builder_.Finish();
        std::string block_with_trailer = BlockTrailer::AddTrailer(
            index_contents, BlockType::kIndex);

        handle->offset = offset_;
        handle->size = block_with_trailer.size();

        stats_.index_size = block_with_trailer.size();

        return WriteRaw(block_with_trailer);
    }

    Status WriteBloomFilter(BlockHandle* handle) {
        if (!options_.use_bloom_filter || bloom_builder_.NumKeys() == 0) {
            handle->offset = 0;
            handle->size = 0;
            return Status::OK();
        }

        std::string bloom_data = bloom_builder_.Finish();

        handle->offset = offset_;
        handle->size = bloom_data.size();

        stats_.bloom_size = bloom_data.size();

        return WriteRaw(bloom_data);
    }

    Status WriteFooter(const BlockHandle& index_handle, const BlockHandle& bloom_handle) {
        Footer footer;
        footer.index_handle = index_handle;
        footer.bloom_handle = bloom_handle;
        footer.num_entries = num_entries_;
        footer.min_sequence = min_sequence_;
        footer.max_sequence = max_sequence_;

        // Extract user keys from internal keys
        if (!first_key_.empty()) {
            footer.min_key = ExtractUserKey(first_key_);
        }
        if (!last_key_.empty()) {
            footer.max_key = ExtractUserKey(last_key_);
        }

        std::string footer_data = footer.Encode();
        return WriteRaw(footer_data);
    }

    std::string ExtractUserKey(const std::string& internal_key) {
        if (internal_key.size() < 8) return internal_key;
        return internal_key.substr(0, internal_key.size() - 8);
    }

    Status WriteRaw(const std::string& data) {
        const char* ptr = data.data();
        size_t remaining = data.size();

        while (remaining > 0) {
            ssize_t written = ::write(fd_, ptr, remaining);
            if (written < 0) {
                if (errno == EINTR) continue;
                return Status::IOError("Failed to write to SSTable");
            }
            ptr += written;
            remaining -= written;
            offset_ += written;
        }

        return Status::OK();
    }

    std::string path_;
    SSTableOptions options_;
    int fd_;
    uint64_t offset_;

    BlockBuilder data_block_;
    IndexBlockBuilder index_builder_;
    BloomFilterBuilder bloom_builder_;

    bool closed_;
    size_t num_entries_;
    std::string first_key_;
    std::string last_key_;
    SequenceNumber min_sequence_;
    SequenceNumber max_sequence_;

    SSTableWriteStats stats_;
};

}  // namespace sstable
}  // namespace lsm