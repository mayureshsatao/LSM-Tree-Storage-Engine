// util/types.h
// Core type definitions for the LSM-Tree Storage Engine

#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <optional>
#include <variant>
#include <memory>
#include <functional>

namespace lsm {

// Sequence numbers for MVCC and ordering
using SequenceNumber = uint64_t;
constexpr SequenceNumber kMaxSequenceNumber = UINT64_MAX;

// Type aliases for keys and values
using Slice = std::string_view;

// Value types for distinguishing puts from deletes
enum class ValueType : uint8_t {
    kValue = 0x01,      // Regular key-value entry
    kDeletion = 0x02,   // Tombstone marker
};

// Internal key format: user_key + sequence_number + value_type
struct InternalKey {
    std::string user_key;
    SequenceNumber sequence;
    ValueType type;

    InternalKey() : sequence(0), type(ValueType::kValue) {}

    InternalKey(Slice key, SequenceNumber seq, ValueType t)
        : user_key(key), sequence(seq), type(t) {}

    // Comparison: sort by user_key ascending, then sequence descending
    bool operator<(const InternalKey& other) const {
        int cmp = user_key.compare(other.user_key);
        if (cmp != 0) return cmp < 0;
        return sequence > other.sequence;
    }

    bool operator==(const InternalKey& other) const {
        return user_key == other.user_key &&
               sequence == other.sequence &&
               type == other.type;
    }
};

// Result of a lookup operation
struct LookupResult {
    bool found;
    bool is_deleted;
    std::string value;

    static LookupResult NotFound() {
        return {false, false, {}};
    }

    static LookupResult Found(std::string val) {
        return {true, false, std::move(val)};
    }

    static LookupResult Deleted() {
        return {true, true, {}};
    }
};

// Status codes for operations
enum class StatusCode {
    kOk = 0,
    kNotFound,
    kCorruption,
    kNotSupported,
    kInvalidArgument,
    kIOError,
    kMemoryLimit,
};

class Status {
public:
    Status() : code_(StatusCode::kOk) {}
    Status(StatusCode code) : code_(code) {}
    Status(StatusCode code, std::string msg)
        : code_(code), message_(std::move(msg)) {}

    static Status OK() { return Status(); }
    static Status NotFound(std::string msg = "") {
        return Status(StatusCode::kNotFound, std::move(msg));
    }
    static Status Corruption(std::string msg = "") {
        return Status(StatusCode::kCorruption, std::move(msg));
    }
    static Status IOError(std::string msg = "") {
        return Status(StatusCode::kIOError, std::move(msg));
    }
    static Status MemoryLimit(std::string msg = "") {
        return Status(StatusCode::kMemoryLimit, std::move(msg));
    }

    bool ok() const { return code_ == StatusCode::kOk; }
    bool IsNotFound() const { return code_ == StatusCode::kNotFound; }
    bool IsCorruption() const { return code_ == StatusCode::kCorruption; }
    bool IsMemoryLimit() const { return code_ == StatusCode::kMemoryLimit; }

    StatusCode code() const { return code_; }
    const std::string& message() const { return message_; }

    std::string ToString() const {
        static const char* names[] = {
            "OK", "NotFound", "Corruption", "NotSupported",
            "InvalidArgument", "IOError", "MemoryLimit"
        };
        std::string result = names[static_cast<int>(code_)];
        if (!message_.empty()) {
            result += ": " + message_;
        }
        return result;
    }

private:
    StatusCode code_;
    std::string message_;
};

// Configuration for memtable behavior
struct MemTableOptions {
    size_t max_size = 4 * 1024 * 1024;  // 4MB default
    int max_height = 12;
    int branching_factor = 4;
};

// Statistics for monitoring
struct MemTableStats {
    size_t entry_count = 0;
    size_t memory_usage = 0;
    size_t total_key_bytes = 0;
    size_t total_value_bytes = 0;
    uint64_t write_count = 0;
    uint64_t read_count = 0;
};

// Forward declarations
class MemTable;
class MemTableIterator;

}  // namespace lsm