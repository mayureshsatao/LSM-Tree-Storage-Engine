// wal/wal_reader.h
// WAL reader for recovery and iteration

#pragma once

#include "util/types.h"
#include "wal/wal_format.h"

#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/mman.h>

#include <string>
#include <vector>
#include <functional>

namespace lsm {
namespace wal {

// Result of reading a single record
struct ReadResult {
    Status status;
    bool eof;
    std::string payload;

    static ReadResult OK(std::string data) {
        return {Status::OK(), false, std::move(data)};
    }
    static ReadResult Eof() {
        return {Status::OK(), true, {}};
    }
    static ReadResult Error(Status s) {
        return {std::move(s), false, {}};
    }
};

class WALReader {
public:
    explicit WALReader(const std::string& path)
        : path_(path), fd_(-1), data_(nullptr), size_(0), pos_(0) {}

    ~WALReader() {
        Close();
    }

    WALReader(const WALReader&) = delete;
    WALReader& operator=(const WALReader&) = delete;

    Status Open() {
        fd_ = ::open(path_.c_str(), O_RDONLY);
        if (fd_ < 0) {
            return Status::IOError("Failed to open WAL for reading: " + path_);
        }

        struct stat st;
        if (::fstat(fd_, &st) != 0) {
            Close();
            return Status::IOError("Failed to stat WAL file");
        }
        size_ = st.st_size;

        if (size_ == 0) {
            return Status::OK();  // Empty file is valid
        }

        // Memory map the file for efficient reading
        data_ = static_cast<char*>(
            ::mmap(nullptr, size_, PROT_READ, MAP_PRIVATE, fd_, 0));
        if (data_ == MAP_FAILED) {
            data_ = nullptr;
            Close();
            return Status::IOError("Failed to mmap WAL file");
        }

        // Advise sequential access
        ::madvise(data_, size_, MADV_SEQUENTIAL);

        return Status::OK();
    }

    void Close() {
        if (data_ != nullptr) {
            ::munmap(data_, size_);
            data_ = nullptr;
        }
        if (fd_ >= 0) {
            ::close(fd_);
            fd_ = -1;
        }
        size_ = 0;
        pos_ = 0;
    }

    // Read next record
    ReadResult ReadRecord() {
        if (data_ == nullptr || pos_ >= size_) {
            return ReadResult::Eof();
        }

        // Need at least header
        if (pos_ + kHeaderSize > size_) {
            return ReadResult::Error(Status::Corruption("Truncated record header"));
        }

        // Parse header
        const char* header = data_ + pos_;

        uint32_t stored_crc = static_cast<uint8_t>(header[0]) |
                              (static_cast<uint8_t>(header[1]) << 8) |
                              (static_cast<uint8_t>(header[2]) << 16) |
                              (static_cast<uint8_t>(header[3]) << 24);

        uint16_t length = static_cast<uint8_t>(header[4]) |
                          (static_cast<uint8_t>(header[5]) << 8);

        RecordType type = static_cast<RecordType>(header[6]);

        // Validate length
        if (pos_ + kHeaderSize + length > size_) {
            return ReadResult::Error(Status::Corruption("Truncated record payload"));
        }

        // Verify CRC
        uint32_t computed_crc = CRC32::Compute(header + 6, 1 + length);
        computed_crc = CRC32::Update(computed_crc ^ 0xFFFFFFFF, header + 4, 2) ^ 0xFFFFFFFF;

        if (stored_crc != computed_crc) {
            return ReadResult::Error(Status::Corruption("CRC mismatch in WAL record"));
        }

        // Validate type
        if (type != RecordType::kFull) {
            // For now, only support full records
            return ReadResult::Error(Status::Corruption("Unsupported record type"));
        }

        // Extract payload
        std::string payload(header + kHeaderSize, length);
        pos_ += kHeaderSize + length;

        return ReadResult::OK(std::move(payload));
    }

    // Read and decode next entry
    bool ReadEntry(WALEntry* entry, Status* status) {
        ReadResult result = ReadRecord();

        if (result.eof) {
            *status = Status::OK();
            return false;
        }

        if (!result.status.ok()) {
            *status = result.status;
            return false;
        }

        if (!DecodeWALEntry(result.payload, entry)) {
            *status = Status::Corruption("Failed to decode WAL entry");
            return false;
        }

        *status = Status::OK();
        return true;
    }

    // Iterate over all entries with callback
    using EntryCallback = std::function<bool(const WALEntry&)>;

    Status ForEach(EntryCallback callback) {
        WALEntry entry;
        Status status;

        while (ReadEntry(&entry, &status)) {
            if (!callback(entry)) {
                break;
            }
        }

        return status;
    }

    // Reset to beginning
    void Reset() {
        pos_ = 0;
    }

    // Get current position
    size_t Position() const { return pos_; }

    // Get file size
    size_t Size() const { return size_; }

    // Check if at end
    bool AtEnd() const { return pos_ >= size_; }

private:
    std::string path_;
    int fd_;
    char* data_;
    size_t size_;
    size_t pos_;
};

// Recovery statistics
struct RecoveryStats {
    size_t records_read = 0;
    size_t bytes_read = 0;
    size_t puts_recovered = 0;
    size_t deletes_recovered = 0;
    SequenceNumber max_sequence = 0;
    std::chrono::microseconds duration{0};
};

}  // namespace wal
}  // namespace lsm