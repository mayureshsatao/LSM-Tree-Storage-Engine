// wal/wal_writer.h
// WAL writer with configurable sync policies

#pragma once

#include "util/types.h"
#include "wal/wal_format.h"

#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

#include <atomic>
#include <mutex>
#include <string>
#include <memory>
#include <chrono>
#include <thread>
#include <condition_variable>

namespace lsm {
namespace wal {

// Sync policy for durability vs performance tradeoff
enum class SyncPolicy {
    kSyncPerWrite,    // fsync after every write (safest)
    kSyncBatched,     // fsync after batch of writes
    kSyncPeriodic,    // fsync on background thread
    kNoSync,          // OS decides when to flush (fastest, least safe)
};

struct WALOptions {
    SyncPolicy sync_policy = SyncPolicy::kSyncPerWrite;
    size_t sync_batch_size = 1024 * 1024;       // 1MB batch for batched sync
    std::chrono::milliseconds sync_interval{100}; // For periodic sync
    size_t max_file_size = 64 * 1024 * 1024;    // 64MB max log file
};

class WALWriter {
public:
    WALWriter(const std::string& path, const WALOptions& options = WALOptions())
        : path_(path),
          options_(options),
          fd_(-1),
          file_size_(0),
          bytes_since_sync_(0),
          closed_(false),
          sync_requested_(false) {}

    ~WALWriter() {
        Close();
    }

    WALWriter(const WALWriter&) = delete;
    WALWriter& operator=(const WALWriter&) = delete;

    Status Open() {
        std::lock_guard<std::mutex> lock(mutex_);

        fd_ = ::open(path_.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (fd_ < 0) {
            return Status::IOError("Failed to open WAL: " + path_);
        }

        // Get current file size
        struct stat st;
        if (::fstat(fd_, &st) == 0) {
            file_size_ = st.st_size;
        }

        // Start background sync thread if periodic
        if (options_.sync_policy == SyncPolicy::kSyncPeriodic) {
            StartSyncThread();
        }

        return Status::OK();
    }

    Status Close() {
        std::unique_lock<std::mutex> lock(mutex_);

        if (closed_) return Status::OK();
        closed_ = true;

        // Stop sync thread
        if (sync_thread_.joinable()) {
            lock.unlock();
            sync_cv_.notify_all();
            sync_thread_.join();
            lock.lock();
        }

        // Final sync and close
        if (fd_ >= 0) {
            ::fsync(fd_);
            ::close(fd_);
            fd_ = -1;
        }

        return Status::OK();
    }

    // Append a single entry
    Status Append(const WALEntry& entry) {
        std::string payload = EncodeWALEntry(entry);
        return AppendRecord(payload);
    }

    // Append a Put operation
    Status AppendPut(SequenceNumber seq, Slice key, Slice value) {
        WALEntry entry;
        entry.type = WALEntryType::kPut;
        entry.sequence = seq;
        entry.key = std::string(key);
        entry.value = std::string(value);
        return Append(entry);
    }

    // Append a Delete operation
    Status AppendDelete(SequenceNumber seq, Slice key) {
        WALEntry entry;
        entry.type = WALEntryType::kDelete;
        entry.sequence = seq;
        entry.key = std::string(key);
        return Append(entry);
    }

    // Force sync to disk
    Status Sync() {
        std::lock_guard<std::mutex> lock(mutex_);
        return SyncLocked();
    }

    // Get current file size
    size_t FileSize() const {
        return file_size_.load(std::memory_order_relaxed);
    }

    // Check if file exceeds max size
    bool ShouldRotate() const {
        return FileSize() >= options_.max_file_size;
    }

    const std::string& Path() const { return path_; }

private:
    Status AppendRecord(const std::string& payload) {
        std::lock_guard<std::mutex> lock(mutex_);

        if (fd_ < 0) {
            return Status::IOError("WAL not open");
        }

        // Build record: CRC32 | Length | Type | Payload
        std::string record;
        record.reserve(kHeaderSize + payload.size());

        // Placeholder for CRC (will fill in after)
        size_t crc_pos = record.size();
        record.append(4, '\0');

        // Length (16-bit)
        uint16_t len = static_cast<uint16_t>(payload.size());
        record.push_back(len & 0xff);
        record.push_back((len >> 8) & 0xff);

        // Type (always full record for now)
        record.push_back(static_cast<char>(RecordType::kFull));

        // Payload
        record.append(payload);

        // Compute CRC over type + payload
        uint32_t crc = CRC32::Compute(record.data() + crc_pos + 6,
                                       record.size() - crc_pos - 6);
        // Also include length in CRC
        crc = CRC32::Update(crc ^ 0xFFFFFFFF, record.data() + crc_pos + 4, 2) ^ 0xFFFFFFFF;

        record[crc_pos] = crc & 0xff;
        record[crc_pos + 1] = (crc >> 8) & 0xff;
        record[crc_pos + 2] = (crc >> 16) & 0xff;
        record[crc_pos + 3] = (crc >> 24) & 0xff;

        // Write to file
        ssize_t written = ::write(fd_, record.data(), record.size());
        if (written != static_cast<ssize_t>(record.size())) {
            return Status::IOError("Failed to write WAL record");
        }

        file_size_.fetch_add(record.size(), std::memory_order_relaxed);
        bytes_since_sync_ += record.size();

        // Handle sync based on policy
        return HandleSync();
    }

    Status HandleSync() {
        switch (options_.sync_policy) {
            case SyncPolicy::kSyncPerWrite:
                return SyncLocked();

            case SyncPolicy::kSyncBatched:
                if (bytes_since_sync_ >= options_.sync_batch_size) {
                    return SyncLocked();
                }
                break;

            case SyncPolicy::kSyncPeriodic:
                sync_requested_ = true;
                sync_cv_.notify_one();
                break;

            case SyncPolicy::kNoSync:
                break;
        }
        return Status::OK();
    }

    Status SyncLocked() {
        if (fd_ >= 0 && bytes_since_sync_ > 0) {
            if (::fsync(fd_) != 0) {
                return Status::IOError("Failed to fsync WAL");
            }
            bytes_since_sync_ = 0;
        }
        return Status::OK();
    }

    void StartSyncThread() {
        sync_thread_ = std::thread([this]() {
            std::unique_lock<std::mutex> lock(mutex_);
            while (!closed_) {
                sync_cv_.wait_for(lock, options_.sync_interval, [this]() {
                    return closed_ || sync_requested_;
                });

                if (!closed_ && bytes_since_sync_ > 0) {
                    SyncLocked();
                }
                sync_requested_ = false;
            }
        });
    }

    std::string path_;
    WALOptions options_;

    std::mutex mutex_;
    int fd_;
    std::atomic<size_t> file_size_;
    size_t bytes_since_sync_;

    bool closed_;
    bool sync_requested_;
    std::thread sync_thread_;
    std::condition_variable sync_cv_;
};

}  // namespace wal
}  // namespace lsm