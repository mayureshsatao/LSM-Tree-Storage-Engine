// wal/wal_manager.h
// Manages WAL segments, rotation, and recovery

#pragma once

#include "util/types.h"
#include "wal/wal_format.h"
#include "wal/wal_writer.h"
#include "wal/wal_reader.h"
#include "db/memtable.h"

#include <dirent.h>
#include <sys/stat.h>
#include <cstdio>

#include <algorithm>
#include <memory>
#include <mutex>
#include <vector>
#include <regex>

namespace lsm {
namespace wal {

class WALManager {
public:
    WALManager(const std::string& db_path, const WALOptions& options = WALOptions())
        : db_path_(db_path),
          options_(options),
          current_log_number_(0) {}

    ~WALManager() {
        Close();
    }

    WALManager(const WALManager&) = delete;
    WALManager& operator=(const WALManager&) = delete;

    // Initialize WAL manager, creating directory if needed
    Status Open() {
        std::lock_guard<std::mutex> lock(mutex_);

        // Create WAL directory
        std::string wal_dir = WalDir();
        if (::mkdir(wal_dir.c_str(), 0755) != 0 && errno != EEXIST) {
            return Status::IOError("Failed to create WAL directory");
        }

        // Find existing WAL files
        std::vector<uint64_t> log_numbers;
        Status s = ListLogFiles(&log_numbers);
        if (!s.ok()) return s;

        // Set current log number
        if (!log_numbers.empty()) {
            current_log_number_ = log_numbers.back();
        }

        // Open or create current WAL
        return OpenNewLog();
    }

    void Close() {
        std::lock_guard<std::mutex> lock(mutex_);
        if (current_writer_) {
            current_writer_->Close();
            current_writer_.reset();
        }
    }

    // Append a write operation
    Status Append(const WALEntry& entry) {
        std::lock_guard<std::mutex> lock(mutex_);

        if (!current_writer_) {
            return Status::IOError("WAL not open");
        }

        // Check if rotation needed
        if (current_writer_->ShouldRotate()) {
            Status s = RotateLocked();
            if (!s.ok()) return s;
        }

        return current_writer_->Append(entry);
    }

    Status AppendPut(SequenceNumber seq, Slice key, Slice value) {
        WALEntry entry{WALEntryType::kPut, seq, std::string(key), std::string(value)};
        return Append(entry);
    }

    Status AppendDelete(SequenceNumber seq, Slice key) {
        WALEntry entry{WALEntryType::kDelete, seq, std::string(key), {}};
        return Append(entry);
    }

    // Force sync
    Status Sync() {
        std::lock_guard<std::mutex> lock(mutex_);
        if (current_writer_) {
            return current_writer_->Sync();
        }
        return Status::OK();
    }

    // Rotate to new log file
    Status Rotate() {
        std::lock_guard<std::mutex> lock(mutex_);
        return RotateLocked();
    }

    // Recover memtable from WAL files
    Status Recover(MemTable* memtable, RecoveryStats* stats = nullptr) {
        std::lock_guard<std::mutex> lock(mutex_);

        auto start = std::chrono::high_resolution_clock::now();
        RecoveryStats local_stats;

        // Find all log files
        std::vector<uint64_t> log_numbers;
        Status s = ListLogFiles(&log_numbers);
        if (!s.ok()) return s;

        // Replay each log in order
        for (uint64_t log_num : log_numbers) {
            std::string path = LogPath(log_num);
            WALReader reader(path);

            s = reader.Open();
            if (!s.ok()) {
                // Skip unreadable logs with warning
                continue;
            }

            WALEntry entry;
            Status read_status;
            while (reader.ReadEntry(&entry, &read_status)) {
                local_stats.records_read++;

                if (entry.type == WALEntryType::kPut) {
                    memtable->Put(entry.sequence, entry.key, entry.value);
                    local_stats.puts_recovered++;
                } else if (entry.type == WALEntryType::kDelete) {
                    memtable->Delete(entry.sequence, entry.key);
                    local_stats.deletes_recovered++;
                }

                if (entry.sequence > local_stats.max_sequence) {
                    local_stats.max_sequence = entry.sequence;
                }
            }

            local_stats.bytes_read += reader.Size();

            if (!read_status.ok() && !read_status.IsCorruption()) {
                return read_status;
            }
            // Corruption at end of log is expected (crash during write)
        }

        auto end = std::chrono::high_resolution_clock::now();
        local_stats.duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);

        if (stats) {
            *stats = local_stats;
        }

        return Status::OK();
    }

    // Mark logs up to given number as obsolete (after flush)
    Status MarkFlushed(uint64_t flushed_log_number) {
        std::lock_guard<std::mutex> lock(mutex_);

        std::vector<uint64_t> log_numbers;
        Status s = ListLogFiles(&log_numbers);
        if (!s.ok()) return s;

        // Delete logs older than flushed
        for (uint64_t log_num : log_numbers) {
            if (log_num < flushed_log_number) {
                std::string path = LogPath(log_num);
                if (::unlink(path.c_str()) != 0 && errno != ENOENT) {
                    return Status::IOError("Failed to delete old WAL: " + path);
                }
            }
        }

        return Status::OK();
    }

    // Get current log number
    uint64_t CurrentLogNumber() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return current_log_number_;
    }

    // Get all log numbers
    Status GetLogNumbers(std::vector<uint64_t>* numbers) const {
        std::lock_guard<std::mutex> lock(mutex_);
        return ListLogFiles(numbers);
    }

private:
    std::string WalDir() const {
        return db_path_ + "/wal";
    }

    std::string LogPath(uint64_t number) const {
        char buf[32];
        snprintf(buf, sizeof(buf), "/log.%06llu", static_cast<unsigned long long>(number));
        return WalDir() + buf;
    }

    Status ListLogFiles(std::vector<uint64_t>* numbers) const {
        numbers->clear();

        DIR* dir = ::opendir(WalDir().c_str());
        if (dir == nullptr) {
            if (errno == ENOENT) {
                return Status::OK();  // No WAL dir yet
            }
            return Status::IOError("Failed to open WAL directory");
        }

        std::regex log_pattern("log\\.(\\d{6})");
        struct dirent* entry;
        while ((entry = ::readdir(dir)) != nullptr) {
            std::cmatch match;
            if (std::regex_match(entry->d_name, match, log_pattern)) {
                numbers->push_back(std::stoull(match[1].str()));
            }
        }
        ::closedir(dir);

        std::sort(numbers->begin(), numbers->end());
        return Status::OK();
    }

    Status OpenNewLog() {
        current_log_number_++;
        std::string path = LogPath(current_log_number_);

        current_writer_ = std::make_unique<WALWriter>(path, options_);
        return current_writer_->Open();
    }

    Status RotateLocked() {
        if (current_writer_) {
            current_writer_->Sync();
            current_writer_->Close();
        }
        return OpenNewLog();
    }

    std::string db_path_;
    WALOptions options_;

    mutable std::mutex mutex_;
    uint64_t current_log_number_;
    std::unique_ptr<WALWriter> current_writer_;
};

}  // namespace wal
}  // namespace lsm