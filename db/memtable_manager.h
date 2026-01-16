// db/memtable_manager.h
// Manages active and immutable memtables with atomic rotation

#pragma once

#include "util/types.h"
#include "db/memtable.h"

#include <atomic>
#include <deque>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <condition_variable>
#include <functional>

namespace lsm {

using FlushCallback = std::function<void(MemTable*)>;

class MemTableManager {
public:
    explicit MemTableManager(const MemTableOptions& options = MemTableOptions())
        : options_(options),
          current_sequence_(0),
          total_memory_usage_(0),
          immutable_count_(0) {
        active_ = new MemTable(options_);
        active_->Ref();
    }

    ~MemTableManager() {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        if (active_) {
            active_->Unref();
            active_ = nullptr;
        }
        for (MemTable* imm : immutables_) {
            imm->Unref();
        }
        immutables_.clear();
    }

    MemTableManager(const MemTableManager&) = delete;
    MemTableManager& operator=(const MemTableManager&) = delete;

    Status Put(Slice key, Slice value, bool* rotated = nullptr) {
        return Write(ValueType::kValue, key, value, rotated);
    }

    Status Delete(Slice key, bool* rotated = nullptr) {
        return Write(ValueType::kDeletion, key, Slice(), rotated);
    }

    LookupResult Get(Slice key) const {
        SequenceNumber snapshot = current_sequence_.load(std::memory_order_acquire);
        return Get(key, snapshot);
    }

    LookupResult Get(Slice key, SequenceNumber snapshot) const {
        std::shared_lock<std::shared_mutex> lock(mutex_);

        LookupResult result = active_->Get(key, snapshot);
        if (result.found) {
            return result;
        }

        for (auto it = immutables_.rbegin(); it != immutables_.rend(); ++it) {
            result = (*it)->Get(key, snapshot);
            if (result.found) {
                return result;
            }
        }

        return LookupResult::NotFound();
    }

    Status ForceRotation() {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        return RotateLocked();
    }

    void RemoveFlushedMemTable() {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        if (!immutables_.empty()) {
            MemTable* oldest = immutables_.front();
            immutables_.pop_front();

            size_t mem = oldest->ApproximateMemoryUsage();
            total_memory_usage_.fetch_sub(mem, std::memory_order_relaxed);
            immutable_count_.fetch_sub(1, std::memory_order_relaxed);

            oldest->Unref();
            flush_cv_.notify_all();
        }
    }

    MemTable* GetOldestImmutable() const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        if (immutables_.empty()) {
            return nullptr;
        }
        MemTable* oldest = immutables_.front();
        oldest->Ref();
        return oldest;
    }

    bool WaitForFlush(size_t max_immutables, std::chrono::milliseconds timeout) {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        return flush_cv_.wait_for(lock, timeout, [&]() {
            return immutables_.size() < max_immutables;
        });
    }

    SequenceNumber CurrentSequence() const {
        return current_sequence_.load(std::memory_order_acquire);
    }

    SequenceNumber AllocateSequence() {
        return current_sequence_.fetch_add(1, std::memory_order_acq_rel);
    }

    size_t TotalMemoryUsage() const {
        return total_memory_usage_.load(std::memory_order_relaxed);
    }

    size_t ImmutableCount() const {
        return immutable_count_.load(std::memory_order_relaxed);
    }

    size_t ActiveMemoryUsage() const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        return active_->ApproximateMemoryUsage();
    }

    void SetFlushCallback(FlushCallback callback) {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        flush_callback_ = std::move(callback);
    }

    struct MemTableSet {
        std::vector<MemTable*> tables;
        SequenceNumber snapshot;

        ~MemTableSet() {
            for (MemTable* t : tables) {
                t->Unref();
            }
        }
    };

    std::unique_ptr<MemTableSet> GetCurrentMemTables() const {
        std::shared_lock<std::shared_mutex> lock(mutex_);

        auto set = std::make_unique<MemTableSet>();
        set->snapshot = current_sequence_.load(std::memory_order_acquire);

        active_->Ref();
        set->tables.push_back(active_);

        for (MemTable* imm : immutables_) {
            imm->Ref();
            set->tables.push_back(imm);
        }

        return set;
    }

private:
    Status Write(ValueType type, Slice key, Slice value, bool* rotated) {
        if (rotated) *rotated = false;

        std::unique_lock<std::shared_mutex> lock(mutex_);

        if (active_->ShouldFlush()) {
            Status s = RotateLocked();
            if (!s.ok()) return s;
            if (rotated) *rotated = true;
        }

        SequenceNumber seq = current_sequence_.fetch_add(1, std::memory_order_acq_rel);

        if (type == ValueType::kValue) {
            active_->Put(seq, key, value);
        } else {
            active_->Delete(seq, key);
        }

        size_t entry_size = key.size() + value.size() + 32;
        total_memory_usage_.fetch_add(entry_size, std::memory_order_relaxed);

        return Status::OK();
    }

    Status RotateLocked() {
        MemTable* imm = active_;
        immutables_.push_back(imm);
        immutable_count_.fetch_add(1, std::memory_order_relaxed);

        active_ = new MemTable(options_);
        active_->Ref();

        if (flush_callback_) {
            flush_callback_(imm);
        }

        return Status::OK();
    }

    MemTableOptions options_;

    mutable std::shared_mutex mutex_;
    MemTable* active_;
    std::deque<MemTable*> immutables_;

    std::atomic<SequenceNumber> current_sequence_;
    std::atomic<size_t> total_memory_usage_;
    std::atomic<size_t> immutable_count_;

    std::condition_variable_any flush_cv_;
    FlushCallback flush_callback_;
};

}  // namespace lsm