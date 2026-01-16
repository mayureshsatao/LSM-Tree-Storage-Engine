// db/memtable.h
// Memtable implementation using skip list

#pragma once

#include "util/types.h"
#include "util/arena.h"
#include "memtable/skiplist.h"

#include <atomic>
#include <mutex>
#include <shared_mutex>
#include <string>

namespace lsm {

struct MemTableEntry {
    InternalKey internal_key;
    std::string value;

    MemTableEntry() = default;
    MemTableEntry(const InternalKey& k, Slice v)
        : internal_key(k), value(v) {}
};

struct MemTableKeyComparator {
    int operator()(const MemTableEntry& a, const MemTableEntry& b) const {
        int r = a.internal_key.user_key.compare(b.internal_key.user_key);
        if (r != 0) return r;
        if (a.internal_key.sequence > b.internal_key.sequence) return -1;
        if (a.internal_key.sequence < b.internal_key.sequence) return +1;
        return 0;
    }
};

class MemTable {
public:
    explicit MemTable(const MemTableOptions& options = MemTableOptions())
        : options_(options),
          arena_(new Arena()),
          table_(MemTableKeyComparator(), arena_.get()),
          refs_(0),
          approximate_memory_usage_(0),
          entry_count_(0),
          min_sequence_(kMaxSequenceNumber),
          max_sequence_(0) {}

    void Ref() { ++refs_; }

    void Unref() {
        --refs_;
        assert(refs_ >= 0);
        if (refs_ == 0) {
            delete this;
        }
    }

    void Put(SequenceNumber seq, Slice key, Slice value) {
        Add(seq, ValueType::kValue, key, value);
    }

    void Delete(SequenceNumber seq, Slice key) {
        Add(seq, ValueType::kDeletion, key, Slice());
    }

    LookupResult Get(Slice key, SequenceNumber snapshot_seq) const {
        MemTableEntry lookup_key(InternalKey(key, snapshot_seq, ValueType::kValue), "");

        Table::Iterator iter(&table_);
        iter.Seek(lookup_key);

        if (iter.Valid()) {
            const MemTableEntry& entry = iter.key();
            if (entry.internal_key.user_key == key) {
                if (entry.internal_key.type == ValueType::kDeletion) {
                    return LookupResult::Deleted();
                } else {
                    return LookupResult::Found(entry.value);
                }
            }
        }
        return LookupResult::NotFound();
    }

    size_t ApproximateMemoryUsage() const {
        return approximate_memory_usage_.load(std::memory_order_relaxed);
    }

    bool ShouldFlush() const {
        return ApproximateMemoryUsage() >= options_.max_size;
    }

    size_t EntryCount() const {
        return entry_count_.load(std::memory_order_relaxed);
    }

    SequenceNumber MinSequence() const {
        return min_sequence_.load(std::memory_order_relaxed);
    }

    SequenceNumber MaxSequence() const {
        return max_sequence_.load(std::memory_order_relaxed);
    }

    class Iterator {
    public:
        explicit Iterator(const MemTable* mem) : iter_(&mem->table_) {}

        bool Valid() const { return iter_.Valid(); }
        void SeekToFirst() { iter_.SeekToFirst(); }
        void SeekToLast() { iter_.SeekToLast(); }
        void Seek(const InternalKey& target) {
            MemTableEntry entry(target, "");
            iter_.Seek(entry);
        }
        void Next() { iter_.Next(); }
        void Prev() { iter_.Prev(); }

        Slice UserKey() const { return iter_.key().internal_key.user_key; }
        SequenceNumber Sequence() const { return iter_.key().internal_key.sequence; }
        ValueType Type() const { return iter_.key().internal_key.type; }
        Slice Value() const { return iter_.key().value; }

        const InternalKey& InternalKey() const {
            return iter_.key().internal_key;
        }

    private:
        using TableIterator = SkipList<MemTableEntry, MemTableKeyComparator>::Iterator;
        TableIterator iter_;
    };

    Iterator* NewIterator() const {
        return new Iterator(this);
    }

private:
    ~MemTable() = default;

    void Add(SequenceNumber seq, ValueType type, Slice key, Slice value) {
        MemTableEntry entry(InternalKey(key, seq, type), value);
        table_.Insert(entry);

        size_t entry_size = key.size() + value.size() +
                           sizeof(SequenceNumber) + sizeof(ValueType) +
                           sizeof(MemTableEntry);
        approximate_memory_usage_.fetch_add(entry_size, std::memory_order_relaxed);
        entry_count_.fetch_add(1, std::memory_order_relaxed);

        SequenceNumber expected = min_sequence_.load(std::memory_order_relaxed);
        while (seq < expected &&
               !min_sequence_.compare_exchange_weak(expected, seq,
                   std::memory_order_relaxed));

        expected = max_sequence_.load(std::memory_order_relaxed);
        while (seq > expected &&
               !max_sequence_.compare_exchange_weak(expected, seq,
                   std::memory_order_relaxed));
    }

    using Table = SkipList<MemTableEntry, MemTableKeyComparator>;

    MemTableOptions options_;
    std::unique_ptr<Arena> arena_;
    Table table_;
    std::atomic<int> refs_;
    std::atomic<size_t> approximate_memory_usage_;
    std::atomic<size_t> entry_count_;
    std::atomic<SequenceNumber> min_sequence_;
    std::atomic<SequenceNumber> max_sequence_;
};

}  // namespace lsm