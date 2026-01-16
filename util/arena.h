// util/arena.h
// Arena allocator for efficient memtable memory management

#pragma once

#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <vector>

namespace lsm {

class Arena {
public:
    static constexpr size_t kBlockSize = 4096;
    static constexpr size_t kAlignMask = sizeof(void*) - 1;

    Arena() : alloc_ptr_(nullptr), alloc_remaining_(0), memory_usage_(0) {}

    ~Arena() {
        for (char* block : blocks_) {
            delete[] block;
        }
    }

    Arena(const Arena&) = delete;
    Arena& operator=(const Arena&) = delete;

    char* Allocate(size_t bytes) {
        assert(bytes > 0);
        if (bytes <= alloc_remaining_) {
            char* result = alloc_ptr_;
            alloc_ptr_ += bytes;
            alloc_remaining_ -= bytes;
            return result;
        }
        return AllocateFallback(bytes);
    }

    char* AllocateAligned(size_t bytes, size_t alignment = sizeof(void*)) {
        assert((alignment & (alignment - 1)) == 0);

        size_t current = reinterpret_cast<uintptr_t>(alloc_ptr_);
        size_t padding = (alignment - (current & (alignment - 1))) & (alignment - 1);
        size_t needed = bytes + padding;

        char* result;
        if (needed <= alloc_remaining_) {
            result = alloc_ptr_ + padding;
            alloc_ptr_ += needed;
            alloc_remaining_ -= needed;
        } else {
            result = AllocateFallback(bytes);
        }

        assert((reinterpret_cast<uintptr_t>(result) & (alignment - 1)) == 0);
        return result;
    }

    size_t MemoryUsage() const {
        return memory_usage_.load(std::memory_order_relaxed);
    }

    void Reset() {
        if (!blocks_.empty()) {
            alloc_ptr_ = blocks_.front();
            alloc_remaining_ = kBlockSize;
            for (size_t i = 1; i < blocks_.size(); ++i) {
                delete[] blocks_[i];
            }
            blocks_.resize(1);
            memory_usage_.store(kBlockSize, std::memory_order_relaxed);
        }
    }

private:
    char* AllocateFallback(size_t bytes) {
        if (bytes > kBlockSize / 4) {
            return AllocateNewBlock(bytes);
        }
        alloc_ptr_ = AllocateNewBlock(kBlockSize);
        alloc_remaining_ = kBlockSize;
        char* result = alloc_ptr_;
        alloc_ptr_ += bytes;
        alloc_remaining_ -= bytes;
        return result;
    }

    char* AllocateNewBlock(size_t block_bytes) {
        char* result = new char[block_bytes];
        blocks_.push_back(result);
        memory_usage_.fetch_add(block_bytes + sizeof(char*),
                                std::memory_order_relaxed);
        return result;
    }

    char* alloc_ptr_;
    size_t alloc_remaining_;
    std::vector<char*> blocks_;
    std::atomic<size_t> memory_usage_;
};

}  // namespace lsm