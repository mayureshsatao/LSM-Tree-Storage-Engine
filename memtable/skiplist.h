// memtable/skiplist.h
// Lock-free skip list for LSM-tree memtables

#pragma once

#include "util/arena.h"
#include "util/types.h"

#include <atomic>
#include <cassert>
#include <cstdlib>
#include <random>

namespace lsm {

template <typename Key, typename Comparator>
class SkipList {
public:
    static constexpr int kMaxHeight = 12;

private:
    struct Node {
        Key const key;

        explicit Node(const Key& k) : key(k) {}

        Node* Next(int level) {
            assert(level >= 0);
            return next_[level].load(std::memory_order_acquire);
        }

        void SetNext(int level, Node* x) {
            assert(level >= 0);
            next_[level].store(x, std::memory_order_release);
        }

        Node* NoBarrier_Next(int level) {
            return next_[level].load(std::memory_order_relaxed);
        }

        void NoBarrier_SetNext(int level, Node* x) {
            next_[level].store(x, std::memory_order_relaxed);
        }

    private:
        std::atomic<Node*> next_[1];
    };

public:
    explicit SkipList(Comparator cmp, Arena* arena)
        : compare_(cmp),
          arena_(arena),
          head_(NewNode(Key(), kMaxHeight)),
          max_height_(1),
          rng_(std::random_device{}()) {
        for (int i = 0; i < kMaxHeight; i++) {
            head_->SetNext(i, nullptr);
        }
    }

    SkipList(const SkipList&) = delete;
    SkipList& operator=(const SkipList&) = delete;

    void Insert(const Key& key) {
        Node* prev[kMaxHeight];
        Node* x = FindGreaterOrEqual(key, prev);

        assert(x == nullptr || !Equal(key, x->key));

        int height = RandomHeight();
        if (height > GetMaxHeight()) {
            for (int i = GetMaxHeight(); i < height; i++) {
                prev[i] = head_;
            }
            max_height_.store(height, std::memory_order_relaxed);
        }

        x = NewNode(key, height);
        for (int i = 0; i < height; i++) {
            x->NoBarrier_SetNext(i, prev[i]->NoBarrier_Next(i));
            prev[i]->SetNext(i, x);
        }
    }

    bool Contains(const Key& key) const {
        Node* x = FindGreaterOrEqual(key, nullptr);
        return x != nullptr && Equal(key, x->key);
    }

    Node* FindGreaterOrEqual(const Key& key) const {
        return FindGreaterOrEqual(key, nullptr);
    }

    class Iterator {
    public:
        explicit Iterator(const SkipList* list) : list_(list), node_(nullptr) {}

        bool Valid() const { return node_ != nullptr; }

        const Key& key() const {
            assert(Valid());
            return node_->key;
        }

        void Next() {
            assert(Valid());
            node_ = node_->Next(0);
        }

        void Prev() {
            assert(Valid());
            node_ = list_->FindLessThan(node_->key);
            if (node_ == list_->head_) {
                node_ = nullptr;
            }
        }

        void Seek(const Key& target) {
            node_ = list_->FindGreaterOrEqual(target, nullptr);
        }

        void SeekToFirst() {
            node_ = list_->head_->Next(0);
        }

        void SeekToLast() {
            node_ = list_->FindLast();
            if (node_ == list_->head_) {
                node_ = nullptr;
            }
        }

    private:
        const SkipList* list_;
        Node* node_;
    };

private:
    int GetMaxHeight() const {
        return max_height_.load(std::memory_order_relaxed);
    }

    Node* NewNode(const Key& key, int height) {
        size_t alloc_size = sizeof(Node) + sizeof(std::atomic<Node*>) * (height - 1);
        char* mem = arena_->AllocateAligned(alloc_size);
        return new (mem) Node(key);
    }

    int RandomHeight() {
        static const unsigned int kBranching = 4;
        int height = 1;
        while (height < kMaxHeight && (rng_() % kBranching) == 0) {
            height++;
        }
        return height;
    }

    bool Equal(const Key& a, const Key& b) const {
        return compare_(a, b) == 0;
    }

    bool KeyIsAfterNode(const Key& key, Node* n) const {
        return (n != nullptr) && (compare_(n->key, key) < 0);
    }

    Node* FindGreaterOrEqual(const Key& key, Node** prev) const {
        Node* x = head_;
        int level = GetMaxHeight() - 1;

        while (true) {
            Node* next = x->Next(level);
            if (KeyIsAfterNode(key, next)) {
                x = next;
            } else {
                if (prev != nullptr) prev[level] = x;
                if (level == 0) {
                    return next;
                } else {
                    level--;
                }
            }
        }
    }

    Node* FindLessThan(const Key& key) const {
        Node* x = head_;
        int level = GetMaxHeight() - 1;

        while (true) {
            assert(x == head_ || compare_(x->key, key) < 0);
            Node* next = x->Next(level);
            if (next == nullptr || compare_(next->key, key) >= 0) {
                if (level == 0) {
                    return x;
                } else {
                    level--;
                }
            } else {
                x = next;
            }
        }
    }

    Node* FindLast() const {
        Node* x = head_;
        int level = GetMaxHeight() - 1;

        while (true) {
            Node* next = x->Next(level);
            if (next == nullptr) {
                if (level == 0) {
                    return x;
                } else {
                    level--;
                }
            } else {
                x = next;
            }
        }
    }

    Comparator const compare_;
    Arena* arena_;
    Node* head_;
    std::atomic<int> max_height_;
    mutable std::mt19937 rng_;
};

}  // namespace lsm