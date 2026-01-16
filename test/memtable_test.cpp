// test/memtable_test.cpp
// Comprehensive tests for memtable and skip list

#include "util/types.h"
#include "util/arena.h"
#include "memtable/skiplist.h"
#include "db/memtable.h"
#include "db/memtable_manager.h"

#include <cassert>
#include <iostream>
#include <thread>
#include <vector>
#include <set>
#include <random>
#include <chrono>
#include <algorithm>

using namespace lsm;

#define TEST(name) void test_##name()
#define RUN_TEST(name) do { \
    std::cout << "Running " #name "..." << std::flush; \
    test_##name(); \
    std::cout << " PASSED\n"; \
} while(0)

#define ASSERT(cond) do { \
    if (!(cond)) { \
        std::cerr << "\nAssertion failed: " #cond \
                  << " at " << __FILE__ << ":" << __LINE__ << "\n"; \
        std::abort(); \
    } \
} while(0)

#define ASSERT_EQ(a, b) ASSERT((a) == (b))
#define ASSERT_NE(a, b) ASSERT((a) != (b))
#define ASSERT_TRUE(x) ASSERT(x)
#define ASSERT_FALSE(x) ASSERT(!(x))

// Arena Tests
TEST(arena_basic_allocation) {
    Arena arena;
    char* p1 = arena.Allocate(100);
    ASSERT_NE(p1, nullptr);
    char* p2 = arena.Allocate(200);
    ASSERT_NE(p2, nullptr);
    ASSERT_NE(p1, p2);
    memset(p1, 'A', 100);
    memset(p2, 'B', 200);
    ASSERT_EQ(p1[0], 'A');
    ASSERT_EQ(p2[0], 'B');
}

TEST(arena_aligned_allocation) {
    Arena arena;
    for (int i = 0; i < 100; i++) {
        char* p = arena.AllocateAligned(17, 8);
        ASSERT_EQ(reinterpret_cast<uintptr_t>(p) % 8, 0);
    }
    char* p64 = arena.AllocateAligned(100, 64);
    ASSERT_EQ(reinterpret_cast<uintptr_t>(p64) % 64, 0);
}

TEST(arena_large_allocation) {
    Arena arena;
    char* large = arena.Allocate(8192);
    ASSERT_NE(large, nullptr);
    memset(large, 'X', 8192);
    char* small = arena.Allocate(100);
    ASSERT_NE(small, nullptr);
}

// Skip List Tests
struct IntComparator {
    int operator()(int a, int b) const {
        if (a < b) return -1;
        if (a > b) return +1;
        return 0;
    }
};

TEST(skiplist_empty) {
    Arena arena;
    SkipList<int, IntComparator> list(IntComparator(), &arena);
    ASSERT_FALSE(list.Contains(0));
    ASSERT_FALSE(list.Contains(100));
}

TEST(skiplist_insert_sequential) {
    Arena arena;
    SkipList<int, IntComparator> list(IntComparator(), &arena);
    for (int i = 0; i < 1000; i++) {
        list.Insert(i);
    }
    for (int i = 0; i < 1000; i++) {
        ASSERT_TRUE(list.Contains(i));
    }
    ASSERT_FALSE(list.Contains(-1));
    ASSERT_FALSE(list.Contains(1000));
}

TEST(skiplist_insert_random) {
    Arena arena;
    SkipList<int, IntComparator> list(IntComparator(), &arena);
    std::vector<int> values;
    for (int i = 0; i < 1000; i++) {
        values.push_back(i);
    }
    std::mt19937 rng(42);
    std::shuffle(values.begin(), values.end(), rng);
    for (int v : values) {
        list.Insert(v);
    }
    for (int i = 0; i < 1000; i++) {
        ASSERT_TRUE(list.Contains(i));
    }
}

TEST(skiplist_iterator) {
    Arena arena;
    SkipList<int, IntComparator> list(IntComparator(), &arena);
    std::vector<int> values = {5, 2, 8, 1, 9, 3};
    for (int v : values) {
        list.Insert(v);
    }
    std::sort(values.begin(), values.end());
    SkipList<int, IntComparator>::Iterator iter(&list);
    iter.SeekToFirst();
    size_t idx = 0;
    while (iter.Valid()) {
        ASSERT_EQ(iter.key(), values[idx++]);
        iter.Next();
    }
    ASSERT_EQ(idx, values.size());
}

// MemTable Tests
TEST(memtable_put_get) {
    MemTable* mem = new MemTable();
    mem->Ref();
    mem->Put(1, "key1", "value1");
    mem->Put(2, "key2", "value2");
    auto r1 = mem->Get("key1", 10);
    ASSERT_TRUE(r1.found);
    ASSERT_EQ(r1.value, "value1");
    auto r2 = mem->Get("key2", 10);
    ASSERT_TRUE(r2.found);
    ASSERT_EQ(r2.value, "value2");
    auto r3 = mem->Get("key3", 10);
    ASSERT_FALSE(r3.found);
    mem->Unref();
}

TEST(memtable_delete) {
    MemTable* mem = new MemTable();
    mem->Ref();
    mem->Put(1, "key1", "value1");
    mem->Delete(2, "key1");
    auto r = mem->Get("key1", 10);
    ASSERT_TRUE(r.found);
    ASSERT_TRUE(r.is_deleted);
    mem->Unref();
}

TEST(memtable_snapshot_isolation) {
    MemTable* mem = new MemTable();
    mem->Ref();
    mem->Put(1, "key", "v1");
    mem->Put(5, "key", "v5");
    mem->Put(10, "key", "v10");
    auto r1 = mem->Get("key", 3);
    ASSERT_TRUE(r1.found);
    ASSERT_EQ(r1.value, "v1");
    auto r5 = mem->Get("key", 7);
    ASSERT_TRUE(r5.found);
    ASSERT_EQ(r5.value, "v5");
    auto r10 = mem->Get("key", 15);
    ASSERT_TRUE(r10.found);
    ASSERT_EQ(r10.value, "v10");
    mem->Unref();
}

// Manager Tests
TEST(manager_basic_operations) {
    MemTableManager mgr;
    ASSERT_TRUE(mgr.Put("key1", "value1").ok());
    ASSERT_TRUE(mgr.Put("key2", "value2").ok());
    auto r1 = mgr.Get("key1");
    ASSERT_TRUE(r1.found);
    ASSERT_EQ(r1.value, "value1");
}

TEST(manager_rotation) {
    MemTableOptions opts;
    opts.max_size = 512;
    MemTableManager mgr(opts);
    int rotation_count = 0;
    mgr.SetFlushCallback([&](MemTable*) { rotation_count++; });
    for (int i = 0; i < 100; i++) {
        std::string key = "key" + std::to_string(i);
        std::string val = std::string(50, 'x');
        mgr.Put(key, val);
    }
    ASSERT_TRUE(rotation_count > 0);
}

TEST(manager_read_across_memtables) {
    MemTableOptions opts;
    opts.max_size = 256;
    MemTableManager mgr(opts);
    mgr.Put("key1", "value1");
    mgr.ForceRotation();
    mgr.Put("key2", "value2");
    auto r1 = mgr.Get("key1");
    ASSERT_TRUE(r1.found);
    ASSERT_EQ(r1.value, "value1");
    auto r2 = mgr.Get("key2");
    ASSERT_TRUE(r2.found);
    ASSERT_EQ(r2.value, "value2");
}

// Concurrent Tests
TEST(concurrent_reads) {
    MemTableManager mgr;
    for (int i = 0; i < 1000; i++) {
        mgr.Put("key" + std::to_string(i), "value" + std::to_string(i));
    }
    std::atomic<int> success{0};
    auto reader = [&](int tid) {
        std::mt19937 rng(tid);
        for (int i = 0; i < 1000; i++) {
            int k = rng() % 1000;
            auto r = mgr.Get("key" + std::to_string(k));
            if (r.found && r.value == "value" + std::to_string(k)) success++;
        }
    };
    std::vector<std::thread> threads;
    for (int t = 0; t < 8; t++) threads.emplace_back(reader, t);
    for (auto& t : threads) t.join();
    ASSERT_EQ(success.load(), 8000);
}

TEST(concurrent_write_read) {
    MemTableManager mgr;
    std::atomic<bool> done{false};
    std::atomic<int> writes{0};
    std::thread writer([&]() {
        for (int i = 0; i < 5000; i++) {
            mgr.Put("key" + std::to_string(i), "value" + std::to_string(i));
            writes++;
        }
        done = true;
    });
    auto reader = [&]() {
        std::mt19937 rng(std::random_device{}());
        while (!done) {
            int max = writes.load();
            if (max > 0) mgr.Get("key" + std::to_string(rng() % max));
            std::this_thread::yield();
        }
    };
    std::vector<std::thread> readers;
    for (int t = 0; t < 4; t++) readers.emplace_back(reader);
    writer.join();
    for (auto& t : readers) t.join();
    for (int i = 0; i < 5000; i++) {
        auto r = mgr.Get("key" + std::to_string(i));
        ASSERT_TRUE(r.found);
    }
}

// Benchmarks
void benchmark_writes() {
    MemTableManager mgr;
    const int N = 100000;
    std::string val(100, 'x');
    auto start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < N; i++) {
        mgr.Put("key" + std::to_string(i), val);
    }
    auto end = std::chrono::high_resolution_clock::now();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    std::cout << "Writes: " << N << " ops in " << ms << "ms ("
              << (N * 1000 / (ms + 1)) << " ops/sec)\n";
}

void benchmark_reads() {
    MemTableManager mgr;
    const int N = 100000;
    std::string val(100, 'x');
    for (int i = 0; i < N; i++) mgr.Put("key" + std::to_string(i), val);
    std::mt19937 rng(42);
    auto start = std::chrono::high_resolution_clock::now();
    for (int i = 0; i < N; i++) {
        mgr.Get("key" + std::to_string(rng() % N));
    }
    auto end = std::chrono::high_resolution_clock::now();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    std::cout << "Reads: " << N << " ops in " << ms << "ms ("
              << (N * 1000 / (ms + 1)) << " ops/sec)\n";
}

int main() {
    std::cout << "=== Phase 1: Core Data Structures Tests ===\n\n";

    std::cout << "--- Arena Tests ---\n";
    RUN_TEST(arena_basic_allocation);
    RUN_TEST(arena_aligned_allocation);
    RUN_TEST(arena_large_allocation);

    std::cout << "\n--- Skip List Tests ---\n";
    RUN_TEST(skiplist_empty);
    RUN_TEST(skiplist_insert_sequential);
    RUN_TEST(skiplist_insert_random);
    RUN_TEST(skiplist_iterator);

    std::cout << "\n--- MemTable Tests ---\n";
    RUN_TEST(memtable_put_get);
    RUN_TEST(memtable_delete);
    RUN_TEST(memtable_snapshot_isolation);

    std::cout << "\n--- Manager Tests ---\n";
    RUN_TEST(manager_basic_operations);
    RUN_TEST(manager_rotation);
    RUN_TEST(manager_read_across_memtables);

    std::cout << "\n--- Concurrent Tests ---\n";
    RUN_TEST(concurrent_reads);
    RUN_TEST(concurrent_write_read);

    std::cout << "\n--- Benchmarks ---\n";
    benchmark_writes();
    benchmark_reads();

    std::cout << "\n=== All Tests Passed ===\n";
    return 0;
}