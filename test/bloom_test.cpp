// test/bloom_test.cpp
// Tests for Bloom filter implementation

#include "util/types.h"
#include "util/bloom_filter.h"
#include "sstable/sstable_format.h"
#include "sstable/sstable_writer.h"
#include "db/memtable.h"

#include <cassert>
#include <iostream>
#include <filesystem>
#include <random>
#include <chrono>
#include <set>
#include <cmath>

using namespace lsm;
using namespace lsm::sstable;
namespace fs = std::filesystem;

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
#define ASSERT_TRUE(x) ASSERT(x)
#define ASSERT_FALSE(x) ASSERT(!(x))
#define ASSERT_OK(s) ASSERT((s).ok())

class TestDir {
public:
    TestDir(const std::string& name) : path_("/tmp/lsm_test_" + name) {
        fs::remove_all(path_);
        fs::create_directories(path_);
    }
    ~TestDir() { fs::remove_all(path_); }
    const std::string& path() const { return path_; }
private:
    std::string path_;
};

// ============================================================================
// MurmurHash Tests
// ============================================================================

TEST(murmurhash_basic) {
    const char* data = "hello world";
    uint64_t h1 = MurmurHash::Hash64(data, strlen(data), 0);
    uint64_t h2 = MurmurHash::Hash64(data, strlen(data), 0);

    // Same input, same output
    ASSERT_EQ(h1, h2);

    // Different seed, different output
    uint64_t h3 = MurmurHash::Hash64(data, strlen(data), 12345);
    ASSERT(h1 != h3);
}

TEST(murmurhash_distribution) {
    // Test that hashes are well-distributed
    std::set<uint64_t> hashes;

    for (int i = 0; i < 10000; i++) {
        std::string key = "key" + std::to_string(i);
        uint64_t h = MurmurHash::Hash64(key.data(), key.size(), 0);
        hashes.insert(h);
    }

    // Should have very few collisions
    ASSERT(hashes.size() > 9990);
}

TEST(murmurhash_128) {
    const char* data = "test key";
    uint64_t h1, h2;
    MurmurHash::Hash128(data, strlen(data), &h1, &h2);

    ASSERT(h1 != 0);
    ASSERT(h2 != 0);
    ASSERT(h1 != h2);
}

// ============================================================================
// BloomFilterPolicy Tests
// ============================================================================

TEST(bloom_policy_defaults) {
    BloomFilterPolicy policy;

    ASSERT_EQ(policy.bits_per_key, 10);

    int k = policy.OptimalNumHashes();
    ASSERT(k >= 6 && k <= 8);  // Should be around 7

    double fpr = policy.EstimatedFPR();
    ASSERT(fpr > 0.005 && fpr < 0.02);  // Around 1%
}

TEST(bloom_policy_custom) {
    BloomFilterPolicy policy;
    policy.bits_per_key = 20;  // Lower FPR

    double fpr = policy.EstimatedFPR();
    ASSERT(fpr < 0.001);  // Much lower than 1%
}

// ============================================================================
// BloomFilterBuilder Tests
// ============================================================================

TEST(bloom_builder_empty) {
    BloomFilterBuilder builder;

    ASSERT_EQ(builder.NumKeys(), 0u);

    std::string data = builder.Finish();
    ASSERT(data.size() >= 1);  // At least metadata
}

TEST(bloom_builder_single_key) {
    BloomFilterBuilder builder;
    builder.AddKey("hello");

    ASSERT_EQ(builder.NumKeys(), 1u);

    std::string data = builder.Finish();

    BloomFilterReader reader;
    ASSERT_TRUE(reader.Init(data));
    ASSERT_TRUE(reader.MayContain("hello"));
}

TEST(bloom_builder_multiple_keys) {
    BloomFilterBuilder builder;

    std::vector<std::string> keys;
    for (int i = 0; i < 1000; i++) {
        keys.push_back("key" + std::to_string(i));
        builder.AddKey(keys.back());
    }

    std::string data = builder.Finish();

    BloomFilterReader reader;
    ASSERT_TRUE(reader.Init(data));

    // All inserted keys should be found
    for (const auto& key : keys) {
        ASSERT_TRUE(reader.MayContain(key));
    }
}

TEST(bloom_builder_reset) {
    BloomFilterBuilder builder;

    builder.AddKey("key1");
    builder.AddKey("key2");
    ASSERT_EQ(builder.NumKeys(), 2u);

    builder.Reset();
    ASSERT_EQ(builder.NumKeys(), 0u);

    builder.AddKey("key3");
    ASSERT_EQ(builder.NumKeys(), 1u);
}

// ============================================================================
// BloomFilterReader Tests
// ============================================================================

TEST(bloom_reader_false_positives) {
    // Build filter with known keys
    BloomFilterBuilder builder;
    const int N = 10000;

    for (int i = 0; i < N; i++) {
        builder.AddKey("key" + std::to_string(i));
    }

    std::string data = builder.Finish();

    BloomFilterReader reader;
    ASSERT_TRUE(reader.Init(data));

    // Test with keys NOT in the filter
    int false_positives = 0;
    const int num_tests = 10000;

    for (int i = 0; i < num_tests; i++) {
        std::string key = "notakey" + std::to_string(i);
        if (reader.MayContain(key)) {
            false_positives++;
        }
    }

    double fpr = static_cast<double>(false_positives) / num_tests;

    std::cout << " [FPR=" << (fpr * 100) << "%]";

    // With 10 bits/key, expect ~1% FPR
    ASSERT(fpr < 0.02);  // Allow some margin
}

TEST(bloom_reader_no_false_negatives) {
    BloomFilterBuilder builder;

    std::vector<std::string> keys;
    for (int i = 0; i < 10000; i++) {
        keys.push_back("key" + std::to_string(i));
        builder.AddKey(keys.back());
    }

    std::string data = builder.Finish();

    BloomFilterReader reader;
    ASSERT_TRUE(reader.Init(data));

    // NO false negatives allowed
    for (const auto& key : keys) {
        ASSERT_TRUE(reader.MayContain(key));
    }
}

TEST(bloom_reader_invalid_data) {
    BloomFilterReader reader;

    // Empty data
    ASSERT_FALSE(reader.Init(""));

    // Invalid num_hashes
    std::string bad_data(10, '\0');
    bad_data[9] = 0;  // num_hashes = 0
    ASSERT_FALSE(reader.Init(bad_data));
}

// ============================================================================
// BloomFilter (Full) Tests
// ============================================================================

TEST(bloom_filter_build) {
    std::vector<Slice> keys = {"apple", "banana", "cherry", "date"};

    BloomFilter filter = BloomFilter::Build(keys);

    ASSERT_TRUE(filter.MayContain("apple"));
    ASSERT_TRUE(filter.MayContain("banana"));
    ASSERT_TRUE(filter.MayContain("cherry"));
    ASSERT_TRUE(filter.MayContain("date"));

    // These should (probably) not be found
    // Note: could be false positives, but unlikely with small set
}

TEST(bloom_filter_serialization) {
    std::vector<Slice> keys = {"key1", "key2", "key3"};

    BloomFilter original = BloomFilter::Build(keys);
    std::string serialized = original.Data();

    BloomFilter restored(serialized);

    ASSERT_TRUE(restored.MayContain("key1"));
    ASSERT_TRUE(restored.MayContain("key2"));
    ASSERT_TRUE(restored.MayContain("key3"));
}

// ============================================================================
// BloomFilterUtil Tests
// ============================================================================

TEST(bloom_util_bits_for_fpr) {
    // 1% FPR
    size_t bits_1pct = BloomFilterUtil::BitsForFPR(1000, 0.01);
    ASSERT(bits_1pct > 9000 && bits_1pct < 11000);  // ~10 bits/key

    // 0.1% FPR
    size_t bits_01pct = BloomFilterUtil::BitsForFPR(1000, 0.001);
    ASSERT(bits_01pct > bits_1pct);  // Need more bits for lower FPR
}

TEST(bloom_util_expected_fpr) {
    // With 10 bits/key and optimal hashes
    double fpr = BloomFilterUtil::ExpectedFPR(1000, 10000, 7);
    ASSERT(fpr > 0.005 && fpr < 0.02);
}

TEST(bloom_util_optimal_hashes) {
    // 10 bits/key -> ~7 hash functions
    int k = BloomFilterUtil::OptimalNumHashes(1000, 10000);
    ASSERT(k >= 6 && k <= 8);
}

// ============================================================================
// Integration with SSTable Tests
// ============================================================================

TEST(sstable_with_bloom) {
    TestDir dir("sstable_bloom");
    std::string path = dir.path() + "/test.sst";

    SSTableOptions opts;
    opts.use_bloom_filter = true;
    opts.bloom_policy.bits_per_key = 10;

    SSTableWriter writer(path, opts);
    ASSERT_OK(writer.Open());

    const int N = 1000;
    for (int i = 0; i < N; i++) {
        char key[32];
        snprintf(key, sizeof(key), "key%06d", i);
        ASSERT_OK(writer.Add(key, "value", i + 1, ValueType::kValue));
    }

    SSTableWriteStats stats;
    ASSERT_OK(writer.Finish(&stats));

    ASSERT_TRUE(stats.bloom_size > 0);

    // Expected bloom size: ~N * 10 bits / 8 + 1 byte metadata
    size_t expected_min = (N * 10) / 8;
    ASSERT(stats.bloom_size >= expected_min);

    std::cout << " [bloom_size=" << stats.bloom_size << " bytes]";
}

TEST(sstable_without_bloom) {
    TestDir dir("sstable_no_bloom");
    std::string path = dir.path() + "/test.sst";

    SSTableOptions opts;
    opts.use_bloom_filter = false;

    SSTableWriter writer(path, opts);
    ASSERT_OK(writer.Open());

    ASSERT_OK(writer.Add("key1", "value1", 1, ValueType::kValue));

    SSTableWriteStats stats;
    ASSERT_OK(writer.Finish(&stats));

    ASSERT_EQ(stats.bloom_size, 0u);
}

// ============================================================================
// Benchmarks
// ============================================================================

void benchmark_bloom_build() {
    const int N = 1000000;

    BloomFilterBuilder builder;

    auto start = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < N; i++) {
        std::string key = "key" + std::to_string(i);
        builder.AddKey(key);
    }

    std::string filter = builder.Finish();

    auto end = std::chrono::high_resolution_clock::now();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

    std::cout << "  Build: " << N << " keys in " << ms << "ms ("
              << (N * 1000 / (ms + 1)) << " keys/sec)\n"
              << "    Filter size: " << filter.size() / 1024 << "KB ("
              << (filter.size() * 8.0 / N) << " bits/key)\n";
}

void benchmark_bloom_lookup() {
    const int N = 1000000;

    // Build filter
    BloomFilterBuilder builder;
    for (int i = 0; i < N; i++) {
        builder.AddKey("key" + std::to_string(i));
    }
    std::string filter_data = builder.Finish();

    BloomFilterReader reader;
    reader.Init(filter_data);

    // Benchmark lookups (mix of hits and misses)
    auto start = std::chrono::high_resolution_clock::now();

    volatile int found = 0;  // volatile prevents unused variable warning
    for (int i = 0; i < N; i++) {
        // Alternate between existing and non-existing keys
        std::string key = (i % 2 == 0) ?
            "key" + std::to_string(i / 2) :
            "miss" + std::to_string(i);
        if (reader.MayContain(key)) {
            found++;
        }
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

    std::cout << "  Lookup: " << N << " queries in " << ms << "ms ("
              << (N * 1000 / (ms + 1)) << " queries/sec)\n";
}

void benchmark_fpr_vs_bits() {
    std::cout << "  FPR vs bits_per_key:\n";

    const int N = 100000;

    for (int bits_per_key : {5, 8, 10, 12, 15, 20}) {
        BloomFilterPolicy policy;
        policy.bits_per_key = bits_per_key;

        BloomFilterBuilder builder(policy);
        for (int i = 0; i < N; i++) {
            builder.AddKey("key" + std::to_string(i));
        }

        std::string filter_data = builder.Finish();
        BloomFilterReader reader;
        reader.Init(filter_data);

        // Measure actual FPR
        int false_positives = 0;
        const int tests = 100000;
        for (int i = 0; i < tests; i++) {
            if (reader.MayContain("notakey" + std::to_string(i))) {
                false_positives++;
            }
        }

        double actual_fpr = static_cast<double>(false_positives) / tests * 100;
        double expected_fpr = policy.EstimatedFPR() * 100;

        std::cout << "    " << bits_per_key << " bits/key: "
                  << "expected=" << expected_fpr << "%, "
                  << "actual=" << actual_fpr << "%, "
                  << "size=" << filter_data.size() / 1024 << "KB\n";
    }
}

// ============================================================================
// Main
// ============================================================================

int main() {
    std::cout << "=== Phase 4: Bloom Filter Tests ===\n\n";

    std::cout << "--- MurmurHash Tests ---\n";
    RUN_TEST(murmurhash_basic);
    RUN_TEST(murmurhash_distribution);
    RUN_TEST(murmurhash_128);

    std::cout << "\n--- BloomFilterPolicy Tests ---\n";
    RUN_TEST(bloom_policy_defaults);
    RUN_TEST(bloom_policy_custom);

    std::cout << "\n--- BloomFilterBuilder Tests ---\n";
    RUN_TEST(bloom_builder_empty);
    RUN_TEST(bloom_builder_single_key);
    RUN_TEST(bloom_builder_multiple_keys);
    RUN_TEST(bloom_builder_reset);

    std::cout << "\n--- BloomFilterReader Tests ---\n";
    RUN_TEST(bloom_reader_false_positives);
    RUN_TEST(bloom_reader_no_false_negatives);
    RUN_TEST(bloom_reader_invalid_data);

    std::cout << "\n--- BloomFilter Tests ---\n";
    RUN_TEST(bloom_filter_build);
    RUN_TEST(bloom_filter_serialization);

    std::cout << "\n--- BloomFilterUtil Tests ---\n";
    RUN_TEST(bloom_util_bits_for_fpr);
    RUN_TEST(bloom_util_expected_fpr);
    RUN_TEST(bloom_util_optimal_hashes);

    std::cout << "\n--- SSTable Integration Tests ---\n";
    RUN_TEST(sstable_with_bloom);
    RUN_TEST(sstable_without_bloom);

    std::cout << "\n--- Benchmarks ---\n";
    benchmark_bloom_build();
    benchmark_bloom_lookup();
    benchmark_fpr_vs_bits();

    std::cout << "\n=== All Tests Passed ===\n";
    return 0;
}