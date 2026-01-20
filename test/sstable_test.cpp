// test/sstable_test.cpp
// Tests for SSTable writer components

#include "util/types.h"
#include "sstable/sstable_format.h"
#include "sstable/block_builder.h"
#include "sstable/sstable_writer.h"
#include "db/memtable.h"

#include <cassert>
#include <iostream>
#include <filesystem>
#include <random>
#include <chrono>
#include <fstream>

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
// Varint Tests
// ============================================================================

TEST(varint_encode_decode) {
    std::vector<uint32_t> test_values = {
        0, 1, 127, 128, 255, 256, 16383, 16384,
        0x7FFFFFFF, 0xFFFFFFFF
    };

    for (uint32_t val : test_values) {
        std::string encoded;
        Varint::PutVarint32(&encoded, val);

        const char* p = encoded.data();
        uint32_t decoded;
        ASSERT_TRUE(Varint::GetVarint32(&p, encoded.data() + encoded.size(), &decoded));
        ASSERT_EQ(val, decoded);
    }
}

TEST(varint64_encode_decode) {
    std::vector<uint64_t> test_values = {
        0, 1, 127, 128, 16383, 16384,
        0xFFFFFFFFULL, 0x100000000ULL, 0xFFFFFFFFFFFFFFFFULL
    };

    for (uint64_t val : test_values) {
        std::string encoded;
        Varint::PutVarint64(&encoded, val);

        const char* p = encoded.data();
        uint64_t decoded;
        ASSERT_TRUE(Varint::GetVarint64(&p, encoded.data() + encoded.size(), &decoded));
        ASSERT_EQ(val, decoded);
    }
}

TEST(varint_length) {
    ASSERT_EQ(Varint::VarintLength(0), 1);
    ASSERT_EQ(Varint::VarintLength(127), 1);
    ASSERT_EQ(Varint::VarintLength(128), 2);
    ASSERT_EQ(Varint::VarintLength(16383), 2);
    ASSERT_EQ(Varint::VarintLength(16384), 3);
}

// ============================================================================
// BlockHandle Tests
// ============================================================================

TEST(block_handle_encode_decode) {
    BlockHandle original;
    original.offset = 12345678;
    original.size = 87654321;

    std::string encoded = original.Encode();

    BlockHandle decoded;
    Slice input(encoded);
    ASSERT_TRUE(decoded.Decode(&input));
    ASSERT_EQ(decoded.offset, original.offset);
    ASSERT_EQ(decoded.size, original.size);
}

// ============================================================================
// Footer Tests
// ============================================================================

TEST(footer_encode_decode) {
    Footer original;
    original.index_handle.offset = 100000;
    original.index_handle.size = 5000;
    original.num_entries = 50000;
    original.min_sequence = 1;
    original.max_sequence = 50000;
    original.min_key = "aaa";
    original.max_key = "zzz";

    std::string encoded = original.Encode();
    ASSERT_EQ(encoded.size(), kFooterSize);

    Footer decoded;
    ASSERT_TRUE(decoded.Decode(encoded));
    ASSERT_EQ(decoded.index_handle.offset, original.index_handle.offset);
    ASSERT_EQ(decoded.index_handle.size, original.index_handle.size);
    ASSERT_EQ(decoded.num_entries, original.num_entries);
    ASSERT_EQ(decoded.min_sequence, original.min_sequence);
    ASSERT_EQ(decoded.max_sequence, original.max_sequence);
    ASSERT_EQ(decoded.min_key, original.min_key);
    ASSERT_EQ(decoded.max_key, original.max_key);
}

TEST(footer_magic_validation) {
    Footer footer;
    footer.index_handle.offset = 100;
    footer.index_handle.size = 50;
    footer.num_entries = 10;
    footer.min_key = "a";
    footer.max_key = "z";

    std::string encoded = footer.Encode();

    // Corrupt the magic number
    encoded[encoded.size() - 1] = 0x00;

    Footer decoded;
    ASSERT_FALSE(decoded.Decode(encoded));
}

// ============================================================================
// BlockBuilder Tests
// ============================================================================

TEST(block_builder_empty) {
    BlockBuilder builder;
    ASSERT_TRUE(builder.Empty());

    Slice block = builder.Finish();
    // Should have at least restart array + count
    ASSERT_TRUE(block.size() >= 8);
}

TEST(block_builder_single_entry) {
    BlockBuilder builder;
    builder.Add("key1", "value1");

    ASSERT_FALSE(builder.Empty());
    ASSERT_EQ(builder.LastKey(), "key1");

    Slice block = builder.Finish();
    ASSERT_TRUE(block.size() > 0);
}

TEST(block_builder_multiple_entries) {
    BlockBuilder builder(4);  // Restart every 4 keys

    for (int i = 0; i < 100; i++) {
        char key[32], value[32];
        snprintf(key, sizeof(key), "key%06d", i);
        snprintf(value, sizeof(value), "value%06d", i);
        builder.Add(key, value);
    }

    Slice block = builder.Finish();
    ASSERT_TRUE(block.size() > 0);

    // Verify last key
    ASSERT_EQ(builder.LastKey(), "key000099");
}

TEST(block_builder_prefix_compression) {
    BlockBuilder builder_compressed(16);
    BlockBuilder builder_uncompressed(1);  // No compression

    for (int i = 0; i < 100; i++) {
        char key[32], value[32];
        snprintf(key, sizeof(key), "prefix_key_%06d", i);
        snprintf(value, sizeof(value), "v%d", i);
        builder_compressed.Add(key, value);
        builder_uncompressed.Add(key, value);
    }

    Slice compressed = builder_compressed.Finish();
    Slice uncompressed = builder_uncompressed.Finish();

    // Compressed should be smaller due to shared prefixes
    ASSERT_TRUE(compressed.size() < uncompressed.size());

    std::cout << " [compressed=" << compressed.size()
              << " uncompressed=" << uncompressed.size() << "]";
}

TEST(block_builder_reset) {
    BlockBuilder builder;

    builder.Add("key1", "value1");
    builder.Finish();

    builder.Reset();
    ASSERT_TRUE(builder.Empty());

    builder.Add("key2", "value2");
    ASSERT_EQ(builder.LastKey(), "key2");
}

// ============================================================================
// BlockTrailer Tests
// ============================================================================

TEST(block_trailer_add_verify) {
    std::string data = "test block contents";

    std::string with_trailer = BlockTrailer::AddTrailer(data, BlockType::kData);
    ASSERT_EQ(with_trailer.size(), data.size() + kBlockTrailerSize);

    ASSERT_TRUE(BlockTrailer::VerifyTrailer(with_trailer, BlockType::kData));
    ASSERT_FALSE(BlockTrailer::VerifyTrailer(with_trailer, BlockType::kIndex));
}

TEST(block_trailer_corruption_detection) {
    std::string data = "test block contents";
    std::string with_trailer = BlockTrailer::AddTrailer(data, BlockType::kData);

    // Corrupt the data
    with_trailer[5] ^= 0xFF;

    ASSERT_FALSE(BlockTrailer::VerifyTrailer(with_trailer, BlockType::kData));
}

// ============================================================================
// IndexBlockBuilder Tests
// ============================================================================

TEST(index_builder_basic) {
    IndexBlockBuilder builder;

    BlockHandle h1{0, 4096};
    BlockHandle h2{4096, 4096};
    BlockHandle h3{8192, 2048};

    builder.AddEntry("key_block_0_last", h1);
    builder.AddEntry("key_block_1_last", h2);
    builder.AddEntry("key_block_2_last", h3);

    ASSERT_EQ(builder.EntryCount(), 3u);

    Slice index_block = builder.Finish();
    ASSERT_TRUE(index_block.size() > 0);
}

// ============================================================================
// SSTableWriter Tests
// ============================================================================

TEST(sstable_writer_basic) {
    TestDir dir("sstable_writer_basic");
    std::string path = dir.path() + "/test.sst";

    SSTableWriter writer(path);
    ASSERT_OK(writer.Open());

    ASSERT_OK(writer.Add("key1", "value1", 1, ValueType::kValue));
    ASSERT_OK(writer.Add("key2", "value2", 2, ValueType::kValue));
    ASSERT_OK(writer.Add("key3", "value3", 3, ValueType::kValue));

    SSTableWriteStats stats;
    ASSERT_OK(writer.Finish(&stats));

    ASSERT_EQ(stats.num_entries, 3u);
    ASSERT_EQ(stats.min_seq, 1u);
    ASSERT_EQ(stats.max_seq, 3u);

    // Verify file exists and has content
    ASSERT_TRUE(fs::exists(path));
    ASSERT_TRUE(fs::file_size(path) > 0);
}

TEST(sstable_writer_large) {
    TestDir dir("sstable_writer_large");
    std::string path = dir.path() + "/large.sst";

    SSTableOptions opts;
    opts.block_size = 4096;

    SSTableWriter writer(path, opts);
    ASSERT_OK(writer.Open());

    const int N = 10000;
    std::string value(100, 'x');

    for (int i = 0; i < N; i++) {
        char key[32];
        snprintf(key, sizeof(key), "key%08d", i);
        ASSERT_OK(writer.Add(key, value, i + 1, ValueType::kValue));
    }

    SSTableWriteStats stats;
    ASSERT_OK(writer.Finish(&stats));

    ASSERT_EQ(stats.num_entries, static_cast<size_t>(N));
    ASSERT_TRUE(stats.num_data_blocks > 1);

    std::cout << " [entries=" << stats.num_entries
              << " blocks=" << stats.num_data_blocks
              << " data_size=" << stats.data_size
              << " index_size=" << stats.index_size << "]";
}

TEST(sstable_writer_with_deletes) {
    TestDir dir("sstable_writer_deletes");
    std::string path = dir.path() + "/test.sst";

    SSTableWriter writer(path);
    ASSERT_OK(writer.Open());

    ASSERT_OK(writer.Add("key1", "value1", 1, ValueType::kValue));
    ASSERT_OK(writer.Add("key1", "", 2, ValueType::kDeletion));  // Delete
    ASSERT_OK(writer.Add("key2", "value2", 3, ValueType::kValue));

    SSTableWriteStats stats;
    ASSERT_OK(writer.Finish(&stats));

    ASSERT_EQ(stats.num_entries, 3u);
}

TEST(sstable_flush_memtable) {
    TestDir dir("sstable_flush_memtable");
    std::string path = dir.path() + "/flushed.sst";

    // Create and populate memtable
    MemTable* memtable = new MemTable();
    memtable->Ref();

    const int N = 1000;
    for (int i = 0; i < N; i++) {
        char key[32], value[64];
        snprintf(key, sizeof(key), "key%06d", i);
        snprintf(value, sizeof(value), "value%06d", i);
        memtable->Put(i + 1, key, value);
    }

    // Flush to SSTable
    SSTableWriteStats stats;
    ASSERT_OK(SSTableWriter::FlushMemTable(path, memtable, SSTableOptions(), &stats));

    ASSERT_EQ(stats.num_entries, static_cast<size_t>(N));
    ASSERT_TRUE(fs::exists(path));

    memtable->Unref();
}

TEST(sstable_writer_abandon) {
    TestDir dir("sstable_writer_abandon");
    std::string path = dir.path() + "/abandoned.sst";

    {
        SSTableWriter writer(path);
        ASSERT_OK(writer.Open());
        ASSERT_OK(writer.Add("key1", "value1", 1, ValueType::kValue));
        writer.Abandon();
    }

    // File should be deleted
    ASSERT_FALSE(fs::exists(path));
}

// ============================================================================
// Benchmarks
// ============================================================================

void benchmark_block_builder() {
    const int N = 100000;
    std::string value(100, 'x');

    auto start = std::chrono::high_resolution_clock::now();

    BlockBuilder builder;
    for (int i = 0; i < N; i++) {
        char key[32];
        snprintf(key, sizeof(key), "key%08d", i);
        builder.Add(key, value);

        if (builder.CurrentSizeEstimate() > 4096) {
            builder.Finish();
            builder.Reset();
        }
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

    std::cout << "  BlockBuilder: " << N << " entries in " << ms << "ms ("
              << (N * 1000 / (ms + 1)) << " entries/sec)\n";
}

void benchmark_sstable_write() {
    TestDir dir("bench_sstable");
    std::string path = dir.path() + "/bench.sst";

    const int N = 100000;
    std::string value(100, 'x');

    auto start = std::chrono::high_resolution_clock::now();

    SSTableWriter writer(path);
    ASSERT_OK(writer.Open());

    for (int i = 0; i < N; i++) {
        char key[32];
        snprintf(key, sizeof(key), "key%08d", i);
        writer.Add(key, value, i + 1, ValueType::kValue);
    }

    SSTableWriteStats stats;
    ASSERT_OK(writer.Finish(&stats));

    auto end = std::chrono::high_resolution_clock::now();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

    size_t file_size = fs::file_size(path);

    std::cout << "  SSTableWriter: " << N << " entries in " << ms << "ms ("
              << (N * 1000 / (ms + 1)) << " entries/sec)\n"
              << "    File size: " << file_size / 1024 << "KB, "
              << stats.num_data_blocks << " data blocks\n";
}

// ============================================================================
// Main
// ============================================================================

int main() {
    std::cout << "=== Phase 3: SSTable Writer Tests ===\n\n";

    std::cout << "--- Varint Tests ---\n";
    RUN_TEST(varint_encode_decode);
    RUN_TEST(varint64_encode_decode);
    RUN_TEST(varint_length);

    std::cout << "\n--- BlockHandle Tests ---\n";
    RUN_TEST(block_handle_encode_decode);

    std::cout << "\n--- Footer Tests ---\n";
    RUN_TEST(footer_encode_decode);
    RUN_TEST(footer_magic_validation);

    std::cout << "\n--- BlockBuilder Tests ---\n";
    RUN_TEST(block_builder_empty);
    RUN_TEST(block_builder_single_entry);
    RUN_TEST(block_builder_multiple_entries);
    RUN_TEST(block_builder_prefix_compression);
    RUN_TEST(block_builder_reset);

    std::cout << "\n--- BlockTrailer Tests ---\n";
    RUN_TEST(block_trailer_add_verify);
    RUN_TEST(block_trailer_corruption_detection);

    std::cout << "\n--- IndexBlockBuilder Tests ---\n";
    RUN_TEST(index_builder_basic);

    std::cout << "\n--- SSTableWriter Tests ---\n";
    RUN_TEST(sstable_writer_basic);
    RUN_TEST(sstable_writer_large);
    RUN_TEST(sstable_writer_with_deletes);
    RUN_TEST(sstable_flush_memtable);
    RUN_TEST(sstable_writer_abandon);

    std::cout << "\n--- Benchmarks ---\n";
    benchmark_block_builder();
    benchmark_sstable_write();

    std::cout << "\n=== All Tests Passed ===\n";
    return 0;
}