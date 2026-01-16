// test/wal_test.cpp
// Comprehensive tests for WAL components

#include "util/types.h"
#include "wal/wal_format.h"
#include "wal/wal_writer.h"
#include "wal/wal_reader.h"
#include "wal/wal_manager.h"
#include "db/memtable.h"

#include <cassert>
#include <iostream>
#include <filesystem>
#include <random>
#include <chrono>

using namespace lsm;
using namespace lsm::wal;
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

// Test directory management
class TestDir {
public:
    TestDir(const std::string& name) : path_("/tmp/lsm_test_" + name) {
        fs::remove_all(path_);
        fs::create_directories(path_);
    }
    ~TestDir() {
        fs::remove_all(path_);
    }
    const std::string& path() const { return path_; }
private:
    std::string path_;
};

// ============================================================================
// CRC32 Tests
// ============================================================================

TEST(crc32_basic) {
    const char* data = "hello world";
    uint32_t crc = CRC32::Compute(data, strlen(data));
    ASSERT(crc != 0);

    // Same data should produce same CRC
    uint32_t crc2 = CRC32::Compute(data, strlen(data));
    ASSERT_EQ(crc, crc2);

    // Different data should produce different CRC
    const char* data2 = "hello worle";
    uint32_t crc3 = CRC32::Compute(data2, strlen(data2));
    ASSERT(crc != crc3);
}

TEST(crc32_incremental) {
    const char* data = "hello world";
    uint32_t crc1 = CRC32::Compute(data, strlen(data));

    // Compute incrementally
    uint32_t crc2 = 0xFFFFFFFF;
    crc2 = CRC32::Update(crc2, "hello", 5);
    crc2 = CRC32::Update(crc2, " world", 6);
    crc2 ^= 0xFFFFFFFF;

    ASSERT_EQ(crc1, crc2);
}

// ============================================================================
// Encoding Tests
// ============================================================================

TEST(encoder_decoder_basic) {
    std::string buf;
    Encoder enc(&buf);

    enc.PutFixed32(0x12345678);
    enc.PutFixed64(0xDEADBEEFCAFEBABE);
    enc.PutFixed16(0xABCD);
    enc.PutByte(0x42);
    enc.PutLengthPrefixed("hello");

    Decoder dec(buf.data(), buf.size());

    uint32_t v32;
    ASSERT_TRUE(dec.GetFixed32(&v32));
    ASSERT_EQ(v32, 0x12345678u);

    uint64_t v64;
    ASSERT_TRUE(dec.GetFixed64(&v64));
    ASSERT_EQ(v64, 0xDEADBEEFCAFEBABEull);

    uint16_t v16;
    ASSERT_TRUE(dec.GetFixed16(&v16));
    ASSERT_EQ(v16, 0xABCDu);

    uint8_t v8;
    ASSERT_TRUE(dec.GetByte(&v8));
    ASSERT_EQ(v8, 0x42u);

    std::string str;
    ASSERT_TRUE(dec.GetLengthPrefixed(&str));
    ASSERT_EQ(str, "hello");

    ASSERT_EQ(dec.Remaining(), 0u);
}

TEST(wal_entry_encoding) {
    WALEntry original;
    original.type = WALEntryType::kPut;
    original.sequence = 12345;
    original.key = "test_key";
    original.value = "test_value";

    std::string encoded = EncodeWALEntry(original);

    WALEntry decoded;
    ASSERT_TRUE(DecodeWALEntry(encoded, &decoded));

    ASSERT_EQ(static_cast<int>(decoded.type), static_cast<int>(original.type));
    ASSERT_EQ(decoded.sequence, original.sequence);
    ASSERT_EQ(decoded.key, original.key);
    ASSERT_EQ(decoded.value, original.value);
}

TEST(wal_entry_delete) {
    WALEntry original;
    original.type = WALEntryType::kDelete;
    original.sequence = 99999;
    original.key = "deleted_key";
    original.value = "";

    std::string encoded = EncodeWALEntry(original);

    WALEntry decoded;
    ASSERT_TRUE(DecodeWALEntry(encoded, &decoded));

    ASSERT_EQ(static_cast<int>(decoded.type), static_cast<int>(WALEntryType::kDelete));
    ASSERT_EQ(decoded.key, "deleted_key");
    ASSERT_TRUE(decoded.value.empty());
}

// ============================================================================
// WAL Writer Tests
// ============================================================================

TEST(wal_writer_basic) {
    TestDir dir("wal_writer_basic");
    std::string path = dir.path() + "/test.wal";

    WALOptions opts;
    opts.sync_policy = SyncPolicy::kSyncPerWrite;

    WALWriter writer(path, opts);
    ASSERT_OK(writer.Open());

    ASSERT_OK(writer.AppendPut(1, "key1", "value1"));
    ASSERT_OK(writer.AppendPut(2, "key2", "value2"));
    ASSERT_OK(writer.AppendDelete(3, "key1"));

    ASSERT_TRUE(writer.FileSize() > 0);

    writer.Close();
}

TEST(wal_writer_large_values) {
    TestDir dir("wal_writer_large");
    std::string path = dir.path() + "/test.wal";

    WALWriter writer(path);
    ASSERT_OK(writer.Open());

    std::string large_value(10000, 'x');
    ASSERT_OK(writer.AppendPut(1, "big_key", large_value));

    writer.Close();

    // Verify we can read it back
    WALReader reader(path);
    ASSERT_OK(reader.Open());

    WALEntry entry;
    Status s;
    ASSERT_TRUE(reader.ReadEntry(&entry, &s));
    ASSERT_OK(s);
    ASSERT_EQ(entry.value.size(), 10000u);
}

TEST(wal_writer_sync_policies) {
    TestDir dir("wal_sync_policies");

    // Test each sync policy
    std::vector<SyncPolicy> policies = {
        SyncPolicy::kSyncPerWrite,
        SyncPolicy::kSyncBatched,
        SyncPolicy::kNoSync,
    };

    for (auto policy : policies) {
        std::string path = dir.path() + "/test_" + std::to_string(static_cast<int>(policy)) + ".wal";

        WALOptions opts;
        opts.sync_policy = policy;
        opts.sync_batch_size = 100;

        WALWriter writer(path, opts);
        ASSERT_OK(writer.Open());

        for (int i = 0; i < 100; i++) {
            ASSERT_OK(writer.AppendPut(i, "key" + std::to_string(i), "value"));
        }

        writer.Close();
        ASSERT_TRUE(fs::file_size(path) > 0);
    }
}

// ============================================================================
// WAL Reader Tests
// ============================================================================

TEST(wal_reader_basic) {
    TestDir dir("wal_reader_basic");
    std::string path = dir.path() + "/test.wal";

    // Write some entries
    {
        WALWriter writer(path);
        ASSERT_OK(writer.Open());
        ASSERT_OK(writer.AppendPut(1, "key1", "value1"));
        ASSERT_OK(writer.AppendPut(2, "key2", "value2"));
        ASSERT_OK(writer.AppendDelete(3, "key1"));
        writer.Close();
    }

    // Read them back
    WALReader reader(path);
    ASSERT_OK(reader.Open());

    WALEntry entry;
    Status s;

    ASSERT_TRUE(reader.ReadEntry(&entry, &s));
    ASSERT_OK(s);
    ASSERT_EQ(entry.sequence, 1u);
    ASSERT_EQ(entry.key, "key1");
    ASSERT_EQ(entry.value, "value1");

    ASSERT_TRUE(reader.ReadEntry(&entry, &s));
    ASSERT_OK(s);
    ASSERT_EQ(entry.sequence, 2u);
    ASSERT_EQ(entry.key, "key2");

    ASSERT_TRUE(reader.ReadEntry(&entry, &s));
    ASSERT_OK(s);
    ASSERT_EQ(static_cast<int>(entry.type), static_cast<int>(WALEntryType::kDelete));
    ASSERT_EQ(entry.sequence, 3u);

    ASSERT_FALSE(reader.ReadEntry(&entry, &s));  // EOF
    ASSERT_OK(s);
}

TEST(wal_reader_foreach) {
    TestDir dir("wal_reader_foreach");
    std::string path = dir.path() + "/test.wal";

    const int N = 100;

    {
        WALWriter writer(path);
        ASSERT_OK(writer.Open());
        for (int i = 0; i < N; i++) {
            ASSERT_OK(writer.AppendPut(i, "key" + std::to_string(i), "value" + std::to_string(i)));
        }
        writer.Close();
    }

    WALReader reader(path);
    ASSERT_OK(reader.Open());

    int count = 0;
    Status s = reader.ForEach([&](const WALEntry& entry) {
        ASSERT_EQ(entry.sequence, static_cast<SequenceNumber>(count));
        count++;
        return true;
    });

    ASSERT_OK(s);
    ASSERT_EQ(count, N);
}

TEST(wal_reader_empty_file) {
    TestDir dir("wal_reader_empty");
    std::string path = dir.path() + "/empty.wal";

    // Create empty file
    {
        WALWriter writer(path);
        ASSERT_OK(writer.Open());
        writer.Close();
    }

    WALReader reader(path);
    ASSERT_OK(reader.Open());

    WALEntry entry;
    Status s;
    ASSERT_FALSE(reader.ReadEntry(&entry, &s));
    ASSERT_OK(s);
}

TEST(wal_reader_corruption_detection) {
    TestDir dir("wal_corruption");
    std::string path = dir.path() + "/test.wal";

    // Write valid data
    {
        WALWriter writer(path);
        ASSERT_OK(writer.Open());
        ASSERT_OK(writer.AppendPut(1, "key1", "value1"));
        writer.Close();
    }

    // Corrupt the file
    {
        FILE* f = fopen(path.c_str(), "r+b");
        ASSERT(f != nullptr);
        fseek(f, 10, SEEK_SET);  // Corrupt somewhere in the middle
        char garbage = 0xFF;
        fwrite(&garbage, 1, 1, f);
        fclose(f);
    }

    // Try to read
    WALReader reader(path);
    ASSERT_OK(reader.Open());

    WALEntry entry;
    Status s;
    reader.ReadEntry(&entry, &s);
    ASSERT_TRUE(s.IsCorruption());
}

// ============================================================================
// WAL Manager Tests
// ============================================================================

TEST(wal_manager_basic) {
    TestDir dir("wal_manager_basic");

    WALManager mgr(dir.path());
    ASSERT_OK(mgr.Open());

    ASSERT_OK(mgr.AppendPut(1, "key1", "value1"));
    ASSERT_OK(mgr.AppendPut(2, "key2", "value2"));
    ASSERT_OK(mgr.AppendDelete(3, "key1"));

    mgr.Close();

    // Verify files exist
    ASSERT_TRUE(fs::exists(dir.path() + "/wal"));
}

TEST(wal_manager_recovery) {
    TestDir dir("wal_manager_recovery");

    // Write data and close
    {
        WALManager mgr(dir.path());
        ASSERT_OK(mgr.Open());

        ASSERT_OK(mgr.AppendPut(1, "key1", "value1"));
        ASSERT_OK(mgr.AppendPut(2, "key2", "value2"));
        ASSERT_OK(mgr.AppendPut(3, "key1", "value1_updated"));
        ASSERT_OK(mgr.AppendDelete(4, "key2"));

        mgr.Close();
    }

    // Recover to memtable
    {
        WALManager mgr(dir.path());
        ASSERT_OK(mgr.Open());

        MemTable* memtable = new MemTable();
        memtable->Ref();

        RecoveryStats stats;
        ASSERT_OK(mgr.Recover(memtable, &stats));

        ASSERT_EQ(stats.records_read, 4u);
        ASSERT_EQ(stats.puts_recovered, 3u);
        ASSERT_EQ(stats.deletes_recovered, 1u);
        ASSERT_EQ(stats.max_sequence, 4u);

        // Verify memtable contents
        auto r1 = memtable->Get("key1", 10);
        ASSERT_TRUE(r1.found);
        ASSERT_EQ(r1.value, "value1_updated");

        auto r2 = memtable->Get("key2", 10);
        ASSERT_TRUE(r2.found);
        ASSERT_TRUE(r2.is_deleted);

        memtable->Unref();
        mgr.Close();
    }
}

TEST(wal_manager_rotation) {
    TestDir dir("wal_manager_rotation");

    WALOptions opts;
    opts.max_file_size = 1024;  // Very small to trigger rotation

    WALManager mgr(dir.path(), opts);
    ASSERT_OK(mgr.Open());

    uint64_t initial_log = mgr.CurrentLogNumber();

    // Write enough to trigger rotation
    std::string value(100, 'x');
    for (int i = 0; i < 100; i++) {
        ASSERT_OK(mgr.AppendPut(i, "key" + std::to_string(i), value));
    }

    ASSERT_TRUE(mgr.CurrentLogNumber() > initial_log);

    // Verify recovery still works
    MemTable* memtable = new MemTable();
    memtable->Ref();

    RecoveryStats stats;
    ASSERT_OK(mgr.Recover(memtable, &stats));
    ASSERT_EQ(stats.records_read, 100u);

    memtable->Unref();
    mgr.Close();
}

TEST(wal_manager_truncate) {
    TestDir dir("wal_manager_truncate");

    WALOptions opts;
    opts.max_file_size = 500;

    WALManager mgr(dir.path(), opts);
    ASSERT_OK(mgr.Open());

    // Write data across multiple logs
    for (int i = 0; i < 50; i++) {
        ASSERT_OK(mgr.AppendPut(i, "key" + std::to_string(i), "value"));
    }

    std::vector<uint64_t> logs_before;
    mgr.GetLogNumbers(&logs_before);
    ASSERT_TRUE(logs_before.size() > 1);

    // Mark old logs as flushed
    uint64_t current = mgr.CurrentLogNumber();
    ASSERT_OK(mgr.MarkFlushed(current));

    std::vector<uint64_t> logs_after;
    mgr.GetLogNumbers(&logs_after);
    ASSERT_TRUE(logs_after.size() < logs_before.size());

    mgr.Close();
}

// ============================================================================
// Integration Tests
// ============================================================================

TEST(wal_crash_simulation) {
    TestDir dir("wal_crash_sim");
    std::string wal_path = dir.path() + "/wal/log.000001";

    // Simulate crash by writing partial data
    {
        WALManager mgr(dir.path());
        ASSERT_OK(mgr.Open());

        ASSERT_OK(mgr.AppendPut(1, "key1", "value1"));
        ASSERT_OK(mgr.AppendPut(2, "key2", "value2"));
        ASSERT_OK(mgr.Sync());

        mgr.Close();
    }

    // Append garbage to simulate partial write
    {
        FILE* f = fopen(wal_path.c_str(), "ab");
        ASSERT(f != nullptr);
        char garbage[20] = {0x12, 0x34, 0x56};  // Incomplete record
        fwrite(garbage, 1, sizeof(garbage), f);
        fclose(f);
    }

    // Recovery should handle partial record
    {
        WALManager mgr(dir.path());
        ASSERT_OK(mgr.Open());

        MemTable* memtable = new MemTable();
        memtable->Ref();

        RecoveryStats stats;
        Status s = mgr.Recover(memtable, &stats);
        // Recovery may report corruption but should recover valid records

        ASSERT_EQ(stats.records_read, 2u);

        auto r1 = memtable->Get("key1", 10);
        ASSERT_TRUE(r1.found);
        ASSERT_EQ(r1.value, "value1");

        memtable->Unref();
        mgr.Close();
    }
}

// ============================================================================
// Benchmarks
// ============================================================================

void benchmark_wal_write() {
    const int N = 50000;
    std::string value(100, 'x');

    // Benchmark with sync per write
    {
        TestDir dir("bench_wal_sync");
        WALOptions opts;
        opts.sync_policy = SyncPolicy::kSyncPerWrite;

        WALManager mgr(dir.path(), opts);
        ASSERT_OK(mgr.Open());

        auto start = std::chrono::high_resolution_clock::now();
        for (int i = 0; i < N; i++) {
            mgr.AppendPut(i, "key" + std::to_string(i), value);
        }
        auto end = std::chrono::high_resolution_clock::now();

        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
        std::cout << "  Sync-per-write: " << N << " ops in " << ms << "ms ("
                  << (N * 1000 / (ms + 1)) << " ops/sec)\n";
        mgr.Close();
    }

    // Benchmark with no sync
    {
        TestDir dir("bench_wal_nosync");
        WALOptions opts;
        opts.sync_policy = SyncPolicy::kNoSync;

        WALManager mgr(dir.path(), opts);
        ASSERT_OK(mgr.Open());

        auto start = std::chrono::high_resolution_clock::now();
        for (int i = 0; i < N; i++) {
            mgr.AppendPut(i, "key" + std::to_string(i), value);
        }
        mgr.Sync();
        auto end = std::chrono::high_resolution_clock::now();

        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
        std::cout << "  No-sync: " << N << " ops in " << ms << "ms ("
                  << (N * 1000 / (ms + 1)) << " ops/sec)\n";
        mgr.Close();
    }
}

void benchmark_wal_recovery() {
    TestDir dir("bench_wal_recovery");

    const int N = 100000;
    std::string value(100, 'x');

    // Write data
    {
        WALOptions opts;
        opts.sync_policy = SyncPolicy::kNoSync;

        WALManager mgr(dir.path(), opts);
        ASSERT_OK(mgr.Open());

        for (int i = 0; i < N; i++) {
            mgr.AppendPut(i, "key" + std::to_string(i), value);
        }
        mgr.Sync();
        mgr.Close();
    }

    // Benchmark recovery
    {
        WALManager mgr(dir.path());
        ASSERT_OK(mgr.Open());

        MemTable* memtable = new MemTable();
        memtable->Ref();

        RecoveryStats stats;
        ASSERT_OK(mgr.Recover(memtable, &stats));

        std::cout << "  Recovery: " << N << " records, "
                  << stats.bytes_read / 1024 << "KB in "
                  << stats.duration.count() / 1000 << "ms ("
                  << (static_cast<int64_t>(N) * 1000000 / (stats.duration.count() + 1)) << " records/sec)\n";

        memtable->Unref();
        mgr.Close();
    }
}

// ============================================================================
// Main
// ============================================================================

int main() {
    std::cout << "=== Phase 2: Write-Ahead Log Tests ===\n\n";

    std::cout << "--- CRC32 Tests ---\n";
    RUN_TEST(crc32_basic);
    RUN_TEST(crc32_incremental);

    std::cout << "\n--- Encoding Tests ---\n";
    RUN_TEST(encoder_decoder_basic);
    RUN_TEST(wal_entry_encoding);
    RUN_TEST(wal_entry_delete);

    std::cout << "\n--- WAL Writer Tests ---\n";
    RUN_TEST(wal_writer_basic);
    RUN_TEST(wal_writer_large_values);
    RUN_TEST(wal_writer_sync_policies);

    std::cout << "\n--- WAL Reader Tests ---\n";
    RUN_TEST(wal_reader_basic);
    RUN_TEST(wal_reader_foreach);
    RUN_TEST(wal_reader_empty_file);
    RUN_TEST(wal_reader_corruption_detection);

    std::cout << "\n--- WAL Manager Tests ---\n";
    RUN_TEST(wal_manager_basic);
    RUN_TEST(wal_manager_recovery);
    RUN_TEST(wal_manager_rotation);
    RUN_TEST(wal_manager_truncate);

    std::cout << "\n--- Integration Tests ---\n";
    RUN_TEST(wal_crash_simulation);

    std::cout << "\n--- Benchmarks ---\n";
    benchmark_wal_write();
    benchmark_wal_recovery();

    std::cout << "\n=== All Tests Passed ===\n";
    return 0;
}