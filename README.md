# LSM-Tree Storage Engine

A production-grade, high-performance key-value storage engine built in C++ using Log-Structured Merge-Tree architecture. Designed for write-intensive workloads with strong durability guarantees, verified correctness under concurrency, and predictable latency characteristics.

[![Build Status](https://img.shields.io/badge/build-passing-brightgreen)]()
[![C++](https://img.shields.io/badge/C%2B%2B-17-blue.svg)]()
[![License](https://img.shields.io/badge/license-MIT-green.svg)]()

---

## Overview

Traditional B-tree storage engines suffer from write amplification due to random I/O patterns. This LSM-Tree implementation converts random writes into sequential I/O, targeting **100K+ write operations/second** while maintaining ACID durability guarantees through write-ahead logging.

### Key Features

- **High Write Throughput** — Sequential I/O patterns optimized for SSDs and HDDs
- **Crash Recovery** — WAL-based durability with bounded recovery time
- **Verified Correctness** — Linearizability checking via history recording and offline verification
- **Thread-Safe** — Fine-grained concurrency supporting high thread counts
- **Space Efficient** — Bloom filters reduce unnecessary disk reads
- **Reproducible Benchmarks** — Harness with configurable workload generation

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         Client API                              │
│                    Put() / Get() / Delete()                     │
└──────────────────────────┬──────────────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────────────┐
│                        MemTable                                 │
│                 (Thread-Safe Skip List)                         │
│              In-memory sorted write buffer                      │
└──────────────────────────┬──────────────────────────────────────┘
                           │ flush when full
┌──────────────────────────▼──────────────────────────────────────┐
│                    Immutable MemTables                          │
│              Queued for background flush                        │
└──────────────────────────┬──────────────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────────────┐
│                     SSTable Levels                              │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │ Level 0: Recently flushed (overlapping key ranges)          ││
│  ├─────────────────────────────────────────────────────────────┤│
│  │ Level 1: Compacted (non-overlapping, ~10MB)                 ││
│  ├─────────────────────────────────────────────────────────────┤│
│  │ Level 2: Compacted (non-overlapping, ~100MB)                ││
│  ├─────────────────────────────────────────────────────────────┤│
│  │ Level N: Compacted (non-overlapping, ~10^N MB)              ││
│  └─────────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                    Write-Ahead Log (WAL)                        │
│         Sequential append with CRC32 checksums                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## Quick Start

### Prerequisites

- C++17 compatible compiler (GCC 8+, Clang 7+, MSVC 2019+)
- CMake 3.14+
- pthread library

### Building

```bash
git clone https://github.com/mayureshsatao/lsm-tree-engine.git
cd lsm-tree-engine

mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Release ..
make -j$(nproc)
```

---

## Usage

### Basic Operations

```cpp
#include "lsm/db.h"

int main() {
    lsm::Options options;
    options.create_if_missing = true;
    options.write_buffer_size = 64 * 1024 * 1024;  // 64MB memtable
    options.max_levels = 7;
    options.bloom_filter_bits_per_key = 10;
    
    lsm::DB* db = nullptr;
    lsm::Status status = lsm::DB::Open(options, "/tmp/testdb", &db);
    
    if (!status.ok()) {
        std::cerr << "Failed to open database: " << status.ToString() << std::endl;
        return 1;
    }
    
    // Write operations
    db->Put(lsm::WriteOptions(), "user:1001", "{\"name\": \"alice\"}");
    db->Put(lsm::WriteOptions(), "user:1002", "{\"name\": \"bob\"}");
    
    // Read operations
    std::string value;
    status = db->Get(lsm::ReadOptions(), "user:1001", &value);
    
    if (status.ok()) {
        std::cout << "Found: " << value << std::endl;
    }
    
    // Delete operations
    db->Delete(lsm::WriteOptions(), "user:1002");
    
    // Batch writes for atomicity
    lsm::WriteBatch batch;
    batch.Put("session:abc123", "active");
    batch.Put("session:def456", "active");
    batch.Delete("session:expired");
    db->Write(lsm::WriteOptions(), &batch);
    
    delete db;
    return 0;
}
```

### Range Scans

```cpp
lsm::ReadOptions read_opts;
read_opts.snapshot = db->GetSnapshot();  // Consistent view

std::unique_ptr<lsm::Iterator> iter(db->NewIterator(read_opts));

for (iter->Seek("user:"); iter->Valid() && iter->key().starts_with("user:"); iter->Next()) {
    std::cout << iter->key() << " => " << iter->value() << std::endl;
}

db->ReleaseSnapshot(read_opts.snapshot);
```

### Write Options

```cpp
lsm::WriteOptions write_opts;

// Synchronous writes (fsync per operation) - highest durability
write_opts.sync = true;
db->Put(write_opts, "critical_key", "critical_value");

// Asynchronous writes - higher throughput
write_opts.sync = false;
db->Put(write_opts, "metrics:cpu", "45.2");
```

---

## Configuration

### Core Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `write_buffer_size` | 64MB | MemTable size before flush |
| `max_write_buffer_number` | 3 | Max immutable memtables before stall |
| `max_levels` | 7 | Number of SSTable levels |
| `level0_file_num_compaction_trigger` | 4 | L0 files before compaction |
| `target_file_size_base` | 64MB | Target SSTable size at L1 |
| `max_bytes_for_level_base` | 256MB | Max bytes at L1 |

### Bloom Filter

| Parameter | Default | Description |
|-----------|---------|-------------|
| `bloom_filter_bits_per_key` | 10 | Bits per key (~1% FP rate) |

### WAL Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `wal_dir` | `<db_path>/wal` | WAL directory path |
| `wal_size_limit_mb` | 0 (unlimited) | Max WAL size before rotation |
| `manual_wal_flush` | false | App-controlled WAL flush |

### Compaction

| Parameter | Default | Description |
|-----------|---------|-------------|
| `compaction_style` | Leveled | Leveled compaction strategy |
| `max_background_compactions` | 4 | Concurrent compaction threads |

---

## Testing & Validation

This project implements a comprehensive validation strategy aligned with production-grade correctness requirements.

### Running All Tests

```bash
# Fast unit tests (~30 seconds)
./bin/unit_tests

# Integration tests (~5 minutes)  
./bin/integration_tests

# Full validation suite
make test-all
```

### Correctness Verification

The engine includes linearizability verification using history recording and offline checking, ensuring correct behavior under concurrent operations.

```bash
# Run linearizability checker with history recording
./bin/linearizability_check \
    --threads=64 \
    --duration=300 \
    --operations=1000000 \
    --history_file=history.log

# Offline verification of recorded history
./bin/history_verifier --input=history.log --model=kv_store
```

The linearizability checker records all concurrent operations with timestamps and validates that the execution history is linearizable—meaning there exists a sequential ordering of operations consistent with real-time ordering and the key-value store specification.

**What it validates:**
- Read-after-write consistency across threads
- Atomic visibility of batch operations
- Snapshot isolation correctness
- No stale reads under concurrent writes

### Crash Recovery Tests

```bash
# Crash injection at arbitrary points
./bin/crash_tests \
    --injection_points=mid_flush,mid_compaction,mid_wal_write \
    --iterations=100

# Verify recovery correctness
./bin/recovery_validator --db_path=/tmp/crash_test_db
```

**Injection points tested:**
- Mid-SSTable flush (partial file write)
- Mid-compaction (multiple files in flight)
- Mid-WAL write (partial record)
- Incomplete manifest update

### Stress Testing

```bash
# High thread count stress test
./bin/stress_tests \
    --threads=64 \
    --duration=3600 \
    --write_ratio=0.8 \
    --key_distribution=zipfian

# Adversarial key distributions
./bin/stress_tests --key_distribution=sequential
./bin/stress_tests --key_distribution=reverse_sequential  
./bin/stress_tests --key_distribution=hotspot
```

### Fuzz Testing

```bash
# SSTable parser fuzzing
./bin/fuzz_sstable --corpus=fuzz_corpus/sstable --runs=1000000

# WAL replay fuzzing with malformed inputs
./bin/fuzz_wal_replay --corpus=fuzz_corpus/wal --runs=1000000
```

---

## Benchmark Harness

The project includes a reproducible benchmark harness for performance measurement. All performance claims can be independently verified.

### Running Benchmarks

```bash
# Standard benchmark suite
./bin/db_bench \
    --benchmarks=fillrandom,readrandom,fillseq,readseq,readwhilewriting \
    --num=1000000 \
    --value_size=100 \
    --threads=16 \
    --db=/tmp/bench_db

# Mixed workload (80% write, 20% read)
./bin/db_bench \
    --benchmarks=mixedworkload \
    --num=10000000 \
    --write_ratio=0.8 \
    --threads=32 \
    --duration=600

# Recovery time benchmark
./bin/db_bench \
    --benchmarks=recovery \
    --wal_size_mb=1024
```

### Workload Configuration

The harness supports YCSB-style workload definitions:

```yaml
# benchmarks/workloads/write_heavy.yaml
workload:
  name: "write_heavy"
  record_count: 10000000
  operation_count: 10000000
  read_proportion: 0.2
  write_proportion: 0.8
  key_distribution: "zipfian"
  value_size: 100
  threads: 16
```

```bash
# Run with workload file
./bin/db_bench --workload=benchmarks/workloads/write_heavy.yaml
```

### Performance Targets

| Metric | Target | Notes |
|--------|--------|-------|
| Write Throughput | >100,000 ops/sec | Mixed workload, 80% writes |
| Read Throughput | >50,000 ops/sec | Under compaction pressure |
| Write Latency (p99) | <1 ms | Excluding fsync |
| Recovery Time | <10 sec | Per 1GB WAL |
| Space Amplification | <1.2x | Between compaction cycles |

**Note:** Actual performance depends on hardware configuration. Run benchmarks on your target environment to establish baselines.

### Comparative Benchmarks

```bash
# Compare against LevelDB/RocksDB (requires separate installation)
./bin/comparative_bench \
    --engines=lsm,leveldb,rocksdb \
    --workload=benchmarks/workloads/write_heavy.yaml \
    --output=results/comparison.csv
```

---

## Internals

### SSTable Format

```
┌────────────────────────────────────────┐
│            Data Blocks                 │
│  ┌──────────────────────────────────┐  │
│  │ Block 1: [key1|val1|key2|val2...]│  │
│  │ Block 2: [key5|val5|key6|val6...]│  │
│  │ ...                              │  │
│  └──────────────────────────────────┘  │
├────────────────────────────────────────┤
│            Index Block                 │
│  [block1_last_key, offset, size]       │
│  [block2_last_key, offset, size]       │
│  ...                                   │
├────────────────────────────────────────┤
│           Bloom Filter                 │
│  [serialized bloom filter bits]        │
├────────────────────────────────────────┤
│         Metadata/Properties Block      │
│  ┌──────────────────────────────────┐  │
│  │ min_key: <smallest key in file>  │  │
│  │ max_key: <largest key in file>   │  │
│  │ entry_count: <number of entries> │  │
│  │ creation_timestamp: <unix epoch> │  │
│  │ raw_size: <uncompressed bytes>   │  │
│  │ file_size: <compressed bytes>    │  │
│  └──────────────────────────────────┘  │
├────────────────────────────────────────┤
│             Footer                     │
│  [index_offset | bloom_offset |        │
│   meta_offset | magic_number ]         │
└────────────────────────────────────────┘
```

**Metadata block contents:**
- `min_key` / `max_key`: Key range for compaction scheduling and filtering
- `entry_count`: Number of key-value pairs for statistics
- `creation_timestamp`: Used for compaction prioritization

### WAL Record Format

```
┌─────────┬────────┬──────────┬─────────┐
│ CRC32   │ Length │ Type     │ Payload │
│ 4 bytes │ 2 bytes│ 1 byte   │ N bytes │
└─────────┴────────┴──────────┴─────────┘

Types: Full (1), First (2), Middle (3), Last (4)
```

**Durability guarantees:**
- CRC32 checksum for corruption detection
- Configurable fsync policies (per-write, batched, periodic)
- Atomic record semantics via record spanning

### Concurrency Model

- **MemTable**: Lock-free skip list with atomic CAS for insertion
- **Immutable MemTable Queue**: Mutex-protected deque with condition variable
- **SSTable Access**: Reference counting with epoch-based reclamation
- **Compaction**: Dedicated thread pool, non-blocking manifest updates

---

## Project Structure

```
lsm-tree-engine/
├── include/
│   └── lsm/
│       ├── db.h                 # Public API
│       ├── options.h            # Configuration
│       ├── status.h             # Error handling
│       ├── iterator.h           # Range scan interface
│       └── write_batch.h        # Atomic batch writes
├── src/
│   ├── core/
│   │   ├── memtable.cpp         # Skip list memtable
│   │   ├── skiplist.cpp         # Lock-free skip list
│   │   └── write_batch.cpp      # Batch operations
│   ├── sstable/
│   │   ├── sstable_builder.cpp  # SSTable writer
│   │   ├── sstable_reader.cpp   # SSTable reader
│   │   ├── block.cpp            # Block encoding/decoding
│   │   ├── bloom_filter.cpp     # Bloom filter
│   │   └── metadata.cpp         # Properties block
│   ├── wal/
│   │   ├── wal_writer.cpp       # WAL append path
│   │   ├── wal_reader.cpp       # WAL replay
│   │   └── wal_manager.cpp      # Segment management
│   ├── compaction/
│   │   ├── compaction.cpp       # Compaction orchestration
│   │   ├── level_compaction.cpp # Leveled strategy
│   │   └── version_set.cpp      # MVCC version management
│   ├── util/
│   │   ├── coding.cpp           # Varint encoding
│   │   ├── crc32.cpp            # Checksums
│   │   ├── arena.cpp            # Memory arena
│   │   └── env.cpp              # OS abstraction
│   └── db_impl.cpp              # Core DB implementation
├── tests/
│   ├── unit/                    # Component tests
│   ├── integration/             # End-to-end tests
│   ├── correctness/
│   │   ├── linearizability_check.cpp
│   │   └── history_verifier.cpp
│   ├── crash/                   # Crash injection tests
│   ├── stress/                  # Concurrency stress tests
│   └── fuzz/                    # Fuzzing harnesses
├── benchmarks/
│   ├── db_bench.cpp             # Benchmark harness
│   ├── comparative_bench.cpp    # Cross-engine comparison
│   └── workloads/               # YCSB workload configs
├── tools/
│   ├── lsm_dump.cpp             # SSTable inspector
│   └── lsm_repair.cpp           # Database repair utility
├── docs/
│   ├── ARCHITECTURE.md          # Detailed design doc
│   ├── TUNING.md                # Configuration guide
│   └── CORRECTNESS.md           # Verification methodology
├── CMakeLists.txt
├── README.md
└── LICENSE
```

---

## Deliverables Checklist

Per the project requirements:

- [x] **Storage engine library** with documented public API (`include/lsm/`)
- [x] **Correctness test suite** with linearizability verification (`tests/correctness/`)
- [x] **Performance test suite** for latency and throughput (`tests/stress/`)
- [x] **Failure scenario tests** with crash injection (`tests/crash/`)
- [x] **Benchmark harness** with reproducible workload generation (`benchmarks/`)
- [x] **Technical documentation** covering architecture and tuning (`docs/`)

---

## Roadmap

- [x] Core LSM-Tree architecture
- [x] WAL with crash recovery
- [x] Leveled compaction
- [x] Bloom filters
- [x] Thread-safe concurrent access
- [x] Linearizability verification tooling
- [x] Reproducible benchmark harness
- [ ] Universal compaction strategy
- [ ] Column family support
- [ ] Compression (Snappy, LZ4, Zstd)
- [ ] TTL and compaction filters

---

## Contributing

Contributions are welcome. Please read [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines on code style, testing requirements, and the pull request process.

---

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.

---

## References

- O'Neil, P., et al. "The Log-Structured Merge-Tree (LSM-Tree)" (1996)
- LevelDB Implementation Notes: https://github.com/google/leveldb
- RocksDB Wiki: https://github.com/facebook/rocksdb/wiki
- Kingsbury, K. "Jepsen: Distributed Systems Safety Research"