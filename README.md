# AssemblyDB

A production-grade embedded key-value database written entirely in **AArch64 assembly**. Zero C runtime. Direct Linux syscalls. Hybrid LSM-Tree + B+ Tree storage engine.

## Features

- **Pure Assembly**: ~12,000 lines of AArch64 assembly across 51 source files. No C runtime dependency.
- **Hybrid Storage Engine**: LSM-Tree for writes (skip-list memtable + SSTables), B+ Tree for persistent storage, with a unified query router that merges across all data sources.
- **Hardware Acceleration**: ARM CRC32C instructions for checksums, NEON SIMD for memcpy/memcmp/key comparison, hardware AES instructions for encryption.
- **MVCC Transactions**: Multi-Version Concurrency Control with snapshot isolation, explicit begin/commit/rollback with write-set buffering.
- **Write-Ahead Log**: Crash recovery via WAL with configurable sync modes and EINTR-safe I/O.
- **AES-256-CTR Encryption**: Hardware-accelerated page-level encryption using ARM crypto extensions.
- **LZ4 Compression**: Fast compression for data pages with 64-bit safe loop counters.
- **LRU Page Cache**: O(1) hash-based page cache with power-of-2 sizing and unsigned hash index wrapping.
- **Secondary Indexes**: B+ tree based secondary index support.
- **Backup & Restore**: Atomic backup with `sendfile`, manifest CRC32 integrity, inode-level alias detection, and corruption rejection.
- **Bloom Filters**: CRC32C double-hashing bloom filters for SSTable lookups.
- **Hexagonal Architecture**: Vtable-based ports and adapters pattern for swappable components (storage, cache, crypto, compression, WAL, index, transaction ports).
- **Production Hardened**: 661+ tests across 13 test suites, signed/unsigned audit, NULL validation on all API entry points.

## Target Platform

- **Architecture**: AArch64 (ARMv8-A with CRC, Crypto, and NEON extensions)
- **OS**: Linux (direct syscall interface)
- **Tested on**: Raspberry Pi 4/5 (Cortex-A72/A76)

## Build

```bash
make          # Build shared + static libraries
make test_all # Run core test suites (138 tests, 7 suites)
make bench    # Run benchmarks
make size     # Binary size report
make clean    # Clean build artifacts
```

**Requirements**: GNU `as` (binutils) with AArch64 support, `gcc` for test harness, `ld` for linking.

## Binary Size

| Artifact | Size |
|----------|------|
| Shared library (.text) | ~40 KB |
| Shared library (file) | ~556 KB |
| Static library | ~886 KB |

## Benchmark Results (Raspberry Pi 5)

| Operation | Throughput |
|-----------|-----------|
| CRC32C (4KB) | 5.3M ops/sec (22 GB/s) |
| NEON memcpy (4KB) | 34 GB/s |
| Key compare | 258M ops/sec |
| LRU cache fetch | 119M ops/sec |
| AES-256-CTR (4KB) | 673K ops/sec (2.8 GB/s) |
| LZ4 compress (4KB) | 217K ops/sec (888 MB/s) |
| LZ4 decompress (4KB) | 323K ops/sec (1.3 GB/s) |
| Memtable put (10K) | 2.0M ops/sec |
| Memtable get (10K) | 3.2M ops/sec |
| adb_put (1K keys) | 563K ops/sec |
| adb_get (1K keys) | 3.25M ops/sec |
| Mixed read/write | 969K ops/sec |
| Batch put | 678K ops/sec |
| Delete | 643K ops/sec |
| TX begin+commit | 101K ops/sec |

## Quick Start

```c
#include "assemblydb.h"

int main() {
    adb_t *db;

    // Open database (NULL config = defaults)
    adb_open("/tmp/mydb", NULL, &db);

    // Put a key-value pair
    adb_put(db, "hello", 5, "world", 5);

    // Get it back
    char buf[256];
    uint16_t vlen;
    adb_get(db, "hello", 5, buf, sizeof(buf), &vlen);
    // buf now contains "world", vlen = 5

    // Delete
    adb_delete(db, "hello", 5);

    // Transactions
    uint64_t tx;
    adb_tx_begin(db, ADB_ISO_SNAPSHOT, &tx);
    adb_tx_put(db, tx, "key1", 4, "val1", 4);
    adb_tx_put(db, tx, "key2", 4, "val2", 4);
    adb_tx_commit(db, tx);  // atomic commit

    // Range scan
    adb_scan(db, "key1", 4, "key2", 4, my_callback, NULL);

    // Backup and restore
    adb_backup(db, "/tmp/mydb_backup", ADB_BACKUP_FULL);
    adb_close(db);

    adb_restore("/tmp/mydb_backup", "/tmp/mydb_restored");
    return 0;
}
```

See [`include/assemblydb.h`](include/assemblydb.h) for the full API (23 public functions) and [`docs/usage-guide.md`](docs/usage-guide.md) for the complete usage guide.

## Architecture

```
Public API (23 C-callable functions)
    |
Domain Layer (router, tx_manager, mvcc, backup, metrics)
    |
Port Vtables (storage, cache, crypto, compress, wal, index, tx)
    |
Adapters (B+ tree, LSM engine, LRU cache, AES-256, LZ4, WAL, secondary index)
    |
Infrastructure (syscalls, mmap, mutex/futex, rwlock, threads)
    |
Core Utilities (CRC32, NEON ops, bloom filter, arena allocator, PRNG)
```

## Project Structure

```
src/
  const.s              # Constants, offsets, syscall numbers
  init.s               # Library initialization
  core/                # Hardware-accelerated primitives
  infra/               # OS interface (syscalls, threading, mmap)
  ports/               # Port vtable definitions (7 ports)
  adapters/            # Adapter implementations
    engine/btree/      # B+ tree (search, insert, delete, scan)
    engine/lsm/        # LSM (memtable, sstable, compaction)
    cache/             # LRU page cache
    crypto/            # AES-256-CTR + no-op passthrough
    compress/          # LZ4 + no-op passthrough
    wal/               # Write-ahead log
    index/             # Secondary B+ tree indexes
  domain/              # Domain logic (API, router, MVCC, backup)
tests/                 # C test harness (13 suites, 661+ tests)
include/assemblydb.h   # Public C header
docs/                  # Documentation
```

## Tests

| Suite | Tests | Coverage |
|-------|-------|----------|
| test_core | 39 | CRC32, NEON, keys, bloom, arena, PRNG, strings, memory, errors |
| test_btree | 20 | Page ops, CRUD, splits, scan, stress (1000 keys) |
| test_lsm | 19 | Memtable, LSM adapter, WAL, SSTable flush/read |
| test_mvcc | 16 | Mutex, rwlock, transactions, MVCC visibility, router, API |
| test_crypto | 15 | Key expansion, AES block/CTR, NIST vectors, crypto context |
| test_compress | 13 | LZ4 roundtrip, various patterns, edge cases, no-op |
| test_integration | 16 | LRU cache, index, encrypt+compress, full API, backup |
| test_stress | 28 | Large datasets, persistence, crash recovery, stability |
| test_adversarial | 70 | Input validation, real-world workloads, WAL+TX audit |
| test_edge | 150 | Boundary conditions, CRC integrity, scan correctness |
| test_persist | 8 | Deep persistence: delete/overwrite/reinsert across reopen |
| test_hardening | 320 | TX write-set, scan, persistence, real-world patterns |
| test_production | 30 | App restart, compaction, lock exclusion, full API smoke |

Run extended test suites:

```bash
make test_stress test_adversarial test_edge test_persist test_hardening test_production
```

## License

MIT
