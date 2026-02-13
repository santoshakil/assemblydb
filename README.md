# AssemblyDB

A production-grade embedded key-value database written entirely in **AArch64 assembly**. Zero C runtime. Direct Linux syscalls. Hybrid LSM-Tree + B+ Tree storage engine.

## Features

- **Pure Assembly**: ~12,000 lines of AArch64 assembly across 54 source files. No C runtime dependency.
- **Hybrid Storage Engine**: LSM-Tree for writes (skip-list memtable + SSTables), B+ Tree for persistent storage, with a unified query router that merges across all data sources.
- **Hardware Acceleration**: ARM CRC32C instructions for checksums, NEON SIMD for memcpy/memcmp/key comparison, hardware AES instructions for encryption.
- **MVCC Transactions**: Multi-Version Concurrency Control with snapshot isolation, explicit begin/commit/rollback.
- **Write-Ahead Log**: Crash recovery via WAL with configurable sync modes.
- **AES-256-CTR Encryption**: Hardware-accelerated page-level encryption using ARM crypto extensions.
- **LZ4 Compression**: Fast compression for data pages.
- **LRU Page Cache**: O(1) hash-based page cache with power-of-2 sizing and bitwise AND hashing.
- **Secondary Indexes**: B+ tree based secondary index support.
- **Backup & Restore**: Full backup with zero-copy `sendfile`, manifest with CRC32 integrity checking.
- **Bloom Filters**: CRC32C double-hashing bloom filters for SSTable lookups.
- **Hexagonal Architecture**: Vtable-based ports and adapters pattern for swappable components (storage, cache, crypto, compression, WAL, index, transaction ports).

## Target Platform

- **Architecture**: AArch64 (ARMv8-A with CRC, Crypto, and NEON extensions)
- **OS**: Linux (direct syscall interface)
- **Tested on**: Raspberry Pi 4/5 (Cortex-A72/A76)

## Build

```bash
make          # Build shared + static libraries
make test_all # Run all 134 tests (7 test suites)
make bench    # Run benchmarks
make size     # Binary size report
make clean    # Clean build artifacts
```

**Requirements**: GNU `as` (binutils) with AArch64 support, `gcc` for test harness, `ld` for linking.

## Binary Size

| Artifact | Size |
|----------|------|
| Shared library (.text) | ~35 KB |
| Shared library (file) | ~618 KB |
| Static library | ~876 KB |

## Benchmark Results (Raspberry Pi 5)

| Operation | Throughput |
|-----------|-----------|
| CRC32C | 5.84M ops/sec (24 GB/s) |
| NEON memcpy | 48 GB/s |
| Key compare | 269M ops/sec |
| LRU cache | 85M ops/sec |
| AES-256-CTR | 665K ops/sec (2.7 GB/s) |
| LZ4 compress | 218K ops/sec |
| LZ4 decompress | 327K ops/sec |
| adb_put | 577K ops/sec |
| adb_get | 3.1M ops/sec |
| Mixed workload | 998K ops/sec |

## C API

```c
#include "assemblydb.h"

adb_t *db;
adb_open("/tmp/mydb", NULL, &db);

// Simple key-value
adb_put(db, "hello", 5, "world", 5);

uint16_t vlen;
char buf[256];
adb_get(db, "hello", 5, buf, sizeof(buf), &vlen);

// Transactions
uint64_t tx;
adb_tx_begin(db, ADB_ISO_SNAPSHOT, &tx);
adb_tx_put(db, tx, "key", 3, "val", 3);
adb_tx_commit(db, tx);

// Backup
adb_backup(db, "/tmp/mydb_backup", ADB_BACKUP_FULL);

adb_close(db);
```

See [`include/assemblydb.h`](include/assemblydb.h) for the full API (23 public functions).

## Architecture

```
Public API (C-callable)
    |
Domain Layer (router, tx_manager, mvcc, backup, metrics)
    |
Port Vtables (storage, cache, crypto, compress, wal, index, tx)
    |
Adapters (B+ tree, LSM engine, LRU cache, AES-256, LZ4, WAL, secondary index)
    |
Infrastructure (syscalls, mmap, mutex, rwlock, threads)
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
tests/                 # C test harness (7 test suites, 134 tests)
include/assemblydb.h   # Public C header
```

## Tests

| Suite | Tests | Coverage |
|-------|-------|----------|
| test_core | 39 | CRC32, NEON, keys, bloom, arena, PRNG, strings, memory, errors |
| test_btree | 20 | Page ops, CRUD, splits, scan, stress (1000 keys) |
| test_lsm | 17 | Memtable, LSM adapter, WAL, SSTable flush/read |
| test_mvcc | 16 | Mutex, rwlock, transactions, MVCC visibility, router, API |
| test_crypto | 15 | Key expansion, AES block/CTR, NIST vectors, crypto context |
| test_compress | 13 | LZ4 roundtrip, various patterns, edge cases, no-op |
| test_integration | 14 | LRU cache, index, encrypt+compress, full API lifecycle, backup |

## License

MIT
