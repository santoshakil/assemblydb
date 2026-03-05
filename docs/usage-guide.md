# AssemblyDB Usage Guide

## Overview

AssemblyDB is an embedded key-value database for AArch64 Linux. It provides a C-callable API via `include/assemblydb.h`. Link your application against `libassemblydb.a` (static) or `libassemblydb.so` (shared).

## Building

```bash
make                # Produces libassemblydb.so and libassemblydb.a
```

Link your app:

```bash
# Static linking (recommended for embedded)
gcc -O2 -I include -o myapp myapp.c -L. -l:libassemblydb.a -static

# Dynamic linking
gcc -O2 -I include -o myapp myapp.c -L. -lassemblydb
```

**Platform requirement**: AArch64 Linux with ARMv8.2-A (CRC32, NEON, AES crypto extensions). Tested on Raspberry Pi 4/5.

## Database Lifecycle

### Opening a Database

```c
#include "assemblydb.h"

adb_t *db;
int err = adb_open("/path/to/mydb", NULL, &db);
if (err != ADB_OK) {
    // Handle error
}
```

The path is a directory. AssemblyDB creates it if needed and places these files inside:
- `data.btree` — mmap-backed B+ tree (persistent storage)
- `wal_*.log` — write-ahead log segments
- `L{level}_{seq}.sst` — SSTable files from memtable flushes
- `LOCK` — exclusive file lock (prevents concurrent opens)
- `MANIFEST` — metadata after backup/restore

Only one process can open a database at a time (`flock` exclusion). A second `adb_open` on the same path returns `ADB_ERR_LOCKED`.

### Configuration

Pass `NULL` for defaults, or configure:

```c
adb_config_t cfg = {
    .cache_size_pages = 64,       // LRU cache pages (default: 64)
    .encryption_enabled = 0,      // 1 = AES-256-CTR page encryption
    .compression_enabled = 0,     // 1 = LZ4 page compression
    .wal_sync_mode = ADB_WAL_SYNC, // ADB_WAL_ASYNC, ADB_WAL_SYNC, ADB_WAL_FULL
    .memtable_max_bytes = 0,      // 0 = default (auto)
};
adb_open("/path/to/mydb", &cfg, &db);
```

### Closing

```c
adb_close(db);  // Flushes memtable to B+ tree, syncs WAL, releases lock
```

Always close the database to ensure durability. `adb_close(NULL)` is a safe no-op.

### Sync (Checkpoint)

```c
adb_sync(db);  // Flush memtable -> B+ tree, fsync
```

Forces all in-memory data to persistent storage. Use periodically for durability guarantees without closing.

### Destroy

```c
adb_destroy("/path/to/mydb");  // Deletes database directory and all files
```

Removes data.btree, WAL files, SSTables, MANIFEST, and LOCK. Safe to call on non-existent paths.

## Key-Value Operations

### Limits

- Key: 1-62 bytes (returns `ADB_ERR_KEY_TOO_LONG` if > 62)
- Value: 0-254 bytes (returns `ADB_ERR_VAL_TOO_LONG` if > 254)
- Keys and values are binary-safe (embedded nulls OK)

### Put

```c
int err = adb_put(db, key_ptr, key_len, val_ptr, val_len);
// Returns ADB_OK on success
// Upserts: overwrites if key already exists
```

### Get

```c
char buf[256];
uint16_t vlen;
int err = adb_get(db, key_ptr, key_len, buf, sizeof(buf), &vlen);
if (err == ADB_OK) {
    // buf contains value, vlen = actual length
} else if (err == ADB_ERR_NOT_FOUND) {
    // Key doesn't exist
}
```

- `val_len_out` can be NULL (length won't be returned)
- If `val_buf_len` < actual value length, the value is truncated to fit

### Delete

```c
int err = adb_delete(db, key_ptr, key_len);
// Returns ADB_OK even if key didn't exist
```

Deletes write a tombstone. The key is masked in all reads immediately.

## Range Scan

```c
int my_callback(const void *key, uint16_t key_len,
                const void *val, uint16_t val_len,
                void *user_data) {
    // Process key-value pair
    // Return 0 to continue, non-zero to stop
    return 0;
}

// Scan range [start, end] inclusive
adb_scan(db, start_key, start_len, end_key, end_len, my_callback, user_data);

// Full scan (all keys in sorted order)
adb_scan(db, NULL, 0, NULL, 0, my_callback, user_data);
```

- Keys are returned in lexicographic sorted order
- Start/end are inclusive bounds; pass NULL for unbounded
- If start > end, returns zero results
- Callback return non-zero stops iteration immediately

## Batch Operations

```c
adb_batch_entry_t entries[] = {
    { "key1", 4, "val1", 4 },
    { "key2", 4, "val2", 4 },
    { "key3", 4, "val3", 4 },
};
int err = adb_batch_put(db, entries, 3);
```

- Pre-validates all entries before writing (no partial commits on invalid input)
- If any entry has invalid key/value length, the entire batch is rejected
- `count = 0` is a no-op

## Transactions

AssemblyDB supports one active transaction at a time.

```c
uint64_t tx;
int err = adb_tx_begin(db, ADB_ISO_SNAPSHOT, &tx);
if (err == ADB_ERR_LOCKED) {
    // Another transaction is active
}

// Buffered writes (not visible outside transaction)
adb_tx_put(db, tx, "key", 3, "val", 3);
adb_tx_delete(db, tx, "old", 3);

// Read within transaction (checks write-set first, then storage)
char buf[256];
uint16_t vlen;
adb_tx_get(db, tx, "key", 3, buf, sizeof(buf), &vlen);

// Scan within transaction
adb_tx_scan(db, tx, NULL, 0, NULL, 0, callback, ctx);

// Commit (atomically applies all writes)
adb_tx_commit(db, tx);

// OR rollback (discards all writes)
adb_tx_rollback(db, tx);
```

**Transaction behavior**:
- `tx_put`/`tx_delete` buffer in a write-set (linked list)
- `tx_get` checks the write-set first; if not found, falls through to storage
- `tx_commit` replays the write-set to storage atomically
- `tx_rollback` discards the write-set without applying
- Only one active transaction at a time; second `tx_begin` returns `ADB_ERR_LOCKED`
- Wrong tx_id returns `ADB_ERR_TX_NOT_FOUND`

**Isolation levels** (parameter to `tx_begin`):

| Level | Value |
|-------|-------|
| `ADB_ISO_READ_UNCOMMITTED` | 0 |
| `ADB_ISO_READ_COMMITTED` | 1 |
| `ADB_ISO_REPEATABLE_READ` | 2 |
| `ADB_ISO_SNAPSHOT` | 3 |
| `ADB_ISO_SERIALIZABLE` | 4 |

## Secondary Indexes

```c
// Create an index (name up to 247 bytes)
adb_create_index(db, "by_category", NULL);

// Scan by secondary key
adb_index_scan(db, "by_category", sec_key, sec_key_len, callback, ctx);

// Drop index
adb_drop_index(db, "by_category");
```

**Limitations**: Each index supports up to 12 entries per index (single leaf page). The `extract_key` function parameter is currently ignored (fixed schema).

## Backup and Restore

### Full Backup

```c
int err = adb_backup(db, "/path/to/backup", ADB_BACKUP_FULL);
```

Creates a consistent point-in-time snapshot:
- Calls `adb_sync` first to flush all data
- Copies `data.btree` using `sendfile` (zero-copy)
- Writes MANIFEST with CRC32 integrity check
- All writes are atomic (temp files + rename + fsync)

### Restore

```c
int err = adb_restore("/path/to/backup", "/path/to/restored");
```

**Validation before restore**:
- Verifies MANIFEST exists, has correct magic/version/CRC
- Rejects semantically invalid manifests (invalid root page, page counts)
- Validates data.btree file size and root page type
- Rejects source == destination (path and inode-level alias detection)
- Destroys destination before restoring (clean slate)

### Backup Files

The backup directory contains:
- `data.btree` — B+ tree data file
- `MANIFEST` — 512-byte metadata with CRC32 integrity

## Metrics

```c
adb_metrics_t m;
adb_get_metrics(db, &m);
printf("puts: %lu, gets: %lu, deletes: %lu\n",
       m.puts_total, m.gets_total, m.deletes_total);
```

Available counters:

| Field | Description |
|-------|-------------|
| `puts_total` | Total put operations |
| `gets_total` | Total get operations |
| `deletes_total` | Total delete operations |
| `scans_total` | Total scan operations |
| `cache_hits` | LRU cache hits |
| `cache_misses` | LRU cache misses |
| `bloom_true_positives` | Bloom filter true positives |
| `bloom_false_positives` | Bloom filter false positives |
| `bytes_written` | Total bytes written |
| `bytes_read` | Total bytes read |
| `compactions_run` | Compaction runs |
| `wal_syncs` | WAL sync operations |
| `tx_commits` | Transaction commits |
| `tx_rollbacks` | Transaction rollbacks |
| `page_splits` | B+ tree page splits |
| `memtable_flushes` | Memtable flush operations |

Metrics reset on each `adb_open` (not persisted).

## Error Codes

| Code | Name | Meaning |
|------|------|---------|
| 0 | `ADB_OK` | Success |
| 1 | `ADB_ERR_NOT_FOUND` | Key not found |
| 2 | `ADB_ERR_IO` | I/O error |
| 3 | `ADB_ERR_CORRUPT` | Data corruption detected |
| 4 | `ADB_ERR_KEY_TOO_LONG` | Key > 62 bytes |
| 5 | `ADB_ERR_VAL_TOO_LONG` | Value > 254 bytes |
| 6 | `ADB_ERR_LOCKED` | Database or transaction locked |
| 7 | `ADB_ERR_NOMEM` | Memory allocation failed |
| 8 | `ADB_ERR_INVALID` | Invalid argument (NULL db, etc.) |
| 9 | `ADB_ERR_TX_CONFLICT` | Transaction conflict |
| 10 | `ADB_ERR_TX_NOT_FOUND` | Invalid transaction ID |
| 11 | `ADB_ERR_TX_ABORTED` | Transaction aborted |
| 12 | `ADB_ERR_FULL` | Storage full |
| 13 | `ADB_ERR_EXISTS` | Already exists |
| 14 | `ADB_ERR_DECRYPT` | Decryption failed |
| 15 | `ADB_ERR_COMPRESS` | Compression failed |

All errors are positive integers. `ADB_OK` (0) = success.

## Storage Architecture

### Write Path

1. `adb_put` writes to the skip-list memtable (in-memory)
2. WAL append logs the operation for crash recovery
3. `adb_sync` flushes memtable to B+ tree (mmap-backed, durable)
4. Compaction merges SSTables in the background

### Read Path

1. Check active memtable (most recent writes)
2. Check immutable memtable (being flushed)
3. Check L0 SSTables (newest first, with bloom filter)
4. Check L1 SSTables (oldest first, with bloom filter)
5. Check B+ tree (persistent, mmap-backed)

Tombstones are authoritative: a delete in the memtable masks older values at all lower levels.

### Durability Guarantees

| Scenario | Durability |
|----------|------------|
| `adb_put` then `adb_close` | Durable (close flushes + syncs) |
| `adb_put` then `adb_sync` | Durable (sync flushes to B+ tree + fsync) |
| `adb_put` then crash | Recovered via WAL replay on next open |
| `adb_put` then `kill -9` | Recovered via WAL replay on next open |
| `adb_put` only (no close/sync) | WAL preserves data if `wal_sync_mode != ASYNC` |

### File Layout

```
mydb/
  data.btree     # B+ tree pages (4KB each, mmap-backed)
  wal_0001.log   # WAL segment
  L0_0001.sst    # Level 0 SSTable
  L1_0001.sst    # Level 1 SSTable (post-compaction)
  LOCK           # Exclusive file lock
```

## Complete Example

```c
#include <stdio.h>
#include <string.h>
#include "assemblydb.h"

static int print_cb(const void *key, uint16_t klen,
                    const void *val, uint16_t vlen, void *ctx) {
    printf("  %.*s = %.*s\n", klen, (const char *)key, vlen, (const char *)val);
    return 0;
}

int main() {
    adb_t *db;
    int err;

    // Open with defaults
    err = adb_open("/tmp/example_db", NULL, &db);
    if (err) { fprintf(stderr, "open failed: %d\n", err); return 1; }

    // Insert some data
    adb_put(db, "name", 4, "Alice", 5);
    adb_put(db, "city", 4, "Dhaka", 5);
    adb_put(db, "lang", 4, "Assembly", 8);

    // Read back
    char buf[256];
    uint16_t vlen;
    err = adb_get(db, "name", 4, buf, sizeof(buf), &vlen);
    if (err == ADB_OK) printf("name = %.*s\n", vlen, buf);

    // Transaction: atomic multi-key update
    uint64_t tx;
    adb_tx_begin(db, ADB_ISO_SNAPSHOT, &tx);
    adb_tx_put(db, tx, "score", 5, "100", 3);
    adb_tx_put(db, tx, "level", 5, "42", 2);
    adb_tx_commit(db, tx);

    // Batch insert
    adb_batch_entry_t batch[] = {
        { "b1", 2, "v1", 2 },
        { "b2", 2, "v2", 2 },
    };
    adb_batch_put(db, batch, 2);

    // Scan all keys in order
    printf("All keys:\n");
    adb_scan(db, NULL, 0, NULL, 0, print_cb, NULL);

    // Checkpoint to disk
    adb_sync(db);

    // Backup
    adb_backup(db, "/tmp/example_backup", ADB_BACKUP_FULL);

    // Metrics
    adb_metrics_t m;
    adb_get_metrics(db, &m);
    printf("Stats: %lu puts, %lu gets\n", m.puts_total, m.gets_total);

    // Cleanup
    adb_close(db);

    // Restore from backup
    adb_restore("/tmp/example_backup", "/tmp/example_restored");

    // Clean up
    adb_destroy("/tmp/example_db");
    adb_destroy("/tmp/example_backup");
    adb_destroy("/tmp/example_restored");

    return 0;
}
```

Compile and run:

```bash
gcc -O2 -I include -o example example.c -L. -l:libassemblydb.a -static
./example
```

## Low-Level Core Functions

AssemblyDB also exposes core primitives for use in your application (see `assemblydb.h`):

- **CRC32C**: `hw_crc32c(data, len)` — hardware-accelerated checksums
- **NEON**: `neon_memcpy`, `neon_memset`, `neon_memzero`, `neon_memcmp` — SIMD memory ops
- **Key ops**: `key_compare`, `key_equal`, `build_fixed_key`, `build_fixed_val`
- **Bloom filter**: `bloom_create`, `bloom_add`, `bloom_check`, `bloom_destroy`
- **Arena allocator**: `arena_create`, `arena_alloc`, `arena_reset`, `arena_destroy`
- **AES-256**: `aes_key_expand_256`, `aes_encrypt_block`, `aes_ctr_process`
- **LZ4**: `lz4_compress`, `lz4_decompress`, `lz4_max_compressed_size`
- **String ops**: `asm_strlen`, `asm_strcpy`, `u64_to_dec`
- **Memory**: `page_alloc`, `page_free`, `alloc_zeroed`, `free_mem`
