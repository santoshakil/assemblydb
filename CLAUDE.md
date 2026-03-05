# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

AssemblyDB is an embedded key-value database written entirely in AArch64 assembly. Zero C runtime, direct Linux syscalls, hexagonal architecture with vtable-based ports/adapters. Targets Raspberry Pi 4/5 (ARMv8.2-A with CRC32, NEON, AES hardware).

## Build Commands

```bash
make clean && make          # Build static + shared libraries
make test_all               # Run core test suites
make test                   # Phase 1: core utilities (CRC32, NEON, bloom, arena, key_compare)
make test_btree             # Phase 2: B+ tree CRUD + scan + persistence
make test_lsm              # Phase 3: memtable, SSTable, WAL, compaction
make test_mvcc             # Phase 4: MVCC transactions, snapshot isolation
make test_crypto           # Phase 5a: AES-256-CTR encryption roundtrip
make test_compress         # Phase 5b: LZ4 compression roundtrip
make test_integration      # Phase 6: full API end-to-end + secondary indexes + backup
make test_stress           # Stress testing (20 subtests incl. backup/restore roundtrip)
make test_adversarial      # Adversarial edge cases (26 subtests incl. randomized reopen model-check)
make test_persist          # Deep persistence/reopen tests (8 subtests)
make test_edge             # Edge cases (27 subtests incl. tombstone stale-read guard, restore validation)
make test_hardening        # Production hardening (45 subtests: tx write-set, scan, persistence, real-world)
make bench                 # Performance benchmarks
make size                  # Binary size report
```

All test binaries link statically against `libassemblydb.a`. Tests are C files in `tests/` that call the assembly-implemented API via `include/assemblydb.h`.

## Production Hardening Loop

- Always harden public API boundaries first: validate NULL pointers and key/value lengths before calling `build_fixed_key` / `build_fixed_val`.
- Keep fixed-record invariants strict: `key_len <= 62`, `val_len <= 254`; return `ADB_ERR_KEY_TOO_LONG` / `ADB_ERR_VAL_TOO_LONG`.
- Invalid request contract: return `ADB_ERR_INVALID` (no crash), and zero `vlen_out` on `adb_get` / `adb_tx_get` miss or invalid input.
- Keep batch semantics predictable: `adb_batch_put` must pre-validate all entries before writing, so invalid entries do not cause partial commits.
- Preserve ABI strictly: any function that executes `bl` must save/restore `x30` (and frame) before returning.
- `adb_sync` is a checkpoint: flush memtable -> B+ tree and persist B+ tree metadata before fdatasync.
- Backup/restore must copy `data.btree` and propagate copy/sync errors; never return success on partial backup state.
- Tombstones are authoritative for reads: if key exists in memtable/imm memtable as deleted, router must not fall through to SST/B+tree.
- Run break loops after every substantial change:
  `for i in $(seq 1 5); do ./test_edge && ./test_stress && ./test_adversarial || break; done`
- Track perf after hardening with `make bench`; API safety checks must not materially regress put/get throughput.
- Treat these as release gates: `make test_all test_stress test_adversarial test_edge test_persist` and `make bench`.

## Architecture

### Hexagonal / Ports-and-Adapters (in assembly)

Ports are vtables (fixed-layout structs of function pointers). Domain code calls through `ldr x9, [port, #offset]; blr x9`. Adapters populate the tables at init time (`src/init.s`).

**7 ports**: storage, cache, crypto, compress, WAL, index, tx — each defined in `src/ports/*.s`

### Hybrid Storage Engine

- **Writes** go to LSM path: skip-list memtable -> WAL -> SSTable flush -> compaction
- **Reads** check: memtable -> SSTables (with bloom filter) -> B+ tree (mmap-backed)
- `src/domain/router.s` orchestrates the hybrid read/write routing
- `src/domain/api.s` contains all 23 public API entry points

### Source Layout

```
src/
  const.s              # ALL constants, offsets, syscalls (include-only, not assembled)
  init.s               # Wires ports to adapter implementations at startup
  core/                # Zero-dependency utilities: CRC32, NEON, bloom, arena, PRNG, key_compare
  infra/               # OS interface: syscall wrappers, mutex (futex), rwlock, mmap, threads
  domain/              # Business logic: api.s, router.s, tx_manager.s, mvcc.s, metrics.s, backup
  ports/               # Vtable definitions (7 port interfaces)
  adapters/            # Implementations: btree/, lsm/, wal/, cache/, crypto/, compress/, index/
```

### Key Data Structures

- **DB handle** (`adb_t`): 1024 bytes at fixed offsets defined in `const.s` (DB_* equates). Contains port vtable pointers, B+ tree state, LSM state, WAL state, MVCC state, metrics pointer.
- **Fixed-size keys**: 64 bytes (2B length prefix + 62B data). Built by `build_fixed_key()`.
- **Fixed-size values**: 256 bytes (2B length prefix + 254B data). Built by `build_fixed_val()`.
- **B+ tree pages**: 4096B. Leaf: 12 keys + 12 vals + 12 tx_ids. Internal: 55 keys + 56 children.
- **Skip list nodes**: variable size (base 0x148 + height * 8B forward pointers).
- **MVCC version entries**: 384 bytes (tx_id, end_tx_id, key, value, tombstone, linked list).

### Persistence

- B+ tree: `MAP_SHARED` mmap — writes are immediately visible to OS, persisted on msync/close
- SSTable files: `L{level}_{seq}.sst` in database directory
- WAL segments: `wal_{seq}.log`, crash recovery replays on next open
- LOCK file: `flock(LOCK_EX)` for single-writer exclusion

### Transaction Write-Set

- `adb_tx_put/get/delete` buffer writes in a linked list of TXWN nodes (336B: key+val+is_delete+next)
- Only one active tx at a time (single-slot `DB_VERSION_STORE`); second `tx_begin` returns `ADB_ERR_LOCKED`
- `adb_tx_get` checks write-set first (via `tx_write_set_find`), falls through to `router_get` if not found
- `adb_tx_commit` replays write-set to storage via `tx_replay_write_set`, then frees nodes
- `adb_tx_rollback` discards write-set (frees nodes without replay)

## AArch64 Assembly Conventions

- **Calling convention**: x0-x7 args, x0 return, x8 syscall nr, x19-x28 callee-saved, x29=FP, x30=LR
- **Function calls clobber x0-x18 and x30** — always save x19+ before use, restore before return
- **No red zone on AArch64 Linux** — never write below SP without `sub sp, sp, #N` first
- **`cmp` immediate max 4095** — load larger values into a register for comparison
- **Signed vs unsigned branches**: `b.gt/b.ge/b.lt/b.le` are SIGNED; use `b.hi/b.hs/b.lo/b.ls` for unsigned
- **`btree_alloc_page` may remap** — re-derive all mmap pointers after calling it (capacity doubling does `munmap` + `mmap`)
- **Cross-TU internal symbols** need `.hidden` directive in shared library builds
- **Stack frames**: always `stp x29, x30, [sp, #-N]!` prologue / `ldp x29, x30, [sp], #N` epilogue
- **`str x` writes 8 bytes** — use `str w` for 4-byte struct fields; verify adjacent fields won't be clobbered

## Common Patterns in This Codebase

- `movn x0, #0` = load 0xFFFFFFFFFFFFFFFF (TX_ID_MAX, INVALID_PAGE sentinel)
- `bl build_fixed_key` / `bl build_fixed_val` to convert variable-length C data to fixed-size records
- `bl key_compare` returns negative/zero/positive; compares 64-byte fixed keys (length-prefixed)
- `bl hw_crc32c` for checksums; uses hardware `crc32cx`/`crc32cb` instructions
- Port vtable call: `ldr x9, [x19, #PORT_FN_OFFSET]; blr x9`
- Arena allocation: `bl arena_alloc` for bump allocation, `bl arena_destroy` frees entire arena

## Important Gotchas

- B+ tree delete does NOT merge/redistribute underfilled leaves (intentional, like SQLite)
- `build_fixed_key(dst, NULL, 0)` creates a zero-length key that sorts before everything — scan uses `cbz` guards for NULL start/end boundaries
- Bloom filter hashes the full 64-byte key record (not just key data bytes)
- LZ4 hash table: offset 0 means "no match" (self-reference guard with `cbz`)
- Crypto context is 256 bytes (`CCTX_SIZE`); zero ALL 256B when clearing, not just 240B
- `const.s` is `.include`d (not assembled separately) — it's in every `.o` file's preprocessing
