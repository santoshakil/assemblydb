// AssemblyDB - Public API Entry Points
// C-callable functions matching include/assemblydb.h
// Routes through domain logic to port/adapter layer

.include "src/const.s"

.text

.hidden wal_truncate_all

// ============================================================================
// adb_open(path, config, db_out) -> 0=ok, neg=error
// Open or create database at the given directory path
// x0 = path (null-terminated), x1 = config_ptr (can be NULL), x2 = db_out ptr
// ============================================================================
.global adb_open
.type adb_open, %function
adb_open:
    stp x29, x30, [sp, #-64]!
    mov x29, sp
    stp x19, x20, [sp, #16]
    stp x21, x22, [sp, #32]
    str x23, [sp, #48]

    mov x19, x0                    // path
    mov x20, x1                    // config
    mov x21, x2                    // db_out

    cbz x19, .Lao_invalid
    cbz x21, .Lao_invalid
    str xzr, [x21]

    // Allocate db handle (DB_SIZE = 1024 bytes)
    mov x0, #DB_SIZE
    bl alloc_zeroed
    cbz x0, .Lao_nomem
    mov x22, x0                    // db_ptr

    // Store path
    str x19, [x22, #DB_PATH_PTR]
    mov x0, x19
    bl asm_strlen
    str x0, [x22, #DB_PATH_LEN]

    // Store config if provided
    str x20, [x22, #DB_CONFIG_PTR]

    // Create directory if needed
    mov w0, #AT_FDCWD
    mov x1, x19
    mov w2, #0755
    bl sys_mkdirat
    // Ignore error (may already exist)

    // Open directory
    mov w0, #AT_FDCWD
    mov x1, x19
    mov w2, #O_RDONLY
    orr w2, w2, #O_DIRECTORY
    mov w3, #0
    bl sys_openat
    cmp x0, #0
    b.lt .Lao_fail_free
    str x0, [x22, #DB_DIR_FD]

    // Acquire exclusive LOCK file (non-blocking)
    ldr w0, [x22, #DB_DIR_FD]
    adrp x1, .Llock_name
    add x1, x1, :lo12:.Llock_name
    mov w2, #(O_WRONLY | O_CREAT)
    mov w3, #0644
    bl sys_openat
    cmp x0, #0
    b.lt .Lao_fail_close
    str x0, [x22, #DB_LOCK_FD]
    // flock(fd, LOCK_EX | LOCK_NB)
    mov w1, #(LOCK_EX | LOCK_NB)
    bl sys_flock
    cmp x0, #0
    b.lt .Lao_fail_flock

    // Create wal/ and sst/ subdirectories
    ldr w0, [x22, #DB_DIR_FD]
    adrp x1, .Lwal_dir
    add x1, x1, :lo12:.Lwal_dir
    mov w2, #0755
    bl sys_mkdirat
    // ignore error

    ldr w0, [x22, #DB_DIR_FD]
    adrp x1, .Lsst_dir
    add x1, x1, :lo12:.Lsst_dir
    mov w2, #0755
    bl sys_mkdirat
    // ignore error

    // Initialize B+ tree adapter
    mov x0, x22
    ldr w1, [x22, #DB_DIR_FD]
    bl btree_adapter_init
    cbnz x0, .Lao_fail_lock

    // Initialize LSM adapter (memtable + arena)
    mov x0, x22
    bl lsm_adapter_init
    cbnz x0, .Lao_fail_btree

    // Initialize WAL adapter
    mov x0, x22
    bl wal_adapter_init
    // WAL init failure is non-fatal (operates without WAL)

    // WAL crash recovery: replay records into memtable
    ldr x0, [x22, #DB_WAL_PORT]
    cbz x0, .Lao_skip_recover      // no WAL → skip recovery
    mov x0, x22
    adrp x1, wal_replay_cb
    add x1, x1, :lo12:wal_replay_cb
    mov x2, x22                     // ctx = db_ptr
    bl wal_recover
    // Recovery failure is non-fatal (best-effort)

    // Reopen WAL with updated DB_WAL_SEQ (recovery may have advanced it).
    // Without this, the writing fd still points at segment 0 while
    // DB_WAL_SEQ > 0, causing segment number gaps on rotation.
    mov x0, x22
    bl wal_close
    mov x0, x22
    bl wal_open
    // Failure non-fatal (operates without WAL)
.Lao_skip_recover:

    // Seed PRNG for skip-list random height with real entropy
    sub sp, sp, #16
    mov x0, #CLOCK_MONOTONIC
    mov x1, sp
    bl sys_clock_gettime
    cmp x0, #0
    b.lt .Lao_seed_fallback
    ldr x0, [sp]                   // tv_sec
    ldr x1, [sp, #8]              // tv_nsec
    eor x0, x0, x1                 // mix for entropy
    b .Lao_seed_ready
.Lao_seed_fallback:
    mov x0, x22                    // use db handle address as entropy
.Lao_seed_ready:
    add sp, sp, #16
    bl prng_seed

    // Initialize metrics
    mov x0, x22
    bl metrics_init
    // Metrics failure is non-fatal

    // Initialize next_tx_id to 1
    mov x0, #1
    str x0, [x22, #DB_NEXT_TX_ID]

    // Store db_ptr in output
    str x22, [x21]

    mov x0, #0
    b .Lao_ret

.Lao_fail_btree:
    mov x0, x22
    bl btree_adapter_close

.Lao_fail_flock:
    // flock failure: close lock fd, dir fd, free handle, return LOCKED
    ldr w0, [x22, #DB_LOCK_FD]
    cmp w0, #0
    b.lt .Lao_flock_close_dir
    bl sys_close
.Lao_flock_close_dir:
    ldr w0, [x22, #DB_DIR_FD]
    bl sys_close
    mov x0, x22
    mov x1, #DB_SIZE
    bl free_mem
    mov x0, #ADB_ERR_LOCKED
    b .Lao_ret

.Lao_fail_lock:
    ldr w0, [x22, #DB_LOCK_FD]
    cmp w0, #0
    b.lt .Lao_fail_close
    bl sys_close

.Lao_fail_close:
    ldr w0, [x22, #DB_DIR_FD]
    bl sys_close

.Lao_fail_free:
    mov x0, x22
    mov x1, #DB_SIZE
    bl free_mem
    mov x0, #ADB_ERR_IO
    b .Lao_ret

.Lao_nomem:
    mov x0, #ADB_ERR_NOMEM
    b .Lao_ret

.Lao_invalid:
    mov x0, #ADB_ERR_INVALID

.Lao_ret:
    ldr x23, [sp, #48]
    ldp x21, x22, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #64
    ret

.Llock_name:
    .asciz "LOCK"
    .align 2
.Lwal_dir:
    .asciz "wal"
    .align 2
.Lsst_dir:
    .asciz "sst"
    .align 2
.size adb_open, .-adb_open

// ============================================================================
// adb_close(db_ptr) -> 0
// Close database, free all resources
// x0 = db_ptr
// ============================================================================
.global adb_close
.type adb_close, %function
adb_close:
    stp x29, x30, [sp, #-32]!
    mov x29, sp
    str x19, [sp, #16]

    mov x19, x0
    cbz x19, .Lac2_done

    // Clean up active transaction if user forgot to commit/rollback
    ldr x0, [x19, #DB_VERSION_STORE]
    cbz x0, .Lac2_no_tx
    ldr x0, [x0, #TXD_WRITE_SET]
    bl tx_free_write_set
    ldr x0, [x19, #DB_VERSION_STORE]
    mov x1, #TXD_SIZE
    bl free_mem
    str xzr, [x19, #DB_VERSION_STORE]
.Lac2_no_tx:

    // Flush memtable to B+ tree for persistence
    mov x0, x19
    bl flush_memtable_to_btree
    cbnz x0, .Lac2_skip_wal_trunc

    // Delete ALL WAL segment files ONLY after successful flush
    mov x0, x19
    bl wal_truncate_all
.Lac2_skip_wal_trunc:

    // Close WAL
    mov x0, x19
    bl wal_adapter_close

    // Close LSM
    mov x0, x19
    bl lsm_adapter_close

    // Close B+ tree (writes metadata to page 0)
    mov x0, x19
    bl btree_adapter_close

    // Free metrics
    mov x0, x19
    bl metrics_destroy

    // Close LOCK fd (releases flock)
    ldr w0, [x19, #DB_LOCK_FD]
    cmp w0, #0
    b.lt .Lac2_no_lock
    bl sys_close
.Lac2_no_lock:

    // Close directory fd
    ldr w0, [x19, #DB_DIR_FD]
    cmp w0, #0
    b.lt .Lac2_no_dir
    bl sys_close
.Lac2_no_dir:

    // Free db handle
    mov x0, x19
    mov x1, #DB_SIZE
    bl free_mem

.Lac2_done:
    mov x0, #0
    ldr x19, [sp, #16]
    ldp x29, x30, [sp], #32
    ret
.size adb_close, .-adb_close

// ============================================================================
// flush_memtable_to_btree(db_ptr) -> 0
// Replay active memtable into B+ tree (idempotent)
// ============================================================================
.global flush_memtable_to_btree
.type flush_memtable_to_btree, %function
flush_memtable_to_btree:
    stp x29, x30, [sp, #-48]!
    mov x29, sp
    stp x19, x20, [sp, #16]
    str x21, [sp, #32]

    mov x19, x0
    mov w21, #0                    // error accumulator
    cbz x19, .Lfmt_done

    ldr x0, [x19, #DB_MEMTABLE_PTR]
    cbz x0, .Lfmt_done
    bl memtable_iter_first
    cbz x0, .Lfmt_done

.Lfmt_loop:
    mov x20, x0

    ldrb w1, [x20, #0x141]        // SLN_IS_DELETED
    cbnz w1, .Lfmt_delete

    mov x0, x19
    mov x1, x20
    add x2, x20, #0x40            // val = node + SLN_VAL_LEN
    mov x3, #0
    bl btree_insert
    cbnz w21, .Lfmt_next           // already have error, skip
    mov w21, w0                    // capture first non-zero error
    b .Lfmt_next

.Lfmt_delete:
    mov x0, x19
    mov x1, x20
    bl btree_delete
    cmp w0, #ADB_ERR_NOT_FOUND
    b.eq .Lfmt_next                // tombstone on missing key is normal, not error
    cbnz w21, .Lfmt_next           // already have error, skip
    mov w21, w0                    // capture first non-zero error

.Lfmt_next:
    mov x0, x20
    bl memtable_iter_next
    cbnz x0, .Lfmt_loop

.Lfmt_done:
    mov w0, w21
    ldr x21, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #48
    ret
.size flush_memtable_to_btree, .-flush_memtable_to_btree

// ============================================================================
// wal_replay_cb(op_type, key_ptr, val_ptr, ctx=db_ptr) -> 0
// WAL recovery callback: replay PUT/DELETE into memtable
// w0=op_type, x1=key_ptr, x2=val_ptr, x3=db_ptr
// ============================================================================
.type wal_replay_cb, %function
wal_replay_cb:
    stp x29, x30, [sp, #-16]!
    mov x29, sp

    // Load memtable from db_ptr
    ldr x4, [x3, #DB_MEMTABLE_PTR]
    cbz x4, .Lwrc_ret

    cmp w0, #1                     // WAL_OP_PUT
    b.ne .Lwrc_delete

    // memtable_put(head, key, val, is_delete=0)
    mov x0, x4
    // x1 = key_ptr (already set)
    // x2 = val_ptr (already set)
    mov w3, #0                     // is_delete = 0
    bl memtable_put
    b .Lwrc_ret

.Lwrc_delete:
    cmp w0, #2                     // WAL_OP_DELETE
    b.ne .Lwrc_ret

    // memtable_put(head, key, zero_val, is_delete=1)
    mov x0, x4
    // x1 = key_ptr (already set)
    // x2 = val_ptr (use provided val, content doesn't matter for delete)
    mov w3, #1                     // is_delete = 1
    bl memtable_put

.Lwrc_ret:
    mov x0, #0
    ldp x29, x30, [sp], #16
    ret
.size wal_replay_cb, .-wal_replay_cb

// ============================================================================
// adb_put(db, key, klen, val, vlen) -> 0=ok, neg=error
// x0=db, x1=key, w2=klen, x3=val, w4=vlen
// ============================================================================
.global adb_put
.type adb_put, %function
adb_put:
    stp x29, x30, [sp, #-80]!
    mov x29, sp
    stp x19, x20, [sp, #16]
    stp x21, x22, [sp, #32]
    str x23, [sp, #48]

    mov x19, x0                    // db
    mov x20, x1                    // key_raw
    mov w21, w2                     // klen
    mov x22, x3                    // val_raw
    mov w23, w4                     // vlen

    cbz x19, .Lap_invalid
    cmp w21, #KEY_DATA_MAX
    b.hi .Lap_key_too_long
    cmp w23, #VAL_DATA_MAX
    b.hi .Lap_val_too_long
    cbz w21, .Lap_key_ok
    cbnz x20, .Lap_key_ok
    b .Lap_invalid
.Lap_key_ok:
    cbz w23, .Lap_val_ok
    cbnz x22, .Lap_val_ok
    b .Lap_invalid
.Lap_val_ok:

    // Build fixed-size key (64B) on stack
    sub sp, sp, #320               // 64 key + 256 val
    mov x0, sp
    mov x1, x20
    mov w2, w21
    bl build_fixed_key

    // Build fixed-size val (256B) on stack
    add x0, sp, #64
    mov x1, x22
    mov w2, w23
    bl build_fixed_val

    // Route: write to LSM
    mov x0, x19
    mov x1, sp                     // key
    add x2, sp, #64               // val
    mov x3, #0                     // tx_id = 0 (auto)
    bl router_put
    mov x20, x0                    // save result

    // Increment puts metric only on success
    cbnz x20, .Lap_skip_met
    ldr x0, [x19, #DB_METRICS_PTR]
    cbz x0, .Lap_skip_met
    mov w1, #0x00                  // MET_PUTS offset
    bl metrics_inc
.Lap_skip_met:

    add sp, sp, #320
    mov x0, x20                    // restore result
    b .Lap_ret

.Lap_key_too_long:
    mov x0, #ADB_ERR_KEY_TOO_LONG
    b .Lap_ret

.Lap_val_too_long:
    mov x0, #ADB_ERR_VAL_TOO_LONG
    b .Lap_ret

.Lap_invalid:
    mov x0, #ADB_ERR_INVALID

.Lap_ret:
    ldr x23, [sp, #48]
    ldp x21, x22, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #80
    ret
.size adb_put, .-adb_put

// ============================================================================
// adb_get(db, key, klen, vbuf, vbuf_len, vlen_out) -> 0=found, 1=not_found
// x0=db, x1=key, w2=klen, x3=vbuf, w4=vbuf_len, x5=vlen_out
// ============================================================================
.global adb_get
.type adb_get, %function
adb_get:
    stp x29, x30, [sp, #-80]!
    mov x29, sp
    stp x19, x20, [sp, #16]
    stp x21, x22, [sp, #32]
    stp x23, x24, [sp, #48]

    mov x19, x0                    // db
    mov x20, x1                    // key_raw
    mov w21, w2                     // klen
    mov x22, x3                    // vbuf
    mov w23, w4                     // vbuf_len
    mov x24, x5                    // vlen_out

    cbz x19, .Lag_invalid
    cmp w21, #KEY_DATA_MAX
    b.hi .Lag_key_too_long
    cbz w21, .Lag_key_ok
    cbnz x20, .Lag_key_ok
    b .Lag_invalid
.Lag_key_ok:
    cbz w23, .Lag_buf_ok
    cbnz x22, .Lag_buf_ok
    b .Lag_invalid
.Lag_buf_ok:

    // Build fixed-size key on stack
    sub sp, sp, #320               // 64 key + 256 val_buf
    mov x0, sp
    mov x1, x20
    mov w2, w21
    bl build_fixed_key

    // Increment gets metric
    ldr x0, [x19, #DB_METRICS_PTR]
    cbz x0, .Lag_skip_met
    mov w1, #0x08                  // MET_GETS offset
    bl metrics_inc
.Lag_skip_met:

    // Route: search all sources
    mov x0, x19
    mov x1, sp                     // key
    add x2, sp, #64               // val_buf (internal format)
    mov x3, #0                     // tx_id
    bl router_get
    cbnz x0, .Lag_not_found

    // Copy value from internal format to user buffer
    ldrh w0, [sp, #64]            // val_len from fixed val
    cbz x24, .Lag_skip_vlen
    strh w0, [x24]                 // write vlen_out
.Lag_skip_vlen:
    cmp w0, w23
    csel w0, w0, w23, ls          // min(vlen, vbuf_len) unsigned
    mov x1, x22                    // dst = user vbuf
    add x2, sp, #66               // src = val_data
    mov w3, w0
    // Copy w3 bytes
    cbz w3, .Lag_found
    mov x0, x22
    add x1, sp, #66
    mov x2, x3
    bl neon_memcpy

.Lag_found:
    add sp, sp, #320
    mov x0, #0
    b .Lag_ret

.Lag_not_found:
    add sp, sp, #320
    cbz x24, .Lag_not_found_ret
    strh wzr, [x24]
.Lag_not_found_ret:
    mov x0, #ADB_ERR_NOT_FOUND
    b .Lag_ret

.Lag_key_too_long:
    cbz x24, .Lag_key_too_long_ret
    strh wzr, [x24]
.Lag_key_too_long_ret:
    mov x0, #ADB_ERR_KEY_TOO_LONG
    b .Lag_ret

.Lag_invalid:
    cbz x24, .Lag_invalid_ret
    strh wzr, [x24]
.Lag_invalid_ret:
    mov x0, #ADB_ERR_INVALID

.Lag_ret:
    ldp x23, x24, [sp, #48]
    ldp x21, x22, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #80
    ret
.size adb_get, .-adb_get

// ============================================================================
// adb_delete(db, key, klen) -> 0=ok
// x0=db, x1=key, w2=klen
// ============================================================================
.global adb_delete
.type adb_delete, %function
adb_delete:
    stp x29, x30, [sp, #-48]!
    mov x29, sp
    stp x19, x20, [sp, #16]
    str x21, [sp, #32]

    mov x19, x0                    // db
    mov x20, x1                    // key_raw
    mov w21, w2                     // klen

    cbz x19, .Ladel_invalid
    cmp w21, #KEY_DATA_MAX
    b.hi .Ladel_key_too_long
    cbz w21, .Ladel_key_ok
    cbnz x20, .Ladel_key_ok
    b .Ladel_invalid
.Ladel_key_ok:

    sub sp, sp, #64
    mov x0, sp
    mov x1, x20
    mov w2, w21
    bl build_fixed_key

    mov x0, x19
    mov x1, sp
    mov x2, #0                     // tx_id
    bl router_delete
    mov x20, x0                    // save result

    // Increment deletes metric only on success
    cbnz x20, .Ladel_skip_met
    ldr x0, [x19, #DB_METRICS_PTR]
    cbz x0, .Ladel_skip_met
    mov w1, #0x10                  // MET_DELETES offset
    bl metrics_inc
.Ladel_skip_met:

    add sp, sp, #64
    mov x0, x20                    // restore result
    b .Ladel_ret

.Ladel_key_too_long:
    mov x0, #ADB_ERR_KEY_TOO_LONG
    b .Ladel_ret

.Ladel_invalid:
    mov x0, #ADB_ERR_INVALID

.Ladel_ret:
    ldr x21, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #48
    ret
.size adb_delete, .-adb_delete

// ============================================================================
// adb_sync(db) -> 0=ok
// Sync all data to disk
// x0 = db_ptr
// ============================================================================
.global adb_sync
.type adb_sync, %function
adb_sync:
    stp x29, x30, [sp, #-32]!
    mov x29, sp
    str x19, [sp, #16]

    mov x19, x0

    cbz x19, .Las_invalid

    // Persist current memtable state into B+ tree before syncing files
    mov x0, x19
    bl flush_memtable_to_btree
    cbnz x0, .Las_ret              // flush failed: keep WAL intact, report error

    // Delete ALL WAL segment files ONLY after successful flush
    mov x0, x19
    bl wal_truncate_all

    // Reopen WAL segment 0 for continued operation
    mov x0, x19
    bl wal_open
    cmp x0, #0
    b.lt .Las_ret                  // wal_open failed: report error

    // Sync WAL
    mov x0, x19
    bl wal_sync
    cbnz x0, .Las_ret             // error: return it

    // Sync B+ tree data + metadata checkpoint
.Las_sync_btree:
    mov x0, x19
    bl btree_adapter_flush
    b .Las_ret

.Las_invalid:
    mov x0, #ADB_ERR_INVALID

.Las_ret:
    ldr x19, [sp, #16]
    ldp x29, x30, [sp], #32
    ret
.size adb_sync, .-adb_sync

// ============================================================================
// adb_tx_begin(db, isolation, tx_id_out) -> 0=ok
// ============================================================================
.global adb_tx_begin
.type adb_tx_begin, %function
adb_tx_begin:
    stp x29, x30, [sp, #-32]!
    mov x29, sp
    str x19, [sp, #16]

    mov x19, x2                    // tx_id_out ptr

    cbz x19, .Latb_invalid
    str xzr, [x19]
    cbz x0, .Latb_invalid
    cmp w1, #4
    b.hi .Latb_invalid

    // x0 = db, w1 = isolation
    bl tx_begin
    cmp x0, #0
    b.le .Latb_fail

    // Store tx_id
    str x0, [x19]
    mov x0, #0
    b .Latb_ret

.Latb_fail:
    cmp x0, #0
    b.ge .Latb_ret
    neg x0, x0
    b .Latb_ret

.Latb_invalid:
    mov x0, #ADB_ERR_INVALID

.Latb_ret:
    ldr x19, [sp, #16]
    ldp x29, x30, [sp], #32
    ret
.size adb_tx_begin, .-adb_tx_begin

// ============================================================================
// adb_tx_commit(db, tx_id) -> 0=ok
// ============================================================================
.global adb_tx_commit
.type adb_tx_commit, %function
adb_tx_commit:
    stp x29, x30, [sp, #-16]!
    mov x29, sp
    cbz x0, .Latc_invalid
    bl tx_commit
    cmp x0, #0
    b.ge .Latc_ret
    neg x0, x0
    b .Latc_ret
.Latc_invalid:
    mov x0, #ADB_ERR_INVALID
.Latc_ret:
    ldp x29, x30, [sp], #16
    ret
.size adb_tx_commit, .-adb_tx_commit

// ============================================================================
// adb_tx_rollback(db, tx_id) -> 0=ok
// ============================================================================
.global adb_tx_rollback
.type adb_tx_rollback, %function
adb_tx_rollback:
    stp x29, x30, [sp, #-16]!
    mov x29, sp
    cbz x0, .Latr_invalid
    bl tx_rollback
    cmp x0, #0
    b.ge .Latr_ret
    neg x0, x0
    b .Latr_ret
.Latr_invalid:
    mov x0, #ADB_ERR_INVALID
.Latr_ret:
    ldp x29, x30, [sp], #16
    ret
.size adb_tx_rollback, .-adb_tx_rollback

// ============================================================================
// adb_get_metrics(db, out) -> 0
// ============================================================================
.global adb_get_metrics
.type adb_get_metrics, %function
adb_get_metrics:
    cbz x0, .Lagm_invalid
    cbz x1, .Lagm_invalid
    b metrics_get
.Lagm_invalid:
    mov x0, #ADB_ERR_INVALID
    ret
.size adb_get_metrics, .-adb_get_metrics

// ============================================================================
// adb_destroy(path) -> 0=ok
// Delete database files at the given path
// x0 = path (null-terminated)
// ============================================================================
.global adb_destroy
.type adb_destroy, %function
adb_destroy:
    stp x29, x30, [sp, #-64]!
    mov x29, sp
    stp x19, x20, [sp, #16]
    str x21, [sp, #32]

    mov x19, x0                    // path

    cbz x19, .Lad_invalid

    // Open directory
    mov w0, #AT_FDCWD
    mov x1, x19
    mov w2, #O_RDONLY
    orr w2, w2, #O_DIRECTORY
    mov w3, #0
    bl sys_openat
    cmp x0, #0
    b.lt .Lad_done

    mov w20, w0                    // dir_fd

    // Remove data.btree
    mov w0, w20
    adrp x1, .Lad_btree_name
    add x1, x1, :lo12:.Lad_btree_name
    mov w2, #0
    mov x8, #SYS_unlinkat
    svc #0

    // Remove data.btree.tmp
    mov w0, w20
    adrp x1, .Lad_btree_tmp
    add x1, x1, :lo12:.Lad_btree_tmp
    mov w2, #0
    mov x8, #SYS_unlinkat
    svc #0

    // Remove MANIFEST
    mov w0, w20
    adrp x1, .Lad_manifest
    add x1, x1, :lo12:.Lad_manifest
    mov w2, #0
    mov x8, #SYS_unlinkat
    svc #0

    // Remove LOCK file
    mov w0, w20
    adrp x1, .Lad_lock
    add x1, x1, :lo12:.Lad_lock
    mov w2, #0
    mov x8, #SYS_unlinkat
    svc #0

    // Remove wal/ files, then wal/ directory
    mov w0, w20
    adrp x1, .Lad_wal
    add x1, x1, :lo12:.Lad_wal
    mov w2, #O_RDONLY
    orr w2, w2, #O_DIRECTORY
    mov w3, #0
    bl sys_openat
    cmp x0, #0
    b.lt .Lad_wal_rm_dir
    mov w21, w0
    bl destroy_dir_files
    mov w0, w21
    bl sys_close

.Lad_wal_rm_dir:
    mov w0, w20
    adrp x1, .Lad_wal
    add x1, x1, :lo12:.Lad_wal
    mov w2, #0x200                 // AT_REMOVEDIR
    mov x8, #SYS_unlinkat
    svc #0

    // Remove sst/ files, then sst/ directory
    mov w0, w20
    adrp x1, .Lad_sst
    add x1, x1, :lo12:.Lad_sst
    mov w2, #O_RDONLY
    orr w2, w2, #O_DIRECTORY
    mov w3, #0
    bl sys_openat
    cmp x0, #0
    b.lt .Lad_sst_rm_dir
    mov w21, w0
    bl destroy_dir_files
    mov w0, w21
    bl sys_close

.Lad_sst_rm_dir:
    mov w0, w20
    adrp x1, .Lad_sst
    add x1, x1, :lo12:.Lad_sst
    mov w2, #0x200                 // AT_REMOVEDIR
    mov x8, #SYS_unlinkat
    svc #0

    // Close dir
    mov w0, w20
    bl sys_close

.Lad_done:
    mov x0, #0
    b .Lad_ret

.Lad_invalid:
    mov x0, #ADB_ERR_INVALID

.Lad_ret:
    ldr x21, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #64
    ret

.Lad_btree_name:
    .asciz "data.btree"
.Lad_btree_tmp:
    .asciz "data.btree.tmp"
.Lad_manifest:
    .asciz "MANIFEST"
.Lad_lock:
    .asciz "LOCK"
.Lad_wal:
    .asciz "wal"
.Lad_sst:
    .asciz "sst"
    .align 2
.size adb_destroy, .-adb_destroy

// ============================================================================
// destroy_dir_files(dir_fd) -> 0
// Remove all regular entries in a directory (non-recursive)
// x0 = directory file descriptor
// ============================================================================
.global destroy_dir_files
.hidden destroy_dir_files
.type destroy_dir_files, %function
destroy_dir_files:
    stp x29, x30, [sp, #-64]!
    mov x29, sp
    stp x19, x20, [sp, #16]
    stp x21, x22, [sp, #32]
    stp x23, x24, [sp, #48]

    mov w19, w0
    sub sp, sp, #512

.Lddf_read:
    mov w0, w19
    mov x1, sp
    mov x2, #512
    bl sys_getdents64
    cmp x0, #0
    b.le .Lddf_done

    mov x20, x0                    // bytes_read
    mov x21, #0                    // offset

.Lddf_entry:
    cmp x21, x20
    b.ge .Lddf_read

    add x22, sp, x21               // dirent ptr
    ldrh w23, [x22, #DIRENT_RECLEN]
    cbz w23, .Lddf_done

    add x24, x22, #DIRENT_NAME     // name ptr

    // Skip "." and ".."
    ldrb w0, [x24]
    cmp w0, #'.'
    b.ne .Lddf_try_unlink
    ldrb w1, [x24, #1]
    cbz w1, .Lddf_next
    cmp w1, #'.'
    b.ne .Lddf_try_unlink
    ldrb w1, [x24, #2]
    cbz w1, .Lddf_next

.Lddf_try_unlink:
    mov w0, w19
    mov x1, x24
    mov w2, #0
    mov x8, #SYS_unlinkat
    svc #0

.Lddf_next:
    add x21, x21, x23
    b .Lddf_entry

.Lddf_done:
    add sp, sp, #512
    mov x0, #0

    ldp x23, x24, [sp, #48]
    ldp x21, x22, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #64
    ret
.size destroy_dir_files, .-destroy_dir_files

// ============================================================================
// adb_scan(db, start_key, start_klen, end_key, end_klen, callback, user_data)
// Range scan through all data sources
// x0=db, x1=start_key, w2=start_klen, x3=end_key, w4=end_klen,
//    x5=callback, x6=user_data
// ============================================================================
.global adb_scan
.type adb_scan, %function
adb_scan:
    stp x29, x30, [sp, #-96]!
    mov x29, sp
    stp x19, x20, [sp, #16]
    stp x21, x22, [sp, #32]
    stp x23, x24, [sp, #48]
    stp x25, x26, [sp, #64]

    mov x19, x0                    // db
    mov x20, x1                    // start_key
    mov w21, w2                     // start_klen
    mov x22, x3                    // end_key
    mov w23, w4                     // end_klen
    mov x24, x5                    // callback
    mov x25, x6                    // user_data

    cbz x19, .Lsc_invalid
    cbz x24, .Lsc_invalid
    cmp w21, #KEY_DATA_MAX
    b.hi .Lsc_key_too_long
    cmp w23, #KEY_DATA_MAX
    b.hi .Lsc_key_too_long
    cbz w21, .Lsc_start_ok
    cbnz x20, .Lsc_start_ok
    b .Lsc_invalid
.Lsc_start_ok:
    cbz w23, .Lsc_end_ok
    cbnz x22, .Lsc_end_ok
    b .Lsc_invalid
.Lsc_end_ok:

    // Increment scans metric
    ldr x0, [x19, #DB_METRICS_PTR]
    cbz x0, .Lsc_skip_met
    mov w1, #0x18                  // MET_SCANS offset
    bl metrics_inc
.Lsc_skip_met:

    // Build fixed-size start key
    sub sp, sp, #128
    mov x0, sp
    mov x1, x20
    mov w2, w21
    bl build_fixed_key

    // Build fixed-size end key
    add x0, sp, #64
    mov x1, x22
    mov w2, w23
    bl build_fixed_key

    // Phase 1: Scan memtable for entries in range
    ldr x0, [x19, #DB_MEMTABLE_PTR]
    cbz x0, .Lsc_btree
    bl memtable_iter_first
    cbz x0, .Lsc_btree

.Lsc_mt_loop:
    mov x26, x0                    // current node

    // Skip deleted entries
    ldrb w0, [x26, #0x141]        // SLN_IS_DELETED
    cbnz w0, .Lsc_mt_next

    // Compare key with start_key: skip if key < start
    cbz x20, .Lsc_check_end       // NULL start = no lower bound
    mov x0, x26                    // node key (offset 0)
    mov x1, sp                     // start_key
    bl key_compare
    cmp x0, #0
    b.lt .Lsc_mt_next
.Lsc_check_end:

    // Compare key with end_key: stop if key > end
    cbz x22, .Lsc_mt_emit         // NULL end = no upper bound
    mov x0, x26
    add x1, sp, #64               // end_key
    bl key_compare
    cmp x0, #0
    b.gt .Lsc_btree
.Lsc_mt_emit:

    // Key in range: call callback(key_data, klen, val_data, vlen, ctx)
    ldrh w1, [x26, #0x000]        // SLN_KEY_LEN
    add x0, x26, #0x002           // SLN_KEY_DATA
    ldrh w3, [x26, #0x040]        // SLN_VAL_LEN
    add x2, x26, #0x042           // SLN_VAL_DATA
    mov x4, x25                    // user_data
    blr x24                        // callback
    cbnz x0, .Lsc_mt_stop         // callback returned non-zero = stop

.Lsc_mt_next:
    mov x0, x26
    bl memtable_iter_next
    cbnz x0, .Lsc_mt_loop

.Lsc_btree:
    // Phase 2: Scan B+ tree - use dedup wrapper to skip keys in memtable
    // Build scan_dedup context on stack: [memtable_ptr(8), real_cb(8), real_ctx(8)]
    sub sp, sp, #32
    ldr x0, [x19, #DB_MEMTABLE_PTR]
    str x0, [sp, #0]              // dedup_ctx.memtable_ptr
    str x24, [sp, #8]             // dedup_ctx.real_callback
    str x25, [sp, #16]            // dedup_ctx.real_user_data

    mov x0, x19                    // db
    // Pass NULL for start/end if original user pointers were NULL
    add x1, sp, #32               // start_key (fixed) - offset by dedup ctx
    cmp x20, #0
    csel x1, xzr, x1, eq          // NULL if original start was NULL
    add x2, sp, #96               // end_key (fixed)
    cmp x22, #0
    csel x2, xzr, x2, eq          // NULL if original end was NULL
    ldr x3, [x19, #DB_MEMTABLE_PTR]
    cbz x3, .Lsc_btree_no_dedup

    // Use dedup wrapper
    adrp x3, scan_dedup_wrapper
    add x3, x3, :lo12:scan_dedup_wrapper
    mov x4, sp                     // dedup context
    bl btree_adapter_scan
    b .Lsc_btree_done

.Lsc_btree_no_dedup:
    // No memtable, scan directly
    mov x3, x24
    mov x4, x25
    bl btree_adapter_scan

.Lsc_btree_done:
    add sp, sp, #32

.Lsc_end:
    add sp, sp, #128
    b .Lsc_ret

.Lsc_mt_stop:
    // Callback requested stop during memtable scan — return success
    add sp, sp, #128
    mov x0, #0
    b .Lsc_ret

.Lsc_key_too_long:
    mov x0, #ADB_ERR_KEY_TOO_LONG
    b .Lsc_ret

.Lsc_invalid:
    mov x0, #ADB_ERR_INVALID

.Lsc_ret:
    ldp x25, x26, [sp, #64]
    ldp x23, x24, [sp, #48]
    ldp x21, x22, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #96
    ret
.size adb_scan, .-adb_scan

// ============================================================================
// scan_dedup_wrapper - callback wrapper for B+ tree scan dedup
// Skips entries that exist in memtable (already handled by memtable scan)
// Called by btree_scan: int cb(key_data, key_len, val_data, val_len, user_data)
// x0=key_data, w1=key_len, x2=val_data, w3=val_len, x4=dedup_ctx
// dedup_ctx layout: [memtable_ptr(8), real_cb(8), real_ctx(8)]
// ============================================================================
.global scan_dedup_wrapper
.type scan_dedup_wrapper, %function
scan_dedup_wrapper:
    stp x29, x30, [sp, #-64]!
    mov x29, sp
    stp x19, x20, [sp, #16]
    stp x21, x22, [sp, #32]
    str x23, [sp, #48]

    mov x19, x0            // key_data
    mov w20, w1             // key_len
    mov x21, x2            // val_data
    mov w22, w3             // val_len
    mov x23, x4            // dedup_ctx

    // Build fixed key on stack for memtable lookup
    sub sp, sp, #64
    mov x0, sp
    mov x1, x19
    mov w2, w20
    bl build_fixed_key

    // Check if key exists in memtable (regardless of deletion status)
    ldr x0, [x23, #0]      // memtable_ptr
    mov x1, sp              // fixed key
    mov x2, #0              // no value copy
    bl memtable_probe

    add sp, sp, #64

    // memtable_probe: 1 = not_found, 0/2 = present (value/tombstone)
    cmp x0, #ADB_ERR_NOT_FOUND
    b.ne .Lsdw_skip

    // Not in memtable: forward to real callback
    ldr x9, [x23, #8]      // real_cb
    mov x0, x19
    mov w1, w20
    mov x2, x21
    mov w3, w22
    ldr x4, [x23, #16]     // real_ctx
    blr x9
    b .Lsdw_ret

.Lsdw_skip:
    mov x0, #0              // continue scanning

.Lsdw_ret:
    ldr x23, [sp, #48]
    ldp x21, x22, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #64
    ret
.size scan_dedup_wrapper, .-scan_dedup_wrapper

// ============================================================================
// adb_batch_put(db, entries, count) -> 0=ok
// Insert multiple key-value pairs atomically
// x0=db, x1=entries (adb_batch_entry_t*), w2=count
// ============================================================================
.global adb_batch_put
.type adb_batch_put, %function
adb_batch_put:
    stp x29, x30, [sp, #-64]!
    mov x29, sp
    stp x19, x20, [sp, #16]
    stp x21, x22, [sp, #32]
    str x23, [sp, #48]

    mov x19, x0                    // db
    mov x20, x1                    // entries
    mov w21, w2                     // count
    mov w22, #0                     // i = 0

    cbz x19, .Labp_invalid
    cbz w21, .Labp_done
    cbz x20, .Labp_invalid

    mov x23, x20
.Labp_validate:
    cmp w22, w21
    b.hs .Labp_apply_init

    ldr x0, [x23, #0]              // key
    ldrh w1, [x23, #8]             // key_len
    ldr x2, [x23, #16]             // val
    ldrh w3, [x23, #24]            // val_len

    cmp w1, #KEY_DATA_MAX
    b.hi .Labp_key_too_long
    cmp w3, #VAL_DATA_MAX
    b.hi .Labp_val_too_long
    cbz w1, .Labp_val_ptr_check
    cbnz x0, .Labp_val_ptr_check
    b .Labp_invalid
.Labp_val_ptr_check:
    cbz w3, .Labp_validate_next
    cbnz x2, .Labp_validate_next
    b .Labp_invalid

.Labp_validate_next:
    add x23, x23, #32
    add w22, w22, #1
    b .Labp_validate

.Labp_apply_init:
    mov x23, x20
    mov w22, #0

.Labp_loop:
    cmp w22, w21
    b.hs .Labp_done

    ldr x1, [x23, #0]             // key
    ldrh w2, [x23, #8]            // key_len
    ldr x3, [x23, #16]            // val
    ldrh w4, [x23, #24]           // val_len

    mov x0, x19                    // db
    bl adb_put
    cbnz x0, .Labp_ret

    add x23, x23, #32
    add w22, w22, #1
    b .Labp_loop

.Labp_done:
    mov x0, #0
    b .Labp_ret

.Labp_key_too_long:
    mov x0, #ADB_ERR_KEY_TOO_LONG
    b .Labp_ret

.Labp_val_too_long:
    mov x0, #ADB_ERR_VAL_TOO_LONG
    b .Labp_ret

.Labp_invalid:
    mov x0, #ADB_ERR_INVALID

.Labp_ret:
    ldr x23, [sp, #48]
    ldp x21, x22, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #64
    ret
.size adb_batch_put, .-adb_batch_put

// ============================================================================
// adb_tx_put(db, tx_id, key, klen, val, vlen) -> 0=ok
// ============================================================================
.global adb_tx_put
.type adb_tx_put, %function
adb_tx_put:
    stp x29, x30, [sp, #-80]!
    mov x29, sp
    stp x19, x20, [sp, #16]
    stp x21, x22, [sp, #32]
    stp x23, x24, [sp, #48]

    mov x19, x0                    // db
    mov x20, x1                    // tx_id
    mov x21, x2                    // key
    mov w22, w3                     // klen
    mov x23, x4                    // val
    mov w24, w5                     // vlen

    cbz x19, .Latp_invalid
    cmp w22, #KEY_DATA_MAX
    b.hi .Latp_key_too_long
    cmp w24, #VAL_DATA_MAX
    b.hi .Latp_val_too_long
    cbz w22, .Latp_key_ok
    cbnz x21, .Latp_key_ok
    b .Latp_invalid
.Latp_key_ok:
    cbz w24, .Latp_val_ok
    cbnz x23, .Latp_val_ok
    b .Latp_invalid
.Latp_val_ok:

    sub sp, sp, #320
    mov x0, sp
    mov x1, x21
    mov w2, w22
    bl build_fixed_key

    add x0, sp, #64
    mov x1, x23
    mov w2, w24
    bl build_fixed_val

    // Buffer write in tx write set (not directly to memtable)
    ldr x0, [x19, #DB_VERSION_STORE]
    cbz x0, .Latp_no_tx
    // Validate tx_id matches active transaction
    ldr x1, [x0, #TXD_TX_ID]
    cmp x1, x20
    b.ne .Latp_no_tx
    mov x1, sp                     // key
    add x2, sp, #64               // val
    mov w3, #0                     // is_delete = false
    bl tx_write_set_add
    add sp, sp, #320
    cmp x0, #0
    b.ge .Latp_ret
    neg x0, x0
    b .Latp_ret

.Latp_no_tx:
    // No active tx matching tx_id — return error
    add sp, sp, #320
    mov x0, #ADB_ERR_TX_NOT_FOUND
    b .Latp_ret

.Latp_key_too_long:
    mov x0, #ADB_ERR_KEY_TOO_LONG
    b .Latp_ret

.Latp_val_too_long:
    mov x0, #ADB_ERR_VAL_TOO_LONG
    b .Latp_ret

.Latp_invalid:
    mov x0, #ADB_ERR_INVALID

.Latp_ret:
    ldp x23, x24, [sp, #48]
    ldp x21, x22, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #80
    ret
.size adb_tx_put, .-adb_tx_put

// ============================================================================
// adb_tx_get(db, tx_id, key, klen, vbuf, vbuf_len, vlen_out) -> 0=ok
// ============================================================================
.global adb_tx_get
.type adb_tx_get, %function
adb_tx_get:
    stp x29, x30, [sp, #-80]!
    mov x29, sp
    stp x19, x20, [sp, #16]
    stp x21, x22, [sp, #32]
    stp x23, x24, [sp, #48]

    mov x19, x0                    // db
    mov x20, x1                    // tx_id
    mov x21, x2                    // key
    mov w22, w3                     // klen
    mov x23, x4                    // vbuf
    mov w24, w5                     // vbuf_len
    mov x7, x6                     // vlen_out

    cbz x19, .Latg_invalid
    cmp w22, #KEY_DATA_MAX
    b.hi .Latg_key_too_long
    cbz w22, .Latg_key_ok
    cbnz x21, .Latg_key_ok
    b .Latg_invalid
.Latg_key_ok:
    cbz w24, .Latg_buf_ok
    cbnz x23, .Latg_buf_ok
    b .Latg_invalid
.Latg_buf_ok:

    str x7, [sp, #64]             // save vlen_out

    sub sp, sp, #320
    mov x0, sp
    mov x1, x21
    mov w2, w22
    bl build_fixed_key

    // Check tx write set first
    ldr x0, [x19, #DB_VERSION_STORE]
    cbz x0, .Latg_no_tx
    // Validate tx_id matches active transaction
    ldr x1, [x0, #TXD_TX_ID]
    cmp x1, x20
    b.ne .Latg_no_tx
    mov x1, sp                    // key
    add x2, sp, #64              // val_buf (256B on stack)
    bl tx_write_set_find
    cbz x0, .Latg_extract_val   // 0 = found, val at sp+64
    cmp x0, #2
    b.eq .Latg_nf               // tombstone -> not found
    // Not in write set, fall through to storage

.Latg_storage:
    mov x0, x19
    mov x1, sp
    add x2, sp, #64
    mov x3, x20                    // tx_id
    bl router_get
    cbnz x0, .Latg_nf

.Latg_extract_val:
    // Copy value out
    ldrh w0, [sp, #64]            // val_len from fixed val
    ldr x1, [sp, #320+64]         // vlen_out (saved above frame)
    cbz x1, .Latg_skip_vl
    strh w0, [x1]
.Latg_skip_vl:
    cmp w0, w24
    csel w3, w0, w24, ls          // unsigned min
    cbz w3, .Latg_ok
    mov x0, x23                    // dst
    add x1, sp, #66               // val data
    mov x2, x3
    bl neon_memcpy
.Latg_ok:
    add sp, sp, #320
    mov x0, #0
    b .Latg_ret

.Latg_nf:
    add sp, sp, #320
    ldr x1, [sp, #64]
    cbz x1, .Latg_nf_ret
    strh wzr, [x1]
.Latg_nf_ret:
    mov x0, #ADB_ERR_NOT_FOUND
    b .Latg_ret

.Latg_no_tx:
    add sp, sp, #320
    ldr x1, [sp, #64]
    cbz x1, .Latg_no_tx_ret
    strh wzr, [x1]
.Latg_no_tx_ret:
    mov x0, #ADB_ERR_TX_NOT_FOUND
    b .Latg_ret

.Latg_key_too_long:
    cbz x7, .Latg_key_too_long_ret
    strh wzr, [x7]
.Latg_key_too_long_ret:
    mov x0, #ADB_ERR_KEY_TOO_LONG
    b .Latg_ret

.Latg_invalid:
    cbz x7, .Latg_invalid_ret
    strh wzr, [x7]
.Latg_invalid_ret:
    mov x0, #ADB_ERR_INVALID

.Latg_ret:
    ldp x23, x24, [sp, #48]
    ldp x21, x22, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #80
    ret
.size adb_tx_get, .-adb_tx_get

// ============================================================================
// adb_tx_delete(db, tx_id, key, klen) -> 0=ok
// ============================================================================
.global adb_tx_delete
.type adb_tx_delete, %function
adb_tx_delete:
    stp x29, x30, [sp, #-48]!
    mov x29, sp
    stp x19, x20, [sp, #16]
    stp x21, x22, [sp, #32]

    mov x19, x0                    // db
    mov x20, x1                    // tx_id
    mov x21, x2                    // key
    mov w22, w3                    // klen (saved across calls)

    cbz x19, .Latd_invalid
    cmp w22, #KEY_DATA_MAX
    b.hi .Latd_key_too_long
    cbz w22, .Latd_key_ok
    cbnz x21, .Latd_key_ok
    b .Latd_invalid
.Latd_key_ok:

    sub sp, sp, #320
    // Zero val area to prevent leaking stack data into write-set
    add x0, sp, #64
    mov x1, #256
    bl neon_memzero
    mov x0, sp
    mov x1, x21
    mov w2, w22                    // klen
    bl build_fixed_key

    // Check tx write set
    ldr x0, [x19, #DB_VERSION_STORE]
    cbz x0, .Latd_no_ws
    // Validate tx_id matches active transaction
    ldr x1, [x0, #TXD_TX_ID]
    cmp x1, x20
    b.ne .Latd_no_ws
    mov x1, sp                    // key
    add x2, sp, #64              // val (zeroed dummy for delete)
    mov w3, #1                    // is_delete = true
    bl tx_write_set_add
    add sp, sp, #320
    cmp x0, #0
    b.ge .Latd_ret
    neg x0, x0
    b .Latd_ret

.Latd_no_ws:
    // No active tx matching tx_id — return error
    add sp, sp, #320
    mov x0, #ADB_ERR_TX_NOT_FOUND
    b .Latd_ret

.Latd_key_too_long:
    mov x0, #ADB_ERR_KEY_TOO_LONG
    b .Latd_ret

.Latd_invalid:
    mov x0, #ADB_ERR_INVALID

.Latd_ret:
    ldp x21, x22, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #48
    ret
.size adb_tx_delete, .-adb_tx_delete

// ============================================================================
// adb_tx_scan(db, tx_id, start_key, start_klen, end_key, end_klen, cb, ctx)
// ============================================================================
.global adb_tx_scan
.type adb_tx_scan, %function
adb_tx_scan:
    cbz x0, .Lats_invalid
    cbz x6, .Lats_invalid
    cmp w3, #KEY_DATA_MAX
    b.hi .Lats_key_too_long
    cmp w5, #KEY_DATA_MAX
    b.hi .Lats_key_too_long
    cbz w3, .Lats_start_ok
    cbnz x2, .Lats_start_ok
    b .Lats_invalid
.Lats_start_ok:
    cbz w5, .Lats_end_ok
    cbnz x4, .Lats_end_ok
    b .Lats_invalid
.Lats_end_ok:
    // For now, delegate to non-transactional scan
    // shift args: x0=db, x1=tx_id(skip), x2=start, w3=slen, x4=end, w5=elen, x6=cb, x7=ctx
    mov x1, x2                    // start_key
    mov w2, w3                     // start_klen
    mov x3, x4                    // end_key
    mov w4, w5                     // end_klen
    mov x5, x6                    // callback
    mov x6, x7                    // user_data
    b adb_scan
.Lats_key_too_long:
    mov x0, #ADB_ERR_KEY_TOO_LONG
    ret
.Lats_invalid:
    mov x0, #ADB_ERR_INVALID
    ret
.size adb_tx_scan, .-adb_tx_scan

// ============================================================================
// adb_create_index(db, name, extract_key_fn) -> 0=ok
// ============================================================================
.global adb_create_index
.type adb_create_index, %function
adb_create_index:
    stp x29, x30, [sp, #-32]!
    mov x29, sp
    stp x19, x20, [sp, #16]

    mov x19, x0                    // db
    mov x20, x1                    // name

    cbz x19, .Laci_invalid
    cbz x20, .Laci_invalid

    // Get name length
    mov x0, x20
    bl asm_strlen
    cbz x0, .Laci_invalid
    mov x2, x0                     // name_len

    // sec_index_create(dir_fd, name, name_len)
    ldr w0, [x19, #DB_DIR_FD]
    mov x1, x20
    bl sec_index_create

    // Returns index_ptr (non-NULL=success) or NULL (failure)
    cbz x0, .Laci_fail
    mov x0, #0
    b .Laci_ret
.Laci_fail:
    mov x0, #ADB_ERR_IO
    b .Laci_ret
.Laci_invalid:
    mov x0, #ADB_ERR_INVALID
.Laci_ret:
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #32
    ret
.size adb_create_index, .-adb_create_index

// ============================================================================
// adb_drop_index(db, name) -> 0=ok
// ============================================================================
.global adb_drop_index
.type adb_drop_index, %function
adb_drop_index:
    stp x29, x30, [sp, #-32]!
    mov x29, sp
    stp x19, x20, [sp, #16]

    mov x19, x0                    // db
    mov x20, x1                    // name

    cbz x19, .Ladi_invalid
    cbz x20, .Ladi_invalid

    // Get name length
    mov x0, x20
    bl asm_strlen
    cbz x0, .Ladi_invalid
    mov x2, x0                     // name_len

    // Build index filename on stack
    sub sp, sp, #256
    mov x0, sp
    mov x1, x20
    bl build_index_filename

    // Unlink the index file from db directory
    ldr w0, [x19, #DB_DIR_FD]
    mov x1, sp
    mov w2, #0                     // flags = 0 (regular file)
    mov x8, #SYS_unlinkat
    svc #0

    add sp, sp, #256
    mov x0, #0
    b .Ladi_ret

.Ladi_invalid:
    mov x0, #ADB_ERR_INVALID

.Ladi_ret:
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #32
    ret
.size adb_drop_index, .-adb_drop_index

// ============================================================================
// adb_index_scan(db, index_name, key, klen, callback, ctx) -> 0=ok
// ============================================================================
.global adb_index_scan
.type adb_index_scan, %function
adb_index_scan:
    stp x29, x30, [sp, #-96]!
    mov x29, sp
    stp x19, x20, [sp, #16]
    stp x21, x22, [sp, #32]
    stp x23, x24, [sp, #48]
    stp x25, x26, [sp, #64]
    stp x27, x28, [sp, #80]

    mov x19, x0                    // db
    mov x20, x1                    // index_name
    mov x21, x2                    // key data
    mov w22, w3                     // klen
    mov x23, x4                    // callback
    mov x24, x5                    // ctx

    cbz x19, .Lais_invalid
    cbz x20, .Lais_invalid
    cbz x23, .Lais_invalid
    cmp w22, #KEY_DATA_MAX
    b.hi .Lais_key_too_long
    cbz w22, .Lais_key_ok
    cbnz x21, .Lais_key_ok
    b .Lais_invalid
.Lais_key_ok:

    // Build fixed-size search key on stack
    sub sp, sp, #64
    mov x0, sp
    mov x1, x21
    mov w2, w22
    bl build_fixed_key
    mov x25, sp                    // search_key_ptr

    // Get name length
    mov x0, x20
    bl asm_strlen
    mov x2, x0                     // name_len
    cmp x2, #247
    b.hi .Lais_name_too_long

    // Build index filename on stack
    sub sp, sp, #256
    mov x0, sp
    mov x1, x20
    bl build_index_filename

    // Open index file read-only
    ldr w0, [x19, #DB_DIR_FD]
    mov x1, sp
    mov w2, #O_RDONLY
    mov w3, #0
    bl sys_openat
    add sp, sp, #256               // free filename buffer

    cmp x0, #0
    b.lt .Lais_fail
    mov w26, w0                    // idx_fd

    // Get file size via lseek SEEK_END
    mov w0, w26
    mov x1, #0
    mov w2, #SEEK_END
    mov x8, #SYS_lseek
    svc #0
    cmp x0, #PAGE_SIZE
    b.lt .Lais_close
    mov x27, x0                    // file_size

    // mmap read-only
    mov x0, #0                     // addr
    mov x1, x27                    // length
    mov x2, #PROT_READ
    mov x3, #MAP_SHARED
    mov w4, w26                    // fd
    mov x5, #0                     // offset
    mov x8, #SYS_mmap
    svc #0
    cmn x0, #4096
    b.hi .Lais_close
    mov x28, x0                    // mmap_base (= root leaf page)

    // Search leaf page 0 for matching keys
    ldrh w20, [x28, #PH_NUM_KEYS]
    mov w21, #0                     // i = 0 (reuse x20/x21)

.Lais_loop:
    cmp w21, w20
    b.ge .Lais_done

    // Compute key[i] pointer
    mov x0, #64
    mul x0, x0, x21
    add x0, x0, #BTREE_KEYS_OFFSET
    add x1, x28, x0               // page key ptr

    // Compare with search key
    mov x0, x25
    bl key_compare
    cbnz x0, .Lais_next

    // Match: compute val[i] pointer (primary key stored as value)
    mov x0, #256
    mul x0, x0, x21
    add x0, x0, #BTREE_LEAF_VALS_OFF
    add x1, x28, x0               // val ptr (2B len + data)

    ldrh w2, [x1]                  // primary key length
    add x3, x1, #2                // primary key data

    // callback(pri_key_data, pri_klen, NULL, 0, ctx)
    mov x0, x3
    mov w1, w2
    mov x2, #0
    mov w3, #0
    mov x4, x24
    blr x23
    cbnz x0, .Lais_done

.Lais_next:
    add w21, w21, #1
    b .Lais_loop

.Lais_done:
    // munmap
    mov x0, x28
    mov x1, x27
    bl sys_munmap

.Lais_close:
    mov w0, w26
    bl sys_close

    add sp, sp, #64                // free key buffer
    mov x0, #ADB_OK
    b .Lais_ret

.Lais_fail:
    add sp, sp, #64
    mov x0, #ADB_ERR_NOT_FOUND
    b .Lais_ret

.Lais_name_too_long:
    add sp, sp, #64                // free search key
    mov x0, #ADB_ERR_INVALID
    b .Lais_ret

.Lais_key_too_long:
    mov x0, #ADB_ERR_KEY_TOO_LONG
    b .Lais_ret

.Lais_invalid:
    mov x0, #ADB_ERR_INVALID

.Lais_ret:
    ldp x27, x28, [sp, #80]
    ldp x25, x26, [sp, #64]
    ldp x23, x24, [sp, #48]
    ldp x21, x22, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #96
    ret
.size adb_index_scan, .-adb_index_scan

// ============================================================================
// adb_backup(db, dest_path, backup_type) -> 0=ok
// ============================================================================
.global adb_backup
.type adb_backup, %function
adb_backup:
    // x0=db, x1=dest_path, w2=backup_type
    // For now, always do full backup
    cbz x0, .Labk_invalid
    cbz x1, .Labk_invalid
    cmp w2, #1
    b.hi .Labk_invalid
    b backup_full
.Labk_invalid:
    mov x0, #ADB_ERR_INVALID
    ret
.size adb_backup, .-adb_backup

// ============================================================================
// adb_restore(backup_path, dest_path) -> 0=ok
// ============================================================================
.global adb_restore
.type adb_restore, %function
adb_restore:
    stp x29, x30, [sp, #-48]!
    mov x29, sp
    stp x19, x20, [sp, #16]
    stp x21, x22, [sp, #32]

    mov x19, x0                    // backup_path
    mov x20, x1                    // dest_path

    cbz x19, .Lar_invalid
    cbz x20, .Lar_invalid

    // Reject restore when backup and destination paths are the same
    mov x0, x19
    mov x1, x20
.Lar_path_cmp_loop:
    ldrb w2, [x0], #1
    ldrb w3, [x1], #1
    cmp w2, w3
    b.ne .Lar_path_cmp_mismatch
    cbz w2, .Lar_invalid
    b .Lar_path_cmp_loop

.Lar_path_cmp_mismatch:
    cbz w2, .Lar_path_tail_dest
    cbz w3, .Lar_path_tail_src
    b .Lar_path_not_same

.Lar_path_tail_dest:
    cmp w3, #'/'
    b.ne .Lar_path_not_same
.Lar_path_tail_dest_loop:
    ldrb w3, [x1], #1
    cbz w3, .Lar_invalid
    cmp w3, #'/'
    b.eq .Lar_path_tail_dest_loop
    b .Lar_path_not_same

.Lar_path_tail_src:
    cmp w2, #'/'
    b.ne .Lar_path_not_same
.Lar_path_tail_src_loop:
    ldrb w2, [x0], #1
    cbz w2, .Lar_invalid
    cmp w2, #'/'
    b.eq .Lar_path_tail_src_loop

.Lar_path_not_same:
    // Open backup dir
    mov w0, #AT_FDCWD
    mov x1, x19
    mov w2, #(O_RDONLY | O_DIRECTORY)
    mov w3, #0
    bl sys_openat
    cmp x0, #0
    b.lt .Lar_fail
    mov w21, w0                    // src_dir_fd

    // Guard against alias paths resolving to same directory inode
    mov w0, #AT_FDCWD
    mov x1, x20
    mov w2, #(O_RDONLY | O_DIRECTORY)
    mov w3, #0
    bl sys_openat
    cmp x0, #0
    b.lt .Lar_probe_done
    mov w22, w0

    sub sp, sp, #256
    mov w0, w21
    mov x1, sp
    bl sys_fstat
    cmp x0, #0
    b.lt .Lar_probe_close

    mov w0, w22
    add x1, sp, #128
    bl sys_fstat
    cmp x0, #0
    b.lt .Lar_probe_close

    ldr x0, [sp]
    ldr x1, [sp, #128]
    cmp x0, x1
    b.ne .Lar_probe_close

    ldr x0, [sp, #8]
    ldr x1, [sp, #136]
    cmp x0, x1
    b.ne .Lar_probe_close

    add sp, sp, #256
    mov w0, w22
    bl sys_close
    mov w0, w21
    bl sys_close
    mov x0, #ADB_ERR_INVALID
    b .Lar_ret

.Lar_probe_close:
    add sp, sp, #256
    mov w0, w22
    bl sys_close

.Lar_probe_done:
    mov w0, w21
    bl backup_validate_manifest
    cbz x0, .Lar_manifest_ok
    mov x22, x0
    mov w0, w21
    bl sys_close
    mov x0, x22
    b .Lar_ret

.Lar_manifest_ok:
    mov x0, x20
    bl adb_destroy
    cbnz x0, .Lar_fail

    // Create dest directory
    mov w0, #AT_FDCWD
    mov x1, x20
    mov w2, #MODE_DIR
    mov x8, #SYS_mkdirat
    svc #0
    cmp x0, #0
    b.ge .Lar_mkdir_ok
    cmn x0, #17
    b.eq .Lar_mkdir_ok
    b .Lar_close_src

.Lar_mkdir_ok:

    // Open dest dir
    mov w0, #AT_FDCWD
    mov x1, x20
    mov w2, #(O_RDONLY | O_DIRECTORY)
    mov w3, #0
    bl sys_openat
    cmp x0, #0
    b.lt .Lar_close_src
    mov w22, w0                    // dst_dir_fd

    // Copy data.btree from backup to dest
    mov w0, w21
    mov w1, w22
    bl backup_copy_btree
    cbz x0, .Lar_copy_ok
    mov x20, x0
    mov w0, w22
    bl sys_close
    mov w0, w21
    bl sys_close
    mov x0, x20
    b .Lar_ret

.Lar_copy_ok:
    mov w0, w21
    mov w1, w22
    bl backup_copy_manifest
    cbz x0, .Lar_manifest_copy_ok
    mov x20, x0
    mov w0, w22
    bl sys_close
    mov w0, w21
    bl sys_close
    mov x0, x20
    b .Lar_ret

.Lar_manifest_copy_ok:
    mov w0, w22
    bl sys_fsync
    cbz x0, .Lar_close_dest
    mov x20, x0
    mov w0, w22
    bl sys_close
    mov w0, w21
    bl sys_close
    mov x0, x20
    b .Lar_ret

    // Close dest
.Lar_close_dest:
    mov w0, w22
    bl sys_close

    // Close src
    mov w0, w21
    bl sys_close

    mov x0, #ADB_OK
    b .Lar_ret

.Lar_close_src:
    mov w0, w21
    bl sys_close

.Lar_fail:
    mov x0, #ADB_ERR_IO
    b .Lar_ret

.Lar_invalid:
    mov x0, #ADB_ERR_INVALID

.Lar_ret:
    ldp x21, x22, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #48
    ret
.size adb_restore, .-adb_restore

.hidden alloc_zeroed
.hidden free_mem
.hidden asm_strlen
.hidden build_fixed_key
.hidden build_fixed_val
.hidden neon_memcpy
.hidden router_put
.hidden router_get
.hidden router_delete
.hidden tx_begin
.hidden tx_commit
.hidden tx_rollback
.hidden tx_write_set_add
.hidden tx_write_set_find
.hidden metrics_init
.hidden metrics_destroy
.hidden metrics_get
.hidden btree_adapter_init
.hidden btree_adapter_close
.hidden btree_adapter_flush
.hidden btree_adapter_scan
.hidden lsm_adapter_init
.hidden lsm_adapter_close
.hidden wal_adapter_init
.hidden wal_adapter_close
.hidden wal_sync
.hidden wal_recover
.hidden memtable_put
.hidden backup_full
.hidden backup_copy_btree
.hidden backup_copy_manifest
.hidden backup_validate_manifest
.hidden sec_index_create
.hidden build_index_filename
.hidden key_compare
.hidden memtable_iter_first
.hidden memtable_iter_next
.hidden btree_insert
.hidden btree_delete
.hidden memtable_probe
.hidden scan_dedup_wrapper
.hidden metrics_inc
.hidden prng_seed
.hidden sys_mkdirat
.hidden sys_openat
.hidden sys_fstat
.hidden sys_close
.hidden sys_fsync
.hidden sys_clock_gettime
.hidden sys_fdatasync
.hidden sys_ftruncate
.hidden sys_getdents64
.hidden neon_memzero
.hidden sys_flock
.hidden sys_munmap
.hidden tx_free_write_set
.hidden wal_open
.hidden wal_close
.hidden flush_memtable_to_btree
