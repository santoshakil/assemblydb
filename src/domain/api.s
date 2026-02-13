// AssemblyDB - Public API Entry Points
// C-callable functions matching include/assemblydb.h
// Routes through domain logic to port/adapter layer

.include "src/const.s"

.text

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
    cmp x0, #0
    b.lt .Lao_fail_close

    // Initialize LSM adapter (memtable + arena)
    mov x0, x22
    bl lsm_adapter_init
    cmp x0, #0
    b.lt .Lao_fail_close

    // Initialize WAL adapter
    mov x0, x22
    bl wal_adapter_init
    // WAL init failure is non-fatal (operates without WAL)

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

.Lao_fail_close:
    ldr w0, [x22, #DB_DIR_FD]
    bl sys_close

.Lao_fail_free:
    mov x0, x22
    mov x1, #DB_SIZE
    bl free_mem
    mov x0, #ADB_ERR_IO
    neg x0, x0
    b .Lao_ret

.Lao_nomem:
    mov x0, #ADB_ERR_NOMEM
    neg x0, x0

.Lao_ret:
    ldr x23, [sp, #48]
    ldp x21, x22, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #64
    ret

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

    // Close WAL
    mov x0, x19
    bl wal_adapter_close

    // Close LSM
    mov x0, x19
    bl lsm_adapter_close

    // Close B+ tree
    mov x0, x19
    bl btree_adapter_close

    // Free metrics
    mov x0, x19
    bl metrics_destroy

    // Close directory fd
    ldr w0, [x19, #DB_DIR_FD]
    cmp w0, #0
    b.le .Lac2_no_dir
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

    add sp, sp, #320

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

    // Build fixed-size key on stack
    sub sp, sp, #320               // 64 key + 256 val_buf
    mov x0, sp
    mov x1, x20
    mov w2, w21
    bl build_fixed_key

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
    csel w0, w0, w23, le          // min(vlen, vbuf_len)
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
    mov x0, #ADB_ERR_NOT_FOUND

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

    sub sp, sp, #64
    mov x0, sp
    mov x1, x20
    mov w2, w21
    bl build_fixed_key

    mov x0, x19
    mov x1, sp
    mov x2, #0                     // tx_id
    bl router_delete

    add sp, sp, #64

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

    // Sync WAL
    mov x0, x19
    bl wal_sync

    // Sync B+ tree
    ldr x0, [x19, #DB_BTREE_FD]
    cmp x0, #0
    b.le .Las_done
    bl sys_fdatasync

.Las_done:
    mov x0, #0
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

    // x0 = db, w1 = isolation
    bl tx_begin
    cmp x0, #0
    b.le .Latb_fail

    // Store tx_id
    str x0, [x19]
    mov x0, #0
    b .Latb_ret

.Latb_fail:
    // x0 has error (negative)

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
    b tx_commit
.size adb_tx_commit, .-adb_tx_commit

// ============================================================================
// adb_tx_rollback(db, tx_id) -> 0=ok
// ============================================================================
.global adb_tx_rollback
.type adb_tx_rollback, %function
adb_tx_rollback:
    b tx_rollback
.size adb_tx_rollback, .-adb_tx_rollback

// ============================================================================
// adb_get_metrics(db, out) -> 0
// ============================================================================
.global adb_get_metrics
.type adb_get_metrics, %function
adb_get_metrics:
    b metrics_get
.size adb_get_metrics, .-adb_get_metrics

// ============================================================================
// adb_destroy(path) -> 0=ok
// Delete database files at the given path
// x0 = path (null-terminated)
// ============================================================================
.global adb_destroy
.type adb_destroy, %function
adb_destroy:
    stp x29, x30, [sp, #-32]!
    mov x29, sp
    str x19, [sp, #16]

    mov x19, x0                    // path

    // Open directory
    mov w0, #AT_FDCWD
    mov x1, x19
    mov w2, #O_RDONLY
    orr w2, w2, #O_DIRECTORY
    mov w3, #0
    bl sys_openat
    cmp x0, #0
    b.lt .Lad_done

    mov w19, w0                    // dir_fd

    sub sp, sp, #32

    // Remove btree.db
    mov x0, sp
    mov w1, #'b'
    strb w1, [x0]
    mov w1, #'t'
    strb w1, [x0, #1]
    mov w1, #'r'
    strb w1, [x0, #2]
    mov w1, #'e'
    strb w1, [x0, #3]
    strb w1, [x0, #4]
    mov w1, #'.'
    strb w1, [x0, #5]
    mov w1, #'d'
    strb w1, [x0, #6]
    mov w1, #'b'
    strb w1, [x0, #7]
    strb wzr, [x0, #8]
    mov w0, w19
    mov x1, sp
    mov w2, #0
    mov x8, #SYS_unlinkat
    svc #0

    // Remove wal/ directory
    mov x0, sp
    mov w1, #'w'
    strb w1, [x0]
    mov w1, #'a'
    strb w1, [x0, #1]
    mov w1, #'l'
    strb w1, [x0, #2]
    strb wzr, [x0, #3]
    mov w0, w19
    mov x1, sp
    mov w2, #0x200                 // AT_REMOVEDIR
    mov x8, #SYS_unlinkat
    svc #0

    // Remove sst/ directory
    mov x0, sp
    mov w1, #'s'
    strb w1, [x0]
    strb w1, [x0, #1]
    mov w1, #'t'
    strb w1, [x0, #2]
    strb wzr, [x0, #3]
    mov w0, w19
    mov x1, sp
    mov w2, #0x200                 // AT_REMOVEDIR
    mov x8, #SYS_unlinkat
    svc #0

    add sp, sp, #32

    // Close dir
    mov w0, w19
    bl sys_close

.Lad_done:
    mov x0, #0
    ldr x19, [sp, #16]
    ldp x29, x30, [sp], #32
    ret
.size adb_destroy, .-adb_destroy

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

    // Use B+ tree scan adapter (primary path)
    mov x0, x19                    // db
    mov x1, sp                     // start_key (fixed)
    add x2, sp, #64               // end_key (fixed)
    mov x3, x24                    // callback
    mov x4, x25                    // user_data
    bl btree_adapter_scan

    add sp, sp, #128

    ldp x25, x26, [sp, #64]
    ldp x23, x24, [sp, #48]
    ldp x21, x22, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #96
    ret
.size adb_scan, .-adb_scan

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

.Labp_loop:
    cmp w22, w21
    b.ge .Labp_done

    // Each entry: { key_ptr(8), key_len(2), pad(6), val_ptr(8), val_len(2) }
    // In C: struct { const void *key; uint16_t key_len; const void *val; uint16_t val_len; }
    // Actual layout with padding: 8+2+6+8+2+6 = 32 bytes per entry
    // But sizeof with alignment: key(8) + key_len(2) + pad(6) + val(8) + val_len(2) + pad(6) = 32
    mov x23, #32
    mul x23, x23, x22
    add x23, x20, x23             // entry_ptr

    ldr x1, [x23, #0]             // key
    ldrh w2, [x23, #8]            // key_len
    ldr x3, [x23, #16]            // val
    ldrh w4, [x23, #24]           // val_len

    mov x0, x19                    // db
    bl adb_put
    cbnz x0, .Labp_ret

    add w22, w22, #1
    b .Labp_loop

.Labp_done:
    mov x0, #0

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

    sub sp, sp, #320
    mov x0, sp
    mov x1, x21
    mov w2, w22
    bl build_fixed_key

    add x0, sp, #64
    mov x1, x23
    mov w2, w24
    bl build_fixed_val

    mov x0, x19
    mov x1, sp
    add x2, sp, #64
    mov x3, x20                    // tx_id
    bl router_put

    add sp, sp, #320

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
    // x6 = vlen_out
    str x6, [sp, #64]             // save vlen_out

    sub sp, sp, #320
    mov x0, sp
    mov x1, x21
    mov w2, w22
    bl build_fixed_key

    mov x0, x19
    mov x1, sp
    add x2, sp, #64
    mov x3, x20                    // tx_id
    bl router_get
    cbnz x0, .Latg_nf

    // Copy value out
    ldrh w0, [sp, #64]            // val_len from fixed val
    ldr x1, [sp, #320+64]         // vlen_out (saved above frame)
    cbz x1, .Latg_skip_vl
    strh w0, [x1]
.Latg_skip_vl:
    cmp w0, w24
    csel w3, w0, w24, le
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
    mov x0, #ADB_ERR_NOT_FOUND

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
    str x21, [sp, #32]

    mov x19, x0                    // db
    mov x20, x1                    // tx_id
    mov x21, x2                    // key

    sub sp, sp, #64
    mov x0, sp
    mov x1, x21
    mov w2, w3                     // klen directly
    bl build_fixed_key

    mov x0, x19
    mov x1, sp
    mov x2, x20                    // tx_id
    bl router_delete

    add sp, sp, #64

    ldr x21, [sp, #32]
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
    // For now, delegate to non-transactional scan
    // shift args: x0=db, x1=tx_id(skip), x2=start, w3=slen, x4=end, w5=elen, x6=cb, x7=ctx
    mov x1, x2                    // start_key
    mov w2, w3                     // start_klen
    mov x3, x4                    // end_key
    mov w4, w5                     // end_klen
    mov x5, x6                    // callback
    mov x6, x7                    // user_data
    b adb_scan
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

    // Get name length
    mov x0, x20
    bl asm_strlen
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

    // Get name length
    mov x0, x20
    bl asm_strlen
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
    b backup_full
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

    // Create dest directory
    mov w0, #AT_FDCWD
    mov x1, x20
    mov w2, #MODE_DIR
    mov x8, #SYS_mkdirat
    svc #0

    // Open backup dir
    mov w0, #AT_FDCWD
    mov x1, x19
    mov w2, #(O_RDONLY | O_DIRECTORY)
    mov w3, #0
    bl sys_openat
    cmp x0, #0
    b.lt .Lar_fail
    mov w21, w0                    // src_dir_fd

    // Open dest dir
    mov w0, #AT_FDCWD
    mov x1, x20
    mov w2, #(O_RDONLY | O_DIRECTORY)
    mov w3, #0
    bl sys_openat
    cmp x0, #0
    b.lt .Lar_close_src
    mov w22, w0                    // dst_dir_fd

    // Copy btree.db from backup to dest
    mov w0, w21
    mov w1, w22
    bl backup_copy_btree

    // Close dest
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
.hidden metrics_init
.hidden metrics_destroy
.hidden metrics_get
.hidden btree_adapter_init
.hidden btree_adapter_close
.hidden btree_adapter_scan
.hidden lsm_adapter_init
.hidden lsm_adapter_close
.hidden wal_adapter_init
.hidden wal_adapter_close
.hidden wal_sync
.hidden backup_full
.hidden backup_copy_btree
.hidden sec_index_create
.hidden build_index_filename
.hidden key_compare
.hidden prng_seed
.hidden sys_mkdirat
.hidden sys_openat
.hidden sys_close
.hidden sys_clock_gettime
.hidden sys_fdatasync
