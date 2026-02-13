// AssemblyDB - LSM Engine Adapter
// Wire LSM (memtable + SSTable) into storage_port vtable

.include "src/const.s"

.text

// ============================================================================
// lsm_adapter_init(db_ptr) -> 0=ok, neg=error
// Initialize LSM engine: create arena, memtable, open WAL
// x0 = db_ptr
// ============================================================================
.global lsm_adapter_init
.type lsm_adapter_init, %function
lsm_adapter_init:
    stp x29, x30, [sp, #-48]!
    mov x29, sp
    stp x19, x20, [sp, #16]
    str x21, [sp, #32]

    mov x19, x0                    // db_ptr

    // Create arena for memtable
    mov x0, #ARENA_CHUNK_DEFAULT   // 1 MB initial chunk
    bl arena_create
    cbz x0, .Llai_nomem
    mov x20, x0                    // arena_ptr
    str x0, [x19, #DB_ARENA_PTR]

    // Create memtable (skip list)
    mov x0, x20
    bl memtable_create2
    cbz x0, .Llai_nomem
    str x0, [x19, #DB_MEMTABLE_PTR]
    str xzr, [x19, #DB_MEMTABLE_SIZE]
    str xzr, [x19, #DB_IMM_MEMTABLE]

    // Init SSTable counters
    str wzr, [x19, #DB_SST_COUNT_L0]
    str wzr, [x19, #DB_SST_COUNT_L1]

    mov x0, #0
    b .Llai_ret

.Llai_nomem:
    mov x0, #ADB_ERR_NOMEM

.Llai_ret:
    ldr x21, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #48
    ret
.size lsm_adapter_init, .-lsm_adapter_init

// ============================================================================
// lsm_put(db_ptr, key_ptr, val_ptr, tx_id) -> 0=ok, neg=error
// Write key/val to memtable (and WAL)
// x0 = db_ptr, x1 = key_ptr, x2 = val_ptr, x3 = tx_id
// ============================================================================
.global lsm_put
.type lsm_put, %function
lsm_put:
    stp x29, x30, [sp, #-48]!
    mov x29, sp
    stp x19, x20, [sp, #16]
    stp x21, x22, [sp, #32]

    mov x19, x0                    // db_ptr
    mov x20, x1                    // key_ptr
    mov x21, x2                    // val_ptr
    mov x22, x3                    // tx_id

    // Write to WAL first (if WAL port exists)
    ldr x0, [x19, #DB_WAL_PORT]
    cbz x0, .Llp_skip_wal

    mov x0, x19
    mov w1, #WAL_OP_PUT
    mov x2, x20
    mov x3, x21
    mov x4, x22
    bl wal_append
    cmp x0, #0
    b.lt .Llp_fail

.Llp_skip_wal:
    // Write to memtable
    ldr x0, [x19, #DB_MEMTABLE_PTR]
    mov x1, x20
    mov x2, x21
    mov w3, #0                     // not a delete
    bl memtable_put
    cmp x0, #0
    b.lt .Llp_fail

    // Update memtable size
    ldr x0, [x19, #DB_MEMTABLE_PTR]
    ldr x0, [x0, #SLH_DATA_SIZE]
    str x0, [x19, #DB_MEMTABLE_SIZE]

    // Check if memtable is full (needs flush)
    mov x1, #MEMTABLE_MAX
    cmp x0, x1
    b.lt .Llp_ok

    // TODO: trigger flush (for now, just continue)

.Llp_ok:
    mov x0, #0
    b .Llp_ret

.Llp_fail:
    // x0 has error

.Llp_ret:
    ldp x21, x22, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #48
    ret
.size lsm_put, .-lsm_put

// ============================================================================
// lsm_get(db_ptr, key_ptr, val_buf, tx_id) -> 0=found, 1=not_found
// Search memtable first, then SSTables
// x0 = db_ptr, x1 = key_ptr, x2 = val_buf, x3 = tx_id
// ============================================================================
.global lsm_get
.type lsm_get, %function
lsm_get:
    stp x29, x30, [sp, #-48]!
    mov x29, sp
    stp x19, x20, [sp, #16]
    stp x21, x22, [sp, #32]

    mov x19, x0                    // db_ptr
    mov x20, x1                    // key_ptr
    mov x21, x2                    // val_buf

    // Check active memtable
    ldr x0, [x19, #DB_MEMTABLE_PTR]
    cbz x0, .Llg_check_imm
    mov x1, x20
    mov x2, x21
    bl memtable_get
    cbz x0, .Llg_ret               // found in memtable

.Llg_check_imm:
    // Check immutable memtable (being flushed)
    ldr x0, [x19, #DB_IMM_MEMTABLE]
    cbz x0, .Llg_check_sst
    mov x1, x20
    mov x2, x21
    bl memtable_get
    cbz x0, .Llg_ret               // found in immutable memtable

.Llg_check_sst:
    // TODO: search L0 SSTables, then L1 SSTables
    mov x0, #ADB_ERR_NOT_FOUND

.Llg_ret:
    ldp x21, x22, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #48
    ret
.size lsm_get, .-lsm_get

// ============================================================================
// lsm_delete(db_ptr, key_ptr, tx_id) -> 0=ok
// Write tombstone to memtable
// x0 = db_ptr, x1 = key_ptr, x2 = tx_id
// ============================================================================
.global lsm_delete
.type lsm_delete, %function
lsm_delete:
    stp x29, x30, [sp, #-48]!
    mov x29, sp
    stp x19, x20, [sp, #16]
    str x21, [sp, #32]

    mov x19, x0                    // db_ptr
    mov x20, x1                    // key_ptr
    mov x21, x2                    // tx_id

    // Write to WAL
    ldr x0, [x19, #DB_WAL_PORT]
    cbz x0, .Lld_skip_wal

    mov x0, x19
    mov w1, #WAL_OP_DELETE
    mov x2, x20
    mov x3, #0                     // no val for delete
    mov x4, x21
    bl wal_append

.Lld_skip_wal:
    // Write tombstone to memtable
    ldr x0, [x19, #DB_MEMTABLE_PTR]
    mov x1, x20
    bl memtable_delete2

    mov x0, #0
    ldr x21, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #48
    ret
.size lsm_delete, .-lsm_delete

// ============================================================================
// lsm_adapter_close(db_ptr) -> 0
// Close LSM engine: destroy memtable and arena
// x0 = db_ptr
// ============================================================================
.global lsm_adapter_close
.type lsm_adapter_close, %function
lsm_adapter_close:
    stp x29, x30, [sp, #-32]!
    mov x29, sp
    str x19, [sp, #16]

    mov x19, x0

    // Destroy arena (frees all memtable nodes)
    ldr x0, [x19, #DB_ARENA_PTR]
    cbz x0, .Llac_done
    bl arena_destroy
    str xzr, [x19, #DB_ARENA_PTR]
    str xzr, [x19, #DB_MEMTABLE_PTR]
    str xzr, [x19, #DB_IMM_MEMTABLE]

.Llac_done:
    mov x0, #0
    ldr x19, [sp, #16]
    ldp x29, x30, [sp], #32
    ret
.size lsm_adapter_close, .-lsm_adapter_close
