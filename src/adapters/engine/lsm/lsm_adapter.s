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
    stp x29, x30, [sp, #-64]!
    mov x29, sp
    stp x19, x20, [sp, #16]
    stp x21, x22, [sp, #32]

    mov x19, x0                    // db_ptr

    // Create arena for memtable
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

    // Clear SSTable descriptor slots
    add x21, x19, #DB_SST_LIST_L0
    mov w22, #0
.Llai_zero_l0:
    cmp w22, #MAX_SST_PER_LEVEL
    b.ge .Llai_zero_l1_start
    str xzr, [x21, w22, uxtw #3]
    add w22, w22, #1
    b .Llai_zero_l0

.Llai_zero_l1_start:
    add x21, x19, #DB_SST_LIST_L1
    mov w22, #0
.Llai_zero_l1:
    cmp w22, #MAX_SST_PER_LEVEL
    b.ge .Llai_ok
    str xzr, [x21, w22, uxtw #3]
    add w22, w22, #1
    b .Llai_zero_l1

.Llai_ok:
    mov x0, #0
    b .Llai_ret

.Llai_nomem:
    // Clean up arena if it was allocated
    ldr x0, [x19, #DB_ARENA_PTR]
    cbz x0, .Llai_nomem_ret
    bl arena_destroy
    str xzr, [x19, #DB_ARENA_PTR]
.Llai_nomem_ret:
    mov x0, #ADB_ERR_NOMEM

.Llai_ret:
    ldp x21, x22, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #64
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
    cbnz x0, .Llp_fail

.Llp_skip_wal:
    // Write to memtable
    ldr x0, [x19, #DB_MEMTABLE_PTR]
    mov x1, x20
    mov x2, x21
    mov w3, #0                     // not a delete
    bl memtable_put
    cbnz x0, .Llp_fail

    // Update memtable size
    ldr x0, [x19, #DB_MEMTABLE_PTR]
    ldr x0, [x0, #SLH_DATA_SIZE]
    str x0, [x19, #DB_MEMTABLE_SIZE]

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
    stp x29, x30, [sp, #-80]!
    mov x29, sp
    stp x19, x20, [sp, #16]
    stp x21, x22, [sp, #32]
    stp x23, x24, [sp, #48]

    mov x19, x0                    // db_ptr
    mov x20, x1                    // key_ptr
    mov x21, x2                    // val_buf

    // Check active memtable
    ldr x0, [x19, #DB_MEMTABLE_PTR]
    cbz x0, .Llg_check_imm
    mov x1, x20
    mov x2, x21
    bl memtable_probe
    cbz x0, .Llg_ret               // found in memtable
    cmp x0, #2
    b.eq .Llg_not_found

.Llg_check_imm:
    // Check immutable memtable (being flushed)
    ldr x0, [x19, #DB_IMM_MEMTABLE]
    cbz x0, .Llg_check_sst
    mov x1, x20
    mov x2, x21
    bl memtable_probe
    cbz x0, .Llg_ret               // found in immutable memtable
    cmp x0, #2
    b.eq .Llg_not_found

.Llg_check_sst:
    // Search L0 SSTables (newest first)
    ldr w22, [x19, #DB_SST_COUNT_L0]
    cbz w22, .Llg_check_l1
    sub w23, w22, #1
    add x24, x19, #DB_SST_LIST_L0

.Llg_l0_loop:
    cmp w23, #0
    b.lt .Llg_check_l1
    ldr x0, [x24, w23, uxtw #3]
    cbz x0, .Llg_l0_next
    mov x1, x20
    mov x2, x21
    bl sstable_get
    cbz x0, .Llg_ret
    cmp x0, #ADB_ERR_NOT_FOUND
    b.ne .Llg_ret

.Llg_l0_next:
    sub w23, w23, #1
    b .Llg_l0_loop

.Llg_check_l1:
    // Search L1 SSTables (oldest first)
    ldr w22, [x19, #DB_SST_COUNT_L1]
    cbz w22, .Llg_not_found
    mov w23, #0
    add x24, x19, #DB_SST_LIST_L1

.Llg_l1_loop:
    cmp w23, w22
    b.ge .Llg_not_found
    ldr x0, [x24, w23, uxtw #3]
    cbz x0, .Llg_l1_next
    mov x1, x20
    mov x2, x21
    bl sstable_get
    cbz x0, .Llg_ret
    cmp x0, #ADB_ERR_NOT_FOUND
    b.ne .Llg_ret

.Llg_l1_next:
    add w23, w23, #1
    b .Llg_l1_loop

.Llg_not_found:
    mov x0, #ADB_ERR_NOT_FOUND

.Llg_ret:
    ldp x23, x24, [sp, #48]
    ldp x21, x22, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #80
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
    cbnz x0, .Lld_ret

.Lld_skip_wal:
    // Write tombstone to memtable
    ldr x0, [x19, #DB_MEMTABLE_PTR]
    mov x1, x20
    bl memtable_delete2
    // Update memtable size (tombstones consume arena memory)
    str x0, [sp, #40]             // save return code in frame pad
    ldr x1, [x19, #DB_MEMTABLE_PTR]
    ldr x1, [x1, #SLH_DATA_SIZE]
    str x1, [x19, #DB_MEMTABLE_SIZE]
    ldr x0, [sp, #40]             // restore return code

.Lld_ret:
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
    stp x29, x30, [sp, #-64]!
    mov x29, sp
    stp x19, x20, [sp, #16]
    stp x21, x22, [sp, #32]

    mov x19, x0

    // Close and free L0 SST descriptors
    ldr w20, [x19, #DB_SST_COUNT_L0]
    cbz w20, .Llac_check_l1
    add x22, x19, #DB_SST_LIST_L0
    mov w21, #0
.Llac_l0_loop:
    cmp w21, w20
    b.ge .Llac_l0_done
    ldr x0, [x22, w21, uxtw #3]
    cbz x0, .Llac_l0_next
    bl sstable_close
    ldr x0, [x22, w21, uxtw #3]
    mov x1, #SSTD_SIZE
    bl free_mem
    str xzr, [x22, w21, uxtw #3]
.Llac_l0_next:
    add w21, w21, #1
    b .Llac_l0_loop
.Llac_l0_done:
    str wzr, [x19, #DB_SST_COUNT_L0]

.Llac_check_l1:
    // Close and free L1 SST descriptors
    ldr w20, [x19, #DB_SST_COUNT_L1]
    cbz w20, .Llac_close_arena
    add x22, x19, #DB_SST_LIST_L1
    mov w21, #0
.Llac_l1_loop:
    cmp w21, w20
    b.ge .Llac_l1_done
    ldr x0, [x22, w21, uxtw #3]
    cbz x0, .Llac_l1_next
    bl sstable_close
    ldr x0, [x22, w21, uxtw #3]
    mov x1, #SSTD_SIZE
    bl free_mem
    str xzr, [x22, w21, uxtw #3]
.Llac_l1_next:
    add w21, w21, #1
    b .Llac_l1_loop
.Llac_l1_done:
    str wzr, [x19, #DB_SST_COUNT_L1]

.Llac_close_arena:
    // Destroy arena (frees all memtable nodes)
    ldr x0, [x19, #DB_ARENA_PTR]
    cbz x0, .Llac_done
    bl arena_destroy
    str xzr, [x19, #DB_ARENA_PTR]
    str xzr, [x19, #DB_MEMTABLE_PTR]
    str xzr, [x19, #DB_IMM_MEMTABLE]

.Llac_done:
    mov x0, #0
    ldp x21, x22, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #64
    ret
.size lsm_adapter_close, .-lsm_adapter_close

// ============================================================================
// lsm_purge_sstables(db_ptr) -> void
// Close, free, and unlink all L0+L1 SSTable files and descriptors.
// Called after flush_memtable_to_btree when B+ tree has all data.
// x0 = db_ptr
// ============================================================================
.global lsm_purge_sstables
.type lsm_purge_sstables, %function
lsm_purge_sstables:
    stp x29, x30, [sp, #-48]!
    mov x29, sp
    stp x19, x20, [sp, #16]
    stp x21, x22, [sp, #32]

    mov x19, x0                    // db_ptr

    // --- Purge L0 SSTables ---
    ldr w20, [x19, #DB_SST_COUNT_L0]
    cbz w20, .Lps_l1

    add x22, x19, #DB_SST_LIST_L0
    mov w21, #0

.Lps_l0_loop:
    cmp w21, w20
    b.ge .Lps_l0_unlink
    // Close and free descriptor
    ldr x0, [x22, w21, uxtw #3]
    cbz x0, .Lps_l0_next
    bl sstable_close
    ldr x0, [x22, w21, uxtw #3]
    mov x1, #SSTD_SIZE
    bl free_mem
    str xzr, [x22, w21, uxtw #3]
.Lps_l0_next:
    add w21, w21, #1
    b .Lps_l0_loop

.Lps_l0_unlink:
    // Unlink SSTable files from disk
    mov w21, #0
.Lps_l0_unlink_loop:
    cmp w21, w20
    b.ge .Lps_l0_done
    sub sp, sp, #128
    mov x0, sp
    mov w1, #0                     // level 0
    mov w2, w21                    // seq
    bl build_sst_name
    ldr w0, [x19, #DB_DIR_FD]
    mov x1, sp
    mov w2, #0
    bl sys_unlinkat
    add sp, sp, #128
    add w21, w21, #1
    b .Lps_l0_unlink_loop
.Lps_l0_done:
    str wzr, [x19, #DB_SST_COUNT_L0]

.Lps_l1:
    // --- Purge L1 SSTables ---
    ldr w20, [x19, #DB_SST_COUNT_L1]
    cbz w20, .Lps_done

    add x22, x19, #DB_SST_LIST_L1
    mov w21, #0

.Lps_l1_loop:
    cmp w21, w20
    b.ge .Lps_l1_unlink
    ldr x0, [x22, w21, uxtw #3]
    cbz x0, .Lps_l1_next
    bl sstable_close
    ldr x0, [x22, w21, uxtw #3]
    mov x1, #SSTD_SIZE
    bl free_mem
    str xzr, [x22, w21, uxtw #3]
.Lps_l1_next:
    add w21, w21, #1
    b .Lps_l1_loop

.Lps_l1_unlink:
    mov w21, #0
.Lps_l1_unlink_loop:
    cmp w21, w20
    b.ge .Lps_l1_done
    sub sp, sp, #128
    mov x0, sp
    mov w1, #1                     // level 1
    mov w2, w21
    bl build_sst_name
    ldr w0, [x19, #DB_DIR_FD]
    mov x1, sp
    mov w2, #0
    bl sys_unlinkat
    add sp, sp, #128
    add w21, w21, #1
    b .Lps_l1_unlink_loop
.Lps_l1_done:
    str wzr, [x19, #DB_SST_COUNT_L1]

.Lps_done:
    ldp x21, x22, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #48
    ret
.size lsm_purge_sstables, .-lsm_purge_sstables

.hidden sstable_get
.hidden sstable_close
.hidden free_mem
.hidden arena_destroy
.hidden build_sst_name
.hidden sys_unlinkat
