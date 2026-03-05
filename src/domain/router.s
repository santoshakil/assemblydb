// AssemblyDB - Hybrid Query Router
// Routes writes to LSM, reads merge across all sources:
//   1. Active memtable
//   2. Immutable memtable
//   3. L0 SSTables (newest first)
//   4. L1 SSTables
//   5. B+ tree

.include "src/const.s"

.text

// ============================================================================
// router_put(db_ptr, key_ptr, val_ptr, tx_id) -> 0=ok, neg=error
// Route write to LSM engine
// x0 = db_ptr, x1 = key_ptr, x2 = val_ptr, x3 = tx_id
// ============================================================================
.global router_put
.type router_put, %function
router_put:
    // Just delegate to lsm_put
    b lsm_put
.size router_put, .-router_put

// ============================================================================
// router_get(db_ptr, key_ptr, val_buf, tx_id) -> 0=found, positive=not_found
// Search all sources in freshness order
// x0 = db_ptr, x1 = key_ptr, x2 = val_buf, x3 = tx_id
// ============================================================================
.global router_get
.type router_get, %function
router_get:
    stp x29, x30, [sp, #-64]!
    mov x29, sp
    stp x19, x20, [sp, #16]
    stp x21, x23, [sp, #32]
    stp x24, x25, [sp, #48]

    mov x19, x0                    // db_ptr
    mov x20, x1                    // key_ptr
    mov x21, x2                    // val_buf

    // 1. Check active memtable
    ldr x0, [x19, #DB_MEMTABLE_PTR]
    cbz x0, .Lrg_check_imm
    mov x1, x20
    mov x2, x21
    bl memtable_probe
    cbz x0, .Lrg_found
    cmp x0, #2
    b.eq .Lrg_not_found

.Lrg_check_imm:
    // 2. Check immutable memtable
    ldr x0, [x19, #DB_IMM_MEMTABLE]
    cbz x0, .Lrg_check_l0
    mov x1, x20
    mov x2, x21
    bl memtable_probe
    cbz x0, .Lrg_found
    cmp x0, #2
    b.eq .Lrg_not_found

.Lrg_check_l0:
    // 3. Check L0 SSTables (newest first)
    ldr w23, [x19, #DB_SST_COUNT_L0]
    cbz w23, .Lrg_check_l1
    sub w24, w23, #1
    add x25, x19, #DB_SST_LIST_L0

.Lrg_l0_loop:
    ldr x0, [x25, w24, uxtw #3]
    cbz x0, .Lrg_l0_next
    mov x1, x20
    mov x2, x21
    bl sstable_get
    cbz x0, .Lrg_found
    cmp x0, #ADB_ERR_NOT_FOUND
    b.ne .Lrg_ret

.Lrg_l0_next:
    cbz w24, .Lrg_check_l1
    sub w24, w24, #1
    b .Lrg_l0_loop

.Lrg_check_l1:
    // 4. Check L1 SSTables
    ldr w23, [x19, #DB_SST_COUNT_L1]
    cbz w23, .Lrg_check_btree
    mov w24, #0
    add x25, x19, #DB_SST_LIST_L1

.Lrg_l1_loop:
    cmp w24, w23
    b.hs .Lrg_check_btree         // unsigned compare
    ldr x0, [x25, w24, uxtw #3]
    cbz x0, .Lrg_l1_next
    mov x1, x20
    mov x2, x21
    bl sstable_get
    cbz x0, .Lrg_found
    cmp x0, #ADB_ERR_NOT_FOUND
    b.ne .Lrg_ret

.Lrg_l1_next:
    add w24, w24, #1
    b .Lrg_l1_loop

.Lrg_check_btree:
    // 5. Check B+ tree
    ldr x0, [x19, #DB_BTREE_MMAP]
    cbz x0, .Lrg_not_found
    ldr w1, [x19, #DB_BTREE_ROOT]
    cmn w1, #1                     // INVALID_PAGE = 0xFFFFFFFF
    b.eq .Lrg_not_found
    mov x2, x20                    // key
    mov x3, x21                    // val_buf
    bl btree_get
    b .Lrg_ret

.Lrg_not_found:
    mov x0, #ADB_ERR_NOT_FOUND
    b .Lrg_ret

.Lrg_found:
    mov x0, #0

.Lrg_ret:
    ldp x24, x25, [sp, #48]
    ldp x21, x23, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #64
    ret
.size router_get, .-router_get

// ============================================================================
// router_delete(db_ptr, key_ptr, tx_id) -> 0=ok
// Route delete to LSM (tombstone)
// x0 = db_ptr, x1 = key_ptr, x2 = tx_id
// ============================================================================
.global router_delete
.type router_delete, %function
router_delete:
    b lsm_delete
.size router_delete, .-router_delete

.hidden lsm_put
.hidden lsm_delete
.hidden memtable_probe
.hidden btree_get
.hidden sstable_get
