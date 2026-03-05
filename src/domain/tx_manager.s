// AssemblyDB - Transaction Manager
// Begin, commit, rollback transactions with MVCC support
// Transaction IDs are monotonically increasing 64-bit values

.include "src/const.s"

.text

// Transaction descriptor layout (128 bytes):
// 0x00  tx_id (8B)
// 0x08  start_timestamp (8B)
// 0x10  state (8B): 0=active, 1=committed, 2=aborted
// 0x18  write_set_ptr (8B)
// 0x20  write_set_count (8B)
// 0x28  read_set_ptr (8B)
// 0x30  read_set_count (8B)
// 0x38  isolation_level (4B)
// 0x3C  _pad (4B)
// 0x40  _reserved (64B)

// Use TX_ACTIVE, TX_COMMITTED, TX_ABORTED from const.s

// ============================================================================
// tx_begin(db_ptr, isolation_level) -> tx_id or negative error
// Start a new transaction
// x0 = db_ptr, w1 = isolation level
// Returns: x0 = transaction ID (positive), or negative error
// ============================================================================
.global tx_begin
.type tx_begin, %function
tx_begin:
    stp x29, x30, [sp, #-48]!
    mov x29, sp
    stp x19, x20, [sp, #16]
    str x21, [sp, #32]

    mov x19, x0                    // db_ptr
    mov w20, w1                     // isolation

    // Guard: only one active tx at a time (single-slot)
    ldr x0, [x19, #DB_VERSION_STORE]
    cbnz x0, .Ltb_busy

    // Atomic increment next_tx_id
    add x0, x19, #DB_NEXT_TX_ID
.Ltb_cas:
    ldaxr x1, [x0]
    add x2, x1, #1
    stlxr w3, x2, [x0]
    cbnz w3, .Ltb_cas
    mov x21, x2                    // our tx_id = new value

    // Allocate transaction descriptor
    mov x0, #TXD_SIZE
    bl alloc_zeroed
    cbz x0, .Ltb_nomem

    // Fill descriptor
    str x21, [x0, #TXD_TX_ID]
    mov x1, #TX_ACTIVE
    str x1, [x0, #TXD_STATE]
    str w20, [x0, #TXD_ISOLATION]
    str xzr, [x0, #TXD_WRITE_SET]
    str xzr, [x0, #TXD_WRITE_COUNT]

    // Get timestamp
    stp x0, x19, [sp, #-16]!
    sub sp, sp, #16                // struct timespec on stack
    mov x0, #0                     // CLOCK_REALTIME
    mov x1, sp
    bl sys_clock_gettime
    ldr x2, [sp]                   // tv_sec
    add sp, sp, #16
    ldp x0, x19, [sp], #16
    str x2, [x0, #TXD_START_TS]

    // Add to active transaction list
    ldr x1, [x19, #DB_ACTIVE_TX_LIST]
    cbz x1, .Ltb_store_direct

    // Find empty slot in active tx list
    // List layout: [8B count][8B max][array of TXD_SIZE descriptors]
    // For simplicity, just store the descriptor pointer in the db
    // TODO: proper active tx list management

.Ltb_store_direct:
    // Store in version_store for now (simplified)
    str x0, [x19, #DB_VERSION_STORE]

    mov x0, x21                    // return tx_id
    b .Ltb_ret

.Ltb_busy:
    mov x0, #ADB_ERR_LOCKED
    neg x0, x0
    b .Ltb_ret

.Ltb_nomem:
    mov x0, #ADB_ERR_NOMEM
    neg x0, x0

.Ltb_ret:
    ldr x21, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #48
    ret
.size tx_begin, .-tx_begin

// ============================================================================
// tx_commit(db_ptr, tx_id) -> 0=ok, negative=error
// Commit a transaction
// x0 = db_ptr, x1 = tx_id
// ============================================================================
.global tx_commit
.type tx_commit, %function
tx_commit:
    stp x29, x30, [sp, #-48]!
    mov x29, sp
    stp x19, x20, [sp, #16]
    str x21, [sp, #32]

    mov x19, x0                    // db_ptr
    mov x20, x1                    // tx_id

    // Find transaction descriptor
    ldr x0, [x19, #DB_VERSION_STORE]
    cbz x0, .Ltc_not_found

    ldr x1, [x0, #TXD_TX_ID]
    cmp x1, x20
    b.ne .Ltc_not_found

    // Check state is active
    ldr x1, [x0, #TXD_STATE]
    cbnz x1, .Ltc_invalid

    // Mark as committed
    mov x1, #TX_COMMITTED
    str x1, [x0, #TXD_STATE]

    // Replay write-set to memtable
    mov x20, x0
    mov x0, x19
    ldr x1, [x20, #TXD_WRITE_SET]
    ldr x2, [x20, #TXD_TX_ID]
    bl tx_replay_write_set
    mov x21, x0                // save replay error

    // Free write-set nodes
    ldr x0, [x20, #TXD_WRITE_SET]
    bl tx_free_write_set

    // Free descriptor
    mov x0, x20
    mov x1, #TXD_SIZE
    bl free_mem
    str xzr, [x19, #DB_VERSION_STORE]

    // Increment tx_commits metric
    ldr x0, [x19, #DB_METRICS_PTR]
    cbz x0, .Ltc_no_met
    mov w1, #MET_TX_COMMITS
    bl metrics_inc
.Ltc_no_met:

    mov x0, x21                // propagate replay error (0 = ok)
    b .Ltc2_ret

.Ltc_not_found:
    mov x0, #ADB_ERR_TX_NOT_FOUND
    neg x0, x0
    b .Ltc2_ret

.Ltc_invalid:
    mov x0, #ADB_ERR_TX_ABORTED
    neg x0, x0

.Ltc2_ret:
    ldr x21, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #48
    ret
.size tx_commit, .-tx_commit

// ============================================================================
// tx_rollback(db_ptr, tx_id) -> 0=ok, negative=error
// Abort a transaction
// x0 = db_ptr, x1 = tx_id
// ============================================================================
.global tx_rollback
.type tx_rollback, %function
tx_rollback:
    stp x29, x30, [sp, #-32]!
    mov x29, sp
    stp x19, x20, [sp, #16]

    mov x19, x0
    mov x20, x1

    ldr x0, [x19, #DB_VERSION_STORE]
    cbz x0, .Ltr_not_found

    ldr x1, [x0, #TXD_TX_ID]
    cmp x1, x20
    b.ne .Ltr_not_found

    // Mark as aborted
    mov x1, #TX_ABORTED
    str x1, [x0, #TXD_STATE]

    // Free write-set nodes
    mov x20, x0
    ldr x0, [x20, #TXD_WRITE_SET]
    bl tx_free_write_set

    // Free descriptor
    mov x0, x20
    mov x1, #TXD_SIZE
    bl free_mem
    str xzr, [x19, #DB_VERSION_STORE]

    // Increment tx_rollbacks metric
    ldr x0, [x19, #DB_METRICS_PTR]
    cbz x0, .Ltr_no_met
    mov w1, #MET_TX_ROLLBACKS
    bl metrics_inc
.Ltr_no_met:

    mov x0, #0
    b .Ltr_ret

.Ltr_not_found:
    mov x0, #ADB_ERR_TX_NOT_FOUND
    neg x0, x0

.Ltr_ret:
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #32
    ret
.size tx_rollback, .-tx_rollback

// ============================================================================
// tx_write_set_add(txd_ptr, key_ptr, val_ptr, is_delete) -> 0=ok, neg=error
// Prepend a write entry to the transaction's write set
// x0 = txd, x1 = key_ptr (64B fixed), x2 = val_ptr (256B fixed), w3 = is_delete
// ============================================================================
.global tx_write_set_add
.type tx_write_set_add, %function
tx_write_set_add:
    stp x29, x30, [sp, #-64]!
    mov x29, sp
    stp x19, x20, [sp, #16]
    stp x21, x22, [sp, #32]
    str x23, [sp, #48]

    mov x19, x0            // txd
    mov x20, x1            // key_ptr
    mov x21, x2            // val_ptr
    mov w22, w3             // is_delete

    // Check if key already in write set (update in place)
    ldr x23, [x19, #TXD_WRITE_SET]
.Ltwa_search:
    cbz x23, .Ltwa_alloc
    mov x0, x20
    mov x1, x23             // node key at offset 0
    bl key_compare
    cbnz x0, .Ltwa_search_next

    // Found: update val and is_delete in place
    cbz x21, .Ltwa_update_no_val
    add x0, x23, #TXWN_VAL
    mov x1, x21
    bl neon_copy_256
.Ltwa_update_no_val:
    strb w22, [x23, #TXWN_IS_DELETE]
    mov x0, #0
    b .Ltwa_ret

.Ltwa_search_next:
    ldr x23, [x23, #TXWN_NEXT]
    b .Ltwa_search

.Ltwa_alloc:
    // Allocate new write-set node
    mov x0, #TXWN_SIZE
    bl alloc_zeroed
    cbz x0, .Ltwa_nomem
    mov x23, x0

    // Copy key
    mov x1, x20
    bl neon_copy_64

    // Copy val (skip if NULL — delete ops may pass NULL val_ptr)
    cbz x21, .Ltwa_alloc_no_val
    add x0, x23, #TXWN_VAL
    mov x1, x21
    bl neon_copy_256
.Ltwa_alloc_no_val:

    // Set is_delete
    strb w22, [x23, #TXWN_IS_DELETE]

    // Prepend to list
    ldr x0, [x19, #TXD_WRITE_SET]
    str x0, [x23, #TXWN_NEXT]
    str x23, [x19, #TXD_WRITE_SET]

    // Increment count
    ldr x0, [x19, #TXD_WRITE_COUNT]
    add x0, x0, #1
    str x0, [x19, #TXD_WRITE_COUNT]

    mov x0, #0
    b .Ltwa_ret

.Ltwa_nomem:
    mov x0, #ADB_ERR_NOMEM
    neg x0, x0

.Ltwa_ret:
    ldr x23, [sp, #48]
    ldp x21, x22, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #64
    ret
.size tx_write_set_add, .-tx_write_set_add

// ============================================================================
// tx_write_set_find(txd_ptr, key_ptr, val_buf) -> 0=found, 1=not_found, 2=tombstone
// Search write set for a key; copy val to val_buf if found and val_buf != NULL
// x0 = txd, x1 = key_ptr (64B fixed), x2 = val_buf (256B or NULL)
// ============================================================================
.global tx_write_set_find
.type tx_write_set_find, %function
tx_write_set_find:
    stp x29, x30, [sp, #-48]!
    mov x29, sp
    stp x19, x20, [sp, #16]
    str x21, [sp, #32]

    mov x19, x1            // key_ptr
    mov x20, x2            // val_buf
    ldr x21, [x0, #TXD_WRITE_SET]

.Ltwf_loop:
    cbz x21, .Ltwf_not_found
    mov x0, x19
    mov x1, x21
    bl key_compare
    cbnz x0, .Ltwf_next

    // Found: check tombstone
    ldrb w0, [x21, #TXWN_IS_DELETE]
    cbnz w0, .Ltwf_tombstone

    // Copy val if buf provided
    cbz x20, .Ltwf_found
    mov x0, x20
    add x1, x21, #TXWN_VAL
    bl neon_copy_256

.Ltwf_found:
    mov x0, #0
    b .Ltwf_ret

.Ltwf_tombstone:
    mov x0, #2
    b .Ltwf_ret

.Ltwf_next:
    ldr x21, [x21, #TXWN_NEXT]
    b .Ltwf_loop

.Ltwf_not_found:
    mov x0, #ADB_ERR_NOT_FOUND

.Ltwf_ret:
    ldr x21, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #48
    ret
.size tx_write_set_find, .-tx_write_set_find

// ============================================================================
// tx_replay_write_set(db_ptr, write_set_head, tx_id) -> void
// Replay all writes to memtable on commit
// x0 = db_ptr, x1 = write_set_head, x2 = tx_id
// ============================================================================
.global tx_replay_write_set
.type tx_replay_write_set, %function
tx_replay_write_set:
    stp x29, x30, [sp, #-48]!
    mov x29, sp
    stp x19, x20, [sp, #16]
    stp x21, x22, [sp, #32]

    mov x19, x0            // db_ptr
    mov x20, x1            // current node
    mov x21, x2            // tx_id
    mov x22, #0            // error accumulator

.Ltwr_loop:
    cbz x20, .Ltwr_done

    ldrb w0, [x20, #TXWN_IS_DELETE]
    cbnz w0, .Ltwr_delete

    // Put: router_put(db, key, val, tx_id)
    mov x0, x19
    mov x1, x20             // key at node offset 0
    add x2, x20, #TXWN_VAL
    mov x3, x21
    bl router_put
    cbz x0, .Ltwr_next
    // Save first error, continue best-effort replay
    cbnz x22, .Ltwr_next
    mov x22, x0
    b .Ltwr_next

.Ltwr_delete:
    // Delete: router_delete(db, key, tx_id)
    mov x0, x19
    mov x1, x20
    mov x2, x21
    bl router_delete
    cbz x0, .Ltwr_next
    cbnz x22, .Ltwr_next
    mov x22, x0

.Ltwr_next:
    ldr x20, [x20, #TXWN_NEXT]
    b .Ltwr_loop

.Ltwr_done:
    mov x0, x22             // return first error (0 = all ok)
    ldp x21, x22, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #48
    ret
.size tx_replay_write_set, .-tx_replay_write_set

// ============================================================================
// tx_free_write_set(write_set_head) -> void
// Free all nodes in the write set linked list
// x0 = write_set_head
// ============================================================================
.global tx_free_write_set
.type tx_free_write_set, %function
tx_free_write_set:
    stp x29, x30, [sp, #-32]!
    mov x29, sp
    stp x19, x20, [sp, #16]

    mov x19, x0

.Ltff_loop:
    cbz x19, .Ltff_done
    ldr x20, [x19, #TXWN_NEXT]
    mov x0, x19
    mov x1, #TXWN_SIZE
    bl free_mem
    mov x19, x20
    b .Ltff_loop

.Ltff_done:
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #32
    ret
.size tx_free_write_set, .-tx_free_write_set

.hidden alloc_zeroed
.hidden free_mem
.hidden key_compare
.hidden neon_copy_64
.hidden neon_copy_256
.hidden router_put
.hidden router_delete
.hidden metrics_inc
