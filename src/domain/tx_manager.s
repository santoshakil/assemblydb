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

.equ TXD_TX_ID,            0x000
.equ TXD_TIMESTAMP,        0x008
.equ TXD_STATE,            0x010
.equ TXD_WRITE_SET,        0x018
.equ TXD_WRITE_COUNT,      0x020
.equ TXD_READ_SET,         0x028
.equ TXD_READ_COUNT,       0x030
.equ TXD_ISOLATION,        0x038
.equ TXD_SIZE,             0x080

.equ TX_STATE_ACTIVE,      0
.equ TX_STATE_COMMITTED,   1
.equ TX_STATE_ABORTED,     2

.equ MAX_ACTIVE_TX,        64

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
    mov x1, #TX_STATE_ACTIVE
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
    str x2, [x0, #TXD_TIMESTAMP]

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
    stp x29, x30, [sp, #-32]!
    mov x29, sp
    stp x19, x20, [sp, #16]

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
    mov x1, #TX_STATE_COMMITTED
    str x1, [x0, #TXD_STATE]

    // Free descriptor
    mov x1, #TXD_SIZE
    bl free_mem
    str xzr, [x19, #DB_VERSION_STORE]

    mov x0, #0
    b .Ltc2_ret

.Ltc_not_found:
    mov x0, #ADB_ERR_TX_NOT_FOUND
    neg x0, x0
    b .Ltc2_ret

.Ltc_invalid:
    mov x0, #ADB_ERR_TX_ABORTED
    neg x0, x0

.Ltc2_ret:
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #32
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
    mov x1, #TX_STATE_ABORTED
    str x1, [x0, #TXD_STATE]

    // Free descriptor
    mov x1, #TXD_SIZE
    bl free_mem
    str xzr, [x19, #DB_VERSION_STORE]

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
