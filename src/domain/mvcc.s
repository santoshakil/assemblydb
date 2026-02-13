// AssemblyDB - MVCC (Multi-Version Concurrency Control)
// Version store for snapshot isolation
// Each write creates a version entry; reads check visibility

.include "src/const.s"

.text

// MVCC Version Entry (384 bytes) - from plan:
// 0x000  tx_id (8B)
// 0x008  end_tx_id (8B) - MAX=alive
// 0x010  timestamp (8B)
// 0x018  key (64B)
// 0x058  value (256B)
// 0x158  is_deleted (1B)
// 0x159  _pad (7B)
// 0x160  next_version (8B) - older
// 0x168  prev_version (8B) - newer
// 0x170  _reserved (16B)

.equ MVE_TX_ID,         0x000
.equ MVE_END_TX_ID,     0x008
.equ MVE_TIMESTAMP,     0x010
.equ MVE_KEY,           0x018
.equ MVE_VALUE,         0x058
.equ MVE_IS_DELETED,    0x158
.equ MVE_NEXT_VER,      0x160
.equ MVE_PREV_VER,      0x168
.equ MVE_SIZE,          0x180

.equ TX_ID_MAX,         0x7FFFFFFFFFFFFFFF

// ============================================================================
// mvcc_is_visible(version_ptr, tx_id) -> 0=visible, 1=not_visible
// Check if a version is visible to the given transaction
// x0 = version entry ptr, x1 = reading tx_id
// ============================================================================
.global mvcc_is_visible
.type mvcc_is_visible, %function
mvcc_is_visible:
    // Visible if: version.tx_id <= reader_tx_id < version.end_tx_id
    ldr x2, [x0, #MVE_TX_ID]      // creating tx
    ldr x3, [x0, #MVE_END_TX_ID]  // invalidating tx

    cmp x2, x1
    b.hi .Lmv_not_vis              // created after reader started (unsigned)

    cmp x1, x3
    b.hs .Lmv_not_vis              // invalidated before reader started (unsigned)

    mov x0, #0
    ret

.Lmv_not_vis:
    mov x0, #1
    ret
.size mvcc_is_visible, .-mvcc_is_visible

// ============================================================================
// mvcc_create_version(arena, tx_id, key, val, is_delete) -> version_ptr or NULL
// Create a new MVCC version entry
// x0 = arena_ptr, x1 = tx_id, x2 = key_ptr, x3 = val_ptr, w4 = is_delete
// ============================================================================
.global mvcc_create_version
.type mvcc_create_version, %function
mvcc_create_version:
    stp x29, x30, [sp, #-64]!
    mov x29, sp
    stp x19, x20, [sp, #16]
    stp x21, x22, [sp, #32]
    str x23, [sp, #48]

    mov x19, x0                    // arena
    mov x20, x1                    // tx_id
    mov x21, x2                    // key
    mov x22, x3                    // val
    mov w23, w4                     // is_delete

    // Allocate from arena
    mov x0, x19
    mov x1, #MVE_SIZE
    bl arena_alloc
    cbz x0, .Lmcv_fail

    mov x19, x0                    // version_ptr

    // Zero the entry
    mov x0, x19
    mov x1, #MVE_SIZE
    bl neon_memzero

    // Fill fields
    str x20, [x19, #MVE_TX_ID]
    movn x0, #0                    // TX_ID_MAX = 0xFFFFFFFFFFFFFFFF
    str x0, [x19, #MVE_END_TX_ID]

    // Copy key
    add x0, x19, #MVE_KEY
    mov x1, x21
    bl neon_copy_64

    // Copy value
    cbz x22, .Lmcv_skip_val
    add x0, x19, #MVE_VALUE
    mov x1, x22
    bl neon_copy_256
.Lmcv_skip_val:

    strb w23, [x19, #MVE_IS_DELETED]
    str xzr, [x19, #MVE_NEXT_VER]
    str xzr, [x19, #MVE_PREV_VER]

    mov x0, x19
    b .Lmcv_ret

.Lmcv_fail:
    mov x0, #0

.Lmcv_ret:
    ldr x23, [sp, #48]
    ldp x21, x22, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #64
    ret
.size mvcc_create_version, .-mvcc_create_version
