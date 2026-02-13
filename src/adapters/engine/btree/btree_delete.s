// AssemblyDB - B+ Tree Delete Operations
// Delete with underflow handling and node merging/redistribution

.include "src/const.s"

.text

// Helper macro: page_id to pointer (page_id in wSrc, base in xBase, result in xDst)
// Uses xTmp as scratch
.macro PAGE_PTR xDst, xBase, wSrc, xTmp
    lsl \xTmp, \wSrc, #PAGE_SHIFT
    add \xDst, \xBase, \xTmp
.endm

// ============================================================================
// btree_delete(db_ptr, key_ptr) -> error
// Delete a key from the B+ tree
// x0 = db_ptr, x1 = key_ptr
// Returns: x0 = ADB_OK or ADB_ERR_NOT_FOUND
// ============================================================================
.global btree_delete
.type btree_delete, %function
btree_delete:
    stp x29, x30, [sp, #-96]!
    mov x29, sp
    stp x19, x20, [sp, #16]
    stp x21, x22, [sp, #32]
    stp x23, x24, [sp, #48]
    stp x25, x26, [sp, #64]
    str x27, [sp, #80]

    mov x19, x0            // db_ptr
    mov x20, x1            // key_ptr

    ldr x21, [x19, #DB_BTREE_MMAP]
    ldr w22, [x19, #DB_BTREE_ROOT]

    // Empty tree check: INVALID_PAGE = 0xFFFFFFFF = -1
    cmn w22, #1
    b.eq .Lbd_not_found

    // Find the leaf containing the key
    mov x0, x21
    mov w1, w22
    mov x2, x20
    bl btree_find_leaf
    // x0 = leaf_ptr, w1 = index, w2 = found

    cbz x0, .Lbd_not_found
    cbz w2, .Lbd_not_found

    mov x23, x0            // leaf_ptr
    mov w24, w1             // delete_index

    // Remove entry by shifting left
    ldrh w25, [x23, #PH_NUM_KEYS]
    sub w25, w25, #1        // new num_keys

    // Entries after delete_index need to shift left
    sub w2, w25, w24        // count = (num_keys-1) - delete_index
    cmp w2, #0
    b.le .Lbd_no_shift

    mov x0, x23
    mov w1, w24
    bl btree_leaf_shift_left

.Lbd_no_shift:
    strh w25, [x23, #PH_NUM_KEYS]

    // Check if this is root leaf
    ldr w0, [x23, #PH_PAGE_ID]
    ldr w1, [x19, #DB_BTREE_ROOT]
    cmp w0, w1
    b.eq .Lbd_ok

    // Check for underflow: min keys = BTREE_LEAF_MAX_KEYS / 2
    mov w0, #BTREE_LEAF_MAX_KEYS
    lsr w0, w0, #1
    cmp w25, w0
    b.ge .Lbd_ok

    // Underflow: try redistribute or merge
    b .Lbd_handle_underflow

.Lbd_ok:
    mov x0, #ADB_OK
    b .Lbd_ret

.Lbd_not_found:
    mov x0, #ADB_ERR_NOT_FOUND
    b .Lbd_ret

// ============================================================================
// Handle leaf underflow
// ============================================================================
.Lbd_handle_underflow:
    ldr x21, [x19, #DB_BTREE_MMAP]

    // Try left sibling
    ldr w26, [x23, #PH_PREV_PAGE]
    cmn w26, #1
    b.eq .Lbd_try_right

    lsl x0, x26, #PAGE_SHIFT
    add x27, x21, x0
    // x27 = left_sibling_ptr

    ldrh w0, [x27, #PH_NUM_KEYS]
    mov w1, #BTREE_LEAF_MAX_KEYS
    lsr w1, w1, #1
    add w1, w1, #1
    cmp w0, w1
    b.ge .Lbd_redist_left

.Lbd_try_right:
    ldr w26, [x23, #PH_NEXT_PAGE]
    cmn w26, #1
    b.eq .Lbd_merge_left

    lsl x0, x26, #PAGE_SHIFT
    add x27, x21, x0

    ldrh w0, [x27, #PH_NUM_KEYS]
    mov w1, #BTREE_LEAF_MAX_KEYS
    lsr w1, w1, #1
    add w1, w1, #1
    cmp w0, w1
    b.ge .Lbd_redist_right

    b .Lbd_merge_right

// ============================================================================
// Redistribute: borrow last entry from left sibling
// ============================================================================
.Lbd_redist_left:
    // Shift current leaf right by 1 at index 0
    ldrh w2, [x23, #PH_NUM_KEYS]
    mov x0, x23
    mov w1, #0
    bl btree_leaf_shift_right

    // Copy last key from left to current[0]
    ldrh w1, [x27, #PH_NUM_KEYS]
    sub w1, w1, #1
    mov x0, x27
    bl btree_page_get_key_ptr
    mov x26, x0

    mov x0, x23
    mov w1, #0
    bl btree_page_get_key_ptr
    mov x1, x26
    bl neon_copy_64

    // Copy last val
    ldrh w1, [x27, #PH_NUM_KEYS]
    sub w1, w1, #1
    mov x0, x27
    bl btree_leaf_get_val_ptr
    mov x26, x0

    mov x0, x23
    mov w1, #0
    bl btree_leaf_get_val_ptr
    mov x1, x26
    bl neon_copy_256

    // Copy txid
    ldrh w1, [x27, #PH_NUM_KEYS]
    sub w1, w1, #1
    mov x0, x27
    add x0, x0, #BTREE_LEAF_TXIDS_OFF
    ldr x26, [x0, w1, uxtw #3]
    add x0, x23, #BTREE_LEAF_TXIDS_OFF
    str x26, [x0]

    // Update counts
    ldrh w0, [x23, #PH_NUM_KEYS]
    add w0, w0, #1
    strh w0, [x23, #PH_NUM_KEYS]

    ldrh w0, [x27, #PH_NUM_KEYS]
    sub w0, w0, #1
    strh w0, [x27, #PH_NUM_KEYS]

    // Update parent separator key
    bl .Lbd_update_parent_sep_left
    mov x0, #ADB_OK
    b .Lbd_ret

// ============================================================================
// Redistribute: borrow first entry from right sibling
// ============================================================================
.Lbd_redist_right:
    // Append first entry of right to end of current
    ldrh w1, [x23, #PH_NUM_KEYS]

    // Copy key
    mov x0, x27
    stp x19, x1, [sp, #-16]!
    mov w1, #0
    bl btree_page_get_key_ptr
    mov x26, x0
    ldp x19, x1, [sp], #16

    mov x0, x23
    stp x19, x1, [sp, #-16]!
    bl btree_page_get_key_ptr
    mov x1, x26
    bl neon_copy_64
    ldp x19, x1, [sp], #16

    // Copy val
    mov x0, x27
    stp x19, x1, [sp, #-16]!
    mov w1, #0
    bl btree_leaf_get_val_ptr
    mov x26, x0
    ldp x19, x1, [sp], #16

    mov x0, x23
    stp x19, x1, [sp, #-16]!
    bl btree_leaf_get_val_ptr
    mov x1, x26
    bl neon_copy_256
    ldp x19, x1, [sp], #16

    // Copy txid
    add x0, x27, #BTREE_LEAF_TXIDS_OFF
    ldr x26, [x0]
    ldrh w1, [x23, #PH_NUM_KEYS]
    add x0, x23, #BTREE_LEAF_TXIDS_OFF
    str x26, [x0, w1, uxtw #3]

    // Increment current count
    ldrh w0, [x23, #PH_NUM_KEYS]
    add w0, w0, #1
    strh w0, [x23, #PH_NUM_KEYS]

    // Shift right sibling left by 1
    ldrh w2, [x27, #PH_NUM_KEYS]
    sub w2, w2, #1
    mov x0, x27
    mov w1, #0
    bl btree_leaf_shift_left

    ldrh w0, [x27, #PH_NUM_KEYS]
    sub w0, w0, #1
    strh w0, [x27, #PH_NUM_KEYS]

    // Update parent separator for right sibling
    bl .Lbd_update_parent_sep_right
    mov x0, #ADB_OK
    b .Lbd_ret

// ============================================================================
// Update parent separator after left redistribution
// The separator key between left and current should be current[0]
// ============================================================================
.Lbd_update_parent_sep_left:
    stp x29, x30, [sp, #-32]!
    mov x29, sp
    str x19, [sp, #16]

    ldr w0, [x23, #PH_PARENT_PAGE]
    cmn w0, #1
    b.eq .Lbd_ups_done

    ldr x21, [x19, #DB_BTREE_MMAP]
    lsl x1, x0, #PAGE_SHIFT
    add x26, x21, x1           // parent_ptr

    // Find child index of current leaf
    ldr w2, [x23, #PH_PAGE_ID]
    ldrh w3, [x26, #PH_NUM_KEYS]
    mov w4, #0

.Lbd_fc_loop:
    cmp w4, w3
    b.gt .Lbd_ups_done

    add x5, x26, #BTREE_INT_CHILDREN_OFF
    ldr x5, [x5, w4, uxtw #3]
    cmp w5, w2
    b.eq .Lbd_fc_found

    add w4, w4, #1
    b .Lbd_fc_loop

.Lbd_fc_found:
    cbz w4, .Lbd_ups_done

    sub w1, w4, #1
    mov x0, x26
    bl btree_page_get_key_ptr
    mov x26, x0

    mov x0, x23
    mov w1, #0
    bl btree_page_get_key_ptr
    mov x1, x0
    mov x0, x26
    bl neon_copy_64

.Lbd_ups_done:
    ldr x19, [sp, #16]
    ldp x29, x30, [sp], #32
    ret

// ============================================================================
// Update parent separator after right redistribution
// The separator key for right sibling should be right[0]
// ============================================================================
.Lbd_update_parent_sep_right:
    stp x29, x30, [sp, #-32]!
    mov x29, sp
    str x19, [sp, #16]

    ldr w0, [x27, #PH_PARENT_PAGE]
    cmn w0, #1
    b.eq .Lbd_upsr_done

    ldr x21, [x19, #DB_BTREE_MMAP]
    lsl x1, x0, #PAGE_SHIFT
    add x26, x21, x1

    ldr w2, [x27, #PH_PAGE_ID]
    ldrh w3, [x26, #PH_NUM_KEYS]
    mov w4, #0

.Lbd_fcr_loop:
    cmp w4, w3
    b.gt .Lbd_upsr_done

    add x5, x26, #BTREE_INT_CHILDREN_OFF
    ldr x5, [x5, w4, uxtw #3]
    cmp w5, w2
    b.eq .Lbd_fcr_found

    add w4, w4, #1
    b .Lbd_fcr_loop

.Lbd_fcr_found:
    cbz w4, .Lbd_upsr_done
    sub w1, w4, #1
    mov x0, x26
    bl btree_page_get_key_ptr
    mov x26, x0

    mov x0, x27
    mov w1, #0
    bl btree_page_get_key_ptr
    mov x1, x0
    mov x0, x26
    bl neon_copy_64

.Lbd_upsr_done:
    ldr x19, [sp, #16]
    ldp x29, x30, [sp], #32
    ret

// ============================================================================
// Merge with left sibling
// ============================================================================
.Lbd_merge_left:
    ldr w26, [x23, #PH_PREV_PAGE]
    cmn w26, #1
    b.eq .Lbd_merge_right

    ldr x21, [x19, #DB_BTREE_MMAP]
    lsl x0, x26, #PAGE_SHIFT
    add x27, x21, x0

    // Append all from current to left sibling
    ldrh w0, [x27, #PH_NUM_KEYS]    // dst_start
    ldrh w1, [x23, #PH_NUM_KEYS]    // count
    mov w4, #0                        // src index

.Lbd_ml_loop:
    cmp w4, w1
    b.ge .Lbd_ml_done

    add w5, w0, w4          // dst_idx

    // Save loop vars
    stp x0, x1, [sp, #-16]!

    // Copy key[src] -> left[dst]
    mov x0, x23
    mov w1, w4
    bl btree_page_get_key_ptr
    mov x6, x0

    ldp x0, x1, [sp]       // peek (don't pop yet)
    add w7, w0, w4
    mov x0, x27
    mov w1, w7
    bl btree_page_get_key_ptr
    mov x1, x6
    bl neon_copy_64

    // Copy val[src] -> left[dst]
    ldp x0, x1, [sp]
    mov x0, x23
    mov w1, w4
    bl btree_leaf_get_val_ptr
    mov x6, x0

    ldp x0, x1, [sp]
    add w7, w0, w4
    mov x0, x27
    mov w1, w7
    bl btree_leaf_get_val_ptr
    mov x1, x6
    bl neon_copy_256

    ldp x0, x1, [sp], #16  // pop

    // Copy txid
    add x6, x23, #BTREE_LEAF_TXIDS_OFF
    ldr x6, [x6, w4, uxtw #3]
    add w5, w0, w4
    add x7, x27, #BTREE_LEAF_TXIDS_OFF
    str x6, [x7, w5, uxtw #3]

    add w4, w4, #1
    b .Lbd_ml_loop

.Lbd_ml_done:
    // Update left count
    ldrh w0, [x27, #PH_NUM_KEYS]
    ldrh w1, [x23, #PH_NUM_KEYS]
    add w0, w0, w1
    strh w0, [x27, #PH_NUM_KEYS]

    // Update links
    ldr w0, [x23, #PH_NEXT_PAGE]
    str w0, [x27, #PH_NEXT_PAGE]

    cmn w0, #1
    b.eq .Lbd_ml_free

    ldr x21, [x19, #DB_BTREE_MMAP]
    lsl x1, x0, #PAGE_SHIFT
    add x1, x21, x1
    str w26, [x1, #PH_PREV_PAGE]

.Lbd_ml_free:
    mov w0, #PAGE_TYPE_FREE
    strh w0, [x23, #PH_PAGE_TYPE]
    mov x0, #ADB_OK
    b .Lbd_ret

// ============================================================================
// Merge with right sibling
// ============================================================================
.Lbd_merge_right:
    ldr w26, [x23, #PH_NEXT_PAGE]
    cmn w26, #1
    b.eq .Lbd_ok            // No sibling, accept underflow

    ldr x21, [x19, #DB_BTREE_MMAP]
    lsl x0, x26, #PAGE_SHIFT
    add x27, x21, x0

    ldrh w0, [x23, #PH_NUM_KEYS]    // dst_start
    ldrh w1, [x27, #PH_NUM_KEYS]    // count
    mov w4, #0

.Lbd_mr_loop:
    cmp w4, w1
    b.ge .Lbd_mr_done

    stp x0, x1, [sp, #-16]!

    // Copy key
    mov x0, x27
    mov w1, w4
    bl btree_page_get_key_ptr
    mov x6, x0

    ldp x0, x1, [sp]
    add w7, w0, w4
    mov x0, x23
    mov w1, w7
    bl btree_page_get_key_ptr
    mov x1, x6
    bl neon_copy_64

    // Copy val
    ldp x0, x1, [sp]
    mov x0, x27
    mov w1, w4
    bl btree_leaf_get_val_ptr
    mov x6, x0

    ldp x0, x1, [sp]
    add w7, w0, w4
    mov x0, x23
    mov w1, w7
    bl btree_leaf_get_val_ptr
    mov x1, x6
    bl neon_copy_256

    ldp x0, x1, [sp], #16

    // Copy txid
    add x6, x27, #BTREE_LEAF_TXIDS_OFF
    ldr x6, [x6, w4, uxtw #3]
    add w5, w0, w4
    add x7, x23, #BTREE_LEAF_TXIDS_OFF
    str x6, [x7, w5, uxtw #3]

    add w4, w4, #1
    b .Lbd_mr_loop

.Lbd_mr_done:
    ldrh w0, [x23, #PH_NUM_KEYS]
    ldrh w1, [x27, #PH_NUM_KEYS]
    add w0, w0, w1
    strh w0, [x23, #PH_NUM_KEYS]

    // Update links
    ldr w0, [x27, #PH_NEXT_PAGE]
    str w0, [x23, #PH_NEXT_PAGE]

    cmn w0, #1
    b.eq .Lbd_mr_free

    ldr x21, [x19, #DB_BTREE_MMAP]
    lsl x1, x0, #PAGE_SHIFT
    add x2, x21, x1
    ldr w1, [x23, #PH_PAGE_ID]
    str w1, [x2, #PH_PREV_PAGE]

.Lbd_mr_free:
    mov w0, #PAGE_TYPE_FREE
    strh w0, [x27, #PH_PAGE_TYPE]
    mov x0, #ADB_OK
    b .Lbd_ret

.Lbd_ret:
    ldr x27, [sp, #80]
    ldp x25, x26, [sp, #64]
    ldp x23, x24, [sp, #48]
    ldp x21, x22, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #96
    ret
.size btree_delete, .-btree_delete
