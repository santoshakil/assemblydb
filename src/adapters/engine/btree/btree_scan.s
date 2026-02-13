// AssemblyDB - B+ Tree Range Scan
// Scan leaf chain from start_key to end_key, calling callback per entry

.include "src/const.s"

.text

// ============================================================================
// btree_scan(mmap_base, root_page_id, start_key, end_key, callback, user_data) -> error
// Range scan from start_key to end_key (inclusive)
// x0 = mmap_base
// w1 = root_page_id
// x2 = start_key (or NULL for beginning)
// x3 = end_key (or NULL for end)
// x4 = callback fn: int cb(key_ptr, key_len, val_ptr, val_len, user_data)
// x5 = user_data
// Returns: x0 = ADB_OK or error
// Callback returns 0 to continue, non-zero to stop early
// ============================================================================
.global btree_scan
.type btree_scan, %function
btree_scan:
    stp x29, x30, [sp, #-96]!
    mov x29, sp
    stp x19, x20, [sp, #16]
    stp x21, x22, [sp, #32]
    stp x23, x24, [sp, #48]
    stp x25, x26, [sp, #64]
    stp x27, x28, [sp, #80]

    mov x19, x0            // mmap_base
    mov w20, w1             // root_page_id
    mov x21, x2            // start_key (or NULL)
    mov x22, x3            // end_key (or NULL)
    mov x23, x4            // callback
    mov x24, x5            // user_data

    // Empty tree
    cmn w20, #1
    b.eq .Lbs_ok

    // Find starting leaf
    cbz x21, .Lbs_find_first

    // Start key provided: find leaf containing start_key
    mov x0, x19
    mov w1, w20
    mov x2, x21
    bl btree_find_leaf
    cbz x0, .Lbs_ok
    mov x25, x0            // leaf_ptr
    mov w26, w1             // start_index
    b .Lbs_scan_loop

.Lbs_find_first:
    // No start key: find leftmost leaf
    mov x0, x19
    mov w1, w20
    bl btree_page_get_ptr
    mov x25, x0

.Lbs_find_leftmost:
    ldrh w0, [x25, #PH_PAGE_TYPE]
    cmp w0, #PAGE_TYPE_LEAF
    b.eq .Lbs_at_leftmost

    // Internal: go to child[0]
    mov x0, x25
    mov w1, #0
    bl btree_int_get_child
    mov x1, x19
    add x25, x1, x0, lsl #PAGE_SHIFT
    b .Lbs_find_leftmost

.Lbs_at_leftmost:
    mov w26, #0             // start at index 0

// ============================================================================
// Main scan loop: iterate through leaf chain
// x25 = current leaf, w26 = current index
// ============================================================================
.Lbs_scan_loop:
    // Check if we're past the end of this leaf
    ldrh w27, [x25, #PH_NUM_KEYS]
    cmp w26, w27
    b.ge .Lbs_next_leaf

    // Get key at current position
    mov x0, x25
    mov w1, w26
    bl btree_page_get_key_ptr
    mov x28, x0             // key_ptr

    // Check end boundary
    cbz x22, .Lbs_in_range

    mov x0, x28
    mov x1, x22
    bl key_compare
    cmp w0, #0
    b.gt .Lbs_ok            // key > end_key, stop

.Lbs_in_range:
    // Get value
    mov x0, x25
    mov w1, w26
    bl btree_leaf_get_val_ptr
    mov x27, x0             // val_ptr

    // Extract key_len from fixed key (first 2 bytes)
    ldrh w1, [x28]          // key_len
    add x0, x28, #2         // key_data (skip length prefix)

    // Extract val_len from fixed val (first 2 bytes)
    ldrh w3, [x27]          // val_len
    add x2, x27, #2         // val_data

    // Call callback(key_data, key_len, val_data, val_len, user_data)
    mov x4, x24             // user_data
    blr x23

    // Check callback return
    cbnz w0, .Lbs_ok        // non-zero = stop

    add w26, w26, #1
    b .Lbs_scan_loop

.Lbs_next_leaf:
    // Move to next leaf via linked list
    ldr w0, [x25, #PH_NEXT_PAGE]
    cmn w0, #1
    b.eq .Lbs_ok

    // Get next leaf pointer
    lsl x0, x0, #PAGE_SHIFT
    add x25, x19, x0
    mov w26, #0
    b .Lbs_scan_loop

.Lbs_ok:
    mov x0, #ADB_OK

    ldp x27, x28, [sp, #80]
    ldp x25, x26, [sp, #64]
    ldp x23, x24, [sp, #48]
    ldp x21, x22, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #96
    ret
.size btree_scan, .-btree_scan

// ============================================================================
// btree_scan_all(mmap_base, root_page_id, callback, user_data) -> error
// Scan all entries in key order
// x0 = mmap_base, w1 = root_page_id, x2 = callback, x3 = user_data
// ============================================================================
.global btree_scan_all
.type btree_scan_all, %function
btree_scan_all:
    // Delegate to btree_scan with NULL start/end
    mov x4, x2             // callback
    mov x5, x3             // user_data
    mov x2, #0             // start_key = NULL
    mov x3, #0             // end_key = NULL
    b btree_scan
.size btree_scan_all, .-btree_scan_all

// ============================================================================
// btree_count(mmap_base, root_page_id) -> count
// Count total entries in tree by walking leaf chain
// x0 = mmap_base, w1 = root_page_id
// Returns: x0 = total count
// ============================================================================
.global btree_count
.type btree_count, %function
btree_count:
    stp x29, x30, [sp, #-48]!
    mov x29, sp
    stp x19, x20, [sp, #16]
    str x21, [sp, #32]

    mov x19, x0            // mmap_base
    mov x20, #0            // count = 0

    cmn w1, #1
    b.eq .Lbc_done

    // Find leftmost leaf
    lsl x1, x1, #PAGE_SHIFT
    add x21, x19, x1

.Lbc_find_left:
    ldrh w0, [x21, #PH_PAGE_TYPE]
    cmp w0, #PAGE_TYPE_LEAF
    b.eq .Lbc_count_loop

    add x0, x21, #BTREE_INT_CHILDREN_OFF
    ldr x0, [x0]            // child[0]
    add x21, x19, x0, lsl #PAGE_SHIFT
    b .Lbc_find_left

.Lbc_count_loop:
    ldrh w0, [x21, #PH_NUM_KEYS]
    add x20, x20, x0

    ldr w0, [x21, #PH_NEXT_PAGE]
    cmn w0, #1
    b.eq .Lbc_done

    lsl x0, x0, #PAGE_SHIFT
    add x21, x19, x0
    b .Lbc_count_loop

.Lbc_done:
    mov x0, x20

    ldr x21, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #48
    ret
.size btree_count, .-btree_count
