// AssemblyDB - B+ Tree Page Operations
// Page initialization, key/val access helpers, page header management
// Page size: 4096 bytes, Internal order: 55, Leaf order: 12

.include "src/const.s"

.text

// ============================================================================
// btree_page_init_internal(page_ptr, page_id)
// Initialize a page as internal node
// x0 = page_ptr (4096 bytes, must be zeroed)
// w1 = page_id
// ============================================================================
.global btree_page_init_internal
.type btree_page_init_internal, %function
btree_page_init_internal:
    str w1, [x0, #PH_PAGE_ID]
    mov w2, #PAGE_TYPE_INTERNAL
    strh w2, [x0, #PH_PAGE_TYPE]
    strh wzr, [x0, #PH_NUM_KEYS]
    mvn w2, wzr
    str w2, [x0, #PH_PARENT_PAGE]
    str wzr, [x0, #PH_CRC32]
    str xzr, [x0, #PH_LSN]
    str xzr, [x0, #PH_NEXT_PAGE]
    str xzr, [x0, #PH_PREV_PAGE]
    ret
.size btree_page_init_internal, .-btree_page_init_internal

// ============================================================================
// btree_page_init_leaf(page_ptr, page_id)
// Initialize a page as leaf node
// x0 = page_ptr (4096 bytes, must be zeroed)
// w1 = page_id
// ============================================================================
.global btree_page_init_leaf
.type btree_page_init_leaf, %function
btree_page_init_leaf:
    str w1, [x0, #PH_PAGE_ID]
    mov w2, #PAGE_TYPE_LEAF
    strh w2, [x0, #PH_PAGE_TYPE]
    strh wzr, [x0, #PH_NUM_KEYS]
    mvn w2, wzr
    str w2, [x0, #PH_PARENT_PAGE]
    str wzr, [x0, #PH_CRC32]
    str xzr, [x0, #PH_LSN]
    mvn w2, wzr
    str w2, [x0, #PH_NEXT_PAGE]
    mvn w2, wzr
    str w2, [x0, #PH_PREV_PAGE]
    ret
.size btree_page_init_leaf, .-btree_page_init_leaf

// ============================================================================
// btree_page_get_key_ptr(page_ptr, index) -> key_ptr
// Get pointer to key[index] in any node type
// x0 = page_ptr, w1 = index
// Returns: x0 = pointer to 64-byte key slot
// key_ptr = page_ptr + BTREE_KEYS_OFFSET + index * KEY_SIZE
//         = page_ptr + 0x040 + index << 6
// ============================================================================
.global btree_page_get_key_ptr
.type btree_page_get_key_ptr, %function
btree_page_get_key_ptr:
    add x0, x0, #BTREE_KEYS_OFFSET
    add x0, x0, x1, lsl #KEY_SHIFT
    ret
.size btree_page_get_key_ptr, .-btree_page_get_key_ptr

// ============================================================================
// btree_leaf_get_val_ptr(page_ptr, index) -> val_ptr
// Get pointer to val[index] in a leaf node
// x0 = page_ptr, w1 = index
// Returns: x0 = pointer to 256-byte value slot
// val_ptr = page_ptr + BTREE_LEAF_VALS_OFF + index * VAL_SIZE
//         = page_ptr + 0x340 + index << 8
// ============================================================================
.global btree_leaf_get_val_ptr
.type btree_leaf_get_val_ptr, %function
btree_leaf_get_val_ptr:
    add x0, x0, #BTREE_LEAF_VALS_OFF
    add x0, x0, x1, lsl #VAL_SHIFT
    ret
.size btree_leaf_get_val_ptr, .-btree_leaf_get_val_ptr

// ============================================================================
// btree_int_get_child(page_ptr, index) -> child_page_id
// Get child pointer at index in internal node
// x0 = page_ptr, w1 = index
// Returns: x0 = child page number (64-bit)
// child = *(page_ptr + BTREE_INT_CHILDREN_OFF + index * 8)
// ============================================================================
.global btree_int_get_child
.type btree_int_get_child, %function
btree_int_get_child:
    add x0, x0, #BTREE_INT_CHILDREN_OFF
    ldr x0, [x0, x1, lsl #3]
    ret
.size btree_int_get_child, .-btree_int_get_child

// ============================================================================
// btree_int_set_child(page_ptr, index, child_page_id)
// Set child pointer at index in internal node
// x0 = page_ptr, w1 = index, x2 = child page number
// ============================================================================
.global btree_int_set_child
.type btree_int_set_child, %function
btree_int_set_child:
    add x0, x0, #BTREE_INT_CHILDREN_OFF
    str x2, [x0, x1, lsl #3]
    ret
.size btree_int_set_child, .-btree_int_set_child

// ============================================================================
// btree_leaf_get_txid(page_ptr, index) -> tx_id
// Get transaction ID at index in leaf node
// x0 = page_ptr, w1 = index
// Returns: x0 = tx_id
// ============================================================================
.global btree_leaf_get_txid
.type btree_leaf_get_txid, %function
btree_leaf_get_txid:
    add x0, x0, #BTREE_LEAF_TXIDS_OFF
    ldr x0, [x0, x1, lsl #3]
    ret
.size btree_leaf_get_txid, .-btree_leaf_get_txid

// ============================================================================
// btree_leaf_set_txid(page_ptr, index, tx_id)
// Set transaction ID at index in leaf node
// x0 = page_ptr, w1 = index, x2 = tx_id
// ============================================================================
.global btree_leaf_set_txid
.type btree_leaf_set_txid, %function
btree_leaf_set_txid:
    add x0, x0, #BTREE_LEAF_TXIDS_OFF
    str x2, [x0, x1, lsl #3]
    ret
.size btree_leaf_set_txid, .-btree_leaf_set_txid

// ============================================================================
// btree_page_num_keys(page_ptr) -> count
// x0 = page_ptr
// Returns: w0 = num_keys
// ============================================================================
.global btree_page_num_keys
.type btree_page_num_keys, %function
btree_page_num_keys:
    ldrh w0, [x0, #PH_NUM_KEYS]
    ret
.size btree_page_num_keys, .-btree_page_num_keys

// ============================================================================
// btree_page_type(page_ptr) -> type
// x0 = page_ptr
// Returns: w0 = page type
// ============================================================================
.global btree_page_type
.type btree_page_type, %function
btree_page_type:
    ldrh w0, [x0, #PH_PAGE_TYPE]
    ret
.size btree_page_type, .-btree_page_type

// ============================================================================
// btree_page_is_leaf(page_ptr) -> bool
// x0 = page_ptr
// Returns: w0 = 1 if leaf, 0 if not
// ============================================================================
.global btree_page_is_leaf
.type btree_page_is_leaf, %function
btree_page_is_leaf:
    ldrh w0, [x0, #PH_PAGE_TYPE]
    cmp w0, #PAGE_TYPE_LEAF
    cset w0, eq
    ret
.size btree_page_is_leaf, .-btree_page_is_leaf

// ============================================================================
// btree_page_compute_crc(page_ptr) -> crc32
// Compute CRC32C of page, skipping the CRC field itself
// x0 = page_ptr
// Returns: w0 = CRC32C
// ============================================================================
.global btree_page_compute_crc
.type btree_page_compute_crc, %function
btree_page_compute_crc:
    stp x29, x30, [sp, #-32]!
    mov x29, sp
    str x19, [sp, #16]

    mov x19, x0

    // CRC = crc32c(page[0..PH_CRC32]) ^ crc32c(page[PH_CRC32+4..4096])
    // Part 1: bytes 0..12 (before CRC field)
    mov x1, #PH_CRC32
    bl hw_crc32c
    mov w1, w0              // partial CRC

    // Part 2: bytes 16..4096 (after CRC field)
    add x0, x19, #PH_CRC32
    add x0, x0, #4          // skip 4-byte CRC field
    mov x1, #PAGE_SIZE
    sub x1, x1, #PH_CRC32
    sub x1, x1, #4
    // Use hw_crc32c_combine to chain with partial CRC
    // w1 already has partial CRC, but hw_crc32c_combine expects w2 as init
    mov w2, w1
    bl hw_crc32c_combine

    ldr x19, [sp, #16]
    ldp x29, x30, [sp], #32
    ret
.size btree_page_compute_crc, .-btree_page_compute_crc

// ============================================================================
// btree_page_set_crc(page_ptr)
// Compute and store CRC in page header
// x0 = page_ptr
// ============================================================================
.global btree_page_set_crc
.type btree_page_set_crc, %function
btree_page_set_crc:
    stp x29, x30, [sp, #-32]!
    mov x29, sp
    str x19, [sp, #16]

    mov x19, x0
    bl btree_page_compute_crc
    str w0, [x19, #PH_CRC32]

    ldr x19, [sp, #16]
    ldp x29, x30, [sp], #32
    ret
.size btree_page_set_crc, .-btree_page_set_crc

// ============================================================================
// btree_page_verify_crc(page_ptr) -> bool
// Verify CRC of page
// x0 = page_ptr
// Returns: w0 = 1 if valid, 0 if corrupt
// ============================================================================
.global btree_page_verify_crc
.type btree_page_verify_crc, %function
btree_page_verify_crc:
    stp x29, x30, [sp, #-32]!
    mov x29, sp
    str x19, [sp, #16]

    mov x19, x0
    bl btree_page_compute_crc
    ldr w1, [x19, #PH_CRC32]
    cmp w0, w1
    cset w0, eq

    ldr x19, [sp, #16]
    ldp x29, x30, [sp], #32
    ret
.size btree_page_verify_crc, .-btree_page_verify_crc

// ============================================================================
// btree_leaf_shift_right(page_ptr, from_idx, count)
// Shift keys, vals, and txids right by 1 starting at from_idx
// Used for insertion in sorted order
// x0 = page_ptr, w1 = from_idx, w2 = count (num items to shift)
// ============================================================================
.global btree_leaf_shift_right
.type btree_leaf_shift_right, %function
btree_leaf_shift_right:
    stp x29, x30, [sp, #-64]!
    mov x29, sp
    stp x19, x20, [sp, #16]
    stp x21, x22, [sp, #32]
    str x23, [sp, #48]

    mov x19, x0            // page_ptr
    mov w20, w1             // from_idx
    mov w21, w2             // count

    cbz w21, .Llsr_done

    // Shift from (from_idx + count - 1) down to from_idx
    // i.e., move backwards: dst = from_idx+count, src = from_idx+count-1
    add w22, w20, w21       // end = from + count (one past last to shift)
    sub w22, w22, #1        // start from last item

.Llsr_key_loop:
    cmp w22, w20
    b.lt .Llsr_vals

    // Copy key[i] -> key[i+1]
    add w23, w22, #1
    mov x0, x19
    mov w1, w23
    bl btree_page_get_key_ptr
    mov x23, x0             // dst key ptr

    mov x0, x19
    mov w1, w22
    bl btree_page_get_key_ptr
    // x0 = src key ptr, x23 = dst key ptr
    mov x1, x0
    mov x0, x23
    bl neon_copy_64

    sub w22, w22, #1
    b .Llsr_key_loop

.Llsr_vals:
    add w22, w20, w21
    sub w22, w22, #1

.Llsr_val_loop:
    cmp w22, w20
    b.lt .Llsr_txids

    // Copy val[i] -> val[i+1]
    add w23, w22, #1
    mov x0, x19
    mov w1, w23
    bl btree_leaf_get_val_ptr
    mov x23, x0             // dst val ptr

    mov x0, x19
    mov w1, w22
    bl btree_leaf_get_val_ptr
    // x0 = src val ptr, x23 = dst val ptr
    mov x1, x0
    mov x0, x23
    bl neon_copy_256

    sub w22, w22, #1
    b .Llsr_val_loop

.Llsr_txids:
    add w22, w20, w21
    sub w22, w22, #1

.Llsr_txid_loop:
    cmp w22, w20
    b.lt .Llsr_done

    // Copy txid[i] -> txid[i+1]
    add x0, x19, #BTREE_LEAF_TXIDS_OFF
    ldr x1, [x0, w22, uxtw #3]
    add w23, w22, #1
    str x1, [x0, w23, uxtw #3]

    sub w22, w22, #1
    b .Llsr_txid_loop

.Llsr_done:
    ldr x23, [sp, #48]
    ldp x21, x22, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #64
    ret
.size btree_leaf_shift_right, .-btree_leaf_shift_right

// ============================================================================
// btree_leaf_shift_left(page_ptr, from_idx, count)
// Shift keys, vals, and txids left by 1 starting at from_idx
// Used for deletion - items from from_idx to from_idx+count-1 shift left
// x0 = page_ptr, w1 = from_idx (first to overwrite), w2 = count
// ============================================================================
.global btree_leaf_shift_left
.type btree_leaf_shift_left, %function
btree_leaf_shift_left:
    stp x29, x30, [sp, #-64]!
    mov x29, sp
    stp x19, x20, [sp, #16]
    stp x21, x22, [sp, #32]
    str x23, [sp, #48]

    mov x19, x0
    mov w20, w1             // from_idx
    mov w21, w2             // count

    cbz w21, .Llsl_done

    mov w22, w20            // i = from_idx

.Llsl_key_loop:
    cmp w22, w20
    add w23, w20, w21
    cmp w22, w23
    b.ge .Llsl_vals_init

    // Copy key[i+1] -> key[i]
    mov x0, x19
    mov w1, w22
    bl btree_page_get_key_ptr
    mov x23, x0             // dst

    add w1, w22, #1
    mov x0, x19
    bl btree_page_get_key_ptr
    mov x1, x0
    mov x0, x23
    bl neon_copy_64

    add w22, w22, #1
    b .Llsl_key_loop

.Llsl_vals_init:
    mov w22, w20

.Llsl_val_loop:
    add w23, w20, w21
    cmp w22, w23
    b.ge .Llsl_txids_init

    // Copy val[i+1] -> val[i]
    mov x0, x19
    mov w1, w22
    bl btree_leaf_get_val_ptr
    mov x23, x0

    add w1, w22, #1
    mov x0, x19
    bl btree_leaf_get_val_ptr
    mov x1, x0
    mov x0, x23
    bl neon_copy_256

    add w22, w22, #1
    b .Llsl_val_loop

.Llsl_txids_init:
    mov w22, w20

.Llsl_txid_loop:
    add w23, w20, w21
    cmp w22, w23
    b.ge .Llsl_done

    add x0, x19, #BTREE_LEAF_TXIDS_OFF
    add w23, w22, #1
    ldr x1, [x0, w23, uxtw #3]
    str x1, [x0, w22, uxtw #3]

    add w22, w22, #1
    b .Llsl_txid_loop

.Llsl_done:
    ldr x23, [sp, #48]
    ldp x21, x22, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #64
    ret
.size btree_leaf_shift_left, .-btree_leaf_shift_left

// ============================================================================
// btree_int_shift_keys_right(page_ptr, from_idx, count)
// Shift keys and children right in internal node
// x0 = page_ptr, w1 = from_idx, w2 = count
// ============================================================================
.global btree_int_shift_keys_right
.type btree_int_shift_keys_right, %function
btree_int_shift_keys_right:
    stp x29, x30, [sp, #-64]!
    mov x29, sp
    stp x19, x20, [sp, #16]
    stp x21, x22, [sp, #32]
    str x23, [sp, #48]

    mov x19, x0
    mov w20, w1
    mov w21, w2

    cbz w21, .Lisr_done

    // Shift keys right (backwards iteration)
    add w22, w20, w21
    sub w22, w22, #1

.Lisr_key_loop:
    cmp w22, w20
    b.lt .Lisr_children

    add w23, w22, #1
    mov x0, x19
    mov w1, w23
    bl btree_page_get_key_ptr
    mov x23, x0

    mov x0, x19
    mov w1, w22
    bl btree_page_get_key_ptr
    mov x1, x0
    mov x0, x23
    bl neon_copy_64

    sub w22, w22, #1
    b .Lisr_key_loop

.Lisr_children:
    // Shift children right (children has one more than keys)
    // Shift children[from+1..from+count+1] <- children[from..from+count]
    add w22, w20, w21  // last child to shift = from + count

.Lisr_child_loop:
    cmp w22, w20
    b.lt .Lisr_done

    add x0, x19, #BTREE_INT_CHILDREN_OFF
    ldr x1, [x0, w22, uxtw #3]
    add w23, w22, #1
    str x1, [x0, w23, uxtw #3]

    sub w22, w22, #1
    b .Lisr_child_loop

.Lisr_done:
    ldr x23, [sp, #48]
    ldp x21, x22, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #64
    ret
.size btree_int_shift_keys_right, .-btree_int_shift_keys_right

// ============================================================================
// btree_int_shift_keys_left(page_ptr, from_idx, count)
// Shift keys and children left in internal node
// x0 = page_ptr, w1 = from_idx, w2 = count
// ============================================================================
.global btree_int_shift_keys_left
.type btree_int_shift_keys_left, %function
btree_int_shift_keys_left:
    stp x29, x30, [sp, #-64]!
    mov x29, sp
    stp x19, x20, [sp, #16]
    stp x21, x22, [sp, #32]
    str x23, [sp, #48]

    mov x19, x0
    mov w20, w1
    mov w21, w2

    cbz w21, .Lisl_done

    mov w22, w20

.Lisl_key_loop:
    add w23, w20, w21
    cmp w22, w23
    b.ge .Lisl_children

    add w1, w22, #1
    mov x0, x19
    bl btree_page_get_key_ptr
    mov x23, x0

    mov x0, x19
    mov w1, w22
    bl btree_page_get_key_ptr
    // Copy key[i+1] -> key[i]
    mov x1, x23
    bl neon_copy_64

    add w22, w22, #1
    b .Lisl_key_loop

.Lisl_children:
    // Also shift children[from+1..from+count+1] left
    add w22, w20, #1  // children start one ahead

.Lisl_child_loop:
    add w23, w20, w21
    add w23, w23, #1
    cmp w22, w23
    b.ge .Lisl_done

    add x0, x19, #BTREE_INT_CHILDREN_OFF
    ldr x1, [x0, w22, uxtw #3]
    sub w23, w22, #1
    str x1, [x0, w23, uxtw #3]

    add w22, w22, #1
    b .Lisl_child_loop

.Lisl_done:
    ldr x23, [sp, #48]
    ldp x21, x22, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #64
    ret
.size btree_int_shift_keys_left, .-btree_int_shift_keys_left

// ============================================================================
// btree_page_get_ptr(mmap_base, page_id) -> page_ptr
// Convert page_id to memory pointer
// x0 = mmap_base, w1 = page_id
// Returns: x0 = page_ptr (mmap_base + page_id * PAGE_SIZE)
// ============================================================================
.global btree_page_get_ptr
.type btree_page_get_ptr, %function
btree_page_get_ptr:
    lsl x1, x1, #PAGE_SHIFT
    add x0, x0, x1
    ret
.size btree_page_get_ptr, .-btree_page_get_ptr
