// AssemblyDB - B+ Tree Search Operations
// Binary search within pages + tree descent for point lookups
// Uses NEON-accelerated key comparison

.include "src/const.s"

.text

// ============================================================================
// btree_page_binary_search(page_ptr, key_ptr) -> (index, found)
// Binary search for key within a page's key array
// x0 = page_ptr, x1 = key_ptr (64-byte fixed key)
// Returns: w0 = index (insertion point or exact match position)
//          w1 = 1 if exact match found, 0 if not
// For internal nodes: returns child index to descend into
// For leaf nodes: returns slot index (exact or insertion point)
// ============================================================================
.global btree_page_binary_search
.type btree_page_binary_search, %function
btree_page_binary_search:
    stp x29, x30, [sp, #-64]!
    mov x29, sp
    stp x19, x20, [sp, #16]
    stp x21, x22, [sp, #32]
    stp x23, x24, [sp, #48]

    mov x19, x0            // page_ptr
    mov x20, x1            // search key_ptr

    ldrh w21, [x19, #PH_NUM_KEYS]

    // Handle empty page (check before sub to avoid unsigned underflow)
    cbz w21, .Lbs_not_found_zero

    // Binary search: lo=0, hi=num_keys-1
    mov w22, #0             // lo
    sub w23, w21, #1        // hi = num_keys - 1

.Lbs_loop:
    cmp w22, w23
    b.gt .Lbs_not_found

    // mid = (lo + hi) / 2
    add w24, w22, w23
    lsr w24, w24, #1

    // Get key_ptr for key[mid]
    mov x0, x19
    mov w1, w24
    bl btree_page_get_key_ptr
    // x0 = key[mid] ptr

    // Compare search_key vs key[mid]
    mov x1, x0
    mov x0, x20
    bl key_compare
    // w0: <0 if search < mid, 0 if equal, >0 if search > mid

    cbz w0, .Lbs_found
    cmp w0, #0
    b.lt .Lbs_go_left

    // search > mid: lo = mid + 1
    add w22, w24, #1
    b .Lbs_loop

.Lbs_go_left:
    // search < mid: hi = mid - 1
    sub w23, w24, #1
    b .Lbs_loop

.Lbs_found:
    mov w0, w24             // index of exact match
    mov w1, #1              // found = true
    b .Lbs_ret

.Lbs_not_found:
    // lo > hi: insertion point = lo
    mov w0, w22
    mov w1, #0              // found = false
    b .Lbs_ret

.Lbs_not_found_zero:
    mov w0, #0
    mov w1, #0
    b .Lbs_ret

.Lbs_ret:
    ldp x23, x24, [sp, #48]
    ldp x21, x22, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #64
    ret
.size btree_page_binary_search, .-btree_page_binary_search

// ============================================================================
// btree_find_leaf(mmap_base, root_page_id, key_ptr) -> (leaf_page_ptr, index, found)
// Descend from root to the leaf containing key
// x0 = mmap_base, w1 = root_page_id, x2 = key_ptr
// Returns: x0 = leaf page_ptr (or 0 on error)
//          w1 = index in leaf (insertion point or match)
//          w2 = 1 if exact match, 0 if not
// ============================================================================
.global btree_find_leaf
.type btree_find_leaf, %function
btree_find_leaf:
    stp x29, x30, [sp, #-64]!
    mov x29, sp
    stp x19, x20, [sp, #16]
    stp x21, x22, [sp, #32]
    stp x23, x24, [sp, #48]

    mov x19, x0            // mmap_base
    mov w20, w1             // current page_id
    mov x21, x2            // key_ptr
    mov w24, #0             // depth counter

    // Start at root
    cmn w20, #1
    b.eq .Lfl_error

    // Bounds check root page_id against num_pages (mmap[16])
    ldr x0, [x19, #16]
    cmp x20, x0
    b.hs .Lfl_error

.Lfl_descend:
    // Depth guard: max 64 levels (prevents infinite loop on corrupt tree)
    cmp w24, #64
    b.hs .Lfl_error

    // Get page pointer
    mov x0, x19
    mov w1, w20
    bl btree_page_get_ptr
    mov x22, x0            // current page_ptr

    // Prefetch for next level (speculative)
    prfm pldl1keep, [x22]

    // Check page type
    ldrh w0, [x22, #PH_PAGE_TYPE]
    cmp w0, #PAGE_TYPE_LEAF
    b.eq .Lfl_at_leaf
    cmp w0, #PAGE_TYPE_INTERNAL
    b.ne .Lfl_error                // corrupt: page is neither leaf nor internal

    // Internal node: binary search to find child
    mov x0, x22
    mov x1, x21
    bl btree_page_binary_search
    // w0 = index, w1 = found

    // For internal node: if found, child = index + 1
    // if not found, child = index (insertion point = child index)
    cbz w1, .Lfl_use_index
    add w0, w0, #1         // exact match: go right child

.Lfl_use_index:
    // Get child page_id
    mov w23, w0             // save child_index (w0 will be clobbered)
    mov x0, x22             // page_ptr
    mov w1, w23             // child_index
    bl btree_int_get_child
    mov w20, w0             // next page_id

    // Bounds check child page_id
    ldr x0, [x19, #16]     // num_pages
    cmp x20, x0
    b.hs .Lfl_error

    // Prefetch child page (inline to avoid function call overhead)
    lsl x0, x20, #PAGE_SHIFT
    add x0, x19, x0
    prfm pldl1keep, [x0]

    add w24, w24, #1       // increment depth
    b .Lfl_descend

.Lfl_at_leaf:
    // At leaf: binary search for key
    mov x0, x22
    mov x1, x21
    bl btree_page_binary_search
    // w0 = index, w1 = found

    mov w23, w0             // save index
    mov w24, w1             // save found

    mov x0, x22             // leaf page_ptr
    mov w1, w23             // index
    mov w2, w24             // found
    b .Lfl_ret

.Lfl_error:
    mov x0, #0
    mov w1, #0
    mov w2, #0

.Lfl_ret:
    ldp x23, x24, [sp, #48]
    ldp x21, x22, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #64
    ret
.size btree_find_leaf, .-btree_find_leaf

// ============================================================================
// btree_get(mmap_base, root_page_id, key_ptr, val_buf) -> error
// Point lookup: find key and copy value to buffer
// x0 = mmap_base, w1 = root_page_id, x2 = key_ptr, x3 = val_buf (256 bytes)
// Returns: x0 = ADB_OK or ADB_ERR_NOT_FOUND
// ============================================================================
.global btree_get
.type btree_get, %function
btree_get:
    stp x29, x30, [sp, #-48]!
    mov x29, sp
    stp x19, x20, [sp, #16]
    str x21, [sp, #32]

    mov x19, x3            // val_buf

    // Find leaf containing key
    bl btree_find_leaf
    // x0 = leaf_ptr, w1 = index, w2 = found

    cbz x0, .Lbg_not_found
    cbz w2, .Lbg_not_found

    // Found: copy value to buffer
    mov x20, x0            // leaf_ptr
    mov w21, w1             // index

    mov w1, w21
    bl btree_leaf_get_val_ptr
    // x0 = val_ptr in leaf

    mov x1, x0             // src = val in leaf
    mov x0, x19            // dst = val_buf
    bl neon_copy_256

    mov x0, #ADB_OK
    b .Lbg_ret

.Lbg_not_found:
    mov x0, #ADB_ERR_NOT_FOUND

.Lbg_ret:
    ldr x21, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #48
    ret
.size btree_get, .-btree_get
