// AssemblyDB - B+ Tree Insert Operations
// Insert with leaf/internal page splitting and key promotion
// Handles page allocation via mmap file extension

.include "src/const.s"

.text

// ============================================================================
// btree_alloc_page(db_ptr) -> page_id
// Allocate a new B+ tree page by extending the mmap'd file
// x0 = db_ptr (db_t handle)
// Returns: w0 = new page_id, or INVALID_PAGE on failure
// ============================================================================
.global btree_alloc_page
.type btree_alloc_page, %function
btree_alloc_page:
    stp x29, x30, [sp, #-48]!
    mov x29, sp
    stp x19, x20, [sp, #16]
    str x21, [sp, #32]

    mov x19, x0                    // db_ptr

    ldr x20, [x19, #DB_BTREE_NUM_PAGES]    // current num_pages
    ldr x21, [x19, #DB_BTREE_CAPACITY]     // file capacity in pages

    // Check if we need to grow
    cmp x20, x21
    b.lo .Lap_have_space

    // Grow file: double capacity (min 64 pages)
    mov x1, #64
    cmp x21, x1
    csel x21, x21, x1, hi            // capacity = max(capacity, 64)
    lsl x21, x21, #1              // double

    // ftruncate to new size
    ldr x0, [x19, #DB_BTREE_FD]
    lsl x1, x21, #PAGE_SHIFT      // new_capacity * PAGE_SIZE
    bl sys_ftruncate
    cmp x0, #0
    b.lt .Lap_fail

    // Remap: munmap old, mmap new
    ldr x0, [x19, #DB_BTREE_MMAP]
    ldr x1, [x19, #DB_BTREE_MMAP_LEN]
    cbz x0, .Lap_fresh_map
    bl sys_munmap

.Lap_fresh_map:
    ldr x0, [x19, #DB_BTREE_FD]
    lsl x1, x21, #PAGE_SHIFT      // length
    bl mmap_file_rw
    cmn x0, #4096
    b.hi .Lap_fail                 // mmap failed (returned -errno)

    // Update db state
    str x0, [x19, #DB_BTREE_MMAP]
    lsl x1, x21, #PAGE_SHIFT
    str x1, [x19, #DB_BTREE_MMAP_LEN]
    str x21, [x19, #DB_BTREE_CAPACITY]

.Lap_have_space:
    // Allocate page: page_id = num_pages++
    mov w0, w20                    // return page_id
    add x20, x20, #1
    str x20, [x19, #DB_BTREE_NUM_PAGES]
    // Keep page 0 metadata in sync (for btree_scan bounds checks)
    ldr x1, [x19, #DB_BTREE_MMAP]
    str x20, [x1, #16]

    // Zero the new page (x1 still holds DB_BTREE_MMAP)
    lsl x2, x0, #PAGE_SHIFT
    add x1, x1, x2
    mov w20, w0                    // save page_id (x20 free after str at line 68)
    mov x0, x1
    mov x1, #PAGE_SIZE
    bl neon_memzero
    mov w0, w20

    b .Lap_ret

.Lap_fail:
    mvn w0, wzr

.Lap_ret:
    ldr x21, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #48
    ret
.size btree_alloc_page, .-btree_alloc_page

// ============================================================================
// btree_leaf_insert_at(page_ptr, index, key_ptr, val_ptr, tx_id)
// Insert key/val at position index in leaf, shifting existing entries right
// x0 = page_ptr, w1 = index, x2 = key_ptr, x3 = val_ptr, x4 = tx_id
// Assumes there is room (num_keys < BTREE_LEAF_MAX_KEYS)
// ============================================================================
.global btree_leaf_insert_at
.type btree_leaf_insert_at, %function
btree_leaf_insert_at:
    stp x29, x30, [sp, #-80]!
    mov x29, sp
    stp x19, x20, [sp, #16]
    stp x21, x22, [sp, #32]
    stp x23, x24, [sp, #48]
    str x25, [sp, #64]

    mov x19, x0            // page_ptr
    mov w20, w1             // index
    mov x21, x2            // key_ptr
    mov x22, x3            // val_ptr
    mov x23, x4            // tx_id

    // Shift entries right from index to num_keys-1
    ldrh w24, [x19, #PH_NUM_KEYS]
    sub w25, w24, w20       // count = num_keys - index

    cmp w25, #0
    b.le .Llia_no_shift

    mov x0, x19
    mov w1, w20
    mov w2, w25
    bl btree_leaf_shift_right

.Llia_no_shift:
    // Copy key into slot
    mov x0, x19
    mov w1, w20
    bl btree_page_get_key_ptr
    mov x1, x21             // src key
    bl neon_copy_64

    // Copy val into slot
    mov x0, x19
    mov w1, w20
    bl btree_leaf_get_val_ptr
    mov x1, x22             // src val
    bl neon_copy_256

    // Set tx_id
    add x0, x19, #BTREE_LEAF_TXIDS_OFF
    str x23, [x0, w20, uxtw #3]

    // Increment num_keys
    add w24, w24, #1
    strh w24, [x19, #PH_NUM_KEYS]

    ldr x25, [sp, #64]
    ldp x23, x24, [sp, #48]
    ldp x21, x22, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #80
    ret
.size btree_leaf_insert_at, .-btree_leaf_insert_at

// ============================================================================
// btree_leaf_split(db_ptr, old_leaf_ptr) -> new_page_id
// Split a full leaf into two halves
// x0 = db_ptr, x1 = old_leaf_ptr (full, has BTREE_LEAF_MAX_KEYS entries)
// Returns: w0 = new leaf page_id (right half), or INVALID_PAGE on failure
// The split point is mid = num_keys / 2
// Left leaf keeps keys[0..mid-1], right gets keys[mid..num_keys-1]
// The median key (first key of right leaf) must be promoted to parent
// ============================================================================
.global btree_leaf_split
.type btree_leaf_split, %function
btree_leaf_split:
    stp x29, x30, [sp, #-80]!
    mov x29, sp
    stp x19, x20, [sp, #16]
    stp x21, x22, [sp, #32]
    stp x23, x24, [sp, #48]
    str x25, [sp, #64]

    mov x19, x0            // db_ptr
    mov x20, x1            // old_leaf_ptr

    // Save old leaf's page_id BEFORE alloc (alloc may remap mmap)
    ldr w25, [x20, #PH_PAGE_ID]

    // Allocate new leaf page
    mov x0, x19
    bl btree_alloc_page
    cmn w0, #1
    b.eq .Llsp_fail

    mov w21, w0             // new_page_id

    // Re-derive old_leaf_ptr (mmap may have moved during alloc)
    ldr x22, [x19, #DB_BTREE_MMAP]
    add x20, x22, x25, lsl #PAGE_SHIFT

    // Get pointer to new page
    mov x0, x22             // mmap_base (already loaded)
    mov w1, w21
    bl btree_page_get_ptr
    mov x22, x0             // new_leaf_ptr

    // Initialize new leaf
    mov x0, x22
    mov w1, w21
    bl btree_page_init_leaf

    // Calculate split point
    ldrh w23, [x20, #PH_NUM_KEYS]
    lsr w24, w23, #1        // mid = num_keys / 2

    // Copy keys[mid..num_keys-1] to new leaf
    mov w25, w24            // i = mid

.Llsp_copy_loop:
    cmp w25, w23
    b.hs .Llsp_copy_done

    sub w1, w25, w24        // dst_idx = i - mid

    // Copy key
    mov x0, x20
    mov w1, w25
    bl btree_page_get_key_ptr
    mov x3, x0              // src key

    sub w1, w25, w24
    mov x0, x22
    bl btree_page_get_key_ptr
    mov x1, x3
    bl neon_copy_64

    // Copy val
    mov x0, x20
    mov w1, w25
    bl btree_leaf_get_val_ptr
    mov x3, x0

    sub w1, w25, w24
    mov x0, x22
    bl btree_leaf_get_val_ptr
    mov x1, x3
    bl neon_copy_256

    // Copy txid
    add x0, x20, #BTREE_LEAF_TXIDS_OFF
    ldr x3, [x0, w25, uxtw #3]
    sub w1, w25, w24
    add x0, x22, #BTREE_LEAF_TXIDS_OFF
    str x3, [x0, w1, uxtw #3]

    add w25, w25, #1
    b .Llsp_copy_loop

.Llsp_copy_done:
    // Update key counts
    strh w24, [x20, #PH_NUM_KEYS]         // old leaf: mid keys
    sub w1, w23, w24
    strh w1, [x22, #PH_NUM_KEYS]          // new leaf: num_keys - mid

    // Link leaves: old->next = new, new->prev = old, new->next = old_next
    ldr w0, [x20, #PH_NEXT_PAGE]           // old_next
    str w0, [x22, #PH_NEXT_PAGE]           // new->next = old_next
    ldr w0, [x20, #PH_PAGE_ID]
    str w0, [x22, #PH_PREV_PAGE]           // new->prev = old
    str w21, [x20, #PH_NEXT_PAGE]          // old->next = new

    // Update old_next->prev = new (if old_next exists)
    ldr w0, [x22, #PH_NEXT_PAGE]
    cmn w0, #1
    b.eq .Llsp_no_update_next
    // Bounds check old_next page_id
    ldr x1, [x19, #DB_BTREE_NUM_PAGES]
    cmp w0, w1
    b.hs .Llsp_no_update_next

    ldr x1, [x19, #DB_BTREE_MMAP]
    lsl x0, x0, #PAGE_SHIFT
    add x1, x1, x0
    str w21, [x1, #PH_PREV_PAGE]

.Llsp_no_update_next:
    // Copy parent from old leaf to new leaf
    ldr w0, [x20, #PH_PARENT_PAGE]
    str w0, [x22, #PH_PARENT_PAGE]

    mov w0, w21             // return new_page_id
    b .Llsp_ret

.Llsp_fail:
    mvn w0, wzr

.Llsp_ret:
    ldr x25, [sp, #64]
    ldp x23, x24, [sp, #48]
    ldp x21, x22, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #80
    ret
.size btree_leaf_split, .-btree_leaf_split

// ============================================================================
// btree_int_insert_at(page_ptr, index, key_ptr, right_child)
// Insert key + right child pointer at position in internal node
// x0 = page_ptr, w1 = index, x2 = key_ptr, x3 = right_child_page_id
// ============================================================================
.global btree_int_insert_at
.type btree_int_insert_at, %function
btree_int_insert_at:
    stp x29, x30, [sp, #-64]!
    mov x29, sp
    stp x19, x20, [sp, #16]
    stp x21, x22, [sp, #32]
    str x23, [sp, #48]

    mov x19, x0            // page_ptr
    mov w20, w1             // index
    mov x21, x2            // key_ptr
    mov x22, x3            // right_child

    // Shift keys and children right
    ldrh w23, [x19, #PH_NUM_KEYS]
    sub w2, w23, w20        // count = num_keys - index

    cmp w2, #0
    b.le .Liia_no_shift

    mov x0, x19
    mov w1, w20
    bl btree_int_shift_keys_right

.Liia_no_shift:
    // Copy key into slot
    mov x0, x19
    mov w1, w20
    bl btree_page_get_key_ptr
    // x0 = key slot ptr (dst), don't save in x23 - it holds num_keys
    mov x1, x21
    bl neon_copy_64

    // Set right child at index+1
    add w1, w20, #1
    add x0, x19, #BTREE_INT_CHILDREN_OFF
    str x22, [x0, w1, uxtw #3]

    // Increment num_keys (w23 still holds original num_keys)
    add w23, w23, #1
    strh w23, [x19, #PH_NUM_KEYS]

    ldr x23, [sp, #48]
    ldp x21, x22, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #64
    ret
.size btree_int_insert_at, .-btree_int_insert_at

// ============================================================================
// btree_int_split(db_ptr, old_int_ptr) -> new_page_id
// Split a full internal node
// x0 = db_ptr, x1 = old_int_ptr
// Returns: w0 = new internal page_id, or INVALID_PAGE
// The median key is NOT kept in either node - it's promoted to parent
// Left: keys[0..mid-1], children[0..mid]
// Right: keys[mid+1..n-1], children[mid+1..n]
// The promoted key = keys[mid]
// ============================================================================
.global btree_int_split
.type btree_int_split, %function
btree_int_split:
    stp x29, x30, [sp, #-80]!
    mov x29, sp
    stp x19, x20, [sp, #16]
    stp x21, x22, [sp, #32]
    stp x23, x24, [sp, #48]
    stp x25, x26, [sp, #64]

    mov x19, x0            // db_ptr
    mov x20, x1            // old_int_ptr

    // Save old node's page_id BEFORE alloc (alloc may remap mmap)
    ldr w25, [x20, #PH_PAGE_ID]

    // Allocate new internal page
    mov x0, x19
    bl btree_alloc_page
    cmn w0, #1
    b.eq .Lisp_fail

    mov w21, w0             // new_page_id

    // Re-derive old_int_ptr (mmap may have moved during alloc)
    ldr x22, [x19, #DB_BTREE_MMAP]
    add x20, x22, x25, lsl #PAGE_SHIFT

    mov x0, x22             // mmap_base
    mov w1, w21
    bl btree_page_get_ptr
    mov x22, x0             // new_int_ptr

    // Init new internal page
    mov x0, x22
    mov w1, w21
    bl btree_page_init_internal

    ldrh w23, [x20, #PH_NUM_KEYS]
    lsr w24, w23, #1        // mid = num_keys / 2

    // Copy keys[mid+1..num_keys-1] -> new node keys[0..]
    add w25, w24, #1        // i = mid + 1
    mov w26, #0             // dst_idx = 0

.Lisp_key_loop:
    cmp w25, w23
    b.hs .Lisp_children

    mov x0, x20
    mov w1, w25
    bl btree_page_get_key_ptr
    mov x3, x0              // src

    mov x0, x22
    mov w1, w26
    bl btree_page_get_key_ptr
    mov x1, x3
    bl neon_copy_64

    add w25, w25, #1
    add w26, w26, #1
    b .Lisp_key_loop

.Lisp_children:
    // Copy children[mid+1..num_keys] -> new node children[0..]
    add w25, w24, #1
    mov w26, #0

.Lisp_child_loop:
    cmp w25, w23
    b.hi .Lisp_update_counts

    add x0, x20, #BTREE_INT_CHILDREN_OFF
    ldr x3, [x0, w25, uxtw #3]

    add x0, x22, #BTREE_INT_CHILDREN_OFF
    str x3, [x0, w26, uxtw #3]

    // Bounds check child page_id before parent pointer update
    ldr x0, [x19, #DB_BTREE_NUM_PAGES]
    cmp x3, x0
    b.hs .Lisp_child_skip

    // Update child's parent pointer
    ldr x0, [x19, #DB_BTREE_MMAP]
    add x0, x0, x3, lsl #PAGE_SHIFT
    str w21, [x0, #PH_PARENT_PAGE]

.Lisp_child_skip:
    add w25, w25, #1
    add w26, w26, #1
    b .Lisp_child_loop

.Lisp_update_counts:
    // Old node keeps keys[0..mid-1] (mid keys)
    strh w24, [x20, #PH_NUM_KEYS]
    // New node has keys[mid+1..num_keys-1] (num_keys - mid - 1 keys)
    sub w0, w23, w24
    sub w0, w0, #1
    strh w0, [x22, #PH_NUM_KEYS]

    // Copy parent from old
    ldr w0, [x20, #PH_PARENT_PAGE]
    str w0, [x22, #PH_PARENT_PAGE]

    mov w0, w21
    b .Lisp_ret

.Lisp_fail:
    mvn w0, wzr

.Lisp_ret:
    ldp x25, x26, [sp, #64]
    ldp x23, x24, [sp, #48]
    ldp x21, x22, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #80
    ret
.size btree_int_split, .-btree_int_split

// ============================================================================
// btree_insert(db_ptr, key_ptr, val_ptr, tx_id) -> error
// Insert key-value pair into B+ tree
// x0 = db_ptr, x1 = key_ptr, x2 = val_ptr, x3 = tx_id
// Returns: x0 = ADB_OK, ADB_ERR_NOMEM, or ADB_ERR_EXISTS
// ============================================================================
.global btree_insert
.type btree_insert, %function
btree_insert:
    stp x29, x30, [sp, #-112]!
    mov x29, sp
    stp x19, x20, [sp, #16]
    stp x21, x22, [sp, #32]
    stp x23, x24, [sp, #48]
    stp x25, x26, [sp, #64]
    stp x27, x28, [sp, #80]

    mov x19, x0            // db_ptr
    mov x20, x1            // key_ptr
    mov x21, x2            // val_ptr
    mov x22, x3            // tx_id

    ldr x23, [x19, #DB_BTREE_MMAP]
    ldr x24, [x19, #DB_BTREE_ROOT]

    // If tree is empty (no root), create root leaf
    cmn w24, #1
    b.ne .Lbi_find_leaf

    // Create root leaf
    mov x0, x19
    bl btree_alloc_page
    cmn w0, #1
    b.eq .Lbi_nomem

    mov w24, w0
    str x0, [x19, #DB_BTREE_ROOT]

    // Re-read mmap (may have changed)
    ldr x23, [x19, #DB_BTREE_MMAP]

    mov x0, x23            // mmap_base
    mov w1, w24             // page_id
    bl btree_page_get_ptr
    mov x25, x0             // page_ptr

    mov x0, x25
    mov w1, w24
    bl btree_page_init_leaf

    // Insert directly into empty root leaf
    mov x0, x25
    mov w1, #0
    mov x2, x20
    mov x3, x21
    mov x4, x22
    bl btree_leaf_insert_at

    mov x0, #ADB_OK
    b .Lbi_ret

.Lbi_find_leaf:
    // Find the leaf for this key
    mov x0, x23
    mov w1, w24
    mov x2, x20
    bl btree_find_leaf
    // x0 = leaf_ptr, w1 = index, w2 = found

    cbz x0, .Lbi_nomem

    mov x25, x0            // leaf_ptr
    mov w26, w1             // insert_idx
    mov w27, w2             // found

    // If key exists, update value in place (upsert)
    cbz w27, .Lbi_check_full

    // Update existing: copy new val
    mov x0, x25
    mov w1, w26
    bl btree_leaf_get_val_ptr
    mov x1, x21
    bl neon_copy_256

    // Update txid
    add x0, x25, #BTREE_LEAF_TXIDS_OFF
    str x22, [x0, w26, uxtw #3]

    mov x0, #ADB_OK
    b .Lbi_ret

.Lbi_check_full:
    // Check if leaf has room
    ldrh w28, [x25, #PH_NUM_KEYS]
    cmp w28, #BTREE_LEAF_MAX_KEYS
    b.lo .Lbi_insert_direct

    // Leaf is full: need to split
    // Save leaf page_id before split (alloc inside split may remap)
    ldr w28, [x25, #PH_PAGE_ID]
    str w28, [sp, #96]

    mov x0, x19
    mov x1, x25
    bl btree_leaf_split
    cmn w0, #1
    b.eq .Lbi_nomem

    mov w27, w0             // new_leaf_page_id

    // Re-derive old leaf ptr from saved page_id (mmap may have moved)
    ldr x23, [x19, #DB_BTREE_MMAP]
    ldr w0, [sp, #96]       // saved page_id
    add x25, x23, x0, lsl #PAGE_SHIFT

    // Get new leaf ptr
    mov x0, x23
    mov w1, w27
    bl btree_page_get_ptr
    mov x28, x0             // new_leaf_ptr

    // Determine which leaf gets the new key
    // Compare key with first key of new (right) leaf
    mov x0, x28
    mov w1, #0
    bl btree_page_get_key_ptr
    mov x1, x0
    mov x0, x20
    bl key_compare

    cmp w0, #0
    b.lt .Lbi_insert_left

    // Key goes in right (new) leaf
    mov x0, x28
    mov x1, x20
    bl btree_page_binary_search
    mov w26, w0             // insert index in new leaf

    mov x0, x28
    mov w1, w26
    mov x2, x20
    mov x3, x21
    mov x4, x22
    bl btree_leaf_insert_at
    b .Lbi_promote

.Lbi_insert_left:
    // Key goes in left (old) leaf
    mov x0, x25
    mov x1, x20
    bl btree_page_binary_search
    mov w26, w0

    mov x0, x25
    mov w1, w26
    mov x2, x20
    mov x3, x21
    mov x4, x22
    bl btree_leaf_insert_at

.Lbi_promote:
    // Promote first key of new leaf to parent
    // promote_key = new_leaf.keys[0]
    mov x0, x28
    mov w1, #0
    bl btree_page_get_key_ptr
    mov x20, x0             // promote_key_ptr

    // Get parent page
    ldr w0, [x25, #PH_PARENT_PAGE]
    mov w26, w27             // right_child = new_leaf_page_id

    // Propagate up
    b .Lbi_propagate_up

.Lbi_insert_direct:
    // Room in leaf: insert directly
    mov x0, x25
    mov w1, w26
    mov x2, x20
    mov x3, x21
    mov x4, x22
    bl btree_leaf_insert_at

    mov x0, #ADB_OK
    b .Lbi_ret

// ============================================================================
// Propagate split upward through internal nodes
// At entry: x20 = promote_key_ptr, w26 = right_child_page_id
//           w0 = parent_page_id (from PH_PARENT_PAGE)
// ============================================================================
.Lbi_propagate_up:
    mov w24, w0             // parent_page_id

    // If no parent (root was split), create new root
    cmn w24, #1
    b.eq .Lbi_new_root

.Lbi_insert_in_parent:
    // Get parent page
    ldr x23, [x19, #DB_BTREE_MMAP]
    mov x0, x23
    mov w1, w24
    bl btree_page_get_ptr
    mov x25, x0             // parent_ptr

    // Check if parent has room
    ldrh w0, [x25, #PH_NUM_KEYS]
    cmp w0, #BTREE_INT_MAX_KEYS
    b.hs .Lbi_split_internal

    // Room in parent: find position and insert
    mov x0, x25
    mov x1, x20
    bl btree_page_binary_search
    // w0 = insert position

    mov w1, w0              // index
    mov x0, x25             // page_ptr
    mov x2, x20             // key_ptr
    mov x3, x26             // right_child (as x3)
    bl btree_int_insert_at

    // Update right child's parent (with bounds check)
    ldr x0, [x19, #DB_BTREE_NUM_PAGES]
    cmp w26, w0
    b.hs .Lbi_rchild_ok
    ldr x23, [x19, #DB_BTREE_MMAP]
    lsl x0, x26, #PAGE_SHIFT
    add x0, x23, x0
    str w24, [x0, #PH_PARENT_PAGE]

.Lbi_rchild_ok:
    mov x0, #ADB_OK
    b .Lbi_ret

.Lbi_split_internal:
    // Parent is full: split internal node
    // Stack layout: sp+0 = median key (64B), sp+64 = our promote key (64B)
    sub sp, sp, #128

    // Save our promote key at sp+64
    add x0, sp, #64
    mov x1, x20
    bl neon_copy_64

    // Split the internal node
    mov x0, x19
    mov x1, x25
    bl btree_int_split
    cmn w0, #1
    b.eq .Lbi_split_int_fail

    mov w27, w0             // new_int_page_id
    ldr x23, [x19, #DB_BTREE_MMAP]

    // Re-derive x25 (parent_ptr) - btree_int_split may have remapped mmap
    lsl x0, x24, #PAGE_SHIFT
    add x25, x23, x0

    // Save median key to sp+0 BEFORE any insertion (insertion may overwrite it)
    // The median is at old_node.keys[num_keys] (beyond valid range but data intact)
    ldrh w28, [x25, #PH_NUM_KEYS]   // mid (after split)
    mov x0, x25
    mov w1, w28
    bl btree_page_get_key_ptr
    mov x1, x0              // src = median key in old node
    mov x0, sp              // dst = median area on stack
    bl neon_copy_64

    // Compare our key (sp+64) with the median (sp)
    add x0, sp, #64         // our key
    mov x1, sp              // median key
    bl key_compare

    cmp w0, #0
    b.lt .Lbi_insert_left_int

    // Our key goes in the new (right) internal node
    ldr x23, [x19, #DB_BTREE_MMAP]
    mov x0, x23
    mov w1, w27
    bl btree_page_get_ptr
    mov x25, x0             // new_int_ptr

    mov x0, x25
    add x1, sp, #64         // our key
    bl btree_page_binary_search

    mov w1, w0
    mov x0, x25
    add x2, sp, #64         // key
    mov x3, x26             // right_child
    bl btree_int_insert_at

    // Update right child parent (with bounds check)
    ldr x0, [x19, #DB_BTREE_NUM_PAGES]
    cmp w26, w0
    b.hs .Lbi_rp_right_ok
    ldr x23, [x19, #DB_BTREE_MMAP]
    lsl x0, x26, #PAGE_SHIFT
    add x0, x23, x0
    str w27, [x0, #PH_PARENT_PAGE]
.Lbi_rp_right_ok:
    b .Lbi_promote_median

.Lbi_insert_left_int:
    // Re-fetch old_int_ptr
    ldr x23, [x19, #DB_BTREE_MMAP]
    mov x0, x23
    mov w1, w24
    bl btree_page_get_ptr
    mov x25, x0

    mov x0, x25
    add x1, sp, #64         // our key
    bl btree_page_binary_search

    mov w1, w0
    mov x0, x25
    add x2, sp, #64         // key
    mov x3, x26
    bl btree_int_insert_at

    // Bounds check right_child parent update
    ldr x0, [x19, #DB_BTREE_NUM_PAGES]
    cmp w26, w0
    b.hs .Lbi_rp_left_ok
    ldr x23, [x19, #DB_BTREE_MMAP]
    lsl x0, x26, #PAGE_SHIFT
    add x0, x23, x0
    str w24, [x0, #PH_PARENT_PAGE]
.Lbi_rp_left_ok:

.Lbi_promote_median:
    // Write median key from stack to old node's keys[num_keys] (safe, beyond valid range)
    // This ensures x20 points to stable mmap memory, not transient stack
    ldr x23, [x19, #DB_BTREE_MMAP]
    mov x0, x23
    mov w1, w24
    bl btree_page_get_ptr
    mov x28, x0             // old_int_ptr
    ldrh w1, [x28, #PH_NUM_KEYS]
    mov x0, x28
    bl btree_page_get_key_ptr
    // x0 = destination (old_node.keys[num_keys], safe location in mmap)
    mov x1, sp              // src = median on stack
    bl neon_copy_64

    // Re-read the pointer to the median (neon_copy_64 may have clobbered x0)
    ldrh w1, [x28, #PH_NUM_KEYS]
    mov x0, x28
    bl btree_page_get_key_ptr
    mov x20, x0             // x20 = promote key in mmap (stable)

    mov w26, w27             // right child = new_int_page_id

    // Get parent of the split internal node
    ldr w24, [x28, #PH_PARENT_PAGE]

    add sp, sp, #128         // restore stack (both key saves)

    cmn w24, #1
    b.eq .Lbi_new_root
    b .Lbi_insert_in_parent

.Lbi_split_int_fail:
    add sp, sp, #128
    mov x0, #ADB_ERR_NOMEM
    b .Lbi_ret

.Lbi_new_root:
    // Save promote key to stack before alloc (alloc may remap, invalidating x20)
    sub sp, sp, #64
    mov x0, sp
    mov x1, x20
    bl neon_copy_64

    // Create a new root internal node
    mov x0, x19
    bl btree_alloc_page
    cmn w0, #1
    b.eq .Lbi_new_root_fail

    mov w27, w0              // new_root_page_id

    ldr x23, [x19, #DB_BTREE_MMAP]
    mov x0, x23
    mov w1, w27
    bl btree_page_get_ptr
    mov x28, x0              // new_root_ptr

    mov x0, x28
    mov w1, w27
    bl btree_page_init_internal

    // Set the promote key at index 0 from stack copy
    mov x0, x28
    mov w1, #0
    bl btree_page_get_key_ptr
    mov x1, sp               // promote key saved on stack
    bl neon_copy_64

    // Left child = old root (or left split node)
    // We need to figure out left child page_id
    // For leaf split: left = old leaf page_id
    // For internal split propagation: left = w24 (the parent that was split)
    // Actually at this point w24 = INVALID_PAGE (no parent)
    // The left child is the page that was already root
    ldr x0, [x19, #DB_BTREE_ROOT]
    add x1, x28, #BTREE_INT_CHILDREN_OFF
    str x0, [x1]             // children[0] = old root

    // Right child = w26
    str x26, [x1, #8]        // children[1] = right child

    // Set num_keys = 1
    mov w0, #1
    strh w0, [x28, #PH_NUM_KEYS]

    // Update children's parent pointers (with bounds checks)
    ldr x23, [x19, #DB_BTREE_MMAP]
    ldr x3, [x19, #DB_BTREE_NUM_PAGES]

    ldr x0, [x19, #DB_BTREE_ROOT]
    cmp x0, x3
    b.hs .Lbi_nr_skip_left
    add x1, x23, x0, lsl #PAGE_SHIFT
    str w27, [x1, #PH_PARENT_PAGE]
.Lbi_nr_skip_left:

    cmp x26, x3
    b.hs .Lbi_nr_skip_right
    lsl x1, x26, #PAGE_SHIFT
    add x1, x23, x1
    str w27, [x1, #PH_PARENT_PAGE]
.Lbi_nr_skip_right:

    // Update root
    mov w0, w27
    str x0, [x19, #DB_BTREE_ROOT]

    add sp, sp, #64          // restore stack (promote key save)
    mov x0, #ADB_OK
    b .Lbi_ret

.Lbi_new_root_fail:
    add sp, sp, #64          // restore stack (promote key save)

.Lbi_nomem:
    mov x0, #ADB_ERR_NOMEM

.Lbi_ret:
    ldp x27, x28, [sp, #80]
    ldp x25, x26, [sp, #64]
    ldp x23, x24, [sp, #48]
    ldp x21, x22, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #112
    ret
.size btree_insert, .-btree_insert
