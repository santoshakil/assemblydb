// AssemblyDB - B+ Tree Delete Operations
// Delete key from leaf, accept underfilled leaves (no merge/redistribute)

.include "src/const.s"

.text

// ============================================================================
// btree_delete(db_ptr, key_ptr) -> error
// Delete a key from the B+ tree
// x0 = db_ptr, x1 = key_ptr
// Returns: x0 = ADB_OK or ADB_ERR_NOT_FOUND
// ============================================================================
.global btree_delete
.type btree_delete, %function
btree_delete:
    stp x29, x30, [sp, #-64]!
    mov x29, sp
    stp x19, x20, [sp, #16]
    stp x21, x22, [sp, #32]
    stp x23, x24, [sp, #48]

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
    ldrh w0, [x23, #PH_NUM_KEYS]
    sub w0, w0, #1          // new num_keys

    // Entries after delete_index need to shift left
    sub w22, w0, w24        // count = (num_keys-1) - delete_index
    cmp w22, #0
    b.le .Lbd_no_shift

    mov x0, x23
    mov w1, w24
    mov w2, w22             // explicit count arg
    bl btree_leaf_shift_left

.Lbd_no_shift:
    ldrh w0, [x23, #PH_NUM_KEYS]
    sub w0, w0, #1
    strh w0, [x23, #PH_NUM_KEYS]

    // Accept underfilled leaves - no merge/redistribute needed.
    // The tree remains structurally valid and searchable.
    mov x0, #ADB_OK
    b .Lbd_ret

.Lbd_not_found:
    mov x0, #ADB_ERR_NOT_FOUND

.Lbd_ret:
    ldp x23, x24, [sp, #48]
    ldp x21, x22, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #64
    ret
.size btree_delete, .-btree_delete
