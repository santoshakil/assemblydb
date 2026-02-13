// AssemblyDB - MemTable (Skip List Implementation)
// Lock-free read, serialized write skip list for LSM-tree memtable
// Uses arena allocator for fast node allocation

.include "src/const.s"

.text

// ============================================================================
// memtable_create(arena_ptr) -> skip_list_head_ptr or NULL
// Create a new memtable backed by the given arena
// x0 = arena_ptr
// Returns: x0 = pointer to skip_list_head, or NULL on failure
// ============================================================================
.global memtable_create
.type memtable_create, %function
memtable_create:
    b memtable_create2
.size memtable_create, .-memtable_create

// ============================================================================
// memtable_create2(arena_ptr) -> skip_list_head_ptr or NULL
// Cleaner version
// ============================================================================
.global memtable_create2
.type memtable_create2, %function
memtable_create2:
    stp x29, x30, [sp, #-48]!
    mov x29, sp
    stp x19, x20, [sp, #16]
    str x21, [sp, #32]

    mov x19, x0                    // arena_ptr

    // Allocate skip list head from arena
    mov x0, x19
    mov x1, #SLH_SIZE
    bl arena_alloc
    cbz x0, .Lmc2_fail

    mov x20, x0                    // head_ptr

    // Zero the head struct
    mov x0, x20
    mov x1, #SLH_SIZE
    bl neon_memzero

    // Init fields
    str xzr, [x20, #SLH_ENTRY_COUNT]
    mov x1, #1
    str x1, [x20, #SLH_MAX_HEIGHT]
    str xzr, [x20, #SLH_DATA_SIZE]
    str x19, [x20, #SLH_ARENA_PTR]

    mov x0, x20
    b .Lmc2_ret

.Lmc2_fail:
    mov x0, #0

.Lmc2_ret:
    ldr x21, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #48
    ret
.size memtable_create2, .-memtable_create2

// Redirect memtable_create -> memtable_create2 by making original jump here

// ============================================================================
// sl_alloc_node(head, height) -> node_ptr or NULL
// Allocate a skip list node with the given height
// x0 = head_ptr (skip list), w1 = height
// Returns: x0 = node_ptr (zeroed), or NULL
// ============================================================================
.global sl_alloc_node
.type sl_alloc_node, %function
sl_alloc_node:
    stp x29, x30, [sp, #-32]!
    mov x29, sp
    stp x19, x20, [sp, #16]

    mov x19, x0                    // head
    mov w20, w1                     // height

    // Node size = SLN_BASE_SIZE + height * 8
    mov x1, #SLN_BASE_SIZE
    add x1, x1, w20, uxtw #3      // + height * 8

    // Allocate from arena
    ldr x0, [x19, #SLH_ARENA_PTR]
    bl arena_alloc
    cbz x0, .Lsan_ret

    // Zero the node
    mov x19, x0                    // save node ptr
    mov x1, #SLN_BASE_SIZE
    add x1, x1, w20, uxtw #3
    bl neon_memzero
    mov x0, x19

.Lsan_ret:
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #32
    ret
.size sl_alloc_node, .-sl_alloc_node

// ============================================================================
// memtable_put(head, key_ptr, val_ptr, is_delete) -> 0=ok, -1=fail
// Insert or update a key in the skip list
// x0 = head_ptr, x1 = key_ptr, x2 = val_ptr, w3 = is_delete (0 or 1)
// ============================================================================
.global memtable_put
.type memtable_put, %function
memtable_put:
    stp x29, x30, [sp, #-112]!
    mov x29, sp
    stp x19, x20, [sp, #16]
    stp x21, x22, [sp, #32]
    stp x23, x24, [sp, #48]
    stp x25, x26, [sp, #64]
    stp x27, x28, [sp, #80]

    mov x19, x0                    // head
    mov x20, x1                    // key_ptr
    mov x21, x2                    // val_ptr
    mov w22, w3                     // is_delete

    // update[0..MAX_SKIP_HEIGHT-1] on stack (20 * 8 = 160 bytes)
    sub sp, sp, #160

    // Find position: traverse from top level down
    ldr x23, [x19, #SLH_MAX_HEIGHT] // current_height
    sub w24, w23, #1                // level = height - 1
    add x25, x19, #SLH_FORWARD     // x = head (use head's forward array)
    mov x26, x19                    // x_node = head (the node whose forward we follow)

.Lmp_level_loop:
    cmp w24, #0
    b.lt .Lmp_level_done

.Lmp_forward_loop:
    // Get x->forward[level]
    add x0, x26, #SLH_FORWARD
    // If x_node == head, use SLH_FORWARD, else use SLN_FORWARD
    cmp x26, x19
    b.ne .Lmp_use_sln_fwd
    ldr x27, [x19, #SLH_FORWARD + 0]  // won't work for variable level
    // Need level*8 offset
    add x0, x19, #SLH_FORWARD
    ldr x27, [x0, w24, uxtw #3]
    b .Lmp_check_fwd

.Lmp_use_sln_fwd:
    add x0, x26, #SLN_FORWARD
    ldr x27, [x0, w24, uxtw #3]

.Lmp_check_fwd:
    // x27 = forward node at this level
    cbz x27, .Lmp_save_update

    // Compare forward node's key with our key
    mov x0, x20                    // our key
    add x1, x27, #SLN_KEY_LEN     // forward node key (key starts at SLN_KEY_LEN)
    // Actually key format: SLN_KEY_LEN (2B) + SLN_KEY_DATA (62B) = same as our 64B key format
    mov x1, x27                    // node starts with key_len at offset 0
    bl key_compare
    cmp w0, #0
    b.le .Lmp_save_update          // forward >= our key, stop

    // Move forward
    mov x26, x27
    b .Lmp_forward_loop

.Lmp_save_update:
    // update[level] = current node
    str x26, [sp, w24, uxtw #3]
    sub w24, w24, #1
    b .Lmp_level_loop

.Lmp_level_done:
    // Check if key already exists at level 0
    cmp x26, x19
    b.ne .Lmp_get_fwd0_node
    // x26 is head
    ldr x27, [x19, #SLH_FORWARD]
    b .Lmp_check_existing

.Lmp_get_fwd0_node:
    ldr x27, [x26, #SLN_FORWARD]

.Lmp_check_existing:
    cbz x27, .Lmp_new_node

    // Compare x27->key with our key
    mov x0, x20
    mov x1, x27
    bl key_compare
    cbnz w0, .Lmp_new_node

    // Key exists: update value in place
    add x0, x27, #SLN_VAL_LEN
    mov x1, x21
    bl neon_copy_256               // overwrite val

    strb w22, [x27, #SLN_IS_DELETED]

    add sp, sp, #160
    mov x0, #0
    b .Lmp_ret

.Lmp_new_node:
    // Generate random height
    ldr x0, [x19, #SLH_ARENA_PTR]  // use arena ptr as seed source
    bl prng_next
    // Use result to determine level
    mov x0, x0                      // random value in x0
    bl random_level
    mov w23, w0                     // new_height

    // If new_height > current max, extend update[] to point to head
    ldr x24, [x19, #SLH_MAX_HEIGHT]
    cmp w23, w24
    b.le .Lmp_alloc_node

    mov w25, w24                    // i = old max_height
.Lmp_extend:
    cmp w25, w23
    b.ge .Lmp_update_max
    str x19, [sp, w25, uxtw #3]   // update[i] = head
    add w25, w25, #1
    b .Lmp_extend

.Lmp_update_max:
    str x23, [x19, #SLH_MAX_HEIGHT]

.Lmp_alloc_node:
    mov x0, x19
    mov w1, w23
    bl sl_alloc_node
    cbz x0, .Lmp_fail
    mov x25, x0                    // new_node

    // Copy key into node
    mov x0, x25                    // dst = node (key_len is at offset 0)
    mov x1, x20                    // src = key_ptr
    bl neon_copy_64

    // Copy val into node
    add x0, x25, #SLN_VAL_LEN
    mov x1, x21
    bl neon_copy_256

    // Set height and is_deleted
    strb w23, [x25, #SLN_HEIGHT]
    strb w22, [x25, #SLN_IS_DELETED]

    // Link node at each level
    mov w24, #0                     // level = 0
.Lmp_link_loop:
    cmp w24, w23
    b.ge .Lmp_link_done

    ldr x26, [sp, w24, uxtw #3]   // update[level]

    // new_node->forward[level] = update[level]->forward[level]
    cmp x26, x19
    b.ne .Lmp_link_from_node

    // update is head
    add x0, x19, #SLH_FORWARD
    ldr x27, [x0, w24, uxtw #3]
    add x1, x25, #SLN_FORWARD
    str x27, [x1, w24, uxtw #3]
    // head->forward[level] = new_node
    str x25, [x0, w24, uxtw #3]
    b .Lmp_link_next

.Lmp_link_from_node:
    add x0, x26, #SLN_FORWARD
    ldr x27, [x0, w24, uxtw #3]
    add x1, x25, #SLN_FORWARD
    str x27, [x1, w24, uxtw #3]
    str x25, [x0, w24, uxtw #3]

.Lmp_link_next:
    add w24, w24, #1
    b .Lmp_link_loop

.Lmp_link_done:
    // Update entry count
    ldr x0, [x19, #SLH_ENTRY_COUNT]
    add x0, x0, #1
    str x0, [x19, #SLH_ENTRY_COUNT]

    // Update data size (key=64 + val=256 + node overhead)
    ldr x0, [x19, #SLH_DATA_SIZE]
    mov x1, #SLN_BASE_SIZE
    add x1, x1, w23, uxtw #3
    add x0, x0, x1
    str x0, [x19, #SLH_DATA_SIZE]

    add sp, sp, #160
    mov x0, #0
    b .Lmp_ret

.Lmp_fail:
    add sp, sp, #160
    mov x0, #-1

.Lmp_ret:
    ldp x27, x28, [sp, #80]
    ldp x25, x26, [sp, #64]
    ldp x23, x24, [sp, #48]
    ldp x21, x22, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #112
    ret
.size memtable_put, .-memtable_put

// ============================================================================
// memtable_get(head, key_ptr, val_buf) -> 0=found, 1=not_found
// Look up a key in the skip list
// x0 = head_ptr, x1 = key_ptr, x2 = val_buf (256 bytes)
// Returns: x0 = 0 if found (val copied), 1 if not found
// ============================================================================
.global memtable_get
.type memtable_get, %function
memtable_get:
    stp x29, x30, [sp, #-64]!
    mov x29, sp
    stp x19, x20, [sp, #16]
    stp x21, x22, [sp, #32]
    stp x23, x24, [sp, #48]

    mov x19, x0                    // head
    mov x20, x1                    // key_ptr
    mov x21, x2                    // val_buf

    ldr x22, [x19, #SLH_MAX_HEIGHT]
    sub w23, w22, #1               // level = height - 1
    mov x24, x19                    // current = head

.Lmg_level:
    cmp w23, #0
    b.lt .Lmg_check

.Lmg_forward:
    // Get current->forward[level]
    cmp x24, x19
    b.ne .Lmg_node_fwd
    add x0, x19, #SLH_FORWARD
    ldr x22, [x0, w23, uxtw #3]
    b .Lmg_compare

.Lmg_node_fwd:
    add x0, x24, #SLN_FORWARD
    ldr x22, [x0, w23, uxtw #3]

.Lmg_compare:
    cbz x22, .Lmg_down

    mov x0, x20                    // our key
    mov x1, x22                    // node key
    bl key_compare
    cmp w0, #0
    b.le .Lmg_down                 // node >= our key

    mov x24, x22                   // advance
    b .Lmg_forward

.Lmg_down:
    sub w23, w23, #1
    b .Lmg_level

.Lmg_check:
    // Check forward[0] of current
    cmp x24, x19
    b.ne .Lmg_node_fwd0
    ldr x22, [x19, #SLH_FORWARD]
    b .Lmg_final

.Lmg_node_fwd0:
    ldr x22, [x24, #SLN_FORWARD]

.Lmg_final:
    cbz x22, .Lmg_not_found

    // Compare
    mov x0, x20
    mov x1, x22
    bl key_compare
    cbnz w0, .Lmg_not_found

    // Check if deleted
    ldrb w0, [x22, #SLN_IS_DELETED]
    cbnz w0, .Lmg_not_found

    // Found: copy val
    mov x0, x21                    // dst = val_buf
    add x1, x22, #SLN_VAL_LEN     // src = node val area
    bl neon_copy_256

    mov x0, #0
    b .Lmg_ret

.Lmg_not_found:
    mov x0, #ADB_ERR_NOT_FOUND

.Lmg_ret:
    ldp x23, x24, [sp, #48]
    ldp x21, x22, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #64
    ret
.size memtable_get, .-memtable_get

// ============================================================================
// memtable_delete(head, key_ptr) -> 0=ok, 1=not_found
// Mark a key as deleted (tombstone)
// x0 = head_ptr, x1 = key_ptr
// ============================================================================
.global memtable_delete
.type memtable_delete, %function
memtable_delete:
    b memtable_delete2
.size memtable_delete, .-memtable_delete

// ============================================================================
// memtable_delete2(head, key_ptr) -> 0=ok, 1=not_found
// ============================================================================
.global memtable_delete2
.type memtable_delete2, %function
memtable_delete2:
    stp x29, x30, [sp, #-48]!
    mov x29, sp
    stp x19, x20, [sp, #16]
    str x21, [sp, #32]

    mov x19, x0                    // head
    mov x20, x1                    // key

    // Create zero val on stack
    sub sp, sp, #256
    mov x0, sp
    mov x1, #256
    bl neon_memzero

    // Call put with is_delete=1
    mov x0, x19
    mov x1, x20
    mov x2, sp                     // zero val
    mov w3, #1                     // is_delete = true
    bl memtable_put

    add sp, sp, #256

    ldr x21, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #48
    ret
.size memtable_delete2, .-memtable_delete2

// ============================================================================
// memtable_iter_first(head) -> node_ptr or NULL
// Return the first (smallest key) node in the skip list
// x0 = head_ptr
// ============================================================================
.global memtable_iter_first
.type memtable_iter_first, %function
memtable_iter_first:
    ldr x0, [x0, #SLH_FORWARD]    // head->forward[0] = first node
    ret
.size memtable_iter_first, .-memtable_iter_first

// ============================================================================
// memtable_iter_next(node_ptr) -> next_node_ptr or NULL
// Return the next node in sorted order
// x0 = current node_ptr
// ============================================================================
.global memtable_iter_next
.type memtable_iter_next, %function
memtable_iter_next:
    cbz x0, .Lmin_null
    ldr x0, [x0, #SLN_FORWARD]    // node->forward[0] = next node
.Lmin_null:
    ret
.size memtable_iter_next, .-memtable_iter_next

// ============================================================================
// memtable_entry_count(head) -> count
// x0 = head_ptr
// Returns: x0 = number of entries
// ============================================================================
.global memtable_entry_count
.type memtable_entry_count, %function
memtable_entry_count:
    ldr x0, [x0, #SLH_ENTRY_COUNT]
    ret
.size memtable_entry_count, .-memtable_entry_count

// ============================================================================
// memtable_data_size(head) -> bytes
// x0 = head_ptr
// Returns: x0 = approximate data size in bytes
// ============================================================================
.global memtable_data_size
.type memtable_data_size, %function
memtable_data_size:
    ldr x0, [x0, #SLH_DATA_SIZE]
    ret
.size memtable_data_size, .-memtable_data_size
