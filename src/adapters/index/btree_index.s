// AssemblyDB - Secondary Index (B+ Tree Based)
// Maps secondary key -> primary key using a separate B+ tree
// Index stored as a separate file per index
//
// Index descriptor (128 bytes):
//   0x00: fd (file descriptor)
//   0x08: mmap_base
//   0x10: mmap_len
//   0x18: root_page
//   0x20: num_pages
//   0x28: file_capacity
//   0x30: name_ptr (index name, null-terminated)
//   0x38: name_len
//   0x40: db_dir_fd
//   0x48-0x7F: reserved

.include "src/const.s"

.text

.equ IDX_FD,           0x00
.equ IDX_MMAP,         0x08
.equ IDX_MMAP_LEN,     0x10
.equ IDX_ROOT,         0x18
.equ IDX_NUM_PAGES,    0x20
.equ IDX_CAPACITY,     0x28
.equ IDX_NAME_PTR,     0x30
.equ IDX_NAME_LEN,     0x38
.equ IDX_DIR_FD,       0x40
.equ IDX_SIZE,         0x80

// ============================================================================
// sec_index_create(dir_fd, name, name_len) -> index_ptr or NULL
// Create a new secondary index
// x0 = dir_fd, x1 = name, x2 = name_len
// ============================================================================
.global sec_index_create
.type sec_index_create, %function
sec_index_create:
    stp x29, x30, [sp, #-64]!
    mov x29, sp
    stp x19, x20, [sp, #16]
    stp x21, x22, [sp, #32]
    str x23, [sp, #48]

    mov w19, w0                    // dir_fd
    mov x20, x1                    // name
    mov x21, x2                    // name_len

    // Allocate index descriptor
    mov x0, #IDX_SIZE
    bl alloc_zeroed
    cbz x0, .Lsic_fail
    mov x22, x0                    // index ptr

    // Store metadata
    str w19, [x22, #IDX_DIR_FD]
    str x20, [x22, #IDX_NAME_PTR]
    str x21, [x22, #IDX_NAME_LEN]

    // Build index filename: "idx_<name>.btree"
    sub sp, sp, #256
    mov x0, sp
    mov x1, x20
    mov x2, x21
    bl build_index_filename        // writes to [sp], returns len in x0
    mov x23, x0                    // filename length

    // Open/create the index file
    mov w0, w19                    // dir_fd
    mov x1, sp                     // filename
    mov w2, #(O_RDWR | O_CREAT)
    movz w3, #0644, lsl #0
    bl sys_openat
    add sp, sp, #256

    cmn x0, #4096
    b.hi .Lsic_fail_free

    str w0, [x22, #IDX_FD]

    // Initialize: allocate 16 pages
    mov x0, #16
    mov x1, #PAGE_SIZE
    mul x0, x0, x1
    ldr w1, [x22, #IDX_FD]
    bl ftruncate_file

    // mmap the file
    ldr w0, [x22, #IDX_FD]
    mov x1, #16
    mov x2, #PAGE_SIZE
    mul x1, x1, x2
    str x1, [x22, #IDX_MMAP_LEN]
    mov x2, #PROT_RW
    mov x3, #MAP_SHARED
    bl mmap_file

    cmn x0, #4096
    b.hi .Lsic_fail_close
    str x0, [x22, #IDX_MMAP]

    // Initialize root page as empty leaf
    mov w1, #PAGE_TYPE_LEAF
    strh w1, [x0, #PH_PAGE_TYPE]
    str wzr, [x0, #PH_NUM_KEYS]
    mov w1, #INVALID_PAGE
    str w1, [x0, #PH_NEXT_PAGE]
    str w1, [x0, #PH_PREV_PAGE]

    str xzr, [x22, #IDX_ROOT]     // root = page 0
    mov x0, #1
    str x0, [x22, #IDX_NUM_PAGES]
    mov x0, #16
    str x0, [x22, #IDX_CAPACITY]

    mov x0, x22
    b .Lsic_ret

.Lsic_fail_close:
    ldr w0, [x22, #IDX_FD]
    bl sys_close

.Lsic_fail_free:
    mov x0, x22
    mov x1, #IDX_SIZE
    bl free_mem

.Lsic_fail:
    mov x0, #0

.Lsic_ret:
    ldr x23, [sp, #48]
    ldp x21, x22, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #64
    ret
.size sec_index_create, .-sec_index_create

// ============================================================================
// sec_index_destroy(index_ptr) -> void
// Close and free a secondary index
// ============================================================================
.global sec_index_destroy
.type sec_index_destroy, %function
sec_index_destroy:
    stp x29, x30, [sp, #-32]!
    mov x29, sp
    str x19, [sp, #16]

    cbz x0, .Lsid_done
    mov x19, x0

    // munmap
    ldr x0, [x19, #IDX_MMAP]
    ldr x1, [x19, #IDX_MMAP_LEN]
    cbz x0, .Lsid_close
    bl sys_munmap

.Lsid_close:
    ldr w0, [x19, #IDX_FD]
    cmp w0, #0
    b.le .Lsid_free
    bl sys_close

.Lsid_free:
    mov x0, x19
    mov x1, #IDX_SIZE
    bl free_mem

.Lsid_done:
    ldr x19, [sp, #16]
    ldp x29, x30, [sp], #32
    ret
.size sec_index_destroy, .-sec_index_destroy

// ============================================================================
// sec_index_insert(index, secondary_key, primary_key) -> err
// Insert a secondary->primary key mapping
// x0 = index_ptr, x1 = secondary key (64B), x2 = primary key stored as value (256B)
// Uses the existing B+ tree insert with the index's own mmap
// ============================================================================
.global sec_index_insert
.type sec_index_insert, %function
sec_index_insert:
    stp x29, x30, [sp, #-48]!
    mov x29, sp
    stp x19, x20, [sp, #16]
    stp x21, x22, [sp, #32]

    mov x19, x0                    // index
    mov x20, x1                    // sec key
    mov x21, x2                    // pri key (as value)

    // We reuse btree_insert with the index's B+ tree pages
    // btree_insert needs: mmap_base, root_page, num_pages, key, val
    // and updates root/num_pages

    // For now, do a simple linear scan insertion (simplified index)
    // A full implementation would use btree_insert with the index's mmap

    // Simplified: just store the mapping in the leaf pages sequentially
    ldr x0, [x19, #IDX_MMAP]
    ldr x1, [x19, #IDX_ROOT]
    lsl x1, x1, #PAGE_SHIFT
    add x0, x0, x1                 // root page ptr

    ldrh w2, [x0, #PH_NUM_KEYS]
    cmp w2, #BTREE_LEAF_MAX_KEYS
    b.ge .Lsii_full

    // Insert at position num_keys
    // key[i] = base + 0x040 + i*64
    mov x3, #64
    mul x3, x3, x2                 // offset for key slot
    add x3, x3, #BTREE_KEYS_OFFSET
    add x4, x0, x3                 // key slot ptr

    // Copy secondary key
    mov x0, x4
    mov x1, x20
    bl neon_copy_64

    // Copy primary key as value
    // val[i] = base + BTREE_LEAF_VALS_OFF + i*256
    ldr x0, [x19, #IDX_MMAP]
    ldr x1, [x19, #IDX_ROOT]
    lsl x1, x1, #PAGE_SHIFT
    add x0, x0, x1
    ldrh w2, [x0, #PH_NUM_KEYS]
    mov x3, #256
    mul x3, x3, x2
    add x3, x3, #BTREE_LEAF_VALS_OFF
    add x4, x0, x3

    mov x0, x4
    mov x1, x21
    bl neon_copy_256

    // Increment num_keys
    ldr x0, [x19, #IDX_MMAP]
    ldr x1, [x19, #IDX_ROOT]
    lsl x1, x1, #PAGE_SHIFT
    add x0, x0, x1
    ldrh w2, [x0, #PH_NUM_KEYS]
    add w2, w2, #1
    strh w2, [x0, #PH_NUM_KEYS]

    mov x0, #ADB_OK
    b .Lsii_ret

.Lsii_full:
    mov x0, #ADB_ERR_FULL

.Lsii_ret:
    ldp x21, x22, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #48
    ret
.size sec_index_insert, .-sec_index_insert

// ============================================================================
// sec_index_scan(index, secondary_key, callback, ctx) -> err
// Find all primary keys matching a secondary key
// x0 = index, x1 = secondary_key, x2 = callback, x3 = ctx
// ============================================================================
.global sec_index_scan
.type sec_index_scan, %function
sec_index_scan:
    stp x29, x30, [sp, #-80]!
    mov x29, sp
    stp x19, x20, [sp, #16]
    stp x21, x22, [sp, #32]
    stp x23, x24, [sp, #48]
    str x25, [sp, #64]

    mov x19, x0                    // index
    mov x20, x1                    // search key
    mov x21, x2                    // callback
    mov x22, x3                    // ctx

    ldr x0, [x19, #IDX_MMAP]
    ldr x1, [x19, #IDX_ROOT]
    lsl x1, x1, #PAGE_SHIFT
    add x23, x0, x1               // root page ptr

    ldrh w24, [x23, #PH_NUM_KEYS]
    mov w25, #0                     // loop counter (callee-saved)

.Lsis_loop:
    cmp w25, w24
    b.ge .Lsis_done

    // Get key[i]
    mov x1, #64
    mul x1, x1, x25
    add x1, x1, #BTREE_KEYS_OFFSET
    add x1, x23, x1               // key ptr

    // Compare with search key
    mov x0, x1
    mov x1, x20
    bl key_compare
    cbnz x0, .Lsis_next

    // Match found - get value (primary key)
    mov x1, #256
    mul x1, x1, x25
    add x1, x1, #BTREE_LEAF_VALS_OFF
    add x1, x23, x1               // val ptr

    // callback(sec_key, sec_klen, pri_data, pri_klen, ctx)
    ldrh w5, [x1]                  // primary key len
    add x4, x1, #2                // primary key data
    mov x0, x20                    // secondary key ptr
    ldrh w1, [x20]                 // secondary key len
    mov x2, x4                     // primary key data
    mov w3, w5                     // primary key len
    mov x4, x22                    // user ctx
    blr x21

    // If callback returns non-zero, stop
    cbnz x0, .Lsis_done

.Lsis_next:
    add w25, w25, #1
    b .Lsis_loop

.Lsis_done:
    mov x0, #ADB_OK
    ldr x25, [sp, #64]
    ldp x23, x24, [sp, #48]
    ldp x21, x22, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #80
    ret
.size sec_index_scan, .-sec_index_scan

// ============================================================================
// build_index_filename(buf, name, name_len) -> len
// Builds "idx_<name>.dat" in buf
// x0 = buf, x1 = name, x2 = name_len
// ============================================================================
.global build_index_filename
.type build_index_filename, %function
build_index_filename:
    mov x3, x0                    // save buf start
    // Write "idx_"
    mov w4, #'i'
    strb w4, [x0], #1
    mov w4, #'d'
    strb w4, [x0], #1
    mov w4, #'x'
    strb w4, [x0], #1
    mov w4, #'_'
    strb w4, [x0], #1

    // Copy name
    cbz x2, .Lbif_suffix
.Lbif_copy:
    ldrb w4, [x1], #1
    strb w4, [x0], #1
    subs x2, x2, #1
    b.ne .Lbif_copy

.Lbif_suffix:
    // Write ".dat\0"
    mov w4, #'.'
    strb w4, [x0], #1
    mov w4, #'d'
    strb w4, [x0], #1
    mov w4, #'a'
    strb w4, [x0], #1
    mov w4, #'t'
    strb w4, [x0], #1
    strb wzr, [x0]

    sub x0, x0, x3                 // return length (not including null)
    ret
.size build_index_filename, .-build_index_filename

// ============================================================================
// ftruncate_file(size, fd) -> err
// ============================================================================
ftruncate_file:
    // x0 = size, w1 = fd
    mov x2, x0
    mov w0, w1
    mov x1, x2
    mov x8, #SYS_ftruncate
    svc #0
    ret
.size ftruncate_file, .-ftruncate_file

// ============================================================================
// mmap_file(fd, length, prot, flags) -> ptr
// ============================================================================
mmap_file:
    // w0 = fd, x1 = length, x2 = prot, x3 = flags
    mov w4, w0                     // fd
    mov x5, x1                     // length
    mov x0, #0                     // addr = NULL
    mov x1, x5                     // length
    // x2 = prot (already set)
    // x3 = flags (already set)
    mov w4, w4                     // fd (reuse saved)

    // Actually need to redo registers for mmap syscall
    // mmap(addr, length, prot, flags, fd, offset)
    stp x29, x30, [sp, #-16]!
    mov x29, sp

    mov x8, #SYS_mmap
    mov x0, #0                     // addr
    // x1 = length (already)
    // x2 = prot (already)
    // x3 = flags (already)
    // x4 = fd
    mov x5, #0                     // offset
    svc #0

    ldp x29, x30, [sp], #16
    ret
.size mmap_file, .-mmap_file

.hidden alloc_zeroed
.hidden free_mem
.hidden neon_copy_64
.hidden neon_copy_256
.hidden key_compare
.hidden sys_openat
.hidden sys_close
.hidden sys_munmap
