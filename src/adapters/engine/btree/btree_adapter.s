// AssemblyDB - B+ Tree Storage Port Adapter
// Populates the storage_port vtable with B+ tree implementations
// Manages B+ tree file lifecycle (open/create/close)

.include "src/const.s"

.text

// Internal adapter functions - hidden from shared library export
.hidden btree_adapter_put
.hidden btree_adapter_get
.hidden btree_adapter_delete
.hidden btree_adapter_scan
.hidden btree_adapter_flush
.hidden btree_adapter_sync
.hidden btree_adapter_close

// ============================================================================
// btree_adapter_init(db_ptr, dir_fd) -> error
// Open or create the B+ tree data file, mmap it, wire vtable
// x0 = db_ptr, w1 = dir_fd
// Returns: x0 = ADB_OK or error
// ============================================================================
.global btree_adapter_init
.type btree_adapter_init, %function
btree_adapter_init:
    stp x29, x30, [sp, #-64]!
    mov x29, sp
    stp x19, x20, [sp, #16]
    stp x21, x22, [sp, #32]
    str x23, [sp, #48]

    mov x19, x0            // db_ptr
    mov w20, w1             // dir_fd

    // Open/create btree data file
    mov w0, w20             // dir_fd
    adrp x1, .Lbtree_filename
    add x1, x1, :lo12:.Lbtree_filename
    mov w2, #(O_RDWR | O_CREAT)
    mov w3, #MODE_FILE
    bl sys_openat
    cmp x0, #0
    b.lt .Lba_init_fail

    mov w21, w0             // btree_fd
    str x0, [x19, #DB_BTREE_FD]

    // Check file size to determine if new
    mov x0, x21
    mov x1, #0
    mov w2, #SEEK_END
    bl sys_lseek
    mov x22, x0             // file_size

    // If empty, initialize with header page (page 0 = metadata)
    cbnz x22, .Lba_existing

    // New file: allocate initial 64 pages
    mov x23, #64
    lsl x1, x23, #PAGE_SHIFT
    mov x0, x21
    bl sys_ftruncate
    cmp x0, #0
    b.lt .Lba_init_fail_close

    // mmap the file
    mov x0, x21
    lsl x1, x23, #PAGE_SHIFT
    bl mmap_file_rw
    cmn x0, #4096
    b.hi .Lba_init_fail_close

    str x0, [x19, #DB_BTREE_MMAP]
    lsl x1, x23, #PAGE_SHIFT
    str x1, [x19, #DB_BTREE_MMAP_LEN]
    str x23, [x19, #DB_BTREE_CAPACITY]

    // Page 0 is reserved metadata, first usable page is 1
    mov x0, #1
    str x0, [x19, #DB_BTREE_NUM_PAGES]

    // No root yet
    mvn w0, wzr
    str x0, [x19, #DB_BTREE_ROOT]

    // Write initial metadata to page 0
    ldr x0, [x19, #DB_BTREE_MMAP]
    movz x1, #0x4241              // "AB"
    movk x1, #0x5444, lsl #16    // "DT"
    movk x1, #0x4552, lsl #32    // "RE"
    movk x1, #0x4545, lsl #48    // "EE"
    str x1, [x0, #0]             // magic
    mvn x1, xzr
    str x1, [x0, #8]             // root = INVALID
    ldr x1, [x19, #DB_BTREE_NUM_PAGES]
    str x1, [x0, #16]            // num_pages

    b .Lba_wire_vtable

.Lba_existing:
    // Existing file: calculate pages
    lsr x23, x22, #PAGE_SHIFT   // num_pages = file_size / PAGE_SIZE
    str x23, [x19, #DB_BTREE_CAPACITY]

    // mmap existing file
    mov x0, x21
    mov x1, x22
    bl mmap_file_rw
    cmn x0, #4096
    b.hi .Lba_init_fail_close

    str x0, [x19, #DB_BTREE_MMAP]
    str x22, [x19, #DB_BTREE_MMAP_LEN]

    // Read metadata from page 0
    // offset 8 = root_page, offset 16 = num_pages
    ldr x1, [x0, #8]              // root_page
    str x1, [x19, #DB_BTREE_ROOT]
    ldr x1, [x0, #16]             // num_pages
    cbz x1, .Lba_use_file_pages
    str x1, [x19, #DB_BTREE_NUM_PAGES]
    b .Lba_wire_vtable

.Lba_use_file_pages:
    // Metadata not written yet, use file-based count
    str x23, [x19, #DB_BTREE_NUM_PAGES]

.Lba_wire_vtable:
    // Allocate storage_port vtable (64 bytes)
    mov x0, #STORAGE_PORT_SIZE
    bl alloc_zeroed
    cbz x0, .Lba_init_fail_close

    // Populate vtable (PIC-safe: adrp + add)
    adrp x1, btree_adapter_put
    add x1, x1, :lo12:btree_adapter_put
    str x1, [x0, #SP_FN_PUT]
    adrp x1, btree_adapter_get
    add x1, x1, :lo12:btree_adapter_get
    str x1, [x0, #SP_FN_GET]
    adrp x1, btree_adapter_delete
    add x1, x1, :lo12:btree_adapter_delete
    str x1, [x0, #SP_FN_DELETE]
    adrp x1, btree_adapter_scan
    add x1, x1, :lo12:btree_adapter_scan
    str x1, [x0, #SP_FN_SCAN]
    adrp x1, btree_adapter_flush
    add x1, x1, :lo12:btree_adapter_flush
    str x1, [x0, #SP_FN_FLUSH]
    adrp x1, btree_adapter_sync
    add x1, x1, :lo12:btree_adapter_sync
    str x1, [x0, #SP_FN_SYNC]
    adrp x1, btree_adapter_close
    add x1, x1, :lo12:btree_adapter_close
    str x1, [x0, #SP_FN_CLOSE]
    adrp x1, storage_port_noop
    add x1, x1, :lo12:storage_port_noop
    str x1, [x0, #SP_FN_STATS]

    // Store vtable in db
    str x0, [x19, #DB_STORAGE_PORT]

    mov x0, #ADB_OK
    b .Lba_init_ret

.Lba_init_fail_close:
    mov x0, x21
    bl sys_close

.Lba_init_fail:
    mov x0, #ADB_ERR_IO

.Lba_init_ret:
    ldr x23, [sp, #48]
    ldp x21, x22, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #64
    ret

.Lbtree_filename:
    .asciz "data.btree"
    .align 2
.size btree_adapter_init, .-btree_adapter_init

// ============================================================================
// Adapter functions: translate port interface to btree operations
// All take db_ptr as implicit first arg (stored in x19 by caller)
// ============================================================================

// btree_adapter_put(db_ptr, key_ptr, val_ptr, tx_id) -> error
.global btree_adapter_put
.type btree_adapter_put, %function
btree_adapter_put:
    // x0=db_ptr, x1=key_ptr, x2=val_ptr, x3=tx_id
    // Rearrange for btree_insert(db_ptr, key_ptr, val_ptr, tx_id)
    b btree_insert
.size btree_adapter_put, .-btree_adapter_put

// btree_adapter_get(db_ptr, key_ptr, val_buf, tx_id) -> error
.global btree_adapter_get
.type btree_adapter_get, %function
btree_adapter_get:
    stp x29, x30, [sp, #-32]!
    mov x29, sp
    str x19, [sp, #16]

    mov x19, x0            // save db_ptr
    mov x3, x2             // val_buf -> x3
    mov x2, x1             // key_ptr -> x2
    ldr x0, [x19, #DB_BTREE_MMAP]
    ldr w1, [x19, #DB_BTREE_ROOT]
    bl btree_get

    ldr x19, [sp, #16]
    ldp x29, x30, [sp], #32
    ret
.size btree_adapter_get, .-btree_adapter_get

// btree_adapter_delete(db_ptr, key_ptr) -> error
.global btree_adapter_delete
.type btree_adapter_delete, %function
btree_adapter_delete:
    // x0=db_ptr, x1=key_ptr
    b btree_delete
.size btree_adapter_delete, .-btree_adapter_delete

// btree_adapter_scan(db_ptr, start_key, end_key, callback, user_data, tx_id) -> error
.global btree_adapter_scan
.type btree_adapter_scan, %function
btree_adapter_scan:
    stp x29, x30, [sp, #-32]!
    mov x29, sp
    str x19, [sp, #16]

    mov x19, x0
    // Rearrange args for btree_scan(mmap, root, start, end, cb, user_data)
    mov x5, x4             // user_data
    mov x4, x3             // callback
    mov x3, x2             // end_key
    mov x2, x1             // start_key
    ldr x0, [x19, #DB_BTREE_MMAP]
    ldr w1, [x19, #DB_BTREE_ROOT]
    bl btree_scan

    ldr x19, [sp, #16]
    ldp x29, x30, [sp], #32
    ret
.size btree_adapter_scan, .-btree_adapter_scan

// btree_adapter_flush(db_ptr) -> error
.global btree_adapter_flush
.type btree_adapter_flush, %function
btree_adapter_flush:
    stp x29, x30, [sp, #-16]!
    mov x29, sp
    ldr x0, [x0, #DB_BTREE_FD]
    bl sys_fdatasync
    cmp x0, #0
    b.ge 1f
    mov x0, #ADB_ERR_IO
    b 2f
1:  mov x0, #ADB_OK
2:
    ldp x29, x30, [sp], #16
    ret
.size btree_adapter_flush, .-btree_adapter_flush

// btree_adapter_sync(db_ptr) -> error
.global btree_adapter_sync
.type btree_adapter_sync, %function
btree_adapter_sync:
    stp x29, x30, [sp, #-16]!
    mov x29, sp
    ldr x0, [x0, #DB_BTREE_FD]
    bl sys_fsync
    cmp x0, #0
    b.ge 1f
    mov x0, #ADB_ERR_IO
    b 2f
1:  mov x0, #ADB_OK
2:
    ldp x29, x30, [sp], #16
    ret
.size btree_adapter_sync, .-btree_adapter_sync

// btree_adapter_close(db_ptr) -> error
.global btree_adapter_close
.type btree_adapter_close, %function
btree_adapter_close:
    stp x29, x30, [sp, #-32]!
    mov x29, sp
    str x19, [sp, #16]

    mov x19, x0

    // Write metadata to page 0 before closing
    ldr x0, [x19, #DB_BTREE_MMAP]
    cbz x0, .Lac_no_mmap
    ldr x1, [x19, #DB_BTREE_ROOT]
    str x1, [x0, #8]
    ldr x1, [x19, #DB_BTREE_NUM_PAGES]
    str x1, [x0, #16]

    // Sync to disk
    ldr x0, [x19, #DB_BTREE_FD]
    cmp x0, #0
    b.le .Lac_skip_sync
    bl sys_fdatasync
.Lac_skip_sync:

    // munmap
    ldr x0, [x19, #DB_BTREE_MMAP]
    cbz x0, .Lac_no_mmap
    ldr x1, [x19, #DB_BTREE_MMAP_LEN]
    bl sys_munmap
    str xzr, [x19, #DB_BTREE_MMAP]

.Lac_no_mmap:
    // close fd
    ldr x0, [x19, #DB_BTREE_FD]
    cmp x0, #0
    b.le .Lac_no_fd
    bl sys_close
    mov x0, #-1
    str x0, [x19, #DB_BTREE_FD]

.Lac_no_fd:
    // Free vtable
    ldr x0, [x19, #DB_STORAGE_PORT]
    cbz x0, .Lac_done
    mov x1, #STORAGE_PORT_SIZE
    bl free_mem
    str xzr, [x19, #DB_STORAGE_PORT]

.Lac_done:
    mov x0, #ADB_OK
    ldr x19, [sp, #16]
    ldp x29, x30, [sp], #32
    ret
.size btree_adapter_close, .-btree_adapter_close
