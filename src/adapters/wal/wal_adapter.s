// AssemblyDB - WAL Adapter
// Populates wal_port vtable with WAL implementation functions

.include "src/const.s"

.text

// Mark WAL functions as hidden (internal to library) for PIC safety
.hidden wal_append
.hidden wal_sync
.hidden wal_recover
.hidden wal_open
.hidden wal_close

// ============================================================================
// wal_adapter_init(db_ptr) -> 0=ok, neg=error
// Initialize WAL adapter: open WAL file and wire vtable
// x0 = db_ptr
// ============================================================================
.global wal_adapter_init
.type wal_adapter_init, %function
wal_adapter_init:
    stp x29, x30, [sp, #-32]!
    mov x29, sp
    str x19, [sp, #16]

    mov x19, x0                    // db_ptr

    // Initialize WAL sequence to 0
    str xzr, [x19, #DB_WAL_SEQ]
    str xzr, [x19, #DB_WAL_OFFSET]

    // Open WAL file
    mov x0, x19
    bl wal_open
    cmp x0, #0
    b.lt .Lwai_fail

    // Wire vtable
    // Allocate vtable (WAL_PORT_SIZE = 64 bytes)
    mov x0, #WAL_PORT_SIZE
    bl alloc_zeroed
    cbz x0, .Lwai_nomem

    // Store vtable pointer
    str x0, [x19, #DB_WAL_PORT]
    mov x1, x0

    // Fill vtable function pointers
    adrp x0, wal_append
    add x0, x0, :lo12:wal_append
    str x0, [x1, #0]              // fn_append

    adrp x0, wal_sync
    add x0, x0, :lo12:wal_sync
    str x0, [x1, #8]              // fn_sync

    // fn_rotate = NULL for now (handled internally)
    str xzr, [x1, #16]

    adrp x0, wal_recover
    add x0, x0, :lo12:wal_recover
    str x0, [x1, #24]             // fn_recover

    // fn_truncate = NULL
    str xzr, [x1, #32]

    mov x0, #0
    b .Lwai_ret

.Lwai_nomem:
    mov x0, #ADB_ERR_NOMEM
    b .Lwai_ret

.Lwai_fail:
    // x0 already has error

.Lwai_ret:
    ldr x19, [sp, #16]
    ldp x29, x30, [sp], #32
    ret
.size wal_adapter_init, .-wal_adapter_init

// ============================================================================
// wal_adapter_close(db_ptr) -> 0
// Close WAL and free resources
// x0 = db_ptr
// ============================================================================
.global wal_adapter_close
.type wal_adapter_close, %function
wal_adapter_close:
    stp x29, x30, [sp, #-32]!
    mov x29, sp
    str x19, [sp, #16]

    mov x19, x0

    // Sync WAL
    mov x0, x19
    bl wal_sync

    // Close WAL
    mov x0, x19
    bl wal_close

    // Free vtable
    ldr x0, [x19, #DB_WAL_PORT]
    cbz x0, .Lwac_done
    mov x1, #WAL_PORT_SIZE
    bl free_mem
    str xzr, [x19, #DB_WAL_PORT]

.Lwac_done:
    mov x0, #0
    ldr x19, [sp, #16]
    ldp x29, x30, [sp], #32
    ret
.size wal_adapter_close, .-wal_adapter_close
