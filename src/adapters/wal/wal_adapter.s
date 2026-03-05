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
.hidden alloc_zeroed
.hidden free_mem
.hidden not_impl_stub

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
    str x0, [x1, #WP_FN_APPEND]

    adrp x0, wal_sync
    add x0, x0, :lo12:wal_sync
    str x0, [x1, #WP_FN_SYNC]

    // fn_rotate = not_impl_stub (safe fallback)
    adrp x0, not_impl_stub
    add x0, x0, :lo12:not_impl_stub
    str x0, [x1, #WP_FN_ROTATE]

    adrp x0, wal_recover
    add x0, x0, :lo12:wal_recover
    str x0, [x1, #WP_FN_RECOVER]

    // fn_truncate = not_impl_stub (safe fallback)
    adrp x0, not_impl_stub
    add x0, x0, :lo12:not_impl_stub
    str x0, [x1, #WP_FN_TRUNCATE]

    mov x0, #0
    b .Lwai_ret

.Lwai_nomem:
    // Close orphan WAL fd opened by wal_open
    mov x0, x19
    bl wal_close
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
    stp x19, x20, [sp, #16]

    mov x19, x0
    mov w20, #0                    // error accumulator

    // Sync WAL
    mov x0, x19
    bl wal_sync
    cmp x0, #0
    csel w20, w0, w20, lt          // capture sync error

    // Close WAL
    mov x0, x19
    bl wal_close
    cmp w0, #0
    csel w20, w0, w20, lt          // capture close error if sync ok

    // Free vtable
    ldr x0, [x19, #DB_WAL_PORT]
    cbz x0, .Lwac_done
    mov x1, #WAL_PORT_SIZE
    bl free_mem
    str xzr, [x19, #DB_WAL_PORT]

.Lwac_done:
    mov w0, w20                    // return sync error (0=ok)
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #32
    ret
.size wal_adapter_close, .-wal_adapter_close
