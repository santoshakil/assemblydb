// AssemblyDB - WAL Recovery
// Replay WAL records after crash to restore memtable state

.include "src/const.s"

.text

// ============================================================================
// wal_recover(db_ptr, callback, ctx) -> 0=ok, neg=error
// Read WAL segment and replay records via callback
// x0 = db_ptr, x1 = callback(op, key, val, ctx), x2 = ctx
// callback: x0=op_type, x1=key_ptr, x2=val_ptr, x3=ctx -> 0=ok
// ============================================================================
.global wal_recover
.type wal_recover, %function
wal_recover:
    stp x29, x30, [sp, #-80]!
    mov x29, sp
    stp x19, x20, [sp, #16]
    stp x21, x22, [sp, #32]
    stp x23, x24, [sp, #48]
    str x25, [sp, #64]

    mov x19, x0                    // db_ptr
    mov x20, x1                    // callback
    mov x21, x2                    // ctx

    // Try to open WAL file for reading
    sub sp, sp, #128
    mov x0, sp
    ldr x1, [x19, #DB_WAL_SEQ]
    bl build_wal_name

    ldr w0, [x19, #DB_DIR_FD]
    mov x1, sp
    mov w2, #O_RDONLY
    mov w3, #0
    bl sys_openat
    add sp, sp, #128

    cmp x0, #0
    b.lt .Lwr_no_wal              // No WAL file, nothing to recover

    mov x22, x0                    // wal_fd

    // Allocate record buffer on stack (round WAL_RECORD_SIZE up to 352)
    sub sp, sp, #352

.Lwr_read_loop:
    // Read one record
    mov x0, x22
    mov x1, sp
    mov x2, #WAL_RECORD_SIZE
    bl sys_read

    cmp x0, #WAL_RECORD_SIZE
    b.ne .Lwr_read_done            // Short read or EOF

    // Verify CRC
    ldr w23, [sp, #WR_CRC32]      // stored CRC
    add x0, sp, #WR_SEQUENCE
    mov x1, #(WAL_RECORD_SIZE - 8)
    mov w2, #0
    bl hw_crc32c
    cmp w0, w23
    b.ne .Lwr_read_done            // CRC mismatch, stop recovery

    // Build key in fixed format (64 bytes)
    sub sp, sp, #320               // 64 for key + 256 for val
    mov x0, sp                     // key buf
    mov x1, #64
    bl neon_memzero

    ldrh w0, [sp, #(320 + WR_KEY_LEN)]
    strh w0, [sp]                  // key len
    add x0, sp, #2                // key data dst
    add x1, sp, #(320 + WR_KEY_DATA)
    ldrh w2, [sp]
    cmp w2, #62
    mov w3, #62
    csel w2, w2, w3, le
    bl neon_memcpy

    // Build val (256 bytes)
    add x0, sp, #64               // val buf
    mov x1, #256
    bl neon_memzero

    ldrh w0, [sp, #(320 + WR_VAL_LEN)]
    strh w0, [sp, #64]            // val len
    add x0, sp, #66               // val data dst
    add x1, sp, #(320 + WR_VAL_DATA)
    ldrh w2, [sp, #64]
    cmp w2, #254
    mov w3, #254
    csel w2, w2, w3, le
    bl neon_memcpy

    // Call callback(op, key, val, ctx)
    ldrh w0, [sp, #(320 + WR_OP_TYPE)]
    mov x1, sp                     // key
    add x2, sp, #64               // val
    mov x3, x21                    // ctx
    blr x20

    add sp, sp, #320
    b .Lwr_read_loop

.Lwr_read_done:
    add sp, sp, #352

    // Close WAL fd
    mov x0, x22
    bl sys_close

    mov x0, #0
    b .Lwr_ret

.Lwr_no_wal:
    mov x0, #0                    // No WAL is not an error

.Lwr_ret:
    ldr x25, [sp, #64]
    ldp x23, x24, [sp, #48]
    ldp x21, x22, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #80
    ret
.size wal_recover, .-wal_recover
