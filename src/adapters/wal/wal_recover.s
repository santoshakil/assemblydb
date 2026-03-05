// AssemblyDB - WAL Recovery
// Replay WAL records after crash to restore memtable state
// Iterates ALL WAL segments (0, 1, 2, ...) for full crash recovery

.include "src/const.s"

.text

// ============================================================================
// wal_recover(db_ptr, callback, ctx) -> 0=ok, neg=error
// Read ALL WAL segments sequentially and replay records via callback
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
    stp x25, x26, [sp, #64]

    mov x19, x0                    // db_ptr
    mov x20, x1                    // callback
    mov x21, x2                    // ctx
    mov x25, #0                    // current segment sequence

.Lwr_next_segment:
    // Build filename for current segment
    sub sp, sp, #128
    mov x0, sp
    mov x1, x25                    // seq = current segment
    bl build_wal_name

    // Try to open this WAL segment for reading
.Lwr_open_retry:
    ldr w0, [x19, #DB_DIR_FD]
    mov x1, sp
    mov w2, #O_RDONLY
    mov w3, #0
    bl sys_openat
    cmn x0, #4                     // EINTR?
    b.eq .Lwr_open_retry
    add sp, sp, #128

    cmp x0, #0
    b.lt .Lwr_all_done             // No more segments, done

    mov x22, x0                    // wal_fd for this segment

    // Allocate record buffer on stack (round WAL_RECORD_SIZE up to 352)
    sub sp, sp, #352

.Lwr_read_loop:
    // Read one record
    mov x0, x22
    mov x1, sp
    mov x2, #WAL_RECORD_SIZE
    bl sys_read
    cmn x0, #4                     // EINTR?
    b.eq .Lwr_read_loop            // retry with fresh args

    cmp x0, #0
    b.lt .Lwr_read_error           // Negative = I/O error
    cmp x0, #WAL_RECORD_SIZE
    b.ne .Lwr_segment_done         // Short read or EOF

    // Verify CRC
    ldr w23, [sp, #WR_CRC32]      // stored CRC
    add x0, sp, #WR_SEQUENCE
    mov x1, #(WAL_RECORD_SIZE - 8)
    bl hw_crc32c
    cmp w0, w23
    b.ne .Lwr_segment_done         // CRC mismatch, stop this segment

    // Build key in fixed format (64 bytes)
    sub sp, sp, #320               // 64 for key + 256 for val
    mov x0, sp                     // key buf
    mov x1, #64
    bl neon_memzero

    ldrh w0, [sp, #(320 + WR_KEY_LEN)]
    cmp w0, #62
    mov w1, #62
    csel w0, w0, w1, ls            // clamp key_len to max 62
    strh w0, [sp]                  // store clamped key len
    mov w2, w0                     // copy length = clamped len
    add x0, sp, #2                // key data dst
    add x1, sp, #(320 + WR_KEY_DATA)
    bl neon_memcpy

    // Build val (256 bytes)
    add x0, sp, #64               // val buf
    mov x1, #256
    bl neon_memzero

    ldrh w0, [sp, #(320 + WR_VAL_LEN)]
    cmp w0, #254
    mov w1, #254
    csel w0, w0, w1, ls            // clamp val_len to max 254
    strh w0, [sp, #64]            // store clamped val len
    mov w2, w0                     // copy length = clamped len
    add x0, sp, #66               // val data dst
    add x1, sp, #(320 + WR_VAL_DATA)
    bl neon_memcpy

    // Call callback(op, key, val, ctx)
    ldrh w0, [sp, #(320 + WR_OP_TYPE)]
    mov x1, sp                     // key
    add x2, sp, #64               // val
    mov x3, x21                    // ctx
    blr x20

    add sp, sp, #320
    cbnz x0, .Lwr_cb_error        // Callback failed: propagate error
    b .Lwr_read_loop

.Lwr_cb_error:
    mov x26, x0                    // Preserve callback error
    add sp, sp, #352               // Free record buffer
    mov x0, x22
    bl sys_close
    mov x0, x26
    b .Lwr_ret

.Lwr_read_error:
    mov x26, x0                    // Preserve I/O error
    add sp, sp, #352               // Free record buffer
    mov x0, x22
    bl sys_close
    mov x0, x26
    b .Lwr_ret

.Lwr_segment_done:
    add sp, sp, #352

    // Close this segment's fd
    mov x0, x22
    bl sys_close

    // Advance to next segment
    add x25, x25, #1
    b .Lwr_next_segment

.Lwr_all_done:
    // Update DB_WAL_SEQ so next wal_open doesn't overwrite existing segments
    str x25, [x19, #DB_WAL_SEQ]
    mov x0, #0

.Lwr_ret:
    ldp x25, x26, [sp, #64]
    ldp x23, x24, [sp, #48]
    ldp x21, x22, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #80
    ret
.size wal_recover, .-wal_recover
