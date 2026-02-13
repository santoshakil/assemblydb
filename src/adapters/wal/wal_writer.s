// AssemblyDB - WAL Writer
// Append-only Write-Ahead Log with segment rotation
// Fixed-size records (338 bytes) for simplicity

.include "src/const.s"

.text

// ============================================================================
// wal_open(db_ptr) -> 0=ok, negative=error
// Open or create the current WAL segment file
// x0 = db_ptr
// ============================================================================
.global wal_open
.type wal_open, %function
wal_open:
    stp x29, x30, [sp, #-48]!
    mov x29, sp
    stp x19, x20, [sp, #16]
    str x21, [sp, #32]

    mov x19, x0                    // db_ptr

    // Build WAL filename: "wal_NNNNNN.log"
    sub sp, sp, #128
    mov x0, sp
    ldr x1, [x19, #DB_WAL_SEQ]
    bl build_wal_name
    // x0 = length of name

    // Open WAL file (openat relative to dir_fd)
    ldr w0, [x19, #DB_DIR_FD]
    mov x1, sp                     // filename
    mov w2, #(O_WRONLY | O_CREAT | O_APPEND)
    mov w3, #0644
    movk w3, #0, lsl #16
    bl sys_openat
    add sp, sp, #128

    cmp x0, #0
    b.lt .Lwo_fail

    str x0, [x19, #DB_WAL_FD]
    // Seek to end to get offset
    mov x20, x0                    // fd
    mov x0, x20
    mov x1, #0
    mov w2, #2                     // SEEK_END
    bl sys_lseek
    str x0, [x19, #DB_WAL_OFFSET]

    mov x0, #0
    b .Lwo_ret

.Lwo_fail:
    // x0 already has negative error

.Lwo_ret:
    ldr x21, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #48
    ret
.size wal_open, .-wal_open

// ============================================================================
// wal_append(db_ptr, op_type, key_ptr, val_ptr, tx_id) -> 0=ok, neg=error
// Write a WAL record
// x0 = db_ptr, w1 = op_type, x2 = key_ptr, x3 = val_ptr, x4 = tx_id
// ============================================================================
.global wal_append
.type wal_append, %function
wal_append:
    stp x29, x30, [sp, #-80]!
    mov x29, sp
    stp x19, x20, [sp, #16]
    stp x21, x22, [sp, #32]
    stp x23, x24, [sp, #48]
    str x25, [sp, #64]

    mov x19, x0                    // db_ptr
    mov w20, w1                     // op_type
    mov x21, x2                    // key_ptr
    mov x22, x3                    // val_ptr
    mov x23, x4                    // tx_id

    // Check if we need segment rotation
    ldr x0, [x19, #DB_WAL_OFFSET]
    mov x1, #WAL_SEGMENT_MAX
    cmp x0, x1
    b.lt .Lwa_write

    // Rotate: close current, increment seq, open new
    ldr x0, [x19, #DB_WAL_FD]
    bl sys_close
    ldr x0, [x19, #DB_WAL_SEQ]
    add x0, x0, #1
    str x0, [x19, #DB_WAL_SEQ]
    str xzr, [x19, #DB_WAL_OFFSET]
    mov x0, x19
    bl wal_open
    cmp x0, #0
    b.lt .Lwa_fail

.Lwa_write:
    // Build WAL record on stack (338 bytes, round up to 352 for alignment)
    sub sp, sp, #352

    // Zero the record area
    mov x0, sp
    mov x1, #352
    bl neon_memzero

    // Fill record fields
    mov w0, #WAL_RECORD_SIZE
    str w0, [sp, #WR_RECORD_LEN]

    // Sequence number
    ldr x0, [x19, #DB_WAL_OFFSET]
    mov x1, #WAL_RECORD_SIZE
    udiv x0, x0, x1               // simple sequence from offset
    str x0, [sp, #WR_SEQUENCE]

    // Op type
    strh w20, [sp, #WR_OP_TYPE]

    // Copy key (64 bytes: 2B len + 62B data)
    cbz x21, .Lwa_skip_key
    ldrh w0, [x21]                 // key_len
    strh w0, [sp, #WR_KEY_LEN]
    add x0, sp, #WR_KEY_DATA
    add x1, x21, #2               // skip len prefix
    ldrh w2, [x21]
    cmp w2, #62
    csel w2, w2, w2, le
    mov w3, #62
    cmp w2, w3
    csel w2, w2, w3, le
    bl neon_memcpy

.Lwa_skip_key:
    // Copy val (256 bytes: 2B len + 254B data)
    cbz x22, .Lwa_skip_val
    ldrh w0, [x22]                 // val_len
    strh w0, [sp, #WR_VAL_LEN]
    add x0, sp, #WR_VAL_DATA
    add x1, x22, #2
    ldrh w2, [x22]
    cmp w2, #254
    mov w3, #254
    csel w2, w2, w3, le
    bl neon_memcpy

.Lwa_skip_val:
    // Compute CRC32 over record (skip first 8 bytes: len + crc fields)
    add x0, sp, #WR_SEQUENCE
    mov x1, #(WAL_RECORD_SIZE - 8)
    mov w2, #0                     // initial crc
    bl hw_crc32c
    str w0, [sp, #WR_CRC32]

    // Write record to WAL fd
    ldr x0, [x19, #DB_WAL_FD]
    mov x1, sp
    mov x2, #WAL_RECORD_SIZE
    bl sys_write

    add sp, sp, #352

    cmp x0, #WAL_RECORD_SIZE
    b.ne .Lwa_write_fail

    // Update offset
    ldr x0, [x19, #DB_WAL_OFFSET]
    add x0, x0, #WAL_RECORD_SIZE
    str x0, [x19, #DB_WAL_OFFSET]

    mov x0, #0
    b .Lwa_ret

.Lwa_write_fail:
    mov x0, #ADB_ERR_IO
    b .Lwa_ret

.Lwa_fail:
    // x0 has error

.Lwa_ret:
    ldr x25, [sp, #64]
    ldp x23, x24, [sp, #48]
    ldp x21, x22, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #80
    ret
.size wal_append, .-wal_append

// ============================================================================
// wal_sync(db_ptr) -> 0=ok, neg=error
// Sync WAL to disk
// x0 = db_ptr
// ============================================================================
.global wal_sync
.type wal_sync, %function
wal_sync:
    stp x29, x30, [sp, #-16]!
    mov x29, sp

    ldr x0, [x0, #DB_WAL_FD]
    cmp x0, #0
    b.le .Lws_skip
    bl sys_fdatasync
    b .Lws_ret

.Lws_skip:
    mov x0, #0
.Lws_ret:
    ldp x29, x30, [sp], #16
    ret
.size wal_sync, .-wal_sync

// ============================================================================
// wal_close(db_ptr) -> 0=ok
// Close WAL file
// x0 = db_ptr
// ============================================================================
.global wal_close
.type wal_close, %function
wal_close:
    stp x29, x30, [sp, #-16]!
    mov x29, sp

    ldr x0, [x0, #DB_WAL_FD]
    cmp x0, #0
    b.le .Lwc_done
    bl sys_close

.Lwc_done:
    mov x0, #0
    ldp x29, x30, [sp], #16
    ret
.size wal_close, .-wal_close
