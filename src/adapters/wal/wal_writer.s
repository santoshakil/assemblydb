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
    stp x29, x30, [sp, #-32]!
    mov x29, sp
    stp x19, x20, [sp, #16]

    mov x19, x0                    // db_ptr

    // Build WAL filename: "wal_NNNNNN.log"
    sub sp, sp, #128
    mov x0, sp
    ldr x1, [x19, #DB_WAL_SEQ]
    bl build_wal_name

    // Open WAL file (openat relative to dir_fd)
.Lwo_open_retry:
    ldr w0, [x19, #DB_DIR_FD]
    mov x1, sp
    mov w2, #(O_WRONLY | O_CREAT | O_APPEND)
    mov w3, #0644
    bl sys_openat
    cmn x0, #4
    b.eq .Lwo_open_retry
    add sp, sp, #128

    cmp x0, #0
    b.lt .Lwo_fail

    str x0, [x19, #DB_WAL_FD]
    mov x20, x0                    // fd
    mov x1, #0
    mov w2, #2                     // SEEK_END
    bl sys_lseek
    cmp x0, #0
    b.lt .Lwo_lseek_fail
    str x0, [x19, #DB_WAL_OFFSET]

    mov x0, #0
    b .Lwo_ret

.Lwo_lseek_fail:
    // lseek failed: close fd, report error
    mov x20, x0                    // save error
    ldr x0, [x19, #DB_WAL_FD]
    bl sys_close
    movn x0, #0                    // -1
    str x0, [x19, #DB_WAL_FD]
    str xzr, [x19, #DB_WAL_OFFSET]
    mov x0, #ADB_ERR_IO
    neg x0, x0
    b .Lwo_ret

.Lwo_fail:
    // Translate raw errno to standard error
    mov x0, #ADB_ERR_IO
    neg x0, x0

.Lwo_ret:
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #32
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
    stp x29, x30, [sp, #-48]!
    mov x29, sp
    stp x19, x20, [sp, #16]
    stp x21, x22, [sp, #32]

    mov x19, x0                    // db_ptr
    mov w20, w1                     // op_type
    mov x21, x2                    // key_ptr
    mov x22, x3                    // val_ptr
    // x4 = tx_id (unused — not written to WAL record)

    // Check if we need segment rotation
    ldr x0, [x19, #DB_WAL_OFFSET]
    mov x1, #WAL_SEGMENT_MAX
    cmp x0, x1
    b.lo .Lwa_write

    // Rotate: close current, increment seq, open new
    ldr x0, [x19, #DB_WAL_FD]
    bl sys_close
    // Invalidate fd immediately — prevents stale writes if wal_open fails
    movn x0, #0                      // x0 = -1 (invalid fd)
    str x0, [x19, #DB_WAL_FD]
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
    ldrh w2, [x21]                 // key_len
    strh w2, [sp, #WR_KEY_LEN]
    mov w3, #62
    cmp w2, w3
    csel w2, w2, w3, ls            // w2 = min(key_len, 62) unsigned
    add x0, sp, #WR_KEY_DATA
    add x1, x21, #2               // skip len prefix
    bl neon_memcpy

.Lwa_skip_key:
    // Copy val (256 bytes: 2B len + 254B data)
    cbz x22, .Lwa_skip_val
    ldrh w2, [x22]                 // val_len
    strh w2, [sp, #WR_VAL_LEN]
    mov w3, #254
    cmp w2, w3
    csel w2, w2, w3, ls
    add x0, sp, #WR_VAL_DATA
    add x1, x22, #2
    bl neon_memcpy

.Lwa_skip_val:
    // Compute CRC32 over record (skip first 8 bytes: len + crc fields)
    add x0, sp, #WR_SEQUENCE
    mov x1, #(WAL_RECORD_SIZE - 8)
    bl hw_crc32c
    str w0, [sp, #WR_CRC32]

    // Write record to WAL fd
.Lwa_write_retry:
    ldr x0, [x19, #DB_WAL_FD]
    mov x1, sp
    mov x2, #WAL_RECORD_SIZE
    bl sys_write
    cmn x0, #4                     // EINTR?
    b.eq .Lwa_write_retry

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
    // Short write or error: truncate back to pre-write offset
    // to remove any partial record (O_APPEND guarantees atomic seek)
    ldr x0, [x19, #DB_WAL_FD]
    ldr x1, [x19, #DB_WAL_OFFSET]
    bl sys_ftruncate
    mov x0, #ADB_ERR_IO
    b .Lwa_ret

.Lwa_fail:
    // x0 has error

.Lwa_ret:
    ldp x21, x22, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #48
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
    stp x29, x30, [sp, #-32]!
    mov x29, sp
    str x19, [sp, #16]

    ldr x19, [x0, #DB_WAL_FD]
    cmp x19, #0
    b.lt .Lws_skip
.Lws_retry:
    mov x0, x19
    bl sys_fdatasync
    cmn x0, #4
    b.eq .Lws_retry
    b .Lws_ret

.Lws_skip:
    mov x0, #0
.Lws_ret:
    ldr x19, [sp, #16]
    ldp x29, x30, [sp], #32
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
    stp x29, x30, [sp, #-32]!
    mov x29, sp
    str x19, [sp, #16]

    mov x19, x0                     // save db_ptr
    ldr x0, [x19, #DB_WAL_FD]
    cmp x0, #0
    b.lt .Lwc_done
    bl sys_close
    // Invalidate fd to prevent double-close on recycled fd
    movn x0, #0
    str x0, [x19, #DB_WAL_FD]

.Lwc_done:
    mov x0, #0
    ldr x19, [sp, #16]
    ldp x29, x30, [sp], #32
    ret
.size wal_close, .-wal_close

// ============================================================================
// wal_truncate_all(db_ptr) -> 0=ok, neg=error
// Close WAL fd, delete ALL WAL segment files, reset seq/offset to 0
// x0 = db_ptr
// ============================================================================
.hidden destroy_dir_files
.global wal_truncate_all
.hidden wal_truncate_all
.type wal_truncate_all, %function
wal_truncate_all:
    stp x29, x30, [sp, #-32]!
    mov x29, sp
    str x19, [sp, #16]

    mov x19, x0                    // db_ptr

    // Close current WAL fd
    ldr x0, [x19, #DB_WAL_FD]
    cmp x0, #0
    b.lt .Lwta_skip_close
    bl sys_close
    movn x0, #0                    // -1 = invalid
    str x0, [x19, #DB_WAL_FD]
.Lwta_skip_close:

    // Reset sequence and offset
    str xzr, [x19, #DB_WAL_SEQ]
    str xzr, [x19, #DB_WAL_OFFSET]

    // Open "wal/" subdirectory relative to DB dir
    ldr w0, [x19, #DB_DIR_FD]
    adrp x1, .Lwta_wal_dir
    add x1, x1, :lo12:.Lwta_wal_dir
    mov w2, #(O_RDONLY | O_DIRECTORY)
    mov w3, #0
    bl sys_openat
    cmp x0, #0
    b.lt .Lwta_done                // wal/ dir doesn't exist, nothing to clean

    mov x19, x0                    // save wal_dir_fd (reuse x19, db_ptr no longer needed)

    // Delete all files in wal/ directory
    mov x0, x19
    bl destroy_dir_files

    // Close wal/ directory fd
    mov x0, x19
    bl sys_close

.Lwta_done:
    mov x0, #0
    ldr x19, [sp, #16]
    ldp x29, x30, [sp], #32
    ret

.Lwta_wal_dir:
    .asciz "wal"
    .align 2
.size wal_truncate_all, .-wal_truncate_all
