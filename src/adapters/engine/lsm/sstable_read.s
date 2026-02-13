// AssemblyDB - SSTable Reader
// Read and search sorted SSTable files on disk
// Binary search within blocks, sequential block scanning

.include "src/const.s"

.text

// ============================================================================
// sstable_open(db_ptr, level, seq, desc_ptr) -> 0=ok, neg=error
// Open an SSTable file and populate descriptor
// x0 = db_ptr, w1 = level, w2 = seq, x3 = descriptor ptr (SSTD_SIZE bytes)
// ============================================================================
.global sstable_open
.type sstable_open, %function
sstable_open:
    stp x29, x30, [sp, #-64]!
    mov x29, sp
    stp x19, x20, [sp, #16]
    stp x21, x22, [sp, #32]
    str x23, [sp, #48]

    mov x19, x0                    // db_ptr
    mov w20, w1                     // level
    mov w21, w2                     // seq
    mov x22, x3                    // desc_ptr

    // Build filename
    sub sp, sp, #128
    mov x0, sp
    mov w1, w20
    mov w2, w21
    bl build_sst_name

    // Open file
    ldr w0, [x19, #DB_DIR_FD]
    mov x1, sp
    mov w2, #O_RDONLY
    mov w3, #0
    bl sys_openat
    add sp, sp, #128

    cmp x0, #0
    b.lt .Lso_fail
    str x0, [x22, #SSTD_FD]

    // Get file size via lseek
    mov x23, x0                    // fd
    mov x0, x23
    mov x1, #0
    mov w2, #2                     // SEEK_END
    bl sys_lseek
    str x0, [x22, #SSTD_FILE_SIZE]

    // Read footer (last SSTF_SIZE bytes)
    sub sp, sp, #SSTF_SIZE
    sub x1, x0, #SSTF_SIZE        // offset = file_size - SSTF_SIZE
    mov x0, x23
    mov x2, sp                     // buffer (wrong arg order, fix)
    // pread64(fd, buf, count, offset)
    mov x0, x23                    // fd
    mov x1, sp                     // buf
    mov x2, #SSTF_SIZE             // count
    ldr x3, [x22, #SSTD_FILE_SIZE]
    sub x3, x3, #SSTF_SIZE        // offset
    bl sys_pread64

    cmp x0, #SSTF_SIZE
    b.ne .Lso_read_fail

    // Parse footer into descriptor
    ldr w0, [sp, #SSTF_NUM_DATA_BLK]
    str w0, [x22, #SSTD_NUM_DATA_BLOCKS]
    ldr w0, [sp, #SSTF_NUM_IDX_BLK]
    str w0, [x22, #SSTD_NUM_INDEX_BLOCKS]
    ldr x0, [sp, #SSTF_IDX_START]
    str x0, [x22, #SSTD_INDEX_OFFSET]
    ldr x0, [sp, #SSTF_NUM_ENTRIES]
    str x0, [x22, #SSTD_NUM_ENTRIES]

    add sp, sp, #SSTF_SIZE

    mov x0, #0
    b .Lso_ret

.Lso_read_fail:
    add sp, sp, #SSTF_SIZE
    mov x0, x23
    bl sys_close
    mov x0, #ADB_ERR_IO
    b .Lso_ret

.Lso_fail:
    // x0 has error

.Lso_ret:
    ldr x23, [sp, #48]
    ldp x21, x22, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #64
    ret
.size sstable_open, .-sstable_open

// ============================================================================
// sstable_get(desc_ptr, key_ptr, val_buf) -> 0=found, 1=not_found, neg=error
// Search SSTable for a key
// x0 = descriptor ptr, x1 = key_ptr, x2 = val_buf
// ============================================================================
.global sstable_get
.type sstable_get, %function
sstable_get:
    stp x29, x30, [sp, #-80]!
    mov x29, sp
    stp x19, x20, [sp, #16]
    stp x21, x22, [sp, #32]
    stp x23, x24, [sp, #48]
    str x25, [sp, #64]

    mov x19, x0                    // desc_ptr
    mov x20, x1                    // key_ptr
    mov x21, x2                    // val_buf

    ldr w22, [x19, #SSTD_NUM_DATA_BLOCKS]
    ldr x23, [x19, #SSTD_FD]

    // Linear scan through data blocks (TODO: use index for binary search)
    mov w24, #0                     // block_idx

    sub sp, sp, #4096              // block buffer

.Lsg_block_loop:
    cmp w24, w22
    b.ge .Lsg_not_found

    // Read block
    mov x0, x23                    // fd
    mov x1, sp                     // buf
    mov x2, #4096                  // count
    lsl x3, x24, #12              // offset = block_idx * 4096
    bl sys_pread64
    cmp x0, #4096
    b.ne .Lsg_io_error

    // Search within block
    ldrh w25, [sp]                 // num_entries in block
    mov w0, #0                      // entry_idx

.Lsg_entry_loop:
    cmp w0, w25
    b.ge .Lsg_next_block

    // Entry offset = SST_BLOCK_HEADER + entry_idx * 320
    mov w1, #320
    mul w1, w0, w1
    add w1, w1, #SST_BLOCK_HEADER
    add x1, sp, x1                 // entry ptr in block

    // Compare key
    stp x0, x1, [sp, #-16]!
    mov x0, x20                    // our key
    // x1 = entry key ptr (already set)
    bl key_compare
    mov w2, w0
    ldp x0, x1, [sp], #16

    cbz w2, .Lsg_found

    add w0, w0, #1
    b .Lsg_entry_loop

.Lsg_next_block:
    add w24, w24, #1
    b .Lsg_block_loop

.Lsg_found:
    // x1 = entry ptr, val is at offset 64 from entry start
    add x1, x1, #64               // skip key (64B)
    mov x0, x21                    // dst = val_buf
    bl neon_copy_256

    add sp, sp, #4096
    mov x0, #0
    b .Lsg_ret

.Lsg_not_found:
    add sp, sp, #4096
    mov x0, #ADB_ERR_NOT_FOUND
    b .Lsg_ret

.Lsg_io_error:
    add sp, sp, #4096
    mov x0, #ADB_ERR_IO

.Lsg_ret:
    ldr x25, [sp, #64]
    ldp x23, x24, [sp, #48]
    ldp x21, x22, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #80
    ret
.size sstable_get, .-sstable_get

// ============================================================================
// sstable_close(desc_ptr) -> 0
// Close SSTable file
// x0 = descriptor ptr
// ============================================================================
.global sstable_close
.type sstable_close, %function
sstable_close:
    stp x29, x30, [sp, #-16]!
    mov x29, sp

    ldr x0, [x0, #SSTD_FD]
    cmp x0, #0
    b.le .Lsc_done
    bl sys_close

.Lsc_done:
    mov x0, #0
    ldp x29, x30, [sp], #16
    ret
.size sstable_close, .-sstable_close
