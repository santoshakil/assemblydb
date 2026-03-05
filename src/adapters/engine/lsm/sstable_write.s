// AssemblyDB - SSTable Writer
// Flush memtable (skip list) to a sorted SSTable file on disk
// Format: [data blocks] [index block] [bloom filter] [footer]

.include "src/const.s"

.text

// ============================================================================
// sstable_flush(db_ptr, memtable_head, level, seq) -> 0=ok, neg=error
// Write memtable contents to an SSTable file
// x0 = db_ptr, x1 = memtable_head, w2 = level (0 or 1), w3 = sequence number
// ============================================================================
.global sstable_flush
.type sstable_flush, %function
sstable_flush:
    stp x29, x30, [sp, #-112]!
    mov x29, sp
    stp x19, x20, [sp, #16]
    stp x21, x22, [sp, #32]
    stp x23, x24, [sp, #48]
    stp x25, x26, [sp, #64]
    stp x27, x28, [sp, #80]

    mov x19, x0                    // db_ptr
    mov x20, x1                    // memtable_head
    mov w21, w2                     // level
    mov w22, w3                     // sequence

    // Build SSTable filename
    sub sp, sp, #128
    mov x0, sp
    mov w1, w21                     // level
    mov w2, w22                     // seq
    bl build_sst_name

    // Open file for writing
.Lsf_open_retry:
    ldr w0, [x19, #DB_DIR_FD]
    mov x1, sp
    mov w2, #(O_WRONLY | O_CREAT | O_TRUNC)
    mov w3, #0644
    bl sys_openat
    cmn x0, #4
    b.eq .Lsf_open_retry
    add sp, sp, #128

    cmp x0, #0
    b.lt .Lsf_fail
    mov x23, x0                    // sst_fd

    // Create bloom filter for the entries
    ldr x0, [x20, #SLH_ENTRY_COUNT]
    bl bloom_create
    mov x24, x0                    // bloom_ptr (can be NULL if alloc fails)

    // Write data blocks
    // Each block: [8B header][entries...][padding to 4096]
    // Entry: [64B key][256B val] = 320B per entry
    // Entries per block = (4096 - 8) / 320 = 12
    mov x25, x20                    // iterator: start with head
    ldr x25, [x25, #SLH_FORWARD]  // first node
    mov x26, #0                    // total_entries
    mov x27, #0                    // num_data_blocks
    mov x28, #0                    // file offset

    // Allocate a block buffer on stack (4096 bytes)
    sub sp, sp, #4096

.Lsf_block_loop:
    cbz x25, .Lsf_blocks_done     // no more entries

    // Zero block buffer
    mov x0, sp
    mov x1, #4096
    bl neon_memzero

    // Fill entries into block
    mov w0, #0                      // entries_in_block = 0
    add x1, sp, #SST_BLOCK_HEADER  // write position

.Lsf_entry_loop:
    cbz x25, .Lsf_write_block
    cmp w0, #SST_ENTRIES_PER_BLOCK
    b.hs .Lsf_write_block

    // Skip deleted entries
    ldrb w2, [x25, #SLN_IS_DELETED]
    cbnz w2, .Lsf_skip_entry

    // Add key to bloom filter (key record at node offset 0 = 64-byte fixed key)
    cbz x24, .Lsf_skip_bloom
    stp x0, x1, [sp, #-16]!
    mov x0, x24                    // bloom
    mov x1, x25                    // key_ptr = node base (64B key record)
    bl bloom_add
    ldp x0, x1, [sp], #16

.Lsf_skip_bloom:
    // Copy key (64 bytes) into block
    stp x0, x1, [sp, #-16]!
    mov x0, x1                     // dst in block
    mov x1, x25                    // src = node (key starts at offset 0)
    bl neon_copy_64
    ldp x0, x1, [sp], #16
    add x1, x1, #64

    // Copy val (256 bytes)
    stp x0, x1, [sp, #-16]!
    mov x0, x1
    add x1, x25, #SLN_VAL_LEN
    bl neon_copy_256
    ldp x0, x1, [sp], #16
    add x1, x1, #256

    add w0, w0, #1
    add x26, x26, #1              // total_entries++

.Lsf_skip_entry:
    // Advance to next node
    ldr x25, [x25, #SLN_FORWARD]  // forward[0]
    b .Lsf_entry_loop

.Lsf_write_block:
    cbz w0, .Lsf_blocks_done      // no entries in this block

    // Write block header
    strh w0, [sp]                  // num_entries
    strh wzr, [sp, #2]            // padding

    // CRC32 of entire block (CRC field zeroed before compute)
    str wzr, [sp, #4]
    mov x0, sp
    mov x1, #4096
    bl hw_crc32c
    str w0, [sp, #4]              // store CRC

    // Write block to file (with EINTR retry)
.Lsf_wr_retry:
    mov x0, x23                    // fd
    mov x1, sp                     // buffer
    mov x2, #4096
    bl sys_write
    cmn x0, #4                    // EINTR?
    b.eq .Lsf_wr_retry
    cmp x0, #4096
    b.ne .Lsf_io_error

    add x27, x27, #1              // num_data_blocks++
    add x28, x28, #4096           // file_offset += 4096
    b .Lsf_block_loop

.Lsf_blocks_done:
    // Free block buffer
    add sp, sp, #4096

    // If zero entries written (all tombstones), skip writing SSTable
    cbnz x26, .Lsf_write_footer
    // Close and unlink the empty file
    mov x0, x23
    bl sys_close
    // Unlink the empty SST file
    sub sp, sp, #128
    mov x0, sp
    mov w1, w21
    mov w2, w22
    bl build_sst_name
    ldr w0, [x19, #DB_DIR_FD]
    mov x1, sp
    mov w2, #0
    bl sys_unlinkat
    add sp, sp, #128
    // Destroy bloom
    cbz x24, .Lsf_empty_ok
    mov x0, x24
    bl bloom_destroy
.Lsf_empty_ok:
    mov x0, #0
    b .Lsf_ret

.Lsf_write_footer:
    // Write footer (256 bytes, padded to fit)

    sub sp, sp, #SSTF_SIZE         // footer buffer
    mov x0, sp
    mov x1, #SSTF_SIZE
    bl neon_memzero

    // Fill footer - SST_MAGIC = "ASSMDB01" = 0x4153534D44423031
    movz x0, #0x3031
    movk x0, #0x4442, lsl #16
    movk x0, #0x534D, lsl #32
    movk x0, #0x4153, lsl #48
    str x0, [sp, #SSTF_MAGIC]

    str w27, [sp, #SSTF_NUM_DATA_BLK]
    str wzr, [sp, #SSTF_NUM_IDX_BLK]
    str x28, [sp, #SSTF_IDX_START]    // index starts right after data
    str xzr, [sp, #SSTF_BLOOM_START]
    str wzr, [sp, #SSTF_BLOOM_SIZE]
    str w26, [sp, #SSTF_NUM_ENTRIES]

    // Compute footer CRC (CRC field is zero from memzero)
    mov x0, sp
    mov x1, #SSTF_SIZE
    bl hw_crc32c
    str w0, [sp, #SSTF_CRC32]

    // Write footer (with EINTR retry)
.Lsf_ftr_retry:
    mov x0, x23
    mov x1, sp
    mov x2, #SSTF_SIZE
    bl sys_write
    cmn x0, #4                    // EINTR?
    b.eq .Lsf_ftr_retry
    cmp x0, #SSTF_SIZE
    b.ne .Lsf_footer_err

    add sp, sp, #SSTF_SIZE

    // Sync and close
.Lsf_sync_retry:
    mov x0, x23
    bl sys_fdatasync
    cmn x0, #4
    b.eq .Lsf_sync_retry
    cmp x0, #0
    b.lt .Lsf_err_close_unlink
    mov x0, x23
    bl sys_close

    // Destroy bloom (if allocated)
    cbz x24, .Lsf_no_bloom
    mov x0, x24
    bl bloom_destroy
.Lsf_no_bloom:

    // Register SSTable descriptor in db state
    cmp w21, #0
    b.ne .Lsf_reg_l1

    // Register L0 descriptor
    ldr w25, [x19, #DB_SST_COUNT_L0]
    cmp w25, #MAX_SST_PER_LEVEL
    b.hs .Lsf_reg_full
    mov x0, #SSTD_SIZE
    bl alloc_zeroed
    cbz x0, .Lsf_reg_nomem
    mov x28, x0                    // descriptor
    mov x0, x19
    mov w1, w21
    mov w2, w22
    mov x3, x28
    bl sstable_open
    cbnz x0, .Lsf_reg_open_fail
    add x1, x19, #DB_SST_LIST_L0
    str x28, [x1, w25, uxtw #3]
    add w25, w25, #1
    str w25, [x19, #DB_SST_COUNT_L0]
    b .Lsf_ok

.Lsf_reg_l1:
    // Register L1 descriptor
    ldr w25, [x19, #DB_SST_COUNT_L1]
    cmp w25, #MAX_SST_PER_LEVEL
    b.hs .Lsf_reg_full
    mov x0, #SSTD_SIZE
    bl alloc_zeroed
    cbz x0, .Lsf_reg_nomem
    mov x28, x0                    // descriptor
    mov x0, x19
    mov w1, w21
    mov w2, w22
    mov x3, x28
    bl sstable_open
    cbnz x0, .Lsf_reg_open_fail
    add x1, x19, #DB_SST_LIST_L1
    str x28, [x1, w25, uxtw #3]
    add w25, w25, #1
    str w25, [x19, #DB_SST_COUNT_L1]
    b .Lsf_ok

.Lsf_reg_open_fail:
    mov x27, x0
    mov x0, x28
    mov x1, #SSTD_SIZE
    bl free_mem
    mov x0, x27
    b .Lsf_ret

.Lsf_reg_nomem:
    mov x0, #ADB_ERR_NOMEM
    b .Lsf_ret

.Lsf_reg_full:
    mov x0, #ADB_ERR_FULL
    b .Lsf_ret

.Lsf_ok:
    mov x0, #0
    b .Lsf_ret

.Lsf_footer_err:
    add sp, sp, #SSTF_SIZE         // clean up footer buffer
    b .Lsf_err_close_unlink

.Lsf_io_error:
    add sp, sp, #4096              // clean up block buffer
    // fall through

.Lsf_err_close_unlink:
    mov x0, x23
    bl sys_close
    // Unlink partial SSTable to prevent corrupt leftover on disk
    sub sp, sp, #128
    mov x0, sp
    mov w1, w21                     // level
    mov w2, w22                     // seq
    bl build_sst_name
    ldr w0, [x19, #DB_DIR_FD]
    mov x1, sp
    mov w2, #0
    bl sys_unlinkat
    add sp, sp, #128
    cbz x24, .Lsf_err_nobloom
    mov x0, x24
    bl bloom_destroy
.Lsf_err_nobloom:
    mov x0, #ADB_ERR_IO
    b .Lsf_ret

.Lsf_fail:
    // x0 has error

.Lsf_ret:
    ldp x27, x28, [sp, #80]
    ldp x25, x26, [sp, #64]
    ldp x23, x24, [sp, #48]
    ldp x21, x22, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #112
    ret
.size sstable_flush, .-sstable_flush

.hidden alloc_zeroed
.hidden free_mem
.hidden sstable_open
.hidden sys_unlinkat
.hidden hw_crc32c
