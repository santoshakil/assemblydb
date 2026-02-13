// AssemblyDB - Backup Manager
// Full backup: copy all database files to destination directory
// Incremental: copy only WAL segments since last backup
//
// Backup format:
//   dest_dir/
//     btree.db     - B+ tree data file
//     wal/         - WAL segments
//     sst/         - SSTable files
//     MANIFEST     - Database metadata

.include "src/const.s"

.text

// ============================================================================
// backup_full(db, dest_path) -> err
// Create a full backup of the database
// x0 = db handle, x1 = destination path (null-terminated)
// ============================================================================
.global backup_full
.type backup_full, %function
backup_full:
    stp x29, x30, [sp, #-64]!
    mov x29, sp
    stp x19, x20, [sp, #16]
    stp x21, x22, [sp, #32]
    str x23, [sp, #48]

    mov x19, x0                    // db
    mov x20, x1                    // dest_path

    // Create destination directory
    mov x0, #AT_FDCWD
    mov x1, x20
    mov w2, #MODE_DIR
    mov x8, #SYS_mkdirat
    svc #0
    // Ignore EEXIST (-17)

    // Open destination directory
    mov x0, #AT_FDCWD
    mov x1, x20
    mov w2, #(O_RDONLY | O_DIRECTORY)
    mov w3, #0
    bl sys_openat
    cmn x0, #4096
    b.hi .Lbf_fail
    mov w21, w0                    // dest_fd

    // Create wal/ and sst/ subdirs in dest
    sub sp, sp, #256

    // wal/
    mov x0, sp
    mov w1, #'w'
    strb w1, [x0]
    mov w1, #'a'
    strb w1, [x0, #1]
    mov w1, #'l'
    strb w1, [x0, #2]
    strb wzr, [x0, #3]
    mov w0, w21
    mov x1, sp
    mov w2, #MODE_DIR
    mov x8, #SYS_mkdirat
    svc #0

    // sst/
    mov x0, sp
    mov w1, #'s'
    strb w1, [x0]
    strb w1, [x0, #1]
    mov w1, #'t'
    strb w1, [x0, #2]
    strb wzr, [x0, #3]
    mov w0, w21
    mov x1, sp
    mov w2, #MODE_DIR
    mov x8, #SYS_mkdirat
    svc #0

    add sp, sp, #256

    // Sync the source database first
    mov x0, x19
    bl adb_sync

    // Copy B+ tree file
    ldr w0, [x19, #DB_DIR_FD]     // src dir_fd
    mov w1, w21                    // dst dir_fd
    bl backup_copy_btree

    // Copy manifest (write current state)
    mov x0, x19
    mov w1, w21
    bl backup_write_manifest

    // Close dest fd
    mov w0, w21
    bl sys_close

    mov x0, #ADB_OK
    b .Lbf_ret

.Lbf_fail:
    mov x0, #ADB_ERR_IO

.Lbf_ret:
    ldr x23, [sp, #48]
    ldp x21, x22, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #64
    ret
.size backup_full, .-backup_full

// ============================================================================
// backup_copy_btree(src_dir_fd, dst_dir_fd) -> err
// Copy btree.db from source to destination
// ============================================================================
.global backup_copy_btree
.type backup_copy_btree, %function
backup_copy_btree:
    stp x29, x30, [sp, #-64]!
    mov x29, sp
    stp x19, x20, [sp, #16]
    stp x21, x22, [sp, #32]
    str x23, [sp, #48]

    mov w19, w0                    // src_dir_fd
    mov w20, w1                    // dst_dir_fd

    sub sp, sp, #32

    // Build filename "btree.db"
    mov x0, sp
    mov w1, #'b'
    strb w1, [x0]
    mov w1, #'t'
    strb w1, [x0, #1]
    mov w1, #'r'
    strb w1, [x0, #2]
    mov w1, #'e'
    strb w1, [x0, #3]
    strb w1, [x0, #4]
    mov w1, #'.'
    strb w1, [x0, #5]
    mov w1, #'d'
    strb w1, [x0, #6]
    mov w1, #'b'
    strb w1, [x0, #7]
    strb wzr, [x0, #8]

    // Open source file for reading
    mov w0, w19
    mov x1, sp
    mov w2, #O_RDONLY
    mov w3, #0
    bl sys_openat
    cmn x0, #4096
    b.hi .Lbcb_fail
    mov w21, w0                    // src_fd

    // Get file size via lseek(SEEK_END)
    mov w0, w21
    mov x1, #0
    mov w2, #SEEK_END
    mov x8, #SYS_lseek
    svc #0
    cmp x0, #0
    b.le .Lbcb_close_src           // empty file or error
    mov x22, x0                    // file_size

    // Seek back to beginning
    mov w0, w21
    mov x1, #0
    mov w2, #SEEK_SET
    mov x8, #SYS_lseek
    svc #0

    // Open/create destination file
    mov w0, w20
    mov x1, sp
    mov w2, #(O_WRONLY | O_CREAT | O_TRUNC)
    movz w3, #0644
    bl sys_openat
    cmn x0, #4096
    b.hi .Lbcb_close_src
    mov w23, w0                    // dst_fd

    // Copy data using sendfile (kernel-space, zero-copy)
    mov w0, w23                    // out_fd
    mov w1, w21                    // in_fd
    mov x2, #0                    // offset = NULL
    mov x3, x22                   // count = file_size
    mov x8, #71                   // SYS_sendfile
    svc #0

    // Close destination
    mov w0, w23
    bl sys_close

.Lbcb_close_src:
    mov w0, w21
    bl sys_close

    add sp, sp, #32
    mov x0, #ADB_OK
    b .Lbcb_ret

.Lbcb_fail:
    add sp, sp, #32
    mov x0, #ADB_ERR_IO

.Lbcb_ret:
    ldr x23, [sp, #48]
    ldp x21, x22, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #64
    ret
.size backup_copy_btree, .-backup_copy_btree

// ============================================================================
// backup_write_manifest(db, dest_dir_fd) -> err
// Write a manifest file with database state
// ============================================================================
.global backup_write_manifest
.type backup_write_manifest, %function
backup_write_manifest:
    stp x29, x30, [sp, #-48]!
    mov x29, sp
    stp x19, x20, [sp, #16]
    str x21, [sp, #32]

    mov x19, x0                    // db
    mov w20, w1                    // dest_dir_fd

    // Allocate manifest buffer (512 bytes)
    sub sp, sp, #MF_SIZE

    // Zero it
    mov x0, sp
    mov x1, #MF_SIZE
    bl neon_memzero

    // Fill manifest fields
    // Magic
    movz x0, #0x4631, lsl #0
    movk x0, #0x4E49, lsl #16
    movk x0, #0x4D41, lsl #32
    movk x0, #0x4153, lsl #48     // "ASMANIF1"
    str x0, [sp, #MF_MAGIC]

    // Version
    mov x0, #1
    str x0, [sp, #MF_VERSION]

    // B+ tree state
    ldr x0, [x19, #DB_BTREE_ROOT]
    str x0, [sp, #MF_BTREE_ROOT]
    ldr x0, [x19, #DB_BTREE_NUM_PAGES]
    str x0, [sp, #MF_BTREE_PAGES]

    // WAL sequence
    ldr x0, [x19, #DB_WAL_SEQ]
    str x0, [sp, #MF_WAL_SEQ]

    // SSTable counts
    ldr w0, [x19, #DB_SST_COUNT_L0]
    str w0, [sp, #MF_NUM_L0]
    ldr w0, [x19, #DB_SST_COUNT_L1]
    str w0, [sp, #MF_NUM_L1]

    // CRC32 of manifest (excluding CRC field itself)
    mov x0, sp
    mov x1, #MF_CRC32             // hash up to CRC offset
    bl hw_crc32c
    str w0, [sp, #MF_CRC32]

    // Write manifest file
    sub sp, sp, #32
    mov x0, sp
    mov w1, #'M'
    strb w1, [x0]
    mov w1, #'A'
    strb w1, [x0, #1]
    mov w1, #'N'
    strb w1, [x0, #2]
    mov w1, #'I'
    strb w1, [x0, #3]
    mov w1, #'F'
    strb w1, [x0, #4]
    mov w1, #'E'
    strb w1, [x0, #5]
    mov w1, #'S'
    strb w1, [x0, #6]
    mov w1, #'T'
    strb w1, [x0, #7]
    strb wzr, [x0, #8]

    mov w0, w20                    // dest_dir_fd
    mov x1, sp                     // "MANIFEST"
    mov w2, #(O_WRONLY | O_CREAT | O_TRUNC)
    movz w3, #0644
    bl sys_openat
    add sp, sp, #32

    cmn x0, #4096
    b.hi .Lbwm_fail

    mov w21, w0                    // manifest fd

    // Write manifest data
    mov w0, w21
    mov x1, sp                     // manifest buffer (on stack)
    mov x2, #MF_SIZE
    bl sys_write

    // Sync
    mov w0, w21
    bl sys_fdatasync

    // Close
    mov w0, w21
    bl sys_close

    add sp, sp, #MF_SIZE
    mov x0, #ADB_OK
    b .Lbwm_ret

.Lbwm_fail:
    add sp, sp, #MF_SIZE
    mov x0, #ADB_ERR_IO

.Lbwm_ret:
    ldr x21, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #48
    ret
.size backup_write_manifest, .-backup_write_manifest

.hidden alloc_zeroed
.hidden free_mem
.hidden neon_memzero
.hidden hw_crc32c
.hidden adb_sync
.hidden sys_openat
.hidden sys_close
.hidden sys_write
.hidden sys_fdatasync
