// AssemblyDB - Backup Manager
// Full backup: copy all database files to destination directory
// Incremental: copy only WAL segments since last backup
//
// Backup format:
//   dest_dir/
//     data.btree   - B+ tree data file
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

    cbz x0, .Lbf_fail              // NULL db guard
    cbz x1, .Lbf_fail              // NULL dest_path guard

    mov x19, x0                    // db
    mov x20, x1                    // dest_path

    // Create destination directory
    mov x0, #AT_FDCWD
    mov x1, x20
    mov w2, #MODE_DIR
    mov x8, #SYS_mkdirat
    svc #0
    cmp x0, #0
    b.ge .Lbf_mkdir_dest_ok
    cmn x0, #17
    b.eq .Lbf_mkdir_dest_ok
    b .Lbf_fail

.Lbf_mkdir_dest_ok:

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
    cmp x0, #0
    b.ge .Lbf_mkdir_wal_ok
    cmn x0, #17
    b.eq .Lbf_mkdir_wal_ok
    add sp, sp, #256
    mov x0, #ADB_ERR_IO
    b .Lbf_close_err

.Lbf_mkdir_wal_ok:

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
    cmp x0, #0
    b.ge .Lbf_mkdir_sst_ok
    cmn x0, #17
    b.eq .Lbf_mkdir_sst_ok
    add sp, sp, #256
    mov x0, #ADB_ERR_IO
    b .Lbf_close_err

.Lbf_mkdir_sst_ok:

    add sp, sp, #256

    // Sync the source database first
    mov x0, x19
    bl adb_sync
    cbnz x0, .Lbf_close_err

    // Copy B+ tree file
    ldr w0, [x19, #DB_DIR_FD]     // src dir_fd
    mov w1, w21                    // dst dir_fd
    bl backup_copy_btree
    cbnz x0, .Lbf_close_err

    // Copy manifest (write current state)
    mov x0, x19
    mov w1, w21
    bl backup_write_manifest
    cbnz x0, .Lbf_close_err

.Lbf_fsync_retry:
    mov w0, w21
    bl sys_fsync
    cmn x0, #4                     // EINTR
    b.eq .Lbf_fsync_retry
    cbz x0, .Lbf_fsync_ok
    mov x0, #ADB_ERR_IO
    b .Lbf_close_err
.Lbf_fsync_ok:

    // Close dest fd
    mov w0, w21
    bl sys_close

    mov x0, #ADB_OK
    b .Lbf_ret

.Lbf_close_err:
    mov x22, x0
    mov w0, w21
    bl sys_close
    mov x0, x22
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
// Copy data.btree from source to destination
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

    sub sp, sp, #64

    // Build filename "data.btree"
    mov x0, sp
    mov w1, #'d'
    strb w1, [x0]
    mov w1, #'a'
    strb w1, [x0, #1]
    mov w1, #'t'
    strb w1, [x0, #2]
    mov w1, #'a'
    strb w1, [x0, #3]
    mov w1, #'.'
    strb w1, [x0, #4]
    mov w1, #'b'
    strb w1, [x0, #5]
    mov w1, #'t'
    strb w1, [x0, #6]
    mov w1, #'r'
    strb w1, [x0, #7]
    mov w1, #'e'
    strb w1, [x0, #8]
    strb w1, [x0, #9]
    strb wzr, [x0, #10]

    // Build temp filename "data.btree.tmp"
    add x0, sp, #16
    mov w1, #'d'
    strb w1, [x0]
    mov w1, #'a'
    strb w1, [x0, #1]
    mov w1, #'t'
    strb w1, [x0, #2]
    mov w1, #'a'
    strb w1, [x0, #3]
    mov w1, #'.'
    strb w1, [x0, #4]
    mov w1, #'b'
    strb w1, [x0, #5]
    mov w1, #'t'
    strb w1, [x0, #6]
    mov w1, #'r'
    strb w1, [x0, #7]
    mov w1, #'e'
    strb w1, [x0, #8]
    strb w1, [x0, #9]
    mov w1, #'.'
    strb w1, [x0, #10]
    mov w1, #'t'
    strb w1, [x0, #11]
    mov w1, #'m'
    strb w1, [x0, #12]
    mov w1, #'p'
    strb w1, [x0, #13]
    strb wzr, [x0, #14]

    // Open source file for reading
    mov w0, w19
    mov x1, sp
    mov w2, #O_RDONLY
    mov w3, #0
    bl sys_openat
    cmn x0, #4096
    b.hi .Lbcb_cleanup_tmp_fail
    mov w21, w0                    // src_fd

    // Get file size via lseek(SEEK_END)
    mov w0, w21
    mov x1, #0
    mov w2, #SEEK_END
    mov x8, #SYS_lseek
    svc #0
    cmp x0, #0
    b.lt .Lbcb_err_close_src
    cbz x0, .Lbcb_err_close_src   // reject 0-byte source file
    mov x22, x0                    // remaining_bytes

    // Seek back to beginning
    mov w0, w21
    mov x1, #0
    mov w2, #SEEK_SET
    mov x8, #SYS_lseek
    svc #0
    cmp x0, #0
    b.lt .Lbcb_err_close_src

    // Open/create destination temp file
    mov w0, w20
    add x1, sp, #16
    mov w2, #(O_WRONLY | O_CREAT | O_TRUNC)
    movz w3, #0644
    bl sys_openat
    cmn x0, #4096
    b.hi .Lbcb_err_close_src
    mov w23, w0                    // dst_fd

    // Copy data using sendfile (kernel-space, zero-copy)
.Lbcb_copy_loop:
    cbz x22, .Lbcb_sync_dst
    mov w0, w23                    // out_fd
    mov w1, w21                    // in_fd
    mov x2, #0                    // offset = NULL
    mov x3, x22                   // count = remaining
    mov x8, #SYS_sendfile
    svc #0
    cmn x0, #4
    b.eq .Lbcb_copy_loop           // retry on EINTR
    cmp x0, #0
    b.le .Lbcb_err_close_both      // negative=error, zero=no progress
    sub x22, x22, x0
    b .Lbcb_copy_loop

.Lbcb_sync_dst:
    mov w0, w23
    bl sys_fdatasync
    cmn x0, #4
    b.eq .Lbcb_sync_dst
    cbnz x0, .Lbcb_err_close_both

.Lbcb_close_dst:
    mov w0, w23
    bl sys_close
    cbnz x0, .Lbcb_err_close_src

.Lbcb_close_src:
    mov w0, w21
    bl sys_close
    cbnz x0, .Lbcb_cleanup_tmp_fail

    mov w0, w20
    add x1, sp, #16
    mov w2, w20
    mov x3, sp
    mov w4, #0
    bl sys_renameat2
    cbnz x0, .Lbcb_cleanup_tmp_fail

.Lbcb_dir_fsync:
    mov w0, w20
    bl sys_fsync
    cmn x0, #4
    b.eq .Lbcb_dir_fsync
    cbnz x0, .Lbcb_cleanup_tmp_fail

    add sp, sp, #64
    mov x0, #ADB_OK
    b .Lbcb_ret

.Lbcb_err_close_both:
    mov w0, w23
    bl sys_close

.Lbcb_err_close_src:
    mov w0, w21
    bl sys_close
    b .Lbcb_cleanup_tmp_fail

.Lbcb_cleanup_tmp_fail:
    mov w0, w20
    add x1, sp, #16
    mov w2, #0
    bl sys_unlinkat

    add sp, sp, #64
    mov x0, #ADB_ERR_IO

.Lbcb_ret:
    ldr x23, [sp, #48]
    ldp x21, x22, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #64
    ret
.size backup_copy_btree, .-backup_copy_btree

// ============================================================================
// backup_copy_manifest(src_dir_fd, dst_dir_fd) -> err
// Copy MANIFEST file from source to destination
// ============================================================================
.global backup_copy_manifest
.type backup_copy_manifest, %function
backup_copy_manifest:
    stp x29, x30, [sp, #-64]!
    mov x29, sp
    stp x19, x20, [sp, #16]
    stp x21, x22, [sp, #32]
    str x23, [sp, #48]

    mov w19, w0                    // src_dir_fd
    mov w20, w1                    // dst_dir_fd

    sub sp, sp, #(MF_SIZE + 64)
    mov x21, sp                    // manifest buffer
    add x22, x21, #MF_SIZE         // filename buffer

    // Build filename "MANIFEST"
    mov w0, #'M'
    strb w0, [x22]
    mov w0, #'A'
    strb w0, [x22, #1]
    mov w0, #'N'
    strb w0, [x22, #2]
    mov w0, #'I'
    strb w0, [x22, #3]
    mov w0, #'F'
    strb w0, [x22, #4]
    mov w0, #'E'
    strb w0, [x22, #5]
    mov w0, #'S'
    strb w0, [x22, #6]
    mov w0, #'T'
    strb w0, [x22, #7]
    strb wzr, [x22, #8]

    // Open source MANIFEST
    mov w0, w19
    mov x1, x22
    mov w2, #O_RDONLY
    mov w3, #0
    bl sys_openat
    cmn x0, #4096
    b.hi .Lbcm_cleanup_tmp_fail
    mov w23, w0                    // src_fd
    mov x22, #0                    // bytes_read

.Lbcm_read_loop:
    cmp x22, #MF_SIZE
    b.ge .Lbcm_read_done
    mov w0, w23
    add x1, x21, x22
    mov x2, #MF_SIZE
    sub x2, x2, x22
    bl sys_read
    cmn x0, #4
    b.eq .Lbcm_read_loop           // retry on EINTR
    cmp x0, #0
    b.lt .Lbcm_err_close_src
    cbz x0, .Lbcm_err_close_src
    add x22, x22, x0
    b .Lbcm_read_loop

.Lbcm_read_done:
    mov w0, w23
    add x1, x21, #MF_SIZE
    mov x2, #1
    bl sys_read
    cmp x0, #0
    b.ne .Lbcm_err_close_src

    mov w0, w23
    bl sys_close
    cbnz x0, .Lbcm_cleanup_tmp_fail

    // Build destination temp filename "MANIFEST.tmp"
    add x22, x21, #MF_SIZE
    mov w0, #'M'
    strb w0, [x22]
    mov w0, #'A'
    strb w0, [x22, #1]
    mov w0, #'N'
    strb w0, [x22, #2]
    mov w0, #'I'
    strb w0, [x22, #3]
    mov w0, #'F'
    strb w0, [x22, #4]
    mov w0, #'E'
    strb w0, [x22, #5]
    mov w0, #'S'
    strb w0, [x22, #6]
    mov w0, #'T'
    strb w0, [x22, #7]
    mov w0, #'.'
    strb w0, [x22, #8]
    mov w0, #'t'
    strb w0, [x22, #9]
    mov w0, #'m'
    strb w0, [x22, #10]
    mov w0, #'p'
    strb w0, [x22, #11]
    strb wzr, [x22, #12]

    // Open destination MANIFEST.tmp
    mov w0, w20
    mov x1, x22
    mov w2, #(O_WRONLY | O_CREAT | O_TRUNC)
    movz w3, #0644
    bl sys_openat
    cmn x0, #4096
    b.hi .Lbcm_cleanup_tmp_fail
    mov w23, w0                    // dst_fd

    mov x22, #0
.Lbcm_write_loop:
    cmp x22, #MF_SIZE
    b.ge .Lbcm_sync_dst
    mov w0, w23
    add x1, x21, x22
    mov x2, #MF_SIZE
    sub x2, x2, x22
    bl sys_write
    cmn x0, #4
    b.eq .Lbcm_write_loop          // retry on EINTR
    cmp x0, #0
    b.le .Lbcm_err_close_dst
    add x22, x22, x0
    b .Lbcm_write_loop

.Lbcm_sync_dst:
    mov w0, w23
    bl sys_fdatasync
    cmn x0, #4
    b.eq .Lbcm_sync_dst
    cbnz x0, .Lbcm_err_close_dst

    mov w0, w23
    bl sys_close
    cbnz x0, .Lbcm_cleanup_tmp_fail

    // rename MANIFEST.tmp -> MANIFEST
    add x22, x21, #MF_SIZE
    mov w0, #'M'
    strb w0, [x22]
    mov w0, #'A'
    strb w0, [x22, #1]
    mov w0, #'N'
    strb w0, [x22, #2]
    mov w0, #'I'
    strb w0, [x22, #3]
    mov w0, #'F'
    strb w0, [x22, #4]
    mov w0, #'E'
    strb w0, [x22, #5]
    mov w0, #'S'
    strb w0, [x22, #6]
    mov w0, #'T'
    strb w0, [x22, #7]
    mov w0, #'.'
    strb w0, [x22, #8]
    mov w0, #'t'
    strb w0, [x22, #9]
    mov w0, #'m'
    strb w0, [x22, #10]
    mov w0, #'p'
    strb w0, [x22, #11]
    strb wzr, [x22, #12]

    add x3, x21, #(MF_SIZE + 24)
    mov w0, #'M'
    strb w0, [x3]
    mov w0, #'A'
    strb w0, [x3, #1]
    mov w0, #'N'
    strb w0, [x3, #2]
    mov w0, #'I'
    strb w0, [x3, #3]
    mov w0, #'F'
    strb w0, [x3, #4]
    mov w0, #'E'
    strb w0, [x3, #5]
    mov w0, #'S'
    strb w0, [x3, #6]
    mov w0, #'T'
    strb w0, [x3, #7]
    strb wzr, [x3, #8]

    mov w0, w20
    mov x1, x22
    mov w2, w20
    mov w4, #0
    bl sys_renameat2
    cbnz x0, .Lbcm_cleanup_tmp_fail

.Lbcm_dir_fsync:
    mov w0, w20
    bl sys_fsync
    cmn x0, #4
    b.eq .Lbcm_dir_fsync
    cbnz x0, .Lbcm_cleanup_tmp_fail

    add sp, sp, #(MF_SIZE + 64)
    mov x0, #ADB_OK
    b .Lbcm_ret

.Lbcm_err_close_dst:
    mov w0, w23
    bl sys_close
    b .Lbcm_cleanup_tmp_fail

.Lbcm_err_close_src:
    mov w0, w23
    bl sys_close

.Lbcm_cleanup_tmp_fail:
    add x22, x21, #MF_SIZE
    mov w0, #'M'
    strb w0, [x22]
    mov w0, #'A'
    strb w0, [x22, #1]
    mov w0, #'N'
    strb w0, [x22, #2]
    mov w0, #'I'
    strb w0, [x22, #3]
    mov w0, #'F'
    strb w0, [x22, #4]
    mov w0, #'E'
    strb w0, [x22, #5]
    mov w0, #'S'
    strb w0, [x22, #6]
    mov w0, #'T'
    strb w0, [x22, #7]
    mov w0, #'.'
    strb w0, [x22, #8]
    mov w0, #'t'
    strb w0, [x22, #9]
    mov w0, #'m'
    strb w0, [x22, #10]
    mov w0, #'p'
    strb w0, [x22, #11]
    strb wzr, [x22, #12]

    mov w0, w20
    mov x1, x22
    mov w2, #0
    bl sys_unlinkat

    add sp, sp, #(MF_SIZE + 64)
    mov x0, #ADB_ERR_IO

.Lbcm_ret:
    ldr x23, [sp, #48]
    ldp x21, x22, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #64
    ret
.size backup_copy_manifest, .-backup_copy_manifest

// ============================================================================
// backup_write_manifest(db, dest_dir_fd) -> err
// Write a manifest file with database state
// ============================================================================
.global backup_write_manifest
.type backup_write_manifest, %function
backup_write_manifest:
    stp x29, x30, [sp, #-64]!
    mov x29, sp
    stp x19, x20, [sp, #16]
    stp x21, x22, [sp, #32]
    str x23, [sp, #48]

    mov x19, x0                    // db
    mov w20, w1                    // dest_dir_fd

    sub sp, sp, #(MF_SIZE + 64)
    mov x21, sp                    // manifest buffer
    add x22, x21, #MF_SIZE         // filename buffer

    mov x0, x21
    mov x1, #MF_SIZE
    bl neon_memzero

    movz x0, #0x4631, lsl #0
    movk x0, #0x4E49, lsl #16
    movk x0, #0x4D41, lsl #32
    movk x0, #0x4153, lsl #48
    str x0, [x21, #MF_MAGIC]

    mov x0, #1
    str x0, [x21, #MF_VERSION]

    ldr x0, [x19, #DB_BTREE_ROOT]
    str x0, [x21, #MF_BTREE_ROOT]
    ldr x0, [x19, #DB_BTREE_NUM_PAGES]
    str x0, [x21, #MF_BTREE_PAGES]

    ldr x0, [x19, #DB_WAL_SEQ]
    str x0, [x21, #MF_WAL_SEQ]

    // SSTable data is already merged into B+tree by compact_memtable,
    // so backup only needs the B+tree. Set SST counts to 0.
    str wzr, [x21, #MF_NUM_L0]
    str wzr, [x21, #MF_NUM_L1]

    mov x0, x21
    mov x1, #MF_CRC32
    bl hw_crc32c
    str w0, [x21, #MF_CRC32]

    mov w0, #'M'
    strb w0, [x22]
    mov w0, #'A'
    strb w0, [x22, #1]
    mov w0, #'N'
    strb w0, [x22, #2]
    mov w0, #'I'
    strb w0, [x22, #3]
    mov w0, #'F'
    strb w0, [x22, #4]
    mov w0, #'E'
    strb w0, [x22, #5]
    mov w0, #'S'
    strb w0, [x22, #6]
    mov w0, #'T'
    strb w0, [x22, #7]
    mov w0, #'.'
    strb w0, [x22, #8]
    mov w0, #'t'
    strb w0, [x22, #9]
    mov w0, #'m'
    strb w0, [x22, #10]
    mov w0, #'p'
    strb w0, [x22, #11]
    strb wzr, [x22, #12]

    mov w0, w20
    mov x1, x22
    mov w2, #(O_WRONLY | O_CREAT | O_TRUNC)
    movz w3, #0644
    bl sys_openat
    cmn x0, #4096
    b.hi .Lbwm_cleanup_fail
    mov w23, w0                    // tmp fd

    mov x22, #0
.Lbwm_write_loop:
    cmp x22, #MF_SIZE
    b.ge .Lbwm_write_done
    mov w0, w23
    add x1, x21, x22
    mov x2, #MF_SIZE
    sub x2, x2, x22
    bl sys_write
    cmn x0, #4
    b.eq .Lbwm_write_loop          // retry on EINTR
    cmp x0, #0
    b.le .Lbwm_err_close_tmp
    add x22, x22, x0
    b .Lbwm_write_loop

.Lbwm_write_done:
    add x22, x21, #MF_SIZE

.Lbwm_sync_tmp:
    mov w0, w23
    bl sys_fdatasync
    cmn x0, #4
    b.eq .Lbwm_sync_tmp
    cbnz x0, .Lbwm_err_close_tmp

    mov w0, w23
    bl sys_close
    cbnz x0, .Lbwm_cleanup_fail

    add x3, x21, #(MF_SIZE + 24)
    mov w0, #'M'
    strb w0, [x3]
    mov w0, #'A'
    strb w0, [x3, #1]
    mov w0, #'N'
    strb w0, [x3, #2]
    mov w0, #'I'
    strb w0, [x3, #3]
    mov w0, #'F'
    strb w0, [x3, #4]
    mov w0, #'E'
    strb w0, [x3, #5]
    mov w0, #'S'
    strb w0, [x3, #6]
    mov w0, #'T'
    strb w0, [x3, #7]
    strb wzr, [x3, #8]

    mov w0, w20
    mov x1, x22
    mov w2, w20
    mov w4, #0
    bl sys_renameat2
    cbnz x0, .Lbwm_cleanup_fail

.Lbwm_dir_fsync:
    mov w0, w20
    bl sys_fsync
    cmn x0, #4
    b.eq .Lbwm_dir_fsync
    cbnz x0, .Lbwm_cleanup_fail

    add sp, sp, #(MF_SIZE + 64)
    mov x0, #ADB_OK
    b .Lbwm_ret

.Lbwm_err_close_tmp:
    mov w0, w23
    bl sys_close
    add x22, x21, #MF_SIZE
    b .Lbwm_cleanup_fail

.Lbwm_cleanup_fail:
    mov w0, #'M'
    strb w0, [x22]
    mov w0, #'A'
    strb w0, [x22, #1]
    mov w0, #'N'
    strb w0, [x22, #2]
    mov w0, #'I'
    strb w0, [x22, #3]
    mov w0, #'F'
    strb w0, [x22, #4]
    mov w0, #'E'
    strb w0, [x22, #5]
    mov w0, #'S'
    strb w0, [x22, #6]
    mov w0, #'T'
    strb w0, [x22, #7]
    mov w0, #'.'
    strb w0, [x22, #8]
    mov w0, #'t'
    strb w0, [x22, #9]
    mov w0, #'m'
    strb w0, [x22, #10]
    mov w0, #'p'
    strb w0, [x22, #11]
    strb wzr, [x22, #12]

    mov w0, w20
    mov x1, x22
    mov w2, #0
    bl sys_unlinkat

    add sp, sp, #(MF_SIZE + 64)
    mov x0, #ADB_ERR_IO

.Lbwm_ret:
    ldr x23, [sp, #48]
    ldp x21, x22, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #64
    ret
.size backup_write_manifest, .-backup_write_manifest

.global backup_validate_manifest
.type backup_validate_manifest, %function
backup_validate_manifest:
    stp x29, x30, [sp, #-64]!
    mov x29, sp
    stp x19, x20, [sp, #16]
    stp x21, x22, [sp, #32]
    str x23, [sp, #48]

    mov w19, w0

    sub sp, sp, #(MF_SIZE + 32)
    mov x21, sp
    add x22, x21, #MF_SIZE
    mov w0, #'M'
    strb w0, [x22]
    mov w0, #'A'
    strb w0, [x22, #1]
    mov w0, #'N'
    strb w0, [x22, #2]
    mov w0, #'I'
    strb w0, [x22, #3]
    mov w0, #'F'
    strb w0, [x22, #4]
    mov w0, #'E'
    strb w0, [x22, #5]
    mov w0, #'S'
    strb w0, [x22, #6]
    mov w0, #'T'
    strb w0, [x22, #7]
    strb wzr, [x22, #8]

    mov w0, w19
    mov x1, x22
    mov w2, #O_RDONLY
    mov w3, #0
    bl sys_openat
    cmn x0, #4096
    b.hi .Lbvm_fail
    mov w20, w0
    mov x22, #0

.Lbvm_read_loop:
    cmp x22, #MF_SIZE
    b.ge .Lbvm_close_validate
    mov w0, w20
    add x1, x21, x22
    mov x2, #MF_SIZE
    sub x2, x2, x22
    bl sys_read
    cmn x0, #4
    b.eq .Lbvm_read_loop            // retry on EINTR
    cmp x0, #0
    b.lt .Lbvm_close_fail
    cbz x0, .Lbvm_close_fail
    add x22, x22, x0
    b .Lbvm_read_loop

.Lbvm_close_validate:
    mov w0, w20
    add x1, x21, #MF_SIZE
    mov x2, #1
    bl sys_read
    cmp x0, #0
    b.ne .Lbvm_close_fail

    mov w0, w20
    bl sys_close
    cbnz x0, .Lbvm_fail

    ldr x0, [x21, #MF_MAGIC]
    movz x1, #0x4631, lsl #0
    movk x1, #0x4E49, lsl #16
    movk x1, #0x4D41, lsl #32
    movk x1, #0x4153, lsl #48
    cmp x0, x1
    b.ne .Lbvm_fail

    ldr x0, [x21, #MF_VERSION]
    cmp x0, #1
    b.ne .Lbvm_fail

    ldr w23, [x21, #MF_CRC32]
    mov x0, x21
    mov x1, #MF_CRC32
    bl hw_crc32c
    cmp w0, w23
    b.ne .Lbvm_fail

    ldr x0, [x21, #MF_BTREE_PAGES]
    cbz x0, .Lbvm_fail
    lsr x1, x0, #24
    cbnz x1, .Lbvm_fail           // reject > 16M pages (64GB)
    ldr x1, [x21, #MF_BTREE_ROOT]
    cmp x1, x0
    b.hs .Lbvm_fail
    ldr w0, [x21, #MF_NUM_L0]
    cmp w0, #16
    b.hi .Lbvm_fail
    ldr w0, [x21, #MF_NUM_L1]
    cmp w0, #16
    b.hi .Lbvm_fail

    // Validate data.btree is consistent with manifest metadata
    add x22, x21, #MF_SIZE
    mov w0, #'d'
    strb w0, [x22]
    mov w0, #'a'
    strb w0, [x22, #1]
    mov w0, #'t'
    strb w0, [x22, #2]
    mov w0, #'a'
    strb w0, [x22, #3]
    mov w0, #'.'
    strb w0, [x22, #4]
    mov w0, #'b'
    strb w0, [x22, #5]
    mov w0, #'t'
    strb w0, [x22, #6]
    mov w0, #'r'
    strb w0, [x22, #7]
    mov w0, #'e'
    strb w0, [x22, #8]
    strb w0, [x22, #9]
    strb wzr, [x22, #10]

    mov w0, w19
    mov x1, x22
    mov w2, #O_RDONLY
    mov w3, #0
    bl sys_openat
    cmn x0, #4096
    b.hi .Lbvm_fail
    mov w20, w0

    mov w0, w20
    mov x1, #0
    mov w2, #SEEK_END
    mov x8, #SYS_lseek
    svc #0
    cmp x0, #0
    b.lt .Lbvm_close_fail
    mov x22, x0

    and x0, x22, #4095
    cbnz x0, .Lbvm_close_fail

    ldr x1, [x21, #MF_BTREE_PAGES]
    lsl x1, x1, #12
    cmp x22, x1
    b.lo .Lbvm_close_fail

    ldr x3, [x21, #MF_BTREE_ROOT]
    lsl x3, x3, #12
    mov w0, w20
    mov x1, x3
    mov w2, #SEEK_SET
    mov x8, #SYS_lseek
    svc #0
    cmp x0, #0
    b.lt .Lbvm_close_fail

    add x22, x21, #MF_SIZE
.Lbvm_read_root:
    mov w0, w20
    mov x1, x22
    mov x2, #16
    bl sys_read
    cmn x0, #4
    b.eq .Lbvm_read_root            // retry on EINTR
    cmp x0, #16
    b.ne .Lbvm_close_fail

    ldrh w0, [x22, #PH_PAGE_TYPE]
    cmp w0, #PAGE_TYPE_INTERNAL
    b.eq .Lbvm_close_btree_ok
    cmp w0, #PAGE_TYPE_LEAF
    b.ne .Lbvm_close_fail

.Lbvm_close_btree_ok:
    mov w0, w20
    bl sys_close
    cbnz x0, .Lbvm_fail

    add sp, sp, #(MF_SIZE + 32)
    mov x0, #ADB_OK
    b .Lbvm_ret

.Lbvm_close_fail:
    mov w0, w20
    bl sys_close

.Lbvm_fail:
    add sp, sp, #(MF_SIZE + 32)
    mov x0, #ADB_ERR_IO

.Lbvm_ret:
    ldr x23, [sp, #48]
    ldp x21, x22, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #64
    ret
.size backup_validate_manifest, .-backup_validate_manifest

.hidden alloc_zeroed
.hidden free_mem
.hidden neon_memzero
.hidden hw_crc32c
.hidden sys_openat
.hidden sys_read
.hidden sys_close
.hidden sys_unlinkat
.hidden sys_renameat2
.hidden sys_write
.hidden sys_fdatasync
.hidden sys_fsync
