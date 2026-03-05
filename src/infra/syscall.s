// AssemblyDB - Linux AArch64 Syscall Wrappers
// All syscalls: x8=number, args in x0-x5, svc #0, result in x0

.include "src/const.s"

.text

// ----------------------------------------------------------------------------
// sys_openat(dirfd, pathname, flags, mode) -> fd or -errno
// ----------------------------------------------------------------------------
.global sys_openat
.type sys_openat, %function
sys_openat:
    mov x8, #SYS_openat
    svc #0
    ret
.size sys_openat, .-sys_openat

// ----------------------------------------------------------------------------
// sys_close(fd) -> 0 or -errno
// ----------------------------------------------------------------------------
.global sys_close
.type sys_close, %function
sys_close:
    mov x8, #SYS_close
    svc #0
    ret
.size sys_close, .-sys_close

// ----------------------------------------------------------------------------
// sys_read(fd, buf, count) -> bytes_read or -errno
// ----------------------------------------------------------------------------
.global sys_read
.type sys_read, %function
sys_read:
    mov x8, #SYS_read
    svc #0
    ret
.size sys_read, .-sys_read

// ----------------------------------------------------------------------------
// sys_write(fd, buf, count) -> bytes_written or -errno
// ----------------------------------------------------------------------------
.global sys_write
.type sys_write, %function
sys_write:
    mov x8, #SYS_write
    svc #0
    ret
.size sys_write, .-sys_write

// ----------------------------------------------------------------------------
// sys_pread64(fd, buf, count, offset) -> bytes_read or -errno
// ----------------------------------------------------------------------------
.global sys_pread64
.type sys_pread64, %function
sys_pread64:
    mov x8, #SYS_pread64
    svc #0
    ret
.size sys_pread64, .-sys_pread64

// ----------------------------------------------------------------------------
// sys_pwrite64(fd, buf, count, offset) -> bytes_written or -errno
// ----------------------------------------------------------------------------
.global sys_pwrite64
.type sys_pwrite64, %function
sys_pwrite64:
    mov x8, #SYS_pwrite64
    svc #0
    ret
.size sys_pwrite64, .-sys_pwrite64

// ----------------------------------------------------------------------------
// sys_lseek(fd, offset, whence) -> new_offset or -errno
// ----------------------------------------------------------------------------
.global sys_lseek
.type sys_lseek, %function
sys_lseek:
    mov x8, #SYS_lseek
    svc #0
    ret
.size sys_lseek, .-sys_lseek

// ----------------------------------------------------------------------------
// sys_fstat(fd, statbuf) -> 0 or -errno
// ----------------------------------------------------------------------------
.global sys_fstat
.type sys_fstat, %function
sys_fstat:
    mov x8, #SYS_fstat
    svc #0
    ret
.size sys_fstat, .-sys_fstat

// ----------------------------------------------------------------------------
// sys_mmap(addr, length, prot, flags, fd, offset) -> ptr or -errno
// x0=addr, x1=length, x2=prot, x3=flags, x4=fd, x5=offset
// ----------------------------------------------------------------------------
.global sys_mmap
.type sys_mmap, %function
sys_mmap:
    mov x8, #SYS_mmap
    svc #0
    ret
.size sys_mmap, .-sys_mmap

// ----------------------------------------------------------------------------
// sys_munmap(addr, length) -> 0 or -errno
// ----------------------------------------------------------------------------
.global sys_munmap
.type sys_munmap, %function
sys_munmap:
    mov x8, #SYS_munmap
    svc #0
    ret
.size sys_munmap, .-sys_munmap

// ----------------------------------------------------------------------------
// sys_madvise(addr, length, advice) -> 0 or -errno
// ----------------------------------------------------------------------------
.global sys_madvise
.type sys_madvise, %function
sys_madvise:
    mov x8, #SYS_madvise
    svc #0
    ret
.size sys_madvise, .-sys_madvise

// ----------------------------------------------------------------------------
// sys_ftruncate(fd, length) -> 0 or -errno
// ----------------------------------------------------------------------------
.global sys_ftruncate
.type sys_ftruncate, %function
sys_ftruncate:
    mov x8, #SYS_ftruncate
    svc #0
    ret
.size sys_ftruncate, .-sys_ftruncate

// ----------------------------------------------------------------------------
// sys_fsync(fd) -> 0 or -errno
// ----------------------------------------------------------------------------
.global sys_fsync
.type sys_fsync, %function
sys_fsync:
    mov x8, #SYS_fsync
    svc #0
    ret
.size sys_fsync, .-sys_fsync

// ----------------------------------------------------------------------------
// sys_fdatasync(fd) -> 0 or -errno
// ----------------------------------------------------------------------------
.global sys_fdatasync
.type sys_fdatasync, %function
sys_fdatasync:
    mov x8, #SYS_fdatasync
    svc #0
    ret
.size sys_fdatasync, .-sys_fdatasync

// ----------------------------------------------------------------------------
// sys_flock(fd, operation) -> 0 or -errno
// ----------------------------------------------------------------------------
.global sys_flock
.type sys_flock, %function
sys_flock:
    mov x8, #SYS_flock
    svc #0
    ret
.size sys_flock, .-sys_flock

// ----------------------------------------------------------------------------
// sys_mkdirat(dirfd, pathname, mode) -> 0 or -errno
// ----------------------------------------------------------------------------
.global sys_mkdirat
.type sys_mkdirat, %function
sys_mkdirat:
    mov x8, #SYS_mkdirat
    svc #0
    ret
.size sys_mkdirat, .-sys_mkdirat

// ----------------------------------------------------------------------------
// sys_unlinkat(dirfd, pathname, flags) -> 0 or -errno
// ----------------------------------------------------------------------------
.global sys_unlinkat
.type sys_unlinkat, %function
sys_unlinkat:
    mov x8, #SYS_unlinkat
    svc #0
    ret
.size sys_unlinkat, .-sys_unlinkat

// ----------------------------------------------------------------------------
// sys_renameat2(olddirfd, oldpath, newdirfd, newpath, flags) -> 0 or -errno
// ----------------------------------------------------------------------------
.global sys_renameat2
.type sys_renameat2, %function
sys_renameat2:
    mov x8, #SYS_renameat2
    svc #0
    ret
.size sys_renameat2, .-sys_renameat2

// ----------------------------------------------------------------------------
// sys_clone(flags, stack, parent_tid, tls, child_tid) -> tid or -errno
// ----------------------------------------------------------------------------
.global sys_clone
.type sys_clone, %function
sys_clone:
    mov x8, #SYS_clone
    svc #0
    ret
.size sys_clone, .-sys_clone

// ----------------------------------------------------------------------------
// sys_exit(status) -> does not return
// ----------------------------------------------------------------------------
.global sys_exit
.type sys_exit, %function
sys_exit:
    mov x8, #SYS_exit
    svc #0
    ret     // unreachable but keeps assembler happy
.size sys_exit, .-sys_exit

// ----------------------------------------------------------------------------
// sys_exit_group(status) -> does not return
// ----------------------------------------------------------------------------
.global sys_exit_group
.type sys_exit_group, %function
sys_exit_group:
    mov x8, #SYS_exit_group
    svc #0
    ret
.size sys_exit_group, .-sys_exit_group

// ----------------------------------------------------------------------------
// sys_futex(uaddr, op, val, timeout, uaddr2, val3) -> result or -errno
// x0=uaddr, x1=op, x2=val, x3=timeout, x4=uaddr2, x5=val3
// ----------------------------------------------------------------------------
.global sys_futex
.type sys_futex, %function
sys_futex:
    mov x8, #SYS_futex
    svc #0
    ret
.size sys_futex, .-sys_futex

// ----------------------------------------------------------------------------
// sys_getpid() -> pid
// ----------------------------------------------------------------------------
.global sys_getpid
.type sys_getpid, %function
sys_getpid:
    mov x8, #SYS_getpid
    svc #0
    ret
.size sys_getpid, .-sys_getpid

// ----------------------------------------------------------------------------
// sys_clock_gettime(clk_id, timespec_ptr) -> 0 or -errno
// ----------------------------------------------------------------------------
.global sys_clock_gettime
.type sys_clock_gettime, %function
sys_clock_gettime:
    mov x8, #SYS_clock_gettime
    svc #0
    ret
.size sys_clock_gettime, .-sys_clock_gettime

// ----------------------------------------------------------------------------
// sys_getdents64(fd, buf, buf_size) -> bytes_read or -errno
// ----------------------------------------------------------------------------
.global sys_getdents64
.type sys_getdents64, %function
sys_getdents64:
    mov x8, #SYS_getdents64
    svc #0
    ret
.size sys_getdents64, .-sys_getdents64

// ----------------------------------------------------------------------------
// sys_writev(fd, iov, iovcnt) -> bytes_written or -errno
// ----------------------------------------------------------------------------
.global sys_writev
.type sys_writev, %function
sys_writev:
    mov x8, #SYS_writev
    svc #0
    ret
.size sys_writev, .-sys_writev

// ============================================================================
// Helper: check_syscall_error
// x0 = syscall return value
// Returns: x0 unchanged. Sets flags for conditional branch.
// Usage: bl check_syscall_error; b.mi .error_handler
// ============================================================================
.global check_syscall_error
.type check_syscall_error, %function
check_syscall_error:
    cmn x0, #4096
    ret
.size check_syscall_error, .-check_syscall_error

// ============================================================================
// mmap_anon_rw(size) -> ptr or -errno
// Convenience: mmap anonymous read-write private memory
// x0 = size (will be page-aligned)
// ============================================================================
.global mmap_anon_rw
.type mmap_anon_rw, %function
mmap_anon_rw:
    mov x1, x0             // length
    mov x0, #0             // addr = NULL
    mov x2, #PROT_RW       // prot = READ|WRITE
    mov x3, #MAP_ANON_PRIV // flags = PRIVATE|ANONYMOUS
    mov x4, #-1            // fd = -1
    mov x5, #0             // offset = 0
    mov x8, #SYS_mmap
    svc #0
    ret
.size mmap_anon_rw, .-mmap_anon_rw

// ============================================================================
// mmap_file_rw(fd, length) -> ptr or -errno
// Convenience: mmap file shared read-write
// x0 = fd, x1 = length
// ============================================================================
.global mmap_file_rw
.type mmap_file_rw, %function
mmap_file_rw:
    mov x4, x0             // fd
    mov x0, #0             // addr = NULL
                            // x1 already has length
    mov x2, #PROT_RW       // prot = READ|WRITE
    mov x3, #MAP_SHARED    // flags = SHARED
    mov x5, #0             // offset = 0
    mov x8, #SYS_mmap
    svc #0
    ret
.size mmap_file_rw, .-mmap_file_rw
