// AssemblyDB - Error Code Definitions and Helpers
// Error codes are defined in const.s, this provides helper functions

.include "src/const.s"

.text

// ============================================================================
// is_error(code) -> bool
// Check if a return code indicates an error
// x0 = return code
// Returns: x0 = 1 if error (non-zero), 0 if OK
// ============================================================================
.global is_error
.type is_error, %function
is_error:
    cmp x0, #0
    cset w0, ne
    ret
.size is_error, .-is_error

// ============================================================================
// is_syscall_error(retval) -> bool
// Check if a syscall return value indicates error
// x0 = syscall return value
// Returns: x0 = 1 if error, 0 if OK
// ============================================================================
.global is_syscall_error
.type is_syscall_error, %function
is_syscall_error:
    cmn x0, #4096
    cset w0, hi
    ret
.size is_syscall_error, .-is_syscall_error

// ============================================================================
// syscall_to_adb_error(retval) -> adb error code
// Convert negative syscall errno to ADB error code
// x0 = negative errno from syscall
// Returns: x0 = ADB_ERR_* code
// ============================================================================
.global syscall_to_adb_error
.type syscall_to_adb_error, %function
syscall_to_adb_error:
    // If not an error, return OK
    cmn x0, #4096
    b.ls .Lste_ok

    neg x0, x0                  // Make errno positive

    // Map common errnos
    cmp x0, #2                  // ENOENT
    b.eq .Lste_not_found
    cmp x0, #12                 // ENOMEM
    b.eq .Lste_nomem
    cmp x0, #13                 // EACCES
    b.eq .Lste_locked
    cmp x0, #17                 // EEXIST
    b.eq .Lste_exists
    cmp x0, #11                 // EAGAIN
    b.eq .Lste_locked

    // Default: I/O error
    mov x0, #ADB_ERR_IO
    ret

.Lste_ok:
    mov x0, #ADB_OK
    ret
.Lste_not_found:
    mov x0, #ADB_ERR_NOT_FOUND
    ret
.Lste_nomem:
    mov x0, #ADB_ERR_NOMEM
    ret
.Lste_locked:
    mov x0, #ADB_ERR_LOCKED
    ret
.Lste_exists:
    mov x0, #ADB_ERR_EXISTS
    ret
.size syscall_to_adb_error, .-syscall_to_adb_error
