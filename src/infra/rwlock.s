// AssemblyDB - Reader-Writer Lock
// Allows multiple concurrent readers OR one exclusive writer
// Layout: [4B state][4B writer_wait]
//   state: 0=free, >0=reader_count, -1=write_locked
//   writer_wait: futex for writer waiting

.include "src/const.s"

.text

// ============================================================================
// rwlock_init(rwlock_ptr) -> void
// Initialize rwlock to unlocked state
// x0 = pointer to rwlock (8 bytes: 4B state + 4B writer_wait)
// ============================================================================
.global rwlock_init
.type rwlock_init, %function
rwlock_init:
    str xzr, [x0]                 // zero both words
    ret
.size rwlock_init, .-rwlock_init

// ============================================================================
// rwlock_rdlock(rwlock_ptr) -> void
// Acquire read lock (shared)
// x0 = pointer to rwlock
// ============================================================================
.global rwlock_rdlock
.type rwlock_rdlock, %function
rwlock_rdlock:
    stp x29, x30, [sp, #-32]!
    mov x29, sp
    str x19, [sp, #16]

    mov x19, x0

.Lrl_retry:
    ldaxr w1, [x19]
    cmp w1, #0
    b.lt .Lrl_wait                 // write-locked, wait
    add w2, w1, #1                // increment reader count
    stlxr w3, w2, [x19]
    cbnz w3, .Lrl_retry           // CAS failed, retry

    ldr x19, [sp, #16]
    ldp x29, x30, [sp], #32
    ret

.Lrl_wait:
    clrex
    // Spin briefly
    mov w0, #16
.Lrl_spin:
    sub w0, w0, #1
    cbz w0, .Lrl_futex_wait
    yield
    ldr w1, [x19]
    cmp w1, #0
    b.ge .Lrl_retry
    b .Lrl_spin

.Lrl_futex_wait:
    // Wait on the state word
    mov x0, x19
    mov w1, #FUTEX_WAIT_PRIVATE
    mov w2, #-1                    // expected: write-locked
    mov x3, #0
    mov x4, #0
    mov x5, #0
    bl sys_futex
    b .Lrl_retry
.size rwlock_rdlock, .-rwlock_rdlock

// ============================================================================
// rwlock_rdunlock(rwlock_ptr) -> void
// Release read lock
// x0 = pointer to rwlock
// ============================================================================
.global rwlock_rdunlock
.type rwlock_rdunlock, %function
rwlock_rdunlock:
    stp x29, x30, [sp, #-32]!
    mov x29, sp
    str x19, [sp, #16]

    mov x19, x0

.Lru_retry:
    ldaxr w1, [x19]
    sub w2, w1, #1
    stlxr w3, w2, [x19]
    cbnz w3, .Lru_retry

    // If we were the last reader (count now 0), wake any waiting writer
    cbnz w2, .Lru_done

    mov x0, x19
    mov w1, #FUTEX_WAKE_PRIVATE
    mov w2, #1
    mov x3, #0
    mov x4, #0
    mov x5, #0
    bl sys_futex

.Lru_done:
    ldr x19, [sp, #16]
    ldp x29, x30, [sp], #32
    ret
.size rwlock_rdunlock, .-rwlock_rdunlock

// ============================================================================
// rwlock_wrlock(rwlock_ptr) -> void
// Acquire write lock (exclusive)
// x0 = pointer to rwlock
// ============================================================================
.global rwlock_wrlock
.type rwlock_wrlock, %function
rwlock_wrlock:
    stp x29, x30, [sp, #-32]!
    mov x29, sp
    str x19, [sp, #16]

    mov x19, x0

.Lwl_retry:
    mov w1, #0
    mov w2, #-1                    // -1 = write locked
    ldaxr w3, [x19]
    cmp w3, w1                     // must be 0 (free)
    b.ne .Lwl_wait
    stlxr w4, w2, [x19]
    cbnz w4, .Lwl_retry

    ldr x19, [sp, #16]
    ldp x29, x30, [sp], #32
    ret

.Lwl_wait:
    clrex
    // Spin briefly
    mov w0, #16
.Lwl_spin:
    sub w0, w0, #1
    cbz w0, .Lwl_futex
    yield
    ldr w1, [x19]
    cbz w1, .Lwl_retry
    b .Lwl_spin

.Lwl_futex:
    ldr w2, [x19]
    mov x0, x19
    mov w1, #FUTEX_WAIT_PRIVATE
    // w2 = current value (expected)
    mov x3, #0
    mov x4, #0
    mov x5, #0
    bl sys_futex
    b .Lwl_retry
.size rwlock_wrlock, .-rwlock_wrlock

// ============================================================================
// rwlock_wrunlock(rwlock_ptr) -> void
// Release write lock
// x0 = pointer to rwlock
// ============================================================================
.global rwlock_wrunlock
.type rwlock_wrunlock, %function
rwlock_wrunlock:
    stp x29, x30, [sp, #-32]!
    mov x29, sp
    str x19, [sp, #16]

    mov x19, x0

    // Set state to 0 (unlocked)
    stlr wzr, [x19]

    // Wake all waiters (readers and writers)
    mov x0, x19
    mov w1, #FUTEX_WAKE_PRIVATE
    mov w2, #0x7fffffff            // wake all
    mov x3, #0
    mov x4, #0
    mov x5, #0
    bl sys_futex

    ldr x19, [sp, #16]
    ldp x29, x30, [sp], #32
    ret
.size rwlock_wrunlock, .-rwlock_wrunlock
