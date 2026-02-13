// AssemblyDB - Futex-based Mutex
// Simple spin-then-futex mutex for writer serialization
// Mutex state: 0=unlocked, 1=locked_no_waiters, 2=locked_with_waiters
// Uses ldaxr/stlxr CAS loops (no LSE atomics required)

.include "src/const.s"

.text

// ============================================================================
// mutex_init(mutex_ptr) -> void
// Initialize mutex to unlocked state
// x0 = pointer to uint32_t mutex
// ============================================================================
.global mutex_init
.type mutex_init, %function
mutex_init:
    str wzr, [x0]
    ret
.size mutex_init, .-mutex_init

// ============================================================================
// mutex_lock(mutex_ptr) -> void
// Acquire mutex (blocking)
// x0 = pointer to uint32_t mutex
// ============================================================================
.global mutex_lock
.type mutex_lock, %function
mutex_lock:
    stp x29, x30, [sp, #-32]!
    mov x29, sp
    str x19, [sp, #16]

    mov x19, x0                    // mutex_ptr

    // Fast path: try CAS 0 -> 1
    ldaxr w3, [x19]
    cbnz w3, .Lml_slow
    mov w2, #1
    stlxr w4, w2, [x19]
    cbz w4, .Lml_acquired

.Lml_slow:
    clrex
    // Spin a few times first
    mov w5, #32                    // spin count
.Lml_spin:
    sub w5, w5, #1
    cbz w5, .Lml_futex

    // Try CAS 0 -> 1
    ldaxr w3, [x19]
    cbnz w3, .Lml_spin_miss
    mov w2, #1
    stlxr w4, w2, [x19]
    cbz w4, .Lml_acquired
.Lml_spin_miss:
    clrex
    yield
    b .Lml_spin

.Lml_futex:
    // Atomic swap: exchange *mutex with 2, get old value
    // CAS loop: load old, store 2
.Lml_swap:
    ldaxr w0, [x19]
    mov w2, #2
    stlxr w4, w2, [x19]
    cbnz w4, .Lml_swap
    // w0 = old value
    cbz w0, .Lml_acquired          // was unlocked, we got it

.Lml_wait:
    // FUTEX_WAIT_PRIVATE: sleep if *mutex == 2
    mov x0, x19                    // uaddr
    mov w1, #FUTEX_WAIT_PRIVATE
    mov w2, #2                     // expected value
    mov x3, #0                     // timeout = NULL
    mov x4, #0
    mov x5, #0
    bl sys_futex

    // Woke up - try to acquire via swap
.Lml_wake_swap:
    ldaxr w0, [x19]
    mov w2, #2
    stlxr w4, w2, [x19]
    cbnz w4, .Lml_wake_swap
    cbnz w0, .Lml_wait            // still locked, go back to sleep

.Lml_acquired:
    ldr x19, [sp, #16]
    ldp x29, x30, [sp], #32
    ret
.size mutex_lock, .-mutex_lock

// ============================================================================
// mutex_unlock(mutex_ptr) -> void
// Release mutex, wake one waiter if any
// x0 = pointer to uint32_t mutex
// ============================================================================
.global mutex_unlock
.type mutex_unlock, %function
mutex_unlock:
    stp x29, x30, [sp, #-32]!
    mov x29, sp
    str x19, [sp, #16]

    mov x19, x0

    // Atomic swap *mutex with 0, get old value
.Lmu_swap:
    ldaxr w0, [x19]
    stlxr w1, wzr, [x19]
    cbnz w1, .Lmu_swap
    // w0 = old state
    cmp w0, #2
    b.ne .Lmu_done                 // no waiters

    // Wake one waiter
    mov x0, x19
    mov w1, #FUTEX_WAKE_PRIVATE
    mov w2, #1                     // wake 1 thread
    mov x3, #0
    mov x4, #0
    mov x5, #0
    bl sys_futex

.Lmu_done:
    ldr x19, [sp, #16]
    ldp x29, x30, [sp], #32
    ret
.size mutex_unlock, .-mutex_unlock

// ============================================================================
// mutex_trylock(mutex_ptr) -> 0=acquired, 1=busy
// Non-blocking lock attempt
// x0 = pointer to uint32_t mutex
// ============================================================================
.global mutex_trylock
.type mutex_trylock, %function
mutex_trylock:
    ldaxr w3, [x0]
    cbnz w3, .Lmtl_fail
    mov w2, #1
    stlxr w4, w2, [x0]
    cbnz w4, .Lmtl_fail
    mov x0, #0
    ret

.Lmtl_fail:
    clrex
    mov x0, #1
    ret
.size mutex_trylock, .-mutex_trylock
