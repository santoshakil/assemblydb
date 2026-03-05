// AssemblyDB - Thread Management
// clone()-based thread creation for background tasks (compaction)
// Each thread gets its own stack (mmap'd, 64KB)

.include "src/const.s"

.text

// THREAD_STACK_SIZE defined in const.s (65536 = 0x10000)

// ============================================================================
// thread_create(fn, arg) -> tid or negative error
// Create a new thread running fn(arg)
// x0 = function pointer, x1 = argument
// Returns: x0 = thread ID (positive) or negative error
// ============================================================================
.global thread_create
.type thread_create, %function
thread_create:
    stp x29, x30, [sp, #-48]!
    mov x29, sp
    stp x19, x20, [sp, #16]
    str x21, [sp, #32]

    mov x19, x0                    // fn
    mov x20, x1                    // arg

    // Allocate stack for new thread (64 KB)
    mov x0, #THREAD_STACK_SIZE
    bl mmap_anon_rw
    cmn x0, #4096
    b.hi .Ltc_fail
    mov x21, x0                    // stack base

    // Stack grows downward: set sp to top of allocation
    add x1, x21, #THREAD_STACK_SIZE

    // Place fn and arg at top of stack for thread_entry to find
    stp x19, x20, [x1, #-16]!    // push fn, arg onto new stack

    // clone(flags, stack, parent_tid=0, tls=0, child_tid=0)
    // CLONE_VM|FS|FILES|SIGHAND|THREAD|SYSVSEM = 0x50F00
    movz w0, #0x0F00
    movk w0, #0x0005, lsl #16
    // x1 = stack top (already set)
    mov x2, #0                    // parent_tid
    mov x3, #0                    // tls
    mov x4, #0                    // child_tid
    bl sys_clone

    cmp x0, #0
    b.lt .Ltc_fail_free
    b.eq .Ltc_child

    // Parent: x0 = child TID
    b .Ltc_ret

.Ltc_child:
    // Child thread: pop fn and arg from stack
    ldp x19, x20, [sp], #16
    mov x0, x20                    // arg
    blr x19                        // call fn(arg)

    // Thread done: exit this thread only
    mov x0, #0
    bl sys_exit

.Ltc_fail_free:
    mov x0, x21
    mov x1, #THREAD_STACK_SIZE
    bl sys_munmap
    mov x0, #ADB_ERR_NOMEM
    b .Ltc_ret

.Ltc_fail:
    mov x0, #ADB_ERR_NOMEM

.Ltc_ret:
    ldr x21, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #48
    ret
.size thread_create, .-thread_create

// ============================================================================
// thread_yield() -> void
// Hint CPU to yield to other threads
// ============================================================================
.global thread_yield
.type thread_yield, %function
thread_yield:
    yield
    ret
.size thread_yield, .-thread_yield
