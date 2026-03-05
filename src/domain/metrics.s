// AssemblyDB - Metrics Collection
// Atomic counters for database operations
// All counters are 64-bit, atomically incremented

.include "src/const.s"

.text

// Metrics layout: see const.s for MET_* offsets and METRICS_SIZE

// ============================================================================
// metrics_init(db_ptr) -> 0=ok, neg=error
// Allocate and initialize metrics counters
// x0 = db_ptr
// ============================================================================
.global metrics_init
.type metrics_init, %function
metrics_init:
    stp x29, x30, [sp, #-32]!
    mov x29, sp
    str x19, [sp, #16]

    mov x19, x0

    mov x0, #MET_SIZE
    bl alloc_zeroed
    cbz x0, .Lmi_fail

    str x0, [x19, #DB_METRICS_PTR]
    mov x0, #0
    b .Lmi_ret

.Lmi_fail:
    mov x0, #ADB_ERR_NOMEM
    neg x0, x0

.Lmi_ret:
    ldr x19, [sp, #16]
    ldp x29, x30, [sp], #32
    ret
.size metrics_init, .-metrics_init

// ============================================================================
// metrics_inc(metrics_ptr, offset) -> void
// Atomic increment of a counter
// x0 = metrics_ptr, w1 = offset into metrics struct
// ============================================================================
.global metrics_inc
.type metrics_inc, %function
metrics_inc:
    add x0, x0, x1
.Lminc_retry:
    ldaxr x1, [x0]
    add x1, x1, #1
    stlxr w2, x1, [x0]
    cbnz w2, .Lminc_retry
    ret
.size metrics_inc, .-metrics_inc

// ============================================================================
// metrics_add(metrics_ptr, offset, value) -> void
// Atomic add to a counter
// x0 = metrics_ptr, w1 = offset, x2 = value
// ============================================================================
.global metrics_add
.type metrics_add, %function
metrics_add:
    add x0, x0, x1
.Lmadd_retry:
    ldaxr x1, [x0]
    add x1, x1, x2
    stlxr w3, x1, [x0]
    cbnz w3, .Lmadd_retry
    ret
.size metrics_add, .-metrics_add

// ============================================================================
// metrics_get(db_ptr, out_buf) -> 0
// Copy metrics to output buffer (256 bytes)
// x0 = db_ptr, x1 = output buffer
// ============================================================================
.global metrics_get
.type metrics_get, %function
metrics_get:
    stp x29, x30, [sp, #-16]!
    mov x29, sp

    ldr x2, [x0, #DB_METRICS_PTR]
    cbz x2, .Lmg_zero

    mov x0, x1                     // dst
    mov x1, x2                     // src = metrics_ptr
    mov x2, #MET_SIZE
    bl neon_memcpy
    mov x0, #0
    b .Lmg_ret

.Lmg_zero:
    mov x0, x1
    mov x1, #MET_SIZE
    bl neon_memzero
    mov x0, #0

.Lmg_ret:
    ldp x29, x30, [sp], #16
    ret
.size metrics_get, .-metrics_get

// ============================================================================
// metrics_destroy(db_ptr) -> void
// Free metrics counters
// x0 = db_ptr
// ============================================================================
.global metrics_destroy
.type metrics_destroy, %function
metrics_destroy:
    stp x29, x30, [sp, #-32]!
    mov x29, sp
    str x19, [sp, #16]

    mov x19, x0
    ldr x0, [x19, #DB_METRICS_PTR]
    cbz x0, .Lmd_done
    mov x1, #MET_SIZE
    bl free_mem
    str xzr, [x19, #DB_METRICS_PTR]

.Lmd_done:
    ldr x19, [sp, #16]
    ldp x29, x30, [sp], #32
    ret
.size metrics_destroy, .-metrics_destroy

.hidden alloc_zeroed
.hidden free_mem
.hidden neon_memcpy
.hidden neon_memzero
