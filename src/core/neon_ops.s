// AssemblyDB - NEON (ASIMD) Bulk Memory Operations
// 128-bit loads/stores for maximum throughput on Cortex-A76

.include "src/const.s"

.text

// ============================================================================
// neon_memcpy(dst, src, len) -> dst
// Copy len bytes from src to dst using NEON 128-bit loads/stores
// x0 = dst, x1 = src, x2 = len
// Returns: x0 = dst (unchanged)
// ============================================================================
.global neon_memcpy
.type neon_memcpy, %function
neon_memcpy:
    mov x3, x0                 // Save dst for return

    // Fast path: 64 bytes at a time (2x ldp q = 32B, repeated)
.Lmc_loop64:
    cmp x2, #64
    b.lt .Lmc_loop16

    ldp q0, q1, [x1]
    ldp q2, q3, [x1, #32]
    stp q0, q1, [x0]
    stp q2, q3, [x0, #32]
    add x0, x0, #64
    add x1, x1, #64
    sub x2, x2, #64
    b .Lmc_loop64

    // 16 bytes at a time
.Lmc_loop16:
    cmp x2, #16
    b.lt .Lmc_loop8
    ldr q0, [x1], #16
    str q0, [x0], #16
    sub x2, x2, #16
    b .Lmc_loop16

    // 8 bytes at a time
.Lmc_loop8:
    cmp x2, #8
    b.lt .Lmc_loop1
    ldr x4, [x1], #8
    str x4, [x0], #8
    sub x2, x2, #8
    b .Lmc_loop8

    // Remaining bytes
.Lmc_loop1:
    cbz x2, .Lmc_done
    ldrb w4, [x1], #1
    strb w4, [x0], #1
    subs x2, x2, #1
    b.ne .Lmc_loop1

.Lmc_done:
    mov x0, x3                 // Return original dst
    ret
.size neon_memcpy, .-neon_memcpy

// ============================================================================
// neon_memset(dst, byte_val, len) -> dst
// Fill len bytes with byte_val using NEON
// x0 = dst, w1 = byte value (0-255), x2 = len
// Returns: x0 = dst
// ============================================================================
.global neon_memset
.type neon_memset, %function
neon_memset:
    mov x3, x0                 // Save dst

    // Broadcast byte to all lanes of v0
    dup v0.16b, w1

    // 64 bytes at a time
.Lms_loop64:
    cmp x2, #64
    b.lt .Lms_loop16
    stp q0, q0, [x0]
    stp q0, q0, [x0, #32]
    add x0, x0, #64
    sub x2, x2, #64
    b .Lms_loop64

    // 16 bytes at a time
.Lms_loop16:
    cmp x2, #16
    b.lt .Lms_loop1
    str q0, [x0], #16
    sub x2, x2, #16
    b .Lms_loop16

    // Remaining bytes
.Lms_loop1:
    cbz x2, .Lms_done
    strb w1, [x0], #1
    subs x2, x2, #1
    b.ne .Lms_loop1

.Lms_done:
    mov x0, x3
    ret
.size neon_memset, .-neon_memset

// ============================================================================
// neon_memzero(dst, len) -> dst
// Zero len bytes using NEON
// x0 = dst, x1 = len
// Returns: x0 = dst
// ============================================================================
.global neon_memzero
.type neon_memzero, %function
neon_memzero:
    mov x3, x0                 // Save dst
    movi v0.16b, #0            // Zero vector

    // 64 bytes at a time
.Lmz_loop64:
    cmp x1, #64
    b.lt .Lmz_loop16
    stp q0, q0, [x0]
    stp q0, q0, [x0, #32]
    add x0, x0, #64
    sub x1, x1, #64
    b .Lmz_loop64

    // 16 bytes at a time
.Lmz_loop16:
    cmp x1, #16
    b.lt .Lmz_loop8
    str q0, [x0], #16
    sub x1, x1, #16
    b .Lmz_loop16

    // 8 bytes
.Lmz_loop8:
    cmp x1, #8
    b.lt .Lmz_loop1
    str xzr, [x0], #8
    sub x1, x1, #8
    b .Lmz_loop8

    // Remaining bytes
.Lmz_loop1:
    cbz x1, .Lmz_done
    strb wzr, [x0], #1
    subs x1, x1, #1
    b.ne .Lmz_loop1

.Lmz_done:
    mov x0, x3
    ret
.size neon_memzero, .-neon_memzero

// ============================================================================
// neon_memcmp(a, b, len) -> 0 if equal, <0 if a<b, >0 if a>b
// Compare len bytes using NEON
// x0 = a, x1 = b, x2 = len
// Returns: x0 = comparison result
// ============================================================================
.global neon_memcmp
.type neon_memcmp, %function
neon_memcmp:
    // 16 bytes at a time with NEON
.Lmcmp_loop16:
    cmp x2, #16
    b.lt .Lmcmp_loop1

    ldr q0, [x0]
    ldr q1, [x1]

    // Compare all 16 bytes
    cmeq v2.16b, v0.16b, v1.16b
    uminv b3, v2.16b            // min across lanes: 0xFF if all equal
    umov w3, v3.b[0]
    cmp w3, #0xFF
    b.ne .Lmcmp_diff16

    add x0, x0, #16
    add x1, x1, #16
    sub x2, x2, #16
    b .Lmcmp_loop16

    // Byte-by-byte for remainder
.Lmcmp_loop1:
    cbz x2, .Lmcmp_equal
    ldrb w3, [x0], #1
    ldrb w4, [x1], #1
    subs w5, w3, w4
    b.ne .Lmcmp_ret_diff
    subs x2, x2, #1
    b.ne .Lmcmp_loop1

.Lmcmp_equal:
    mov x0, #0
    ret

.Lmcmp_ret_diff:
    sxtw x0, w5
    ret

    // Found difference in 16-byte chunk - find exact byte
.Lmcmp_diff16:
    // XOR to find differing bytes
    eor v3.16b, v0.16b, v1.16b

    // Find first non-zero byte (first difference)
    // Extract lower and upper 8 bytes
    umov x3, v3.d[0]
    cbz x3, .Lmcmp_diff_high

    // Difference in lower 8 bytes
    rbit x3, x3
    clz x3, x3
    lsr x3, x3, #3             // Byte position

    ldrb w4, [x0, x3]
    ldrb w5, [x1, x3]
    sub w0, w4, w5
    ret

.Lmcmp_diff_high:
    umov x3, v3.d[1]
    rbit x3, x3
    clz x3, x3
    lsr x3, x3, #3
    add x3, x3, #8             // Offset by 8 (upper half)

    ldrb w4, [x0, x3]
    ldrb w5, [x1, x3]
    sub w0, w4, w5
    ret
.size neon_memcmp, .-neon_memcmp

// ============================================================================
// neon_copy_64(dst, src)
// Copy exactly 64 bytes (one key or one cache line) using NEON
// x0 = dst, x1 = src
// ============================================================================
.global neon_copy_64
.type neon_copy_64, %function
neon_copy_64:
    ldp q0, q1, [x1]
    ldp q2, q3, [x1, #32]
    stp q0, q1, [x0]
    stp q2, q3, [x0, #32]
    ret
.size neon_copy_64, .-neon_copy_64

// ============================================================================
// neon_copy_256(dst, src)
// Copy exactly 256 bytes (one value) using NEON
// x0 = dst, x1 = src
// ============================================================================
.global neon_copy_256
.type neon_copy_256, %function
neon_copy_256:
    ldp q0, q1, [x1]
    ldp q2, q3, [x1, #32]
    stp q0, q1, [x0]
    stp q2, q3, [x0, #32]

    ldp q0, q1, [x1, #64]
    ldp q2, q3, [x1, #96]
    stp q0, q1, [x0, #64]
    stp q2, q3, [x0, #96]

    ldp q0, q1, [x1, #128]
    ldp q2, q3, [x1, #160]
    stp q0, q1, [x0, #128]
    stp q2, q3, [x0, #160]

    ldp q0, q1, [x1, #192]
    ldp q2, q3, [x1, #224]
    stp q0, q1, [x0, #192]
    stp q2, q3, [x0, #224]
    ret
.size neon_copy_256, .-neon_copy_256

// ============================================================================
// neon_zero_64(dst)
// Zero exactly 64 bytes
// x0 = dst
// ============================================================================
.global neon_zero_64
.type neon_zero_64, %function
neon_zero_64:
    movi v0.16b, #0
    stp q0, q0, [x0]
    stp q0, q0, [x0, #32]
    ret
.size neon_zero_64, .-neon_zero_64

// ============================================================================
// neon_zero_256(dst)
// Zero exactly 256 bytes
// x0 = dst
// ============================================================================
.global neon_zero_256
.type neon_zero_256, %function
neon_zero_256:
    movi v0.16b, #0
    stp q0, q0, [x0]
    stp q0, q0, [x0, #32]
    stp q0, q0, [x0, #64]
    stp q0, q0, [x0, #96]
    stp q0, q0, [x0, #128]
    stp q0, q0, [x0, #160]
    stp q0, q0, [x0, #192]
    stp q0, q0, [x0, #224]
    ret
.size neon_zero_256, .-neon_zero_256

// ============================================================================
// neon_copy_page(dst, src)
// Copy exactly 4096 bytes (one page) using NEON
// x0 = dst, x1 = src
// ============================================================================
.global neon_copy_page
.type neon_copy_page, %function
neon_copy_page:
    mov x2, #64                // 64 iterations * 64 bytes = 4096
.Lcp_loop:
    ldp q0, q1, [x1]
    ldp q2, q3, [x1, #32]
    stp q0, q1, [x0]
    stp q2, q3, [x0, #32]
    add x0, x0, #64
    add x1, x1, #64
    subs x2, x2, #1
    b.ne .Lcp_loop
    ret
.size neon_copy_page, .-neon_copy_page
