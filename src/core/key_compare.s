// AssemblyDB - NEON-Accelerated Key Comparison
// Keys are 64 bytes: [2B length][62B data, zero-padded]
// Lexicographic comparison on data bytes, shorter key < longer key with same prefix

.include "src/const.s"

.text

// ============================================================================
// key_compare(key_a, key_b) -> result
// Compare two 64-byte fixed-size keys lexicographically
// x0 = pointer to key_a (64 bytes)
// x1 = pointer to key_b (64 bytes)
// Returns: x0 = -1 (a<b), 0 (a==b), +1 (a>b)
// ============================================================================
.global key_compare
.type key_compare, %function
key_compare:
    // Load key lengths (first 2 bytes of each key)
    ldrh w2, [x0]              // len_a
    ldrh w3, [x1]              // len_b

    // Compare data bytes at offset 2 (62 bytes of data)
    // Using NEON: compare 16 bytes at a time
    // Since keys are zero-padded, comparing all 62 bytes works correctly

    // Bytes 2-17 (16 bytes)
    ldr q0, [x0, #2]
    ldr q1, [x1, #2]
    cmeq v2.16b, v0.16b, v1.16b
    uminv b3, v2.16b
    umov w4, v3.b[0]
    cmp w4, #0xFF
    b.ne .Lkc_diff_0

    // Bytes 18-33 (16 bytes)
    ldr q0, [x0, #18]
    ldr q1, [x1, #18]
    cmeq v2.16b, v0.16b, v1.16b
    uminv b3, v2.16b
    umov w4, v3.b[0]
    cmp w4, #0xFF
    b.ne .Lkc_diff_16

    // Bytes 34-49 (16 bytes)
    ldr q0, [x0, #34]
    ldr q1, [x1, #34]
    cmeq v2.16b, v0.16b, v1.16b
    uminv b3, v2.16b
    umov w4, v3.b[0]
    cmp w4, #0xFF
    b.ne .Lkc_diff_32

    // Bytes 50-63 (14 bytes - load 16 but only 14 meaningful)
    // Since keys are zero-padded to 64 bytes, bytes 62-63 are both 0
    // so loading 16 from offset 48 (bytes 50-65) is safe if key is
    // in a 64-byte aligned buffer. But we load from 50 = offset 48+2.
    // Safer: do the last 14 bytes scalar
    add x5, x0, #50
    add x6, x1, #50
    mov w7, #14
.Lkc_tail:
    cbz w7, .Lkc_data_equal
    ldrb w8, [x5], #1
    ldrb w9, [x6], #1
    cmp w8, w9
    b.lt .Lkc_less
    b.gt .Lkc_greater
    sub w7, w7, #1
    b .Lkc_tail

.Lkc_data_equal:
    // All data bytes equal - compare lengths
    cmp w2, w3
    b.lt .Lkc_less
    b.gt .Lkc_greater
    mov x0, #0
    ret

.Lkc_less:
    mov x0, #-1
    ret

.Lkc_greater:
    mov x0, #1
    ret

    // Found difference in NEON chunk at offset 0 (bytes 2-17)
.Lkc_diff_0:
    add x5, x0, #2
    add x6, x1, #2
    b .Lkc_find_byte

    // Found difference at offset 16 (bytes 18-33)
.Lkc_diff_16:
    add x5, x0, #18
    add x6, x1, #18
    b .Lkc_find_byte

    // Found difference at offset 32 (bytes 34-49)
.Lkc_diff_32:
    add x5, x0, #34
    add x6, x1, #34
    // fall through to find_byte

    // v0 = bytes from a, v1 = bytes from b at the differing chunk
    // x5 = pointer to a's chunk, x6 = pointer to b's chunk
.Lkc_find_byte:
    eor v3.16b, v0.16b, v1.16b // XOR: non-zero where different

    // Find first non-zero byte (first difference)
    umov x7, v3.d[0]           // Lower 8 bytes of XOR
    cbnz x7, .Lkc_diff_low
    umov x7, v3.d[1]           // Upper 8 bytes
    rbit x7, x7
    clz x7, x7
    lsr x7, x7, #3             // Byte position in upper half
    add x7, x7, #8             // Actual position
    b .Lkc_compare_at

.Lkc_diff_low:
    rbit x7, x7
    clz x7, x7
    lsr x7, x7, #3             // Byte position in lower half

.Lkc_compare_at:
    ldrb w8, [x5, x7]
    ldrb w9, [x6, x7]
    cmp w8, w9
    b.lt .Lkc_less
    b .Lkc_greater
.size key_compare, .-key_compare

// ============================================================================
// key_equal(key_a, key_b) -> bool
// Fast equality check for two 64-byte keys (length + data)
// x0 = key_a, x1 = key_b
// Returns: x0 = 1 if equal, 0 if not
// ============================================================================
.global key_equal
.type key_equal, %function
key_equal:
    // Compare all 64 bytes using NEON (includes length prefix)
    ldp q0, q1, [x0]           // 32 bytes from a
    ldp q2, q3, [x1]           // 32 bytes from b
    cmeq v4.16b, v0.16b, v2.16b
    cmeq v5.16b, v1.16b, v3.16b
    and v4.16b, v4.16b, v5.16b

    ldp q0, q1, [x0, #32]      // Next 32 bytes from a
    ldp q2, q3, [x1, #32]      // Next 32 bytes from b
    cmeq v6.16b, v0.16b, v2.16b
    cmeq v7.16b, v1.16b, v3.16b
    and v6.16b, v6.16b, v7.16b
    and v4.16b, v4.16b, v6.16b

    // Check if all bytes matched (all 0xFF)
    uminv b0, v4.16b
    umov w0, v0.b[0]
    // w0 = 0xFF if all equal, 0 if any differ
    cmp w0, #0xFF
    cset w0, eq                 // 1 if equal, 0 if not
    ret
.size key_equal, .-key_equal

// ============================================================================
// build_fixed_key(dst, src, src_len)
// Build a 64-byte fixed-size key from variable-length input
// x0 = dst (64 bytes), x1 = src data, x2 = src_len (0-62)
// Zeroes the entire key first, then writes length + data
// ============================================================================
.global build_fixed_key
.type build_fixed_key, %function
build_fixed_key:
    stp x29, x30, [sp, #-32]!
    mov x29, sp
    stp x19, x20, [sp, #16]

    mov x19, x0                // dst
    mov x20, x1                // src

    // Zero the entire 64 bytes
    movi v0.16b, #0
    stp q0, q0, [x19]
    stp q0, q0, [x19, #32]

    // Write length prefix (2 bytes, little-endian)
    strh w2, [x19]

    // Copy data bytes
    cbz x2, .Lbfk_done
    add x0, x19, #2            // dst = key + 2
    mov x1, x20                // src
    // Copy x2 bytes
.Lbfk_copy:
    ldrb w3, [x1], #1
    strb w3, [x0], #1
    subs x2, x2, #1
    b.ne .Lbfk_copy

.Lbfk_done:
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #32
    ret
.size build_fixed_key, .-build_fixed_key

// ============================================================================
// build_fixed_val(dst, src, src_len)
// Build a 256-byte fixed-size value from variable-length input
// x0 = dst (256 bytes), x1 = src data, x2 = src_len (0-254)
// ============================================================================
.global build_fixed_val
.type build_fixed_val, %function
build_fixed_val:
    stp x29, x30, [sp, #-32]!
    mov x29, sp
    stp x19, x20, [sp, #16]

    mov x19, x0
    mov x20, x1

    // Zero 256 bytes
    bl neon_zero_256

    // Write length prefix
    strh w2, [x19]

    // Copy data
    cbz x2, .Lbfv_done
    add x0, x19, #2
    mov x1, x20
.Lbfv_copy:
    ldrb w3, [x1], #1
    strb w3, [x0], #1
    subs x2, x2, #1
    b.ne .Lbfv_copy

.Lbfv_done:
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #32
    ret
.size build_fixed_val, .-build_fixed_val
