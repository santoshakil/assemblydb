// AssemblyDB - LZ4 Block Compression
// Simplified LZ4 format: token + literals + offset + match
// Hash table for match finding (4096 entries, allocated in context)

.include "src/const.s"

.text

.equ LZ4_HASH_SIZE,     4096
.equ LZ4_HASH_BYTES,    16384       // 4096 * 4
.equ LZ4_MIN_MATCH,     4
.equ LZ4_MAX_OFFSET,    65535

// Compress context
.equ LZCTX_HASH_TABLE,  0x000
.equ LZCTX_SIZE,        0x4000     // 16 KB

// ============================================================================
// lz4_ctx_create() -> ctx_ptr or NULL
// ============================================================================
.global lz4_ctx_create
.type lz4_ctx_create, %function
lz4_ctx_create:
    stp x29, x30, [sp, #-16]!
    mov x29, sp

    mov x0, #LZCTX_SIZE
    bl alloc_zeroed

    ldp x29, x30, [sp], #16
    ret
.size lz4_ctx_create, .-lz4_ctx_create

// ============================================================================
// lz4_ctx_destroy(ctx) -> void
// ============================================================================
.global lz4_ctx_destroy
.type lz4_ctx_destroy, %function
lz4_ctx_destroy:
    stp x29, x30, [sp, #-16]!
    mov x29, sp

    cbz x0, .Llcd_done
    mov x1, #LZCTX_SIZE
    bl free_mem

.Llcd_done:
    ldp x29, x30, [sp], #16
    ret
.size lz4_ctx_destroy, .-lz4_ctx_destroy

// ============================================================================
// lz4_compress(ctx, input, in_len, output, out_cap) -> out_len or negative err
// x0 = ctx (hash table), x1 = input, x2 = in_len
// x3 = output, x4 = out_capacity
// Returns: x0 = compressed size, or negative on error
// ============================================================================
.global lz4_compress
.type lz4_compress, %function
lz4_compress:
    stp x29, x30, [sp, #-112]!
    mov x29, sp
    stp x19, x20, [sp, #16]
    stp x21, x22, [sp, #32]
    stp x23, x24, [sp, #48]
    stp x25, x26, [sp, #64]
    stp x27, x28, [sp, #80]

    cbz x0, .Llc_null_ctx          // NULL ctx guard
    mov x19, x0                    // ctx (hash table)
    mov x20, x1                    // input base
    mov x21, x2                    // in_len
    mov x22, x3                    // output base
    mov x23, x4                    // out_capacity
    mov x24, #0                    // pos (input position)
    mov x25, #0                    // anchor (start of literals)
    mov x26, #0                    // out_pos (output position)

    // Zero hash table
    mov x0, x19
    mov x1, #LZ4_HASH_BYTES
    bl neon_memzero

    // If input too small, just emit as literals
    cmp x21, #LZ4_MIN_MATCH
    b.lo .Llc_final_lit

    // Hash constant: 0x9E3779B1 (Knuth multiplicative hash)
    movz w27, #0x79B1
    movk w27, #0x9E37, lsl #16
    mov w28, #LZ4_MAX_OFFSET       // hoist constant out of loop

    // Main compression loop
.Llc_main:
    // Need at least 5 bytes ahead (min match + 1 literal after)
    add x0, x24, #5
    cmp x0, x21
    b.hs .Llc_final_lit

    // Read 4 bytes at current pos, compute hash
    ldr w0, [x20, x24]
    mul w1, w0, w27
    lsr w1, w1, #20                // 12-bit hash
    and w1, w1, #0xFFF

    // Look up hash table
    ldr w2, [x19, w1, uxtw #2]    // ref position
    // Update hash table with current pos
    str w24, [x19, w1, uxtw #2]

    // Check if ref is valid
    cmp w2, w25                    // ref >= anchor?
    b.lo .Llc_no_match             // unsigned compare

    // Check offset > 0 and in range (65535 max)
    sub w3, w24, w2
    cbz w3, .Llc_no_match          // offset 0 = self-reference, invalid
    cmp w3, w28
    b.hi .Llc_no_match

    // Verify 4-byte match
    ldr w4, [x20, x2]             // read 4 bytes at ref
    cmp w0, w4
    b.ne .Llc_no_match

    // === Found a match! ===
    // w2 = ref pos, w3 = offset, x24 = current pos

    // Calculate literal length
    sub x5, x24, x25              // literal_len = pos - anchor

    // Extend match forward
    mov x6, #LZ4_MIN_MATCH        // match_len starts at 4
    add x7, x24, x6               // pos + match_len
    add x8, x2, x6                // ref + match_len
.Llc_extend:
    cmp x7, x21
    b.hs .Llc_match_done
    ldrb w9, [x20, x7]
    ldrb w10, [x20, x8]
    cmp w9, w10
    b.ne .Llc_match_done
    add x6, x6, #1
    add x7, x7, #1
    add x8, x8, #1
    b .Llc_extend

.Llc_match_done:
    // x5 = literal_len, x6 = match_len (>= 4)
    // x3 (w3) = offset

    // Build token byte
    sub x9, x6, #LZ4_MIN_MATCH   // match_len - 4

    // Literal nibble: min(lit_len, 15)
    mov x10, #15
    cmp x5, #15
    csel x10, x5, x10, lo         // unsigned: min(lit_len, 15)

    // Match nibble
    mov x11, #15
    cmp x9, #15
    csel x11, x9, x11, lo         // unsigned: min(match_len-4, 15)

    // Token = (lit_nibble << 4) | match_nibble
    lsl w10, w10, #4
    orr w10, w10, w11

    // Check output space: lit + match + extra_bytes + fixed_overhead
    // extra_bytes ≈ lit_len/256 + match_len/256 (fast approximation)
    add x12, x5, x6               // lit_len + match_len
    lsr x13, x5, #8               // lit_len / 256
    add x12, x12, x13
    lsr x13, x6, #8               // match_len / 256
    add x12, x12, x13
    add x12, x12, #6              // token(1) + offset(2) + rounding(3)
    add x13, x26, x12
    cmp x13, x23
    b.hs .Llc_overflow

    // Write token
    strb w10, [x22, x26]
    add x26, x26, #1

    // Write extra literal length bytes
    cmp x5, #15
    b.lo .Llc_copy_lit
    sub x12, x5, #15
.Llc_lit_extra:
    cmp x12, #255
    b.lo .Llc_lit_extra_last
    mov w13, #255
    strb w13, [x22, x26]
    add x26, x26, #1
    sub x12, x12, #255
    b .Llc_lit_extra
.Llc_lit_extra_last:
    strb w12, [x22, x26]
    add x26, x26, #1

.Llc_copy_lit:
    // Copy literal bytes
    cbz x5, .Llc_write_offset
    mov x12, #0
.Llc_lit_copy_loop:
    cmp x12, x5
    b.hs .Llc_write_offset         // unsigned compare
    add x13, x25, x12
    ldrb w14, [x20, x13]
    add x15, x26, x12
    strb w14, [x22, x15]
    add x12, x12, #1
    b .Llc_lit_copy_loop

.Llc_write_offset:
    add x26, x26, x5

    // Write match offset (2 bytes, little-endian)
    strb w3, [x22, x26]
    add x26, x26, #1
    lsr w12, w3, #8
    strb w12, [x22, x26]
    add x26, x26, #1

    // Write extra match length bytes
    cmp x9, #15
    b.lo .Llc_advance
    sub x12, x9, #15
.Llc_match_extra:
    cmp x12, #255
    b.lo .Llc_match_extra_last
    mov w13, #255
    strb w13, [x22, x26]
    add x26, x26, #1
    sub x12, x12, #255
    b .Llc_match_extra
.Llc_match_extra_last:
    strb w12, [x22, x26]
    add x26, x26, #1

.Llc_advance:
    // Advance past match
    add x24, x24, x6
    mov x25, x24                   // new anchor
    b .Llc_main

.Llc_no_match:
    add x24, x24, #1
    b .Llc_main

.Llc_final_lit:
    // Emit remaining bytes as literals
    sub x5, x21, x25              // literal_len
    cbz x5, .Llc_done

    // Check output space: lit_len + extra_bytes + token
    lsr x12, x5, #8               // lit_len / 256 (extra length bytes)
    add x12, x12, x5              // + literal bytes
    add x12, x12, #3              // token(1) + rounding(2)
    add x13, x26, x12
    cmp x13, x23
    b.hs .Llc_overflow

    // Token: literal_len, match_len = 0
    mov x10, #15
    cmp x5, #15
    csel x10, x5, x10, lo         // unsigned
    lsl w10, w10, #4
    strb w10, [x22, x26]
    add x26, x26, #1

    // Extra literal length
    cmp x5, #15
    b.lo .Llc_final_copy
    sub x12, x5, #15
.Llc_final_extra:
    cmp x12, #255
    b.lo .Llc_final_extra_last
    mov w13, #255
    strb w13, [x22, x26]
    add x26, x26, #1
    sub x12, x12, #255
    b .Llc_final_extra
.Llc_final_extra_last:
    strb w12, [x22, x26]
    add x26, x26, #1

.Llc_final_copy:
    mov x12, #0
.Llc_final_loop:
    cmp x12, x5
    b.hs .Llc_done                 // unsigned compare
    add x13, x25, x12
    ldrb w14, [x20, x13]
    add x15, x26, x12
    strb w14, [x22, x15]
    add x12, x12, #1
    b .Llc_final_loop

.Llc_done:
    add x0, x26, x5               // total output size
    b .Llc_ret

.Llc_null_ctx:
    mov x0, #ADB_ERR_INVALID
    neg x0, x0
    b .Llc_ret

.Llc_overflow:
    mov x0, #ADB_ERR_COMPRESS
    neg x0, x0                     // return negative error

.Llc_ret:
    ldp x27, x28, [sp, #80]
    ldp x25, x26, [sp, #64]
    ldp x23, x24, [sp, #48]
    ldp x21, x22, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #112
    ret
.size lz4_compress, .-lz4_compress

// ============================================================================
// lz4_decompress(input, in_len, output, out_cap) -> out_len or negative err
// x0 = input, x1 = in_len, x2 = output, x3 = out_capacity
// Returns: x0 = decompressed size, or negative on error
// ============================================================================
.global lz4_decompress
.type lz4_decompress, %function
lz4_decompress:
    stp x29, x30, [sp, #-80]!
    mov x29, sp
    stp x19, x20, [sp, #16]
    stp x21, x22, [sp, #32]
    stp x23, x24, [sp, #48]
    str x25, [sp, #64]

    mov x19, x0                    // input base
    mov x20, x1                    // in_len
    mov x21, x2                    // output base
    mov x22, x3                    // out_capacity
    mov x23, #0                    // in_pos
    mov x24, #0                    // out_pos

    add x25, x19, x20             // input_end

.Lld_loop:
    // Check if we've consumed all input
    add x0, x19, x23
    cmp x0, x25
    b.hs .Lld_done

    // Read token byte
    ldrb w0, [x19, x23]
    add x23, x23, #1

    // Extract literal length (high nibble)
    lsr w1, w0, #4
    and w2, w0, #0xF               // match length nibble (save for later)

    // Extended literal length
    cmp w1, #15
    b.ne .Lld_copy_lit

.Lld_lit_ext:
    add x3, x19, x23
    cmp x3, x25
    b.hs .Lld_err
    ldrb w3, [x19, x23]
    add x23, x23, #1
    add x1, x1, x3, uxtb          // 64-bit add, zero-extend byte
    cmp w3, #255
    b.eq .Lld_lit_ext

.Lld_copy_lit:
    // Copy literal_len bytes from input to output
    cbz x1, .Lld_check_end
    // Input bounds check: in_pos + literal_len <= in_len
    add x3, x23, x1
    cmp x3, x20
    b.hi .Lld_err
    // Output bounds check: out_pos + literal_len <= out_capacity
    add x3, x24, x1
    cmp x3, x22
    b.hi .Lld_err

    mov x3, #0
.Lld_lit_loop:
    cmp x3, x1
    b.hs .Lld_lit_done             // unsigned compare
    add x4, x23, x3
    ldrb w5, [x19, x4]
    add x6, x24, x3
    strb w5, [x21, x6]
    add x3, x3, #1
    b .Lld_lit_loop

.Lld_lit_done:
    add x23, x23, x1              // advance input past literals
    add x24, x24, x1              // advance output

.Lld_check_end:
    // If we've consumed all input, done (last sequence has no match)
    add x0, x19, x23
    cmp x0, x25
    b.hs .Lld_done

    // Read match offset (2 bytes, little-endian)
    ldrb w3, [x19, x23]
    add x23, x23, #1
    add x0, x19, x23
    cmp x0, x25
    b.hs .Lld_err
    ldrb w4, [x19, x23]
    add x23, x23, #1
    orr w3, w3, w4, lsl #8        // offset

    cbz w3, .Lld_err               // offset 0 is invalid

    // Match length = low nibble + 4
    add w2, w2, #LZ4_MIN_MATCH

    // Extended match length
    // Check if match length needs extension (nibble was 15)
    sub w5, w2, #LZ4_MIN_MATCH    // original nibble value
    cmp w5, #15
    b.ne .Lld_no_match_ext

.Lld_match_ext:
    add x0, x19, x23
    cmp x0, x25
    b.hs .Lld_err
    ldrb w5, [x19, x23]
    add x23, x23, #1
    add x2, x2, x5, uxtb          // 64-bit add, zero-extend byte
    cmp w5, #255
    b.eq .Lld_match_ext

.Lld_no_match_ext:
    // Copy match: from output[out_pos - offset] for match_len bytes
    // Must handle overlapping copies (byte by byte)
    sub x5, x24, x3               // match source = out_pos - offset

    // Bounds check
    cmp x5, #0
    b.lt .Lld_err                  // underflow
    add x6, x24, x2
    cmp x6, x22
    b.hi .Lld_err                  // overflow

    mov x6, #0
.Lld_match_loop:
    cmp x6, x2
    b.hs .Lld_match_done           // unsigned compare
    add x7, x5, x6
    ldrb w8, [x21, x7]
    add x9, x24, x6
    strb w8, [x21, x9]
    add x6, x6, #1
    b .Lld_match_loop

.Lld_match_done:
    add x24, x24, x2
    b .Lld_loop

.Lld_done:
    mov x0, x24                    // return decompressed size
    b .Lld_ret

.Lld_err:
    mov x0, #ADB_ERR_COMPRESS
    neg x0, x0

.Lld_ret:
    ldr x25, [sp, #64]
    ldp x23, x24, [sp, #48]
    ldp x21, x22, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #80
    ret
.size lz4_decompress, .-lz4_decompress

// ============================================================================
// lz4_max_compressed_size(in_len) -> size
// Worst case: input_len + input_len/255 + 16
// x0 = input length
// ============================================================================
.global lz4_max_compressed_size
.type lz4_max_compressed_size, %function
lz4_max_compressed_size:
    mov x1, x0
    mov x2, #255
    udiv x3, x1, x2
    add x0, x1, x3
    add x0, x0, #16
    ret
.size lz4_max_compressed_size, .-lz4_max_compressed_size

.hidden alloc_zeroed
.hidden free_mem
.hidden neon_memzero
