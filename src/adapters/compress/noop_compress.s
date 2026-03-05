// AssemblyDB - No-op Compression Adapter
// Passthrough: returns data without compression

.include "src/const.s"

.text

// ============================================================================
// noop_compress(self, input, in_len, output, out_cap) -> out_len
// Copy input to output unchanged
// ============================================================================
.global noop_compress
.type noop_compress, %function
noop_compress:
    stp x29, x30, [sp, #-32]!
    mov x29, sp
    str x19, [sp, #16]

    mov x19, x2                    // save in_len

    // Check capacity (unsigned)
    cmp x4, x2
    b.lo .Lnc_err

    // Copy input to output
    mov x0, x3                     // dst
    // x1 = src (already there)
    mov x2, x19                    // len
    bl neon_memcpy

    mov x0, x19                    // return in_len
    b .Lnc_ret

.Lnc_err:
    mov x0, #ADB_ERR_COMPRESS
    neg x0, x0

.Lnc_ret:
    ldr x19, [sp, #16]
    ldp x29, x30, [sp], #32
    ret
.size noop_compress, .-noop_compress

// ============================================================================
// noop_decompress(self, input, in_len, output, out_cap) -> out_len
// ============================================================================
.global noop_decompress
.type noop_decompress, %function
noop_decompress:
    stp x29, x30, [sp, #-32]!
    mov x29, sp
    str x19, [sp, #16]

    mov x19, x2

    cmp x4, x2
    b.lo .Lnd_err

    mov x0, x3
    // x1 = src
    mov x2, x19
    bl neon_memcpy

    mov x0, x19
    b .Lnd_ret

.Lnd_err:
    mov x0, #ADB_ERR_COMPRESS
    neg x0, x0

.Lnd_ret:
    ldr x19, [sp, #16]
    ldp x29, x30, [sp], #32
    ret
.size noop_decompress, .-noop_decompress

// ============================================================================
// noop_max_compressed_size(self, in_len) -> in_len
// ============================================================================
.global noop_max_compressed_size
.type noop_max_compressed_size, %function
noop_max_compressed_size:
    mov x0, x1
    ret
.size noop_max_compressed_size, .-noop_max_compressed_size

.hidden neon_memcpy
