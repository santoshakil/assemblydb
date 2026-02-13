// AssemblyDB - Hardware CRC32C using AArch64 crc32cx/crc32cb instructions
// Requires ARMv8-A with CRC extension (Cortex-A76 has it)

.include "src/const.s"

.text

// ============================================================================
// hw_crc32c(data_ptr, length) -> crc32c value
// x0 = pointer to data
// x1 = length in bytes
// Returns: w0 = CRC32C value (32-bit)
// ============================================================================
.global hw_crc32c
.type hw_crc32c, %function
hw_crc32c:
    mvn w2, wzr                  // Initial CRC = 0xFFFFFFFF

    // Process 64 bytes at a time (8x crc32cx for pipeline efficiency)
.Lcrc_loop64:
    cmp x1, #64
    b.lt .Lcrc_loop8

    ldr x3, [x0, #0]
    crc32cx w2, w2, x3
    ldr x3, [x0, #8]
    crc32cx w2, w2, x3
    ldr x3, [x0, #16]
    crc32cx w2, w2, x3
    ldr x3, [x0, #24]
    crc32cx w2, w2, x3
    ldr x3, [x0, #32]
    crc32cx w2, w2, x3
    ldr x3, [x0, #40]
    crc32cx w2, w2, x3
    ldr x3, [x0, #48]
    crc32cx w2, w2, x3
    ldr x3, [x0, #56]
    crc32cx w2, w2, x3

    add x0, x0, #64
    sub x1, x1, #64
    b .Lcrc_loop64

    // Process 8 bytes at a time
.Lcrc_loop8:
    cmp x1, #8
    b.lt .Lcrc_loop4
    ldr x3, [x0], #8
    crc32cx w2, w2, x3
    sub x1, x1, #8
    b .Lcrc_loop8

    // Process 4 bytes at a time
.Lcrc_loop4:
    cmp x1, #4
    b.lt .Lcrc_loop1
    ldr w3, [x0], #4
    crc32cw w2, w2, w3
    sub x1, x1, #4
    b .Lcrc_loop4

    // Process remaining bytes
.Lcrc_loop1:
    cbz x1, .Lcrc_done
    ldrb w3, [x0], #1
    crc32cb w2, w2, w3
    subs x1, x1, #1
    b.ne .Lcrc_loop1

.Lcrc_done:
    mvn w0, w2                   // Finalize: XOR with 0xFFFFFFFF     // Finalize
    ret
.size hw_crc32c, .-hw_crc32c

// ============================================================================
// hw_crc32c_u64(crc, value) -> updated crc
// Incremental CRC of a single 64-bit value
// w0 = current crc, x1 = 64-bit value
// Returns: w0 = updated CRC
// ============================================================================
.global hw_crc32c_u64
.type hw_crc32c_u64, %function
hw_crc32c_u64:
    crc32cx w0, w0, x1
    ret
.size hw_crc32c_u64, .-hw_crc32c_u64

// ============================================================================
// hw_crc32c_u32(crc, value) -> updated crc
// w0 = current crc, w1 = 32-bit value
// Returns: w0 = updated CRC
// ============================================================================
.global hw_crc32c_u32
.type hw_crc32c_u32, %function
hw_crc32c_u32:
    crc32cw w0, w0, w1
    ret
.size hw_crc32c_u32, .-hw_crc32c_u32

// ============================================================================
// hw_crc32c_combine(data_ptr, length, initial_crc) -> crc32c
// Like hw_crc32c but with a custom initial CRC (for chaining)
// x0 = data_ptr, x1 = length, w2 = initial_crc
// Returns: w0 = CRC32C
// ============================================================================
.global hw_crc32c_combine
.type hw_crc32c_combine, %function
hw_crc32c_combine:
    // w2 already has initial CRC, skip the init

.Lcrc2_loop8:
    cmp x1, #8
    b.lt .Lcrc2_loop1
    ldr x3, [x0], #8
    crc32cx w2, w2, x3
    sub x1, x1, #8
    b .Lcrc2_loop8

.Lcrc2_loop1:
    cbz x1, .Lcrc2_done
    ldrb w3, [x0], #1
    crc32cb w2, w2, w3
    subs x1, x1, #1
    b.ne .Lcrc2_loop1

.Lcrc2_done:
    mvn w0, w2                   // Finalize: XOR with 0xFFFFFFFF
    ret
.size hw_crc32c_combine, .-hw_crc32c_combine
