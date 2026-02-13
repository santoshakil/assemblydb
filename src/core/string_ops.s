// AssemblyDB - String Operations
// strlen, integer to string, path building for file naming

.include "src/const.s"

.text

// ============================================================================
// asm_strlen(str) -> length
// x0 = null-terminated string pointer
// Returns: x0 = length (not including null terminator)
// ============================================================================
.global asm_strlen
.type asm_strlen, %function
asm_strlen:
    mov x1, x0                 // Save start
.Lsl_loop:
    ldrb w2, [x0], #1
    cbnz w2, .Lsl_loop
    sub x0, x0, x1
    sub x0, x0, #1             // Don't count null
    ret
.size asm_strlen, .-asm_strlen

// ============================================================================
// asm_strcpy(dst, src) -> dst
// Copy null-terminated string
// x0 = dst, x1 = src
// Returns: x0 = dst
// ============================================================================
.global asm_strcpy
.type asm_strcpy, %function
asm_strcpy:
    mov x2, x0
.Lsc_loop:
    ldrb w3, [x1], #1
    strb w3, [x0], #1
    cbnz w3, .Lsc_loop
    mov x0, x2
    ret
.size asm_strcpy, .-asm_strcpy

// ============================================================================
// asm_strcat(dst, src) -> dst
// Append src to end of dst (null-terminated)
// x0 = dst, x1 = src
// Returns: x0 = dst
// ============================================================================
.global asm_strcat
.type asm_strcat, %function
asm_strcat:
    mov x2, x0                 // Save dst start
    // Find end of dst
.Lscat_find:
    ldrb w3, [x0], #1
    cbnz w3, .Lscat_find
    sub x0, x0, #1             // Back to null terminator
    // Copy src
.Lscat_copy:
    ldrb w3, [x1], #1
    strb w3, [x0], #1
    cbnz w3, .Lscat_copy
    mov x0, x2
    ret
.size asm_strcat, .-asm_strcat

// ============================================================================
// u64_to_dec(value, buf) -> length
// Convert u64 to decimal string
// x0 = value, x1 = buffer (at least 21 bytes)
// Returns: x0 = length of string
// ============================================================================
.global u64_to_dec
.type u64_to_dec, %function
u64_to_dec:
    mov x2, x1                 // Save buf start
    mov x3, x1                 // Write pointer

    // Special case: 0
    cbz x0, .Lutd_zero

    // Build digits in reverse on stack
    mov x4, sp
    mov x5, #0                 // digit count
    mov x6, #10

.Lutd_div:
    cbz x0, .Lutd_reverse
    udiv x7, x0, x6            // q = value / 10
    msub x8, x7, x6, x0       // r = value - q * 10
    add x8, x8, #'0'
    sub x4, x4, #1
    strb w8, [x4]              // push digit
    mov x0, x7                 // value = q
    add x5, x5, #1
    b .Lutd_div

.Lutd_reverse:
    // Copy from stack to buffer (digits are now in order)
    mov x0, x5
.Lutd_copy:
    cbz x5, .Lutd_null
    ldrb w7, [x4], #1
    strb w7, [x3], #1
    subs x5, x5, #1
    b.ne .Lutd_copy
.Lutd_null:
    strb wzr, [x3]             // Null terminate
    ret

.Lutd_zero:
    mov w4, #'0'
    strb w4, [x3], #1
    strb wzr, [x3]
    mov x0, #1
    ret
.size u64_to_dec, .-u64_to_dec

// ============================================================================
// u64_to_padded_dec(value, buf, width) -> length
// Convert u64 to zero-padded decimal string of fixed width
// x0 = value, x1 = buffer, x2 = width (e.g., 6 for "000001")
// Returns: x0 = width
// ============================================================================
.global u64_to_padded_dec
.type u64_to_padded_dec, %function
u64_to_padded_dec:
    stp x29, x30, [sp, #-32]!
    mov x29, sp
    str x19, [sp, #16]

    mov x19, x2                // Save width

    // Fill buffer with '0'
    mov x3, x1
    mov x4, x2
.Lpd_zero:
    cbz x4, .Lpd_convert
    mov w5, #'0'
    strb w5, [x3], #1
    sub x4, x4, #1
    b .Lpd_zero

.Lpd_convert:
    // Null terminate
    strb wzr, [x3]

    // Now fill digits from the right
    add x3, x1, x19
    sub x3, x3, #1             // Point to last digit position
    mov x4, #10

.Lpd_div:
    cbz x0, .Lpd_done
    udiv x5, x0, x4
    msub x6, x5, x4, x0
    add x6, x6, #'0'
    strb w6, [x3]
    sub x3, x3, #1
    mov x0, x5
    b .Lpd_div

.Lpd_done:
    mov x0, x19
    ldr x19, [sp, #16]
    ldp x29, x30, [sp], #32
    ret
.size u64_to_padded_dec, .-u64_to_padded_dec

// ============================================================================
// build_path(buf, base, name) -> length
// Build a path string: "base/name\0"
// x0 = output buffer, x1 = base string, x2 = name string
// Returns: x0 = total length
// ============================================================================
.global build_path
.type build_path, %function
build_path:
    mov x3, x0                 // Save start

    // Copy base
.Lbp_base:
    ldrb w4, [x1], #1
    cbz w4, .Lbp_slash
    strb w4, [x0], #1
    b .Lbp_base

.Lbp_slash:
    mov w4, #'/'
    strb w4, [x0], #1

    // Copy name
.Lbp_name:
    ldrb w4, [x2], #1
    strb w4, [x0], #1
    cbnz w4, .Lbp_name

    // Calculate length (not including null)
    sub x0, x0, x3
    sub x0, x0, #1
    ret
.size build_path, .-build_path

// ============================================================================
// build_wal_name(buf, seq_num) -> length
// Build WAL filename: "wal/NNNNNN.wal\0"
// x0 = buffer (at least 20 bytes), x1 = sequence number
// Returns: x0 = length
// ============================================================================
.global build_wal_name
.type build_wal_name, %function
build_wal_name:
    stp x29, x30, [sp, #-32]!
    mov x29, sp
    stp x19, x20, [sp, #16]

    mov x19, x0                // buf

    // "wal/"
    mov w2, #'w'
    strb w2, [x0], #1
    mov w2, #'a'
    strb w2, [x0], #1
    mov w2, #'l'
    strb w2, [x0], #1
    mov w2, #'/'
    strb w2, [x0], #1

    // 6-digit padded sequence
    mov x0, x1                  // value
    add x1, x19, #4            // buf + 4
    mov x2, #6                  // width
    bl u64_to_padded_dec

    // ".wal\0"
    add x0, x19, #10
    mov w2, #'.'
    strb w2, [x0], #1
    mov w2, #'w'
    strb w2, [x0], #1
    mov w2, #'a'
    strb w2, [x0], #1
    mov w2, #'l'
    strb w2, [x0], #1
    strb wzr, [x0]

    mov x0, #14                // "wal/000001.wal" = 14 chars
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #32
    ret
.size build_wal_name, .-build_wal_name

// ============================================================================
// build_sst_name(buf, level, seq_num) -> length
// Build SSTable filename: "sst/LN-NNNNNN.sst\0"
// x0 = buffer, x1 = level (0 or 1), x2 = sequence number
// Returns: x0 = length
// ============================================================================
.global build_sst_name
.type build_sst_name, %function
build_sst_name:
    stp x29, x30, [sp, #-32]!
    mov x29, sp
    stp x19, x20, [sp, #16]

    mov x19, x0

    // "sst/L"
    mov w3, #'s'
    strb w3, [x0], #1
    strb w3, [x0], #1
    mov w3, #'t'
    strb w3, [x0], #1
    mov w3, #'/'
    strb w3, [x0], #1
    mov w3, #'L'
    strb w3, [x0], #1

    // Level digit
    add w3, w1, #'0'
    strb w3, [x0], #1

    // "-"
    mov w3, #'-'
    strb w3, [x0], #1

    // 6-digit padded sequence
    mov x0, x2
    add x1, x19, #7
    mov x2, #6
    bl u64_to_padded_dec

    // ".sst\0"
    add x0, x19, #13
    mov w2, #'.'
    strb w2, [x0], #1
    mov w2, #'s'
    strb w2, [x0], #1
    strb w2, [x0], #1
    mov w2, #'t'
    strb w2, [x0], #1
    strb wzr, [x0]

    mov x0, #17                // "sst/L0-000001.sst" = 17 chars
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #32
    ret
.size build_sst_name, .-build_sst_name
