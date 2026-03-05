// AssemblyDB - AES-256 Hardware Accelerated Primitives
// Uses ARM Crypto Extension: AESE/AESMC instructions
// Key expansion via S-box table, block encrypt via HW rounds
// CTR mode for page encryption (same op for encrypt/decrypt)

.include "src/const.s"

.text

// ============================================================================
// aes_key_expand_256(key32, expanded240) -> void
// Expand 32-byte AES-256 key to 240-byte round key schedule
// x0 = 32-byte key input, x1 = 240-byte expanded key output
// ============================================================================
.global aes_key_expand_256
.type aes_key_expand_256, %function
aes_key_expand_256:
    stp x29, x30, [sp, #-48]!
    mov x29, sp
    stp x19, x20, [sp, #16]
    stp x21, x22, [sp, #32]

    mov x19, x1                    // expanded key output

    // Copy first 32 bytes (round keys 0 and 1)
    ldp q0, q1, [x0]
    stp q0, q1, [x19]

    // Load S-box address into callee-saved x22
    adrp x22, .Laes_sbox
    add x22, x22, :lo12:.Laes_sbox

    mov w20, #8                    // i = 8 (word index)
    mov w21, #0                    // rcon_index

    // Generate words W[8] through W[59]
.Lke_loop:
    cmp w20, #60
    b.ge .Lke_done

    // w5 = W[i-1]
    sub w4, w20, #1
    ldr w5, [x19, w4, uxtw #2]

    // Check i % 8
    tst w20, #7
    b.ne .Lke_check4

    // i % 8 == 0: RotWord + SubWord + XOR RCON
    ror w5, w5, #8

    // SubWord(w5): 4 S-box lookups
    and x6, x5, #0xFF
    ldrb w6, [x22, x6]
    ubfx x7, x5, #8, #8
    ldrb w7, [x22, x7]
    orr w6, w6, w7, lsl #8
    ubfx x7, x5, #16, #8
    ldrb w7, [x22, x7]
    orr w6, w6, w7, lsl #16
    ubfx x7, x5, #24, #8
    ldrb w7, [x22, x7]
    orr w5, w6, w7, lsl #24

    // XOR with RCON (only first byte)
    adrp x6, .Laes_rcon
    add x6, x6, :lo12:.Laes_rcon
    ldrb w6, [x6, x21]
    eor w5, w5, w6
    add w21, w21, #1
    b .Lke_xor

.Lke_check4:
    and w6, w20, #7
    cmp w6, #4
    b.ne .Lke_xor

    // i % 8 == 4: SubWord only
    and x6, x5, #0xFF
    ldrb w6, [x22, x6]
    ubfx x7, x5, #8, #8
    ldrb w7, [x22, x7]
    orr w6, w6, w7, lsl #8
    ubfx x7, x5, #16, #8
    ldrb w7, [x22, x7]
    orr w6, w6, w7, lsl #16
    ubfx x7, x5, #24, #8
    ldrb w7, [x22, x7]
    orr w5, w6, w7, lsl #24

.Lke_xor:
    // W[i] = W[i-8] XOR temp
    sub w4, w20, #8
    ldr w6, [x19, w4, uxtw #2]
    eor w6, w6, w5
    str w6, [x19, w20, uxtw #2]

    add w20, w20, #1
    b .Lke_loop

.Lke_done:
    ldp x21, x22, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #48
    ret
.size aes_key_expand_256, .-aes_key_expand_256

// ============================================================================
// aes_encrypt_block(expanded_key, input16, output16) -> void
// Encrypt one 16-byte block using AES-256 (14 rounds)
// x0 = expanded key (240B), x1 = input, x2 = output
// ============================================================================
.global aes_encrypt_block
.type aes_encrypt_block, %function
aes_encrypt_block:
    // Load plaintext
    ldr q0, [x1]

    // Load 15 round keys into v16-v30
    ldp q16, q17, [x0]
    ldp q18, q19, [x0, #32]
    ldp q20, q21, [x0, #64]
    ldp q22, q23, [x0, #96]
    ldp q24, q25, [x0, #128]
    ldp q26, q27, [x0, #160]
    ldp q28, q29, [x0, #192]
    ldr q30, [x0, #224]

    // 13 rounds: AESE + AESMC (round keys 0-12)
    aese v0.16b, v16.16b
    aesmc v0.16b, v0.16b
    aese v0.16b, v17.16b
    aesmc v0.16b, v0.16b
    aese v0.16b, v18.16b
    aesmc v0.16b, v0.16b
    aese v0.16b, v19.16b
    aesmc v0.16b, v0.16b
    aese v0.16b, v20.16b
    aesmc v0.16b, v0.16b
    aese v0.16b, v21.16b
    aesmc v0.16b, v0.16b
    aese v0.16b, v22.16b
    aesmc v0.16b, v0.16b
    aese v0.16b, v23.16b
    aesmc v0.16b, v0.16b
    aese v0.16b, v24.16b
    aesmc v0.16b, v0.16b
    aese v0.16b, v25.16b
    aesmc v0.16b, v0.16b
    aese v0.16b, v26.16b
    aesmc v0.16b, v0.16b
    aese v0.16b, v27.16b
    aesmc v0.16b, v0.16b
    aese v0.16b, v28.16b
    aesmc v0.16b, v0.16b

    // Round 14: AESE only + final XOR
    aese v0.16b, v29.16b
    eor v0.16b, v0.16b, v30.16b

    // Store ciphertext
    str q0, [x2]
    ret
.size aes_encrypt_block, .-aes_encrypt_block

// ============================================================================
// aes_ctr_process(expanded_key, input, output, size, page_id) -> void
// AES-256-CTR mode: encrypt or decrypt (same operation)
// Nonce = [page_id (8B) | block_counter (8B)]
// x0 = expanded_key, x1 = input, x2 = output, x3 = size, x4 = page_id
// ============================================================================
.global aes_ctr_process
.type aes_ctr_process, %function
aes_ctr_process:
    stp x29, x30, [sp, #-64]!
    mov x29, sp
    stp x19, x20, [sp, #16]
    stp x21, x22, [sp, #32]
    str x23, [sp, #48]

    mov x19, x0                    // expanded_key
    mov x20, x1                    // input
    mov x21, x2                    // output
    mov x22, x3                    // remaining size
    mov x23, x4                    // page_id

    // Load 15 round keys into v16-v30
    ldp q16, q17, [x19]
    ldp q18, q19, [x19, #32]
    ldp q20, q21, [x19, #64]
    ldp q22, q23, [x19, #96]
    ldp q24, q25, [x19, #128]
    ldp q26, q27, [x19, #160]
    ldp q28, q29, [x19, #192]
    ldr q30, [x19, #224]

    mov x5, #0                    // block counter

    // Process full 16-byte blocks
.Lctr_loop:
    cmp x22, #16
    b.lo .Lctr_tail

    // Build counter: [page_id | counter] on stack, load as q
    stp x23, x5, [sp, #-16]!
    ldr q0, [sp], #16

    // AES-256 encrypt counter (14 rounds)
    aese v0.16b, v16.16b
    aesmc v0.16b, v0.16b
    aese v0.16b, v17.16b
    aesmc v0.16b, v0.16b
    aese v0.16b, v18.16b
    aesmc v0.16b, v0.16b
    aese v0.16b, v19.16b
    aesmc v0.16b, v0.16b
    aese v0.16b, v20.16b
    aesmc v0.16b, v0.16b
    aese v0.16b, v21.16b
    aesmc v0.16b, v0.16b
    aese v0.16b, v22.16b
    aesmc v0.16b, v0.16b
    aese v0.16b, v23.16b
    aesmc v0.16b, v0.16b
    aese v0.16b, v24.16b
    aesmc v0.16b, v0.16b
    aese v0.16b, v25.16b
    aesmc v0.16b, v0.16b
    aese v0.16b, v26.16b
    aesmc v0.16b, v0.16b
    aese v0.16b, v27.16b
    aesmc v0.16b, v0.16b
    aese v0.16b, v28.16b
    aesmc v0.16b, v0.16b
    aese v0.16b, v29.16b
    eor v0.16b, v0.16b, v30.16b

    // XOR keystream with input
    ldr q1, [x20], #16
    eor v0.16b, v0.16b, v1.16b
    str q0, [x21], #16

    add x5, x5, #1
    sub x22, x22, #16
    b .Lctr_loop

.Lctr_tail:
    cbz x22, .Lctr_done

    // Handle remaining bytes (< 16)
    stp x23, x5, [sp, #-16]!
    ldr q0, [sp], #16

    aese v0.16b, v16.16b
    aesmc v0.16b, v0.16b
    aese v0.16b, v17.16b
    aesmc v0.16b, v0.16b
    aese v0.16b, v18.16b
    aesmc v0.16b, v0.16b
    aese v0.16b, v19.16b
    aesmc v0.16b, v0.16b
    aese v0.16b, v20.16b
    aesmc v0.16b, v0.16b
    aese v0.16b, v21.16b
    aesmc v0.16b, v0.16b
    aese v0.16b, v22.16b
    aesmc v0.16b, v0.16b
    aese v0.16b, v23.16b
    aesmc v0.16b, v0.16b
    aese v0.16b, v24.16b
    aesmc v0.16b, v0.16b
    aese v0.16b, v25.16b
    aesmc v0.16b, v0.16b
    aese v0.16b, v26.16b
    aesmc v0.16b, v0.16b
    aese v0.16b, v27.16b
    aesmc v0.16b, v0.16b
    aese v0.16b, v28.16b
    aesmc v0.16b, v0.16b
    aese v0.16b, v29.16b
    eor v0.16b, v0.16b, v30.16b

    // Store keystream to stack, XOR byte-by-byte
    str q0, [sp, #-16]!
    mov x6, sp
.Lctr_tail_loop:
    cbz x22, .Lctr_tail_end
    ldrb w7, [x20], #1
    ldrb w8, [x6], #1
    eor w7, w7, w8
    strb w7, [x21], #1
    sub x22, x22, #1
    b .Lctr_tail_loop

.Lctr_tail_end:
    add sp, sp, #16

.Lctr_done:
    ldr x23, [sp, #48]
    ldp x21, x22, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #64
    ret
.size aes_ctr_process, .-aes_ctr_process

// ============================================================================
// Read-only data: AES S-box and round constants
// ============================================================================
.section .rodata
.align 4

.Laes_sbox:
    .byte 0x63,0x7c,0x77,0x7b,0xf2,0x6b,0x6f,0xc5,0x30,0x01,0x67,0x2b,0xfe,0xd7,0xab,0x76
    .byte 0xca,0x82,0xc9,0x7d,0xfa,0x59,0x47,0xf0,0xad,0xd4,0xa2,0xaf,0x9c,0xa4,0x72,0xc0
    .byte 0xb7,0xfd,0x93,0x26,0x36,0x3f,0xf7,0xcc,0x34,0xa5,0xe5,0xf1,0x71,0xd8,0x31,0x15
    .byte 0x04,0xc7,0x23,0xc3,0x18,0x96,0x05,0x9a,0x07,0x12,0x80,0xe2,0xeb,0x27,0xb2,0x75
    .byte 0x09,0x83,0x2c,0x1a,0x1b,0x6e,0x5a,0xa0,0x52,0x3b,0xd6,0xb3,0x29,0xe3,0x2f,0x84
    .byte 0x53,0xd1,0x00,0xed,0x20,0xfc,0xb1,0x5b,0x6a,0xcb,0xbe,0x39,0x4a,0x4c,0x58,0xcf
    .byte 0xd0,0xef,0xaa,0xfb,0x43,0x4d,0x33,0x85,0x45,0xf9,0x02,0x7f,0x50,0x3c,0x9f,0xa8
    .byte 0x51,0xa3,0x40,0x8f,0x92,0x9d,0x38,0xf5,0xbc,0xb6,0xda,0x21,0x10,0xff,0xf3,0xd2
    .byte 0xcd,0x0c,0x13,0xec,0x5f,0x97,0x44,0x17,0xc4,0xa7,0x7e,0x3d,0x64,0x5d,0x19,0x73
    .byte 0x60,0x81,0x4f,0xdc,0x22,0x2a,0x90,0x88,0x46,0xee,0xb8,0x14,0xde,0x5e,0x0b,0xdb
    .byte 0xe0,0x32,0x3a,0x0a,0x49,0x06,0x24,0x5c,0xc2,0xd3,0xac,0x62,0x91,0x95,0xe4,0x79
    .byte 0xe7,0xc8,0x37,0x6d,0x8d,0xd5,0x4e,0xa9,0x6c,0x56,0xf4,0xea,0x65,0x7a,0xae,0x08
    .byte 0xba,0x78,0x25,0x2e,0x1c,0xa6,0xb4,0xc6,0xe8,0xdd,0x74,0x1f,0x4b,0xbd,0x8b,0x8a
    .byte 0x70,0x3e,0xb5,0x66,0x48,0x03,0xf6,0x0e,0x61,0x35,0x57,0xb9,0x86,0xc1,0x1d,0x9e
    .byte 0xe1,0xf8,0x98,0x11,0x69,0xd9,0x8e,0x94,0x9b,0x1e,0x87,0xe9,0xce,0x55,0x28,0xdf
    .byte 0x8c,0xa1,0x89,0x0d,0xbf,0xe6,0x42,0x68,0x41,0x99,0x2d,0x0f,0xb0,0x54,0xbb,0x16

.Laes_rcon:
    .byte 0x01,0x02,0x04,0x08,0x10,0x20,0x40
