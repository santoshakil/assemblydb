// AssemblyDB - Bloom Filter using Hardware CRC32C Double Hashing
// 10 bits per key, 7 hash functions -> <1% false positive rate

.include "src/const.s"

.text

// Bloom filter structure (in-memory):
// 0x000  u32  num_bits     Total bits (rounded up to multiple of 64)
// 0x004  u32  num_hashes   Number of hash functions (7)
// 0x008  u32  num_keys     Number of keys inserted
// 0x00C  u32  _pad
// 0x010  ...  bit_array[]  Packed bit array

.equ BLM_NUM_BITS,    0x000
.equ BLM_NUM_HASHES,  0x004
.equ BLM_NUM_KEYS,    0x008
.equ BLM_BIT_ARRAY,   0x010

// ============================================================================
// bloom_create(expected_keys) -> bloom_ptr or 0
// Allocate and initialize a bloom filter
// x0 = expected number of keys
// Returns: x0 = pointer to bloom filter, or 0 on failure
// ============================================================================
.global bloom_create
.type bloom_create, %function
bloom_create:
    stp x29, x30, [sp, #-32]!
    mov x29, sp
    str x19, [sp, #16]

    // Calculate number of bits = expected_keys * BLOOM_BITS_PER_KEY
    mov x19, x0
    mov x1, #BLOOM_BITS_PER_KEY
    mul x0, x19, x1

    // Round up to multiple of 64
    add x0, x0, #63
    and x0, x0, #~63
    mov x19, x0                // num_bits

    // Calculate total allocation: header (16 bytes) + bit_array (num_bits / 8)
    lsr x0, x19, #3            // num_bits / 8 = byte count
    add x0, x0, #BLM_BIT_ARRAY // + header

    // Allocate (zeroed by mmap)
    bl alloc_zeroed
    cbz x0, .Lbc_fail

    // Initialize header
    str w19, [x0, #BLM_NUM_BITS]
    mov w1, #BLOOM_NUM_HASHES
    str w1, [x0, #BLM_NUM_HASHES]
    str wzr, [x0, #BLM_NUM_KEYS]

    ldr x19, [sp, #16]
    ldp x29, x30, [sp], #32
    ret

.Lbc_fail:
    ldr x19, [sp, #16]
    ldp x29, x30, [sp], #32
    ret
.size bloom_create, .-bloom_create

// ============================================================================
// bloom_destroy(bloom_ptr)
// Free bloom filter memory
// x0 = bloom_ptr
// ============================================================================
.global bloom_destroy
.type bloom_destroy, %function
bloom_destroy:
    stp x29, x30, [sp, #-16]!
    mov x29, sp

    cbz x0, .Lbd_done
    ldr w1, [x0, #BLM_NUM_BITS]
    lsr x1, x1, #3
    add x1, x1, #BLM_BIT_ARRAY
    bl free_mem

.Lbd_done:
    ldp x29, x30, [sp], #16
    ret
.size bloom_destroy, .-bloom_destroy

// ============================================================================
// bloom_add(bloom_ptr, key_ptr)
// Add a 64-byte key to the bloom filter
// x0 = bloom_ptr, x1 = key_ptr (64 bytes)
// ============================================================================
.global bloom_add
.type bloom_add, %function
bloom_add:
    stp x29, x30, [sp, #-64]!
    mov x29, sp
    stp x19, x20, [sp, #16]
    stp x21, x22, [sp, #32]
    stp x23, x24, [sp, #48]

    mov x19, x0                // bloom_ptr
    mov x20, x1                // key_ptr

    // h1 = crc32c(key, 64) with seed 0xFFFFFFFF (default)
    mov x0, x20
    mov x1, #KEY_SIZE
    bl hw_crc32c
    mov w21, w0                 // h1

    // h2 = crc32c(key, 64) with different seed (0x9E3779B9 = golden ratio)
    // We achieve this by using hw_crc32c_combine with a non-default initial CRC
    mov x0, x20
    mov x1, #KEY_SIZE
    movz w2, #0x79B9            // Load golden ratio constant 0x9E3779B9
    movk w2, #0x9E37, lsl #16
    bl hw_crc32c_combine
    // Make sure h2 is odd (guarantees full period in double hashing)
    orr w22, w0, #1             // h2 | 1 = always odd

    // Load bloom filter parameters
    ldr w23, [x19, #BLM_NUM_BITS]   // num_bits
    cbz w23, .Lba_done              // guard: no bits = nothing to set
    add x24, x19, #BLM_BIT_ARRAY    // bit_array base

    // Set BLOOM_NUM_HASHES bits using enhanced double hashing
    // hash_i = (h1 + i*h2 + i*i) % num_bits  (Kirsch-Mitzenmacker)
    mov w0, w21                 // h = h1
    mov w4, #0                  // i = 0
.Lba_loop:
    cmp w4, #BLOOM_NUM_HASHES
    b.hs .Lba_done

    // bit_pos = h % num_bits
    udiv w5, w0, w23
    msub w5, w5, w23, w0       // w5 = h % num_bits

    // byte_idx = bit_pos >> 3
    lsr w6, w5, #3
    // bit_idx = bit_pos & 7
    and w7, w5, #7
    // Set bit
    mov w8, #1
    lsl w8, w8, w7
    ldrb w9, [x24, x6]
    orr w9, w9, w8
    strb w9, [x24, x6]

    // h = h + h2 + i (Kirsch-Mitzenmacker enhanced double hashing)
    add w0, w0, w22
    add w0, w0, w4
    add w4, w4, #1
    b .Lba_loop

.Lba_done:
    // Increment key count
    ldr w0, [x19, #BLM_NUM_KEYS]
    add w0, w0, #1
    str w0, [x19, #BLM_NUM_KEYS]

    ldp x23, x24, [sp, #48]
    ldp x21, x22, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #64
    ret
.size bloom_add, .-bloom_add

// ============================================================================
// bloom_check(bloom_ptr, key_ptr) -> bool
// Check if a key might be in the set
// x0 = bloom_ptr, x1 = key_ptr (64 bytes)
// Returns: x0 = 1 (maybe present), 0 (definitely not present)
// ============================================================================
.global bloom_check
.type bloom_check, %function
bloom_check:
    stp x29, x30, [sp, #-64]!
    mov x29, sp
    stp x19, x20, [sp, #16]
    stp x21, x22, [sp, #32]
    stp x23, x24, [sp, #48]

    mov x19, x0                // bloom_ptr
    mov x20, x1                // key_ptr

    // h1 = crc32c(key, 64)
    mov x0, x20
    mov x1, #KEY_SIZE
    bl hw_crc32c
    mov w21, w0

    // h2 = crc32c(key, 64) with seed 0x9E3779B9
    mov x0, x20
    mov x1, #KEY_SIZE
    movz w2, #0x79B9
    movk w2, #0x9E37, lsl #16
    bl hw_crc32c_combine
    orr w22, w0, #1             // h2 | 1 = always odd

    ldr w23, [x19, #BLM_NUM_BITS]
    cbz w23, .Lbc_absent            // guard: no bits = definitely absent
    add x24, x19, #BLM_BIT_ARRAY

    mov w0, w21                 // h = h1
    mov w4, #0                  // i = 0
.Lbc_check_loop:
    cmp w4, #BLOOM_NUM_HASHES
    b.hs .Lbc_present

    // bit_pos = h % num_bits
    udiv w5, w0, w23
    msub w5, w5, w23, w0

    lsr w6, w5, #3             // byte_idx
    and w7, w5, #7             // bit_idx
    mov w8, #1
    lsl w8, w8, w7
    ldrb w9, [x24, x6]
    tst w9, w8
    b.eq .Lbc_absent            // bit not set -> definitely not present

    add w0, w0, w22
    add w0, w0, w4
    add w4, w4, #1
    b .Lbc_check_loop

.Lbc_present:
    mov x0, #1
    b .Lbc_ret

.Lbc_absent:
    mov x0, #0

.Lbc_ret:
    ldp x23, x24, [sp, #48]
    ldp x21, x22, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #64
    ret
.size bloom_check, .-bloom_check
