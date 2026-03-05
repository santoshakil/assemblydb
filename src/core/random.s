// AssemblyDB - xorshift64 PRNG
// Fast, simple PRNG for skip list level generation

.include "src/const.s"

.data
.align 3
prng_state:
    .quad 0x123456789ABCDEF0    // Initial seed

.text

// ============================================================================
// prng_seed(seed)
// Set PRNG seed
// x0 = seed value (must be non-zero)
// ============================================================================
.global prng_seed
.type prng_seed, %function
prng_seed:
    // Ensure non-zero
    cbnz x0, 1f
    mov x0, #1
1:
    adrp x1, prng_state
    add x1, x1, :lo12:prng_state
    str x0, [x1]
    ret
.size prng_seed, .-prng_seed

// ============================================================================
// prng_next() -> random u64
// xorshift64 algorithm
// Returns: x0 = random 64-bit value
// ============================================================================
.global prng_next
.type prng_next, %function
prng_next:
    adrp x1, prng_state
    add x1, x1, :lo12:prng_state
    ldr x0, [x1]

    // xorshift64
    eor x0, x0, x0, lsl #13
    eor x0, x0, x0, lsr #7
    eor x0, x0, x0, lsl #17

    str x0, [x1]
    ret
.size prng_next, .-prng_next

// ============================================================================
// random_level() -> level (1..MAX_SKIP_HEIGHT)
// Generate random skip list level with probability p=1/4 per level
// Returns: x0 = level (1 to MAX_SKIP_HEIGHT)
// ============================================================================
.global random_level
.type random_level, %function
random_level:
    stp x29, x30, [sp, #-16]!
    mov x29, sp

    bl prng_next
    // x0 = random bits

    mov w1, #1                  // level = 1

    // Each 2 bits: if (bits & 3) == 0, go up (probability 1/4)
.Lrl_loop:
    cmp w1, #MAX_SKIP_HEIGHT
    b.hs .Lrl_done
    tst x0, #3                 // test bottom 2 bits
    b.ne .Lrl_done              // if not zero, stop
    add w1, w1, #1              // level++
    lsr x0, x0, #2             // shift out used bits
    b .Lrl_loop

.Lrl_done:
    mov x0, x1
    ldp x29, x30, [sp], #16
    ret
.size random_level, .-random_level
