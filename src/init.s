// AssemblyDB - Library Initialization
// Called during adb_open to wire ports to adapters
// This file provides any library-level init that isn't per-db

.include "src/const.s"

.text

// ============================================================================
// adb_lib_init() -> 0=ok
// One-time library initialization (PRNG seed, etc.)
// Called automatically on first adb_open if needed
// ============================================================================
.global adb_lib_init
.type adb_lib_init, %function
adb_lib_init:
    stp x29, x30, [sp, #-16]!
    mov x29, sp

    // Seed PRNG from clock
    sub sp, sp, #16
    mov x0, #0                     // CLOCK_REALTIME
    mov x1, sp
    bl sys_clock_gettime
    ldr x0, [sp]                   // tv_sec
    ldr x1, [sp, #8]              // tv_nsec
    add x0, x0, x1                 // combine for seed
    add sp, sp, #16

    bl prng_seed

    mov x0, #0
    ldp x29, x30, [sp], #16
    ret
.size adb_lib_init, .-adb_lib_init
