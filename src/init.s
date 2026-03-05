// AssemblyDB - Library Initialization
// Port wiring is done by individual adapter init functions called from adb_open.
// PRNG seeding is done inline in adb_open via clock_gettime(CLOCK_MONOTONIC).
// This file is kept as a placeholder for any future library-level init.

.include "src/const.s"

.text
