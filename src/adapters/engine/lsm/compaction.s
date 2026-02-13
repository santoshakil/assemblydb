// AssemblyDB - Compaction
// Merge L0 SSTables into L1, eventually into B+ tree
// Triggered when L0 SSTable count exceeds threshold

.include "src/const.s"

.text

.equ L0_COMPACT_THRESHOLD, 4

// ============================================================================
// compact_check_needed(db_ptr) -> 0=no, 1=yes
// Check if compaction should be triggered
// x0 = db_ptr
// ============================================================================
.global compact_check_needed
.type compact_check_needed, %function
compact_check_needed:
    ldr w1, [x0, #DB_SST_COUNT_L0]
    cmp w1, #L0_COMPACT_THRESHOLD
    cset w0, ge
    ret
.size compact_check_needed, .-compact_check_needed

// ============================================================================
// compact_memtable(db_ptr) -> 0=ok, neg=error
// Flush active memtable to L0 SSTable
// Swaps active memtable to immutable, creates new active
// x0 = db_ptr
// ============================================================================
.global compact_memtable
.type compact_memtable, %function
compact_memtable:
    stp x29, x30, [sp, #-48]!
    mov x29, sp
    stp x19, x20, [sp, #16]
    str x21, [sp, #32]

    mov x19, x0                    // db_ptr

    // Get current memtable
    ldr x20, [x19, #DB_MEMTABLE_PTR]
    cbz x20, .Lcm_ok               // nothing to flush

    // Check if memtable has entries
    ldr x0, [x20, #SLH_ENTRY_COUNT]
    cbz x0, .Lcm_ok                // empty, nothing to do

    // Move active -> immutable
    str x20, [x19, #DB_IMM_MEMTABLE]

    // Create new arena + memtable
    bl arena_create
    cbz x0, .Lcm_nomem
    mov x21, x0
    str x0, [x19, #DB_ARENA_PTR]

    mov x0, x21
    bl memtable_create2
    cbz x0, .Lcm_nomem
    str x0, [x19, #DB_MEMTABLE_PTR]
    str xzr, [x19, #DB_MEMTABLE_SIZE]

    // Flush immutable memtable to SSTable
    ldr w1, [x19, #DB_SST_COUNT_L0]
    mov x0, x19
    mov x1, x20                    // old memtable head
    mov w2, #0                     // level 0
    ldr w3, [x19, #DB_SST_COUNT_L0] // sequence = current count
    bl sstable_flush
    cmp x0, #0
    b.lt .Lcm_flush_fail

    // Clear immutable reference
    str xzr, [x19, #DB_IMM_MEMTABLE]

    // Note: old arena will be leaked for now
    // TODO: destroy old arena after flush completes

.Lcm_ok:
    mov x0, #0
    b .Lcm_ret

.Lcm_nomem:
    mov x0, #ADB_ERR_NOMEM
    neg x0, x0
    b .Lcm_ret

.Lcm_flush_fail:
    // x0 has error

.Lcm_ret:
    ldr x21, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #48
    ret
.size compact_memtable, .-compact_memtable
