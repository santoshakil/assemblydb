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
    cset w0, hs
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
    stp x29, x30, [sp, #-96]!
    mov x29, sp
    stp x19, x20, [sp, #16]
    stp x21, x22, [sp, #32]
    stp x23, x24, [sp, #48]
    str x25, [sp, #64]

    mov x19, x0                    // db_ptr

    // Get current memtable
    ldr x20, [x19, #DB_MEMTABLE_PTR]
    cbz x20, .Lcm_ok               // nothing to flush

    // Check if memtable has entries
    ldr x0, [x20, #SLH_ENTRY_COUNT]
    cbz x0, .Lcm_ok                // empty, nothing to do

    // Flush tombstones (and all entries) to B+ tree BEFORE SSTable flush.
    // sstable_flush skips tombstone entries, so without this step a
    // deleted key that already exists in the B+ tree would reappear
    // after the old memtable is destroyed.
    // Guard: only if B+ tree is initialized (mmap base != NULL)
    ldr x0, [x19, #DB_BTREE_MMAP]
    cbz x0, .Lcm_skip_btree_flush
    mov x0, x19
    bl flush_memtable_to_btree
    cbnz x0, .Lcm_ret              // btree flush failed: abort compaction

    // B+ tree now has all data (puts + deletes). Purge stale L0 SSTables
    // to prevent deleted keys from reappearing via router_get's SSTable scan.
    mov x0, x19
    bl lsm_purge_sstables

.Lcm_skip_btree_flush:

    // Save old arena and memtable size for rollback
    ldr x21, [x19, #DB_ARENA_PTR]
    ldr x24, [x19, #DB_MEMTABLE_SIZE]

    // Create replacement arena + memtable before publishing swap
    bl arena_create
    cbz x0, .Lcm_nomem
    mov x22, x0                    // new_arena
    mov x0, x22
    bl memtable_create2
    cbz x0, .Lcm_new_mt_fail
    mov x23, x0                    // new_memtable

    // Publish swap: old active becomes immutable
    str x20, [x19, #DB_IMM_MEMTABLE]
    str x22, [x19, #DB_ARENA_PTR]
    str x23, [x19, #DB_MEMTABLE_PTR]
    str xzr, [x19, #DB_MEMTABLE_SIZE]

    // Flush immutable memtable to SSTable
    mov x0, x19
    mov x1, x20                    // old memtable head
    mov w2, #0                     // level 0
    ldr w3, [x19, #DB_SST_COUNT_L0] // sequence = current count
    bl sstable_flush
    cbnz x0, .Lcm_flush_fail

    // Clear immutable reference
    str xzr, [x19, #DB_IMM_MEMTABLE]

    // Increment memtable flush metric
    ldr x0, [x19, #DB_METRICS_PTR]
    cbz x0, .Lcm_skip_metric
    mov w1, #MET_MT_FLUSHES
    bl metrics_inc
.Lcm_skip_metric:

    // Destroy old arena now that flush is complete
    mov x0, x21
    bl arena_destroy

.Lcm_ok:
    mov x0, #0
    b .Lcm_ret

.Lcm_new_mt_fail:
    mov x0, x22
    bl arena_destroy
    b .Lcm_nomem

.Lcm_nomem:
    mov x0, #ADB_ERR_NOMEM
    b .Lcm_ret

.Lcm_flush_fail:
    mov x25, x0                    // preserve flush error

    // Rollback to previous active memtable state
    str x21, [x19, #DB_ARENA_PTR]
    str x20, [x19, #DB_MEMTABLE_PTR]
    str xzr, [x19, #DB_IMM_MEMTABLE]
    str x24, [x19, #DB_MEMTABLE_SIZE]

    mov x0, x22
    bl arena_destroy
    mov x0, x25

.Lcm_ret:
    ldr x25, [sp, #64]
    ldp x23, x24, [sp, #48]
    ldp x21, x22, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #96
    ret
.size compact_memtable, .-compact_memtable

.hidden arena_create
.hidden memtable_create2
.hidden sstable_flush
.hidden arena_destroy
.hidden flush_memtable_to_btree
.hidden lsm_purge_sstables
.hidden metrics_inc
