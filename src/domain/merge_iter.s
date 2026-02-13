// AssemblyDB - Merge Iterator
// K-way merge across multiple sorted sources
// Used for range scans and compaction

.include "src/const.s"

.text

// Merge iterator state on stack:
// - Array of source iterators (node pointers or NULL)
// - Each source provides: current key + current val
// - On next(): advance smallest source, find new minimum

// ============================================================================
// merge_scan(db_ptr, start_key, end_key, callback, user_data) -> 0 or error
// Scan across all sources in sorted order
// x0 = db_ptr, x1 = start_key, x2 = end_key, x3 = callback, x4 = user_data
// callback(key_ptr, key_len, val_ptr, val_len, user_data) -> 0=continue, !0=stop
// ============================================================================
.global merge_scan
.type merge_scan, %function
merge_scan:
    stp x29, x30, [sp, #-96]!
    mov x29, sp
    stp x19, x20, [sp, #16]
    stp x21, x22, [sp, #32]
    stp x23, x24, [sp, #48]
    stp x25, x26, [sp, #64]
    str x27, [sp, #80]

    mov x19, x0                    // db_ptr
    mov x20, x1                    // start_key
    mov x21, x2                    // end_key
    mov x22, x3                    // callback
    mov x23, x4                    // user_data

    // Initialize memtable iterator
    ldr x0, [x19, #DB_MEMTABLE_PTR]
    cbz x0, .Lms_no_mt
    bl memtable_iter_first
    mov x24, x0                    // memtable cursor
    b .Lms_skip_start

.Lms_no_mt:
    mov x24, #0

.Lms_skip_start:
    // Skip memtable entries before start_key
    cbz x20, .Lms_loop             // no start_key, scan all
    cbz x24, .Lms_loop

.Lms_skip:
    cbz x24, .Lms_loop
    mov x0, x24                    // node key (at offset 0)
    mov x1, x20                    // start_key
    bl key_compare
    cmp w0, #0
    b.ge .Lms_loop                 // node >= start, stop skipping
    mov x0, x24
    bl memtable_iter_next
    mov x24, x0
    b .Lms_skip

.Lms_loop:
    cbz x24, .Lms_done

    // Check end_key
    cbz x21, .Lms_emit
    mov x0, x24
    mov x1, x21
    bl key_compare
    cmp w0, #0
    b.ge .Lms_done                 // node >= end, stop

.Lms_emit:
    // Skip deleted entries
    ldrb w0, [x24, #SLN_IS_DELETED]
    cbnz w0, .Lms_next

    // Call callback
    ldrh w1, [x24, #SLN_KEY_LEN]
    add x0, x24, #SLN_KEY_DATA    // key_ptr
    // callback(key_ptr, key_len, val_ptr, val_len, user_data)
    ldrh w3, [x24, #SLN_VAL_LEN]
    add x2, x24, #SLN_VAL_DATA    // val_ptr
    mov x4, x23                    // user_data
    blr x22

    // Check if callback wants to stop
    cbnz x0, .Lms_done

.Lms_next:
    mov x0, x24
    bl memtable_iter_next
    mov x24, x0
    b .Lms_loop

.Lms_done:
    // TODO: also scan B+ tree leaves and merge
    // For now, memtable-only scan is sufficient for Phase 4 testing

    mov x0, #0

    ldr x27, [sp, #80]
    ldp x25, x26, [sp, #64]
    ldp x23, x24, [sp, #48]
    ldp x21, x22, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #96
    ret
.size merge_scan, .-merge_scan
