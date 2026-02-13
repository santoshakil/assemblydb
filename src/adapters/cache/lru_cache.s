// AssemblyDB - LRU Page Cache
// Simple LRU cache for B+ tree pages
// Uses a hash table for O(1) lookup + doubly-linked list for eviction order
//
// Cache structure (allocated as one block):
//   0x000: capacity (max frames)
//   0x008: count (current frames in use)
//   0x010: lru_head (least recently used - evict first)
//   0x018: lru_tail (most recently used)
//   0x020: hash_table_ptr (page_id -> frame index, open addressing)
//   0x028: hash_size
//   0x030: frames_ptr (array of cache frames)
//   0x038: pages_ptr (mmap'd page data)
//   0x040: hits
//   0x048: misses
//
// Frame (48 bytes):
//   0x00: page_id (4B)
//   0x04: pin_count (4B)
//   0x08: is_dirty (4B)
//   0x0C: is_valid (4B)
//   0x10: lru_prev (8B) - index, -1 = none
//   0x18: lru_next (8B) - index, -1 = none
//   0x20: page_ptr (8B) - pointer to page data

.include "src/const.s"

.text

.equ LRU_CAPACITY,    0x000
.equ LRU_COUNT,       0x008
.equ LRU_HEAD,        0x010
.equ LRU_TAIL,        0x018
.equ LRU_HASH_PTR,    0x020
.equ LRU_HASH_SIZE,   0x028
.equ LRU_FRAMES_PTR,  0x030
.equ LRU_PAGES_PTR,   0x038
.equ LRU_HITS,        0x040
.equ LRU_MISSES,      0x048
.equ LRU_HEADER_SIZE, 0x050

.equ LF_PAGE_ID,      0x00
.equ LF_PIN_COUNT,    0x04
.equ LF_IS_DIRTY,     0x08
.equ LF_IS_VALID,     0x0C
.equ LF_LRU_PREV,     0x10
.equ LF_LRU_NEXT,     0x18
.equ LF_PAGE_PTR,     0x20
.equ LF_SIZE,         0x28

// ============================================================================
// lru_cache_create(capacity) -> cache_ptr or NULL
// x0 = number of pages to cache
// ============================================================================
.global lru_cache_create
.type lru_cache_create, %function
lru_cache_create:
    stp x29, x30, [sp, #-48]!
    mov x29, sp
    stp x19, x20, [sp, #16]
    stp x21, x22, [sp, #32]

    mov x19, x0                    // capacity

    // Hash table size = next power of 2 >= 2 * capacity
    lsl x20, x19, #1              // 2 * capacity
    sub x0, x20, #1
    clz x0, x0
    mov x1, #64
    sub x0, x1, x0
    mov x1, #1
    lsl x20, x1, x0               // round up to power of 2

    // Allocate header
    mov x0, #LRU_HEADER_SIZE
    bl alloc_zeroed
    cbz x0, .Llcc_fail
    mov x21, x0                    // cache ptr

    // Store metadata
    str x19, [x21, #LRU_CAPACITY]
    str xzr, [x21, #LRU_COUNT]
    movn x0, #0                    // -1
    str x0, [x21, #LRU_HEAD]
    str x0, [x21, #LRU_TAIL]
    str x20, [x21, #LRU_HASH_SIZE]

    // Allocate hash table (hash_size * 8 bytes, stores frame indices)
    lsl x0, x20, #3
    bl alloc_zeroed
    cbz x0, .Llcc_fail_free
    str x0, [x21, #LRU_HASH_PTR]

    // Initialize hash entries to -1 (empty)
    mov x1, x20                    // hash_size entries
    mov x2, x0                    // hash table ptr
.Llcc_hash_init:
    cbz x1, .Llcc_hash_done
    movn x3, #0                    // -1
    str x3, [x2], #8
    sub x1, x1, #1
    b .Llcc_hash_init
.Llcc_hash_done:

    // Allocate frames array (capacity * LF_SIZE)
    mov x0, #LF_SIZE
    mul x0, x0, x19
    bl alloc_zeroed
    cbz x0, .Llcc_fail_free
    str x0, [x21, #LRU_FRAMES_PTR]

    // Initialize frames: all invalid, prev/next = -1
    mov x1, x0                    // frames ptr
    mov x2, x19                   // capacity
    mov x3, #0                    // index
.Llcc_frame_init:
    cbz x2, .Llcc_frame_done
    str wzr, [x1, #LF_IS_VALID]
    movn x4, #0
    str x4, [x1, #LF_LRU_PREV]
    str x4, [x1, #LF_LRU_NEXT]
    add x1, x1, #LF_SIZE
    sub x2, x2, #1
    b .Llcc_frame_init
.Llcc_frame_done:

    // Allocate page data (capacity * PAGE_SIZE)
    mov x0, x19
    bl page_alloc
    cbz x0, .Llcc_fail_free
    str x0, [x21, #LRU_PAGES_PTR]

    // Set page pointers in each frame
    ldr x1, [x21, #LRU_FRAMES_PTR]
    mov x2, x19
    mov x3, x0                    // pages base
.Llcc_pages_init:
    cbz x2, .Llcc_pages_done
    str x3, [x1, #LF_PAGE_PTR]
    add x1, x1, #LF_SIZE
    add x3, x3, #PAGE_SIZE
    sub x2, x2, #1
    b .Llcc_pages_init
.Llcc_pages_done:

    mov x0, x21
    b .Llcc_ret

.Llcc_fail_free:
    mov x0, x21
    mov x1, #LRU_HEADER_SIZE
    bl free_mem
.Llcc_fail:
    mov x0, #0

.Llcc_ret:
    ldp x21, x22, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #48
    ret
.size lru_cache_create, .-lru_cache_create

// ============================================================================
// lru_cache_destroy(cache) -> void
// ============================================================================
.global lru_cache_destroy
.type lru_cache_destroy, %function
lru_cache_destroy:
    stp x29, x30, [sp, #-32]!
    mov x29, sp
    str x19, [sp, #16]

    cbz x0, .Llcd_done
    mov x19, x0

    // Free pages
    ldr x0, [x19, #LRU_PAGES_PTR]
    ldr x1, [x19, #LRU_CAPACITY]
    cbz x0, .Llcd_free_frames
    bl page_free

.Llcd_free_frames:
    ldr x0, [x19, #LRU_FRAMES_PTR]
    cbz x0, .Llcd_free_hash
    mov x1, #LF_SIZE
    ldr x2, [x19, #LRU_CAPACITY]
    mul x1, x1, x2
    bl free_mem

.Llcd_free_hash:
    ldr x0, [x19, #LRU_HASH_PTR]
    cbz x0, .Llcd_free_header
    ldr x1, [x19, #LRU_HASH_SIZE]
    lsl x1, x1, #3
    bl free_mem

.Llcd_free_header:
    mov x0, x19
    mov x1, #LRU_HEADER_SIZE
    bl free_mem

.Llcd_done:
    ldr x19, [sp, #16]
    ldp x29, x30, [sp], #32
    ret
.size lru_cache_destroy, .-lru_cache_destroy

// ============================================================================
// lru_cache_fetch(cache, page_id) -> page_ptr or NULL
// Fetch page: returns pointer to cached page data
// Pins the page (caller must call unpin when done)
// x0 = cache, x1 = page_id (uint32)
// ============================================================================
.global lru_cache_fetch
.type lru_cache_fetch, %function
lru_cache_fetch:
    stp x29, x30, [sp, #-48]!
    mov x29, sp
    stp x19, x20, [sp, #16]
    stp x21, x22, [sp, #32]

    mov x19, x0                    // cache
    mov w20, w1                    // page_id

    // Hash: page_id & (hash_size - 1)
    ldr x2, [x19, #LRU_HASH_SIZE]
    sub x3, x2, #1
    and x3, x1, x3                // hash (power-of-2 modulo)

    // Look up in hash table
    ldr x4, [x19, #LRU_HASH_PTR]
    ldr x5, [x4, x3, lsl #3]     // frame index or -1

    // Linear probe to find the page
    ldr x6, [x19, #LRU_FRAMES_PTR]
    mov x21, x2                    // hash_size for probing
    mov x22, x3                    // starting hash slot

.Llcf_probe:
    ldr x5, [x4, x3, lsl #3]
    cmn x5, #1                    // -1 = empty slot
    b.eq .Llcf_miss

    // Check if this frame has our page_id
    mov x7, #LF_SIZE
    mul x7, x7, x5
    add x7, x6, x7                // frame ptr
    ldr w8, [x7, #LF_PAGE_ID]
    ldr w9, [x7, #LF_IS_VALID]
    cmp w8, w20
    b.ne .Llcf_next_probe
    cbz w9, .Llcf_next_probe

    // Cache hit! Increment pin count, move to MRU
    ldr w10, [x7, #LF_PIN_COUNT]
    add w10, w10, #1
    str w10, [x7, #LF_PIN_COUNT]

    // Increment hits
    ldr x10, [x19, #LRU_HITS]
    add x10, x10, #1
    str x10, [x19, #LRU_HITS]

    ldr x0, [x7, #LF_PAGE_PTR]
    b .Llcf_ret

.Llcf_next_probe:
    add x3, x3, #1
    cmp x3, x21
    csel x3, xzr, x3, ge          // wrap around
    cmp x3, x22
    b.eq .Llcf_miss                // full loop = not found
    b .Llcf_probe

.Llcf_miss:
    // Increment misses
    ldr x10, [x19, #LRU_MISSES]
    add x10, x10, #1
    str x10, [x19, #LRU_MISSES]

    mov x0, #0                    // return NULL (caller must load page)

.Llcf_ret:
    ldp x21, x22, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #48
    ret
.size lru_cache_fetch, .-lru_cache_fetch

// ============================================================================
// lru_cache_insert(cache, page_id, page_data) -> page_ptr or NULL
// Insert a page into cache, evicting LRU if full
// x0 = cache, x1 = page_id, x2 = source page data to copy in
// Returns: cached page pointer (pinned), or NULL on error
// ============================================================================
.global lru_cache_insert
.type lru_cache_insert, %function
lru_cache_insert:
    stp x29, x30, [sp, #-64]!
    mov x29, sp
    stp x19, x20, [sp, #16]
    stp x21, x22, [sp, #32]
    stp x23, x24, [sp, #48]

    mov x19, x0                    // cache
    mov w20, w1                    // page_id
    mov x21, x2                    // source data

    // Find a free frame or evict LRU
    ldr x0, [x19, #LRU_COUNT]
    ldr x1, [x19, #LRU_CAPACITY]
    cmp x0, x1
    b.ge .Llci_evict

    // Use next free frame (count is the index)
    mov x22, x0                    // frame index
    b .Llci_use_frame

.Llci_evict:
    // Walk LRU list from head to find first unpinned frame
    ldr x22, [x19, #LRU_HEAD]
    ldr x2, [x19, #LRU_FRAMES_PTR]

.Llci_find_unpinned:
    cmn x22, #1
    b.eq .Llci_fail                // all frames pinned
    mov x3, #LF_SIZE
    mul x3, x3, x22
    add x3, x2, x3                // frame ptr
    ldr w4, [x3, #LF_PIN_COUNT]
    cbz w4, .Llci_do_evict
    ldr x22, [x3, #LF_LRU_NEXT]
    b .Llci_find_unpinned

.Llci_do_evict:
    // Remove frame x22 from LRU doubly-linked list
    ldr x7, [x3, #LF_LRU_PREV]
    ldr x8, [x3, #LF_LRU_NEXT]

    // Update prev->next or HEAD
    cmn x7, #1
    b.eq .Llci_ev_set_head
    ldr x9, [x19, #LRU_FRAMES_PTR]
    mov x10, #LF_SIZE
    mul x10, x10, x7
    add x9, x9, x10
    str x8, [x9, #LF_LRU_NEXT]
    b .Llci_ev_do_next
.Llci_ev_set_head:
    str x8, [x19, #LRU_HEAD]

.Llci_ev_do_next:
    // Update next->prev or TAIL
    cmn x8, #1
    b.eq .Llci_ev_set_tail
    ldr x9, [x19, #LRU_FRAMES_PTR]
    mov x10, #LF_SIZE
    mul x10, x10, x8
    add x9, x9, x10
    str x7, [x9, #LF_LRU_PREV]
    b .Llci_ev_hash_clear
.Llci_ev_set_tail:
    str x7, [x19, #LRU_TAIL]

.Llci_ev_hash_clear:
    // Clear old page from hash table
    ldr x2, [x19, #LRU_FRAMES_PTR]
    mov x3, #LF_SIZE
    mul x3, x3, x22
    add x2, x2, x3

    ldr w3, [x2, #LF_PAGE_ID]
    ldr x4, [x19, #LRU_HASH_PTR]
    ldr x5, [x19, #LRU_HASH_SIZE]
    sub x6, x5, #1
    and x6, x3, x6
    mov x9, x6                    // save start for termination

.Llci_clear_probe:
    ldr x7, [x4, x6, lsl #3]
    cmp x7, x22
    b.eq .Llci_clear_found
    cmn x7, #1
    b.eq .Llci_evict_done          // empty slot = not in table
    add x6, x6, #1
    cmp x6, x5
    csel x6, xzr, x6, ge
    cmp x6, x9
    b.eq .Llci_evict_done          // wrapped around
    b .Llci_clear_probe

.Llci_clear_found:
    movn x7, #0
    str x7, [x4, x6, lsl #3]

.Llci_evict_done:
    // Decrement count
    ldr x0, [x19, #LRU_COUNT]
    sub x0, x0, #1
    str x0, [x19, #LRU_COUNT]

.Llci_use_frame:
    // x22 = frame index to use
    ldr x2, [x19, #LRU_FRAMES_PTR]
    mov x3, #LF_SIZE
    mul x3, x3, x22
    add x23, x2, x3               // frame ptr

    // Set up frame
    str w20, [x23, #LF_PAGE_ID]
    mov w0, #1
    str w0, [x23, #LF_PIN_COUNT]
    str wzr, [x23, #LF_IS_DIRTY]
    str w0, [x23, #LF_IS_VALID]

    // Copy page data in
    ldr x0, [x23, #LF_PAGE_PTR]
    mov x1, x21
    bl neon_copy_page

    // Insert into hash table
    ldr x4, [x19, #LRU_HASH_PTR]
    ldr x5, [x19, #LRU_HASH_SIZE]
    sub x7, x5, #1
    and x7, x20, x7               // hash = page_id & (hash_size - 1)

.Llci_hash_probe:
    ldr x8, [x4, x7, lsl #3]
    cmn x8, #1                    // empty slot?
    b.eq .Llci_hash_insert
    add x7, x7, #1
    cmp x7, x5
    csel x7, xzr, x7, ge
    b .Llci_hash_probe
.Llci_hash_insert:
    str x22, [x4, x7, lsl #3]

    // Add to LRU tail (MRU position)
    movn x0, #0                    // -1
    str x0, [x23, #LF_LRU_NEXT]
    ldr x1, [x19, #LRU_TAIL]
    str x1, [x23, #LF_LRU_PREV]

    cmn x1, #1
    b.eq .Llci_first_entry

    // Update old tail's next
    ldr x2, [x19, #LRU_FRAMES_PTR]
    mov x3, #LF_SIZE
    mul x3, x3, x1
    add x2, x2, x3
    str x22, [x2, #LF_LRU_NEXT]
    b .Llci_update_tail

.Llci_first_entry:
    str x22, [x19, #LRU_HEAD]

.Llci_update_tail:
    str x22, [x19, #LRU_TAIL]

    // Increment count
    ldr x0, [x19, #LRU_COUNT]
    add x0, x0, #1
    str x0, [x19, #LRU_COUNT]

    ldr x0, [x23, #LF_PAGE_PTR]
    b .Llci_ret

.Llci_fail:
    mov x0, #0

.Llci_ret:
    ldp x23, x24, [sp, #48]
    ldp x21, x22, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #64
    ret
.size lru_cache_insert, .-lru_cache_insert

// ============================================================================
// lru_cache_unpin(cache, page_id) -> void
// Unpin a page (allow it to be evicted)
// x0 = cache, x1 = page_id
// ============================================================================
.global lru_cache_unpin
.type lru_cache_unpin, %function
lru_cache_unpin:
    // Find frame for page_id via hash lookup
    ldr x2, [x0, #LRU_HASH_PTR]
    ldr x3, [x0, #LRU_HASH_SIZE]
    sub x4, x3, #1
    and x4, x1, x4                // hash = page_id & (hash_size - 1)

    ldr x5, [x0, #LRU_FRAMES_PTR]

.Llcu_probe:
    ldr x6, [x2, x4, lsl #3]
    cmn x6, #1
    b.eq .Llcu_done                // not found

    mov x7, #LF_SIZE
    mul x7, x7, x6
    add x7, x5, x7
    ldr w8, [x7, #LF_PAGE_ID]
    ldr w9, [x7, #LF_IS_VALID]
    cmp w8, w1
    b.ne .Llcu_next
    cbz w9, .Llcu_next

    // Found: decrement pin count
    ldr w10, [x7, #LF_PIN_COUNT]
    subs w10, w10, #1
    csel w10, wzr, w10, mi         // clamp to 0
    str w10, [x7, #LF_PIN_COUNT]
    ret

.Llcu_next:
    add x4, x4, #1
    cmp x4, x3
    csel x4, xzr, x4, ge
    b .Llcu_probe

.Llcu_done:
    ret
.size lru_cache_unpin, .-lru_cache_unpin

// ============================================================================
// lru_cache_mark_dirty(cache, page_id) -> void
// ============================================================================
.global lru_cache_mark_dirty
.type lru_cache_mark_dirty, %function
lru_cache_mark_dirty:
    ldr x2, [x0, #LRU_HASH_PTR]
    ldr x3, [x0, #LRU_HASH_SIZE]
    sub x4, x3, #1
    and x4, x1, x4                // hash = page_id & (hash_size - 1)

    ldr x5, [x0, #LRU_FRAMES_PTR]

.Llcmd_probe:
    ldr x6, [x2, x4, lsl #3]
    cmn x6, #1
    b.eq .Llcmd_done

    mov x7, #LF_SIZE
    mul x7, x7, x6
    add x7, x5, x7
    ldr w8, [x7, #LF_PAGE_ID]
    ldr w9, [x7, #LF_IS_VALID]
    cmp w8, w1
    b.ne .Llcmd_next
    cbz w9, .Llcmd_next

    mov w10, #1
    str w10, [x7, #LF_IS_DIRTY]
    ret

.Llcmd_next:
    add x4, x4, #1
    cmp x4, x3
    csel x4, xzr, x4, ge
    b .Llcmd_probe

.Llcmd_done:
    ret
.size lru_cache_mark_dirty, .-lru_cache_mark_dirty

// ============================================================================
// lru_cache_stats(cache, hits_out, misses_out) -> void
// x0 = cache, x1 = ptr to uint64 hits, x2 = ptr to uint64 misses
// ============================================================================
.global lru_cache_stats
.type lru_cache_stats, %function
lru_cache_stats:
    ldr x3, [x0, #LRU_HITS]
    ldr x4, [x0, #LRU_MISSES]
    str x3, [x1]
    str x4, [x2]
    ret
.size lru_cache_stats, .-lru_cache_stats

.hidden alloc_zeroed
.hidden free_mem
.hidden page_alloc
.hidden page_free
.hidden neon_copy_page
