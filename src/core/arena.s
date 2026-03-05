// AssemblyDB - Arena Allocator
// Bump allocation within mmap chunks. Entire arena freed at once.
// Used for memtable skip list nodes (no individual free needed).

.include "src/const.s"

.text

// ============================================================================
// arena_create() -> arena_ptr or 0
// Allocate and initialize a new arena
// Returns: x0 = pointer to arena state (128 bytes), or 0 on failure
// ============================================================================
.global arena_create
.type arena_create, %function
arena_create:
    stp x29, x30, [sp, #-32]!
    mov x29, sp
    str x19, [sp, #16]

    // Allocate arena state struct (128 bytes, page-aligned)
    mov x0, #ARENA_SIZE
    bl alloc_zeroed
    cbz x0, .Lac_fail
    mov x19, x0

    // Allocate first chunk (1 MB)
    mov x0, #ARENA_CHUNK_DEFAULT
    bl mmap_anon_rw
    cmn x0, #4096
    b.hi .Lac_fail_free

    // Initialize arena state
    str x0, [x19, #ARENA_BASE_PTR]         // base_ptr = chunk
    mov x1, #ACHUNK_HEADER_SIZE
    str x1, [x19, #ARENA_CURRENT_OFF]      // current_offset = past header
    mov x1, #ARENA_CHUNK_DEFAULT
    str x1, [x19, #ARENA_CHUNK_SIZE]       // chunk_size
    str xzr, [x19, #ARENA_TOTAL_ALLOC]     // total = 0

    // Initialize chunk header (linked list)
    str xzr, [x0, #ACHUNK_NEXT]            // next = NULL
    mov x1, #ARENA_CHUNK_DEFAULT
    str x1, [x0, #ACHUNK_SIZE]             // size
    str x0, [x19, #ARENA_CHUNK_LIST]       // chunk_list_head = this chunk

    mov x0, x19
    ldr x19, [sp, #16]
    ldp x29, x30, [sp], #32
    ret

.Lac_fail_free:
    mov x0, x19
    mov x1, #ARENA_SIZE
    bl free_mem
.Lac_fail:
    mov x0, #0
    ldr x19, [sp, #16]
    ldp x29, x30, [sp], #32
    ret
.size arena_create, .-arena_create

// ============================================================================
// arena_alloc(arena_ptr, size) -> ptr or 0
// Bump allocate from arena. Returns 8-byte aligned pointer.
// x0 = arena_ptr, x1 = requested size
// Returns: x0 = pointer to allocated memory, or 0 on failure
// ============================================================================
.global arena_alloc
.type arena_alloc, %function
arena_alloc:
    stp x29, x30, [sp, #-48]!
    mov x29, sp
    stp x19, x20, [sp, #16]
    str x21, [sp, #32]

    cbz x0, .Laa_fail          // NULL arena guard
    cbz x1, .Laa_fail          // zero-size guard

    mov x19, x0                // arena
    // Align size up to 8 bytes
    add x20, x1, #7
    and x20, x20, #~7          // aligned size

    // Check if current chunk has room
    ldr x0, [x19, #ARENA_CURRENT_OFF]      // current_offset
    ldr x1, [x19, #ARENA_CHUNK_SIZE]       // chunk_size
    add x2, x0, x20                         // new_offset = current + size
    cmp x2, x1
    b.hi .Laa_new_chunk                      // need new chunk (unsigned)

    // Allocate from current chunk
    ldr x3, [x19, #ARENA_BASE_PTR]
    add x21, x3, x0                         // result = base + current_offset
    str x2, [x19, #ARENA_CURRENT_OFF]       // update offset

    // Update total
    ldr x0, [x19, #ARENA_TOTAL_ALLOC]
    add x0, x0, x20
    str x0, [x19, #ARENA_TOTAL_ALLOC]

    mov x0, x21
    ldr x21, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #48
    ret

.Laa_new_chunk:
    // Determine chunk size (at least ARENA_CHUNK_DEFAULT or requested size + header)
    add x0, x20, #ACHUNK_HEADER_SIZE
    mov x1, #ARENA_CHUNK_DEFAULT
    cmp x0, x1
    csel x21, x0, x1, hi       // max(needed, default) unsigned

    // Align chunk size to page
    add x21, x21, #PAGE_SIZE - 1
    and x21, x21, #PAGE_MASK

    // Allocate new chunk
    mov x0, x21
    bl mmap_anon_rw
    cmn x0, #4096
    b.hi .Laa_fail

    // Link new chunk to list
    ldr x1, [x19, #ARENA_CHUNK_LIST]
    str x1, [x0, #ACHUNK_NEXT]             // new->next = old head
    str x21, [x0, #ACHUNK_SIZE]            // new->size = chunk_size
    str x0, [x19, #ARENA_CHUNK_LIST]       // head = new

    // Update arena state
    str x0, [x19, #ARENA_BASE_PTR]
    str x21, [x19, #ARENA_CHUNK_SIZE]

    // Allocate from new chunk
    mov x1, #ACHUNK_HEADER_SIZE
    add x2, x1, x20                         // new_offset
    str x2, [x19, #ARENA_CURRENT_OFF]

    add x21, x0, x1                         // result = chunk + header_size

    // Update total
    ldr x1, [x19, #ARENA_TOTAL_ALLOC]
    add x1, x1, x20
    str x1, [x19, #ARENA_TOTAL_ALLOC]

    mov x0, x21
    ldr x21, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #48
    ret

.Laa_fail:
    mov x0, #0
    ldr x21, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #48
    ret
.size arena_alloc, .-arena_alloc

// ============================================================================
// arena_destroy(arena_ptr)
// Free all chunks and the arena state itself
// x0 = arena_ptr
// ============================================================================
.global arena_destroy
.type arena_destroy, %function
arena_destroy:
    stp x29, x30, [sp, #-48]!
    mov x29, sp
    stp x19, x20, [sp, #16]
    str x21, [sp, #32]

    cbz x0, .Lad_done
    mov x19, x0                // arena_ptr

    // Walk chunk list and munmap each
    ldr x20, [x19, #ARENA_CHUNK_LIST]

.Lad_loop:
    cbz x20, .Lad_free_state

    // Save next pointer in callee-saved x21
    ldr x21, [x20, #ACHUNK_NEXT]

    // Free this chunk
    mov x0, x20
    ldr x1, [x20, #ACHUNK_SIZE]
    bl free_mem

    mov x20, x21
    b .Lad_loop

.Lad_free_state:
    // Free arena state struct
    mov x0, x19
    mov x1, #ARENA_SIZE
    bl free_mem

.Lad_done:
    ldr x21, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #48
    ret
.size arena_destroy, .-arena_destroy

// ============================================================================
// arena_reset(arena_ptr)
// Reset arena to initial state (keeps first chunk, frees extras)
// x0 = arena_ptr
// ============================================================================
.global arena_reset
.type arena_reset, %function
arena_reset:
    stp x29, x30, [sp, #-48]!
    mov x29, sp
    stp x19, x20, [sp, #16]
    str x21, [sp, #32]

    cbz x0, .Lar_done          // NULL arena guard

    mov x19, x0

    // Free all chunks except the last one in the list (first allocated)
    ldr x20, [x19, #ARENA_CHUNK_LIST]

    // Find the tail (original chunk)
    // Free everything except tail
.Lar_find_tail:
    ldr x0, [x20, #ACHUNK_NEXT]
    cbz x0, .Lar_reset_state    // x20 is the only/last chunk

    // x20 has a next -> free x20, continue with next
    ldr x21, [x20, #ACHUNK_NEXT]
    mov x0, x20
    ldr x1, [x20, #ACHUNK_SIZE]
    bl free_mem
    mov x20, x21
    b .Lar_find_tail

.Lar_reset_state:
    // x20 = last remaining chunk
    str x20, [x19, #ARENA_CHUNK_LIST]
    str x20, [x19, #ARENA_BASE_PTR]
    ldr x0, [x20, #ACHUNK_SIZE]
    str x0, [x19, #ARENA_CHUNK_SIZE]
    mov x0, #ACHUNK_HEADER_SIZE
    str x0, [x19, #ARENA_CURRENT_OFF]
    str xzr, [x19, #ARENA_TOTAL_ALLOC]
    str xzr, [x20, #ACHUNK_NEXT]

.Lar_done:
    ldr x21, [sp, #32]
    ldp x19, x20, [sp, #16]
    ldp x29, x30, [sp], #48
    ret
.size arena_reset, .-arena_reset
