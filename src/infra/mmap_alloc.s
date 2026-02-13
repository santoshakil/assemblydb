// AssemblyDB - mmap-based Page Allocator
// No libc malloc. All memory from mmap anonymous pages.

.include "src/const.s"

.text

// ============================================================================
// page_alloc(num_pages) -> ptr or 0 on failure
// Allocate num_pages * PAGE_SIZE bytes via mmap
// x0 = number of pages
// Returns: x0 = pointer to allocated memory, or 0 on failure
// ============================================================================
.global page_alloc
.type page_alloc, %function
page_alloc:
    stp x29, x30, [sp, #-16]!
    mov x29, sp

    lsl x0, x0, #PAGE_SHIFT    // num_pages * 4096
    bl mmap_anon_rw

    // Check for error (negative return = -errno)
    cmn x0, #4096
    b.hi 1f                     // error: return 0
    ldp x29, x30, [sp], #16
    ret
1:
    mov x0, #0
    ldp x29, x30, [sp], #16
    ret
.size page_alloc, .-page_alloc

// ============================================================================
// page_free(ptr, num_pages) -> 0 or -errno
// Free memory allocated by page_alloc
// x0 = ptr, x1 = number of pages
// ============================================================================
.global page_free
.type page_free, %function
page_free:
    lsl x1, x1, #PAGE_SHIFT    // num_pages * 4096
    mov x8, #SYS_munmap
    svc #0
    ret
.size page_free, .-page_free

// ============================================================================
// alloc_aligned(size, alignment) -> ptr or 0
// Allocate size bytes with given alignment (must be power of 2)
// Falls back to page-aligned mmap (always at least page-aligned)
// x0 = size, x1 = alignment (unused, mmap is always page-aligned)
// ============================================================================
.global alloc_aligned
.type alloc_aligned, %function
alloc_aligned:
    stp x29, x30, [sp, #-16]!
    mov x29, sp

    // Round up to page size
    add x0, x0, #PAGE_SIZE - 1
    and x0, x0, #PAGE_MASK
    bl mmap_anon_rw

    cmn x0, #4096
    b.hi 1f
    ldp x29, x30, [sp], #16
    ret
1:
    mov x0, #0
    ldp x29, x30, [sp], #16
    ret
.size alloc_aligned, .-alloc_aligned

// ============================================================================
// alloc_zeroed(size) -> ptr or 0
// mmap anonymous memory is always zeroed by the kernel
// x0 = size
// ============================================================================
.global alloc_zeroed
.type alloc_zeroed, %function
alloc_zeroed:
    stp x29, x30, [sp, #-16]!
    mov x29, sp

    add x0, x0, #PAGE_SIZE - 1
    and x0, x0, #PAGE_MASK
    bl mmap_anon_rw

    cmn x0, #4096
    b.hi 1f
    ldp x29, x30, [sp], #16
    ret
1:
    mov x0, #0
    ldp x29, x30, [sp], #16
    ret
.size alloc_zeroed, .-alloc_zeroed

// ============================================================================
// free_mem(ptr, size) -> 0 or -errno
// x0 = ptr, x1 = size
// ============================================================================
.global free_mem
.type free_mem, %function
free_mem:
    cbz x0, 1f                 // Don't munmap NULL
    add x1, x1, #PAGE_SIZE - 1
    and x1, x1, #PAGE_MASK
    mov x8, #SYS_munmap
    svc #0
    ret
1:
    mov x0, #0
    ret
.size free_mem, .-free_mem
