// AssemblyDB - Cache Port Adapter
// Wires LRU cache into cache_port vtable

.include "src/const.s"

.text

// ============================================================================
// cache_adapter_init_lru(vtable_ptr) -> void
// ============================================================================
.global cache_adapter_init_lru
.type cache_adapter_init_lru, %function
cache_adapter_init_lru:
    adrp x1, lru_cache_fetch
    add x1, x1, :lo12:lru_cache_fetch
    str x1, [x0, #CP_FN_FETCH]

    adrp x1, lru_cache_unpin
    add x1, x1, :lo12:lru_cache_unpin
    str x1, [x0, #CP_FN_UNPIN]

    adrp x1, lru_cache_mark_dirty
    add x1, x1, :lo12:lru_cache_mark_dirty
    str x1, [x0, #CP_FN_MARK_DIRTY]

    // Evict and flush not wired - safe stub prevents blr NULL
    adrp x1, not_impl_stub
    add x1, x1, :lo12:not_impl_stub
    str x1, [x0, #CP_FN_EVICT]
    str x1, [x0, #CP_FN_FLUSH_ALL]

    adrp x1, lru_cache_stats
    add x1, x1, :lo12:lru_cache_stats
    str x1, [x0, #CP_FN_STATS]

    adrp x1, not_impl_stub
    add x1, x1, :lo12:not_impl_stub
    str x1, [x0, #CP_FN_RESIZE]

    ret
.size cache_adapter_init_lru, .-cache_adapter_init_lru

.hidden not_impl_stub
.hidden lru_cache_fetch
.hidden lru_cache_unpin
.hidden lru_cache_mark_dirty
.hidden lru_cache_stats
