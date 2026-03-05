// AssemblyDB - Index Port Adapter
// Wires secondary index into index_port vtable

.include "src/const.s"

.text

// ============================================================================
// index_adapter_init(vtable_ptr) -> void
// ============================================================================
.global index_adapter_init
.type index_adapter_init, %function
index_adapter_init:
    adrp x1, sec_index_create
    add x1, x1, :lo12:sec_index_create
    str x1, [x0, #IP_FN_CREATE]

    adrp x1, sec_index_destroy
    add x1, x1, :lo12:sec_index_destroy
    str x1, [x0, #IP_FN_DROP]

    adrp x1, sec_index_insert
    add x1, x1, :lo12:sec_index_insert
    str x1, [x0, #IP_FN_INSERT]

    // Delete not yet implemented - safe stub prevents blr NULL
    adrp x1, not_impl_stub
    add x1, x1, :lo12:not_impl_stub
    str x1, [x0, #IP_FN_DELETE]

    adrp x1, sec_index_scan
    add x1, x1, :lo12:sec_index_scan
    str x1, [x0, #IP_FN_SCAN]

    ret
.size index_adapter_init, .-index_adapter_init

.hidden not_impl_stub
.hidden sec_index_create
.hidden sec_index_destroy
.hidden sec_index_insert
.hidden sec_index_scan
