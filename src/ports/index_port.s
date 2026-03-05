// AssemblyDB - Index Port Definition
// Vtable populated dynamically by index_adapter_init()

.include "src/const.s"

.text
// No static vtable needed; adapter allocates via alloc_zeroed
