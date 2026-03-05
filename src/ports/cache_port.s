// AssemblyDB - Cache Port Definition
// Vtable populated dynamically by cache_adapter_init_lru()

.include "src/const.s"

.text
// No static vtable needed; adapter allocates via alloc_zeroed
