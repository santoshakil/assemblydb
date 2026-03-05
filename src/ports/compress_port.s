// AssemblyDB - Compression Port Definition
// Vtable populated dynamically by compress_adapter_init_*()

.include "src/const.s"

.text
// No static vtable needed; adapter allocates via alloc_zeroed
