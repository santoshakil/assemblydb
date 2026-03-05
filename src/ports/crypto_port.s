// AssemblyDB - Crypto Port Definition
// Vtable populated dynamically by crypto_adapter_init_*()

.include "src/const.s"

.text
// No static vtable needed; adapter allocates via alloc_zeroed
