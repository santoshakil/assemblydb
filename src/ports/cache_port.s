// AssemblyDB - Cache Port Definition
// Vtable storage for cache_port_t

.include "src/const.s"

.data
.align 3

.global cache_port_vtable
.type cache_port_vtable, %object
cache_port_vtable:
    .zero CACHE_PORT_SIZE
.size cache_port_vtable, .-cache_port_vtable
