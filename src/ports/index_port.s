// AssemblyDB - Index Port Definition
// Vtable storage for index_port_t

.include "src/const.s"

.data
.align 3

.global index_port_vtable
.type index_port_vtable, %object
index_port_vtable:
    .zero INDEX_PORT_SIZE
.size index_port_vtable, .-index_port_vtable
