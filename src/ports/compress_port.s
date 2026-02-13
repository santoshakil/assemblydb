// AssemblyDB - Compression Port Definition
// Vtable storage for compress_port_t

.include "src/const.s"

.data
.align 3

.global compress_port_vtable
.type compress_port_vtable, %object
compress_port_vtable:
    .zero COMPRESS_PORT_SIZE
.size compress_port_vtable, .-compress_port_vtable
