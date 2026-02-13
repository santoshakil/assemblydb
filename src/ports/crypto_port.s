// AssemblyDB - Crypto Port Definition
// Vtable storage for crypto_port_t

.include "src/const.s"

.data
.align 3

.global crypto_port_vtable
.type crypto_port_vtable, %object
crypto_port_vtable:
    .zero CRYPTO_PORT_SIZE
.size crypto_port_vtable, .-crypto_port_vtable
