// AssemblyDB - Transaction Port Definition
// Vtable layout for transaction management abstraction

.include "src/const.s"

.text

// tx_port vtable layout (64 bytes):
// 0x00  fn_begin(self, isolation) -> tx_id
// 0x08  fn_commit(self, tx_id) -> err
// 0x10  fn_rollback(self, tx_id) -> err
// 0x18  fn_get_state(self, tx_id) -> state
// 0x20-0x38  _reserved

.global tx_port_marker
.type tx_port_marker, %function
tx_port_marker:
    ret
.size tx_port_marker, .-tx_port_marker
