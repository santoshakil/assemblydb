// AssemblyDB - Transaction Port Definition
// Vtable layout for transaction management abstraction
//
// tx_port vtable layout (64 bytes):
// 0x00  fn_begin(self, isolation) -> tx_id
// 0x08  fn_commit(self, tx_id) -> err
// 0x10  fn_rollback(self, tx_id) -> err
// 0x18  fn_get_snapshot(self, tx_id) -> snapshot_id
// 0x20-0x38  _reserved

.include "src/const.s"

.text
// No static vtable needed; tx_manager wires directly
