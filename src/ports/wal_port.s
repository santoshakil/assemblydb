// AssemblyDB - WAL Port Definition
// Vtable layout for Write-Ahead Log abstraction

.include "src/const.s"

.text

// WAL port vtable layout (64 bytes):
// 0x00  fn_append(self, record_ptr, record_len) -> err
// 0x08  fn_sync(self) -> err
// 0x10  fn_rotate(self) -> err
// 0x18  fn_recover(self, callback, ctx) -> err
// 0x20  fn_truncate(self, sequence) -> err
// 0x28-0x38  _reserved

// This file is a placeholder - vtable is populated by wal_adapter.s
// No code needed here, just ensure the .text section exists

.global wal_port_marker
.type wal_port_marker, %function
wal_port_marker:
    ret
.size wal_port_marker, .-wal_port_marker
