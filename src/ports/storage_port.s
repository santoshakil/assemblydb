// AssemblyDB - Storage Port (vtable definition)
// Defines the storage interface that adapters implement

.include "src/const.s"

// This file is a placeholder for shared port utilities.
// The vtable is a struct of 8 function pointers (64 bytes).
// Each adapter allocates and populates its own vtable.
// Domain code loads function pointers via:
//   ldr x9, [x19, #DB_STORAGE_PORT]   // load vtable ptr
//   ldr x10, [x9, #SP_FN_PUT]         // load fn_put
//   blr x10                            // call

.text
.global storage_port_noop
.hidden storage_port_noop
.type storage_port_noop, %function
storage_port_noop:
    mov x0, #ADB_OK
    ret
.size storage_port_noop, .-storage_port_noop
