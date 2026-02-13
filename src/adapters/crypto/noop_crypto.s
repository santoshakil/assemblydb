// AssemblyDB - No-op Crypto Adapter
// Passthrough: copies data without encryption
// Used when encryption is disabled

.include "src/const.s"

.text

// ============================================================================
// noop_encrypt_page(self, plaintext, ciphertext, page_id) -> 0
// Just copy plaintext to ciphertext
// x0 = self (unused), x1 = src, x2 = dst, x3 = page_id (unused)
// ============================================================================
.global noop_encrypt_page
.type noop_encrypt_page, %function
noop_encrypt_page:
    stp x29, x30, [sp, #-16]!
    mov x29, sp

    mov x0, x2                     // dst
    mov x1, x1                     // src (already in x1)
    bl neon_copy_page

    mov x0, #ADB_OK
    ldp x29, x30, [sp], #16
    ret
.size noop_encrypt_page, .-noop_encrypt_page

// ============================================================================
// noop_decrypt_page(self, ciphertext, plaintext, page_id) -> 0
// Just copy ciphertext to plaintext
// ============================================================================
.global noop_decrypt_page
.type noop_decrypt_page, %function
noop_decrypt_page:
    stp x29, x30, [sp, #-16]!
    mov x29, sp

    mov x0, x2                     // dst
    // x1 = src (already there)
    bl neon_copy_page

    mov x0, #ADB_OK
    ldp x29, x30, [sp], #16
    ret
.size noop_decrypt_page, .-noop_decrypt_page

// ============================================================================
// noop_set_key(self, key_ptr, key_len) -> 0
// ============================================================================
.global noop_set_key
.type noop_set_key, %function
noop_set_key:
    mov x0, #ADB_OK
    ret
.size noop_set_key, .-noop_set_key

// ============================================================================
// noop_clear_key(self) -> void
// ============================================================================
.global noop_clear_key
.type noop_clear_key, %function
noop_clear_key:
    ret
.size noop_clear_key, .-noop_clear_key

.hidden neon_copy_page
