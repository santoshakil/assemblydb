// AssemblyDB - Crypto Port Adapter
// Wires AES-256-CTR or no-op implementations into crypto_port vtable

.include "src/const.s"

.text

// ============================================================================
// crypto_adapter_init_aes(vtable_ptr) -> void
// Populate crypto port vtable with AES-256 functions
// ============================================================================
.global crypto_adapter_init_aes
.type crypto_adapter_init_aes, %function
crypto_adapter_init_aes:
    adrp x1, aes_page_encrypt
    add x1, x1, :lo12:aes_page_encrypt
    str x1, [x0, #XP_FN_ENCRYPT]

    adrp x1, aes_page_decrypt
    add x1, x1, :lo12:aes_page_decrypt
    str x1, [x0, #XP_FN_DECRYPT]

    adrp x1, aes_set_key_impl
    add x1, x1, :lo12:aes_set_key_impl
    str x1, [x0, #XP_FN_SET_KEY]

    adrp x1, aes_clear_key_impl
    add x1, x1, :lo12:aes_clear_key_impl
    str x1, [x0, #XP_FN_CLEAR_KEY]

    ret
.size crypto_adapter_init_aes, .-crypto_adapter_init_aes

// ============================================================================
// crypto_adapter_init_noop(vtable_ptr) -> void
// Populate crypto port vtable with no-op passthrough
// ============================================================================
.global crypto_adapter_init_noop
.type crypto_adapter_init_noop, %function
crypto_adapter_init_noop:
    adrp x1, noop_encrypt_page
    add x1, x1, :lo12:noop_encrypt_page
    str x1, [x0, #XP_FN_ENCRYPT]

    adrp x1, noop_decrypt_page
    add x1, x1, :lo12:noop_decrypt_page
    str x1, [x0, #XP_FN_DECRYPT]

    adrp x1, noop_set_key
    add x1, x1, :lo12:noop_set_key
    str x1, [x0, #XP_FN_SET_KEY]

    adrp x1, noop_clear_key
    add x1, x1, :lo12:noop_clear_key
    str x1, [x0, #XP_FN_CLEAR_KEY]

    ret
.size crypto_adapter_init_noop, .-crypto_adapter_init_noop

.hidden aes_page_encrypt
.hidden aes_page_decrypt
.hidden aes_set_key_impl
.hidden aes_clear_key_impl
.hidden noop_encrypt_page
.hidden noop_decrypt_page
.hidden noop_set_key
.hidden noop_clear_key
