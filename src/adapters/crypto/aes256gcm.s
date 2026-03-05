// AssemblyDB - AES-256-CTR Page Encryption Adapter
// Wraps core AES primitives for page-level encrypt/decrypt
// CTR mode: same operation for encrypt and decrypt
//
// Crypto context (CCTX): 256 bytes
//   0x000: expanded_key (240 bytes)
//   0x0F0: key_set flag (8 bytes)
//   0x0F8: reserved (8 bytes)

.include "src/const.s"

.text

.equ CCTX_EXPANDED_KEY,  0x000
.equ CCTX_KEY_SET,       0x0F0
.equ CCTX_SIZE,          0x100

// ============================================================================
// crypto_ctx_create() -> ctx_ptr or NULL
// Allocate and initialize crypto context
// ============================================================================
.global crypto_ctx_create
.type crypto_ctx_create, %function
crypto_ctx_create:
    stp x29, x30, [sp, #-16]!
    mov x29, sp

    mov x0, #CCTX_SIZE
    bl alloc_zeroed

    ldp x29, x30, [sp], #16
    ret
.size crypto_ctx_create, .-crypto_ctx_create

// ============================================================================
// crypto_ctx_destroy(ctx) -> void
// ============================================================================
.global crypto_ctx_destroy
.type crypto_ctx_destroy, %function
crypto_ctx_destroy:
    stp x29, x30, [sp, #-32]!
    mov x29, sp
    str x19, [sp, #16]

    cbz x0, .Lccd_done

    mov x19, x0

    // Zero the key material first
    mov x1, #CCTX_SIZE
    bl neon_memzero

    mov x0, x19
    mov x1, #CCTX_SIZE
    bl free_mem

.Lccd_done:
    ldr x19, [sp, #16]
    ldp x29, x30, [sp], #32
    ret
.size crypto_ctx_destroy, .-crypto_ctx_destroy

// ============================================================================
// aes_page_encrypt(ctx, plaintext, ciphertext, page_id) -> err
// ============================================================================
.global aes_page_encrypt
.type aes_page_encrypt, %function
aes_page_encrypt:
    stp x29, x30, [sp, #-16]!
    mov x29, sp

    // x0 = ctx, x1 = plaintext, x2 = ciphertext, x3 = page_id
    ldr x5, [x0, #CCTX_KEY_SET]
    cbz x5, .Lape_err

    mov x4, x3                    // page_id as nonce (before x3 clobbered)
    add x0, x0, #CCTX_EXPANDED_KEY
    mov x3, #PAGE_SIZE

    bl aes_ctr_process

    mov x0, #ADB_OK
    b .Lape_ret

.Lape_err:
    mov x0, #ADB_ERR_INVALID

.Lape_ret:
    ldp x29, x30, [sp], #16
    ret
.size aes_page_encrypt, .-aes_page_encrypt

// ============================================================================
// aes_page_decrypt(ctx, ciphertext, plaintext, page_id) -> err
// Decrypt = same as encrypt in CTR mode
// ============================================================================
.global aes_page_decrypt
.type aes_page_decrypt, %function
aes_page_decrypt:
    stp x29, x30, [sp, #-16]!
    mov x29, sp

    ldr x5, [x0, #CCTX_KEY_SET]
    cbz x5, .Lapd_err

    mov x4, x3                    // page_id as nonce
    add x0, x0, #CCTX_EXPANDED_KEY
    mov x3, #PAGE_SIZE

    bl aes_ctr_process

    mov x0, #ADB_OK
    b .Lapd_ret

.Lapd_err:
    mov x0, #ADB_ERR_DECRYPT

.Lapd_ret:
    ldp x29, x30, [sp], #16
    ret
.size aes_page_decrypt, .-aes_page_decrypt

// ============================================================================
// aes_set_key_impl(ctx, key_ptr, key_len) -> err
// Set encryption key and expand
// x0 = ctx, x1 = key_ptr, x2 = key_len
// ============================================================================
.global aes_set_key_impl
.type aes_set_key_impl, %function
aes_set_key_impl:
    stp x29, x30, [sp, #-32]!
    mov x29, sp
    str x19, [sp, #16]

    mov x19, x0                    // ctx

    // Verify key_len == 32
    cmp x2, #32
    b.ne .Lask_err

    // Expand key
    // x0 = 32-byte key, x1 = expanded output
    mov x0, x1
    add x1, x19, #CCTX_EXPANDED_KEY
    bl aes_key_expand_256

    // Mark key as set
    mov x0, #1
    str x0, [x19, #CCTX_KEY_SET]

    mov x0, #ADB_OK
    b .Lask_ret

.Lask_err:
    mov x0, #ADB_ERR_INVALID

.Lask_ret:
    ldr x19, [sp, #16]
    ldp x29, x30, [sp], #32
    ret
.size aes_set_key_impl, .-aes_set_key_impl

// ============================================================================
// aes_clear_key_impl(ctx) -> void
// Zero the key material
// x0 = ctx
// ============================================================================
.global aes_clear_key_impl
.type aes_clear_key_impl, %function
aes_clear_key_impl:
    stp x29, x30, [sp, #-16]!
    mov x29, sp

    cbz x0, .Lack_done

    // Zero entire context (key + flag)
    mov x1, #CCTX_SIZE
    bl neon_memzero

.Lack_done:
    ldp x29, x30, [sp], #16
    ret
.size aes_clear_key_impl, .-aes_clear_key_impl

.hidden alloc_zeroed
.hidden free_mem
.hidden neon_memzero
.hidden aes_key_expand_256
.hidden aes_ctr_process
