#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include "assemblydb.h"

static int tests_run = 0;
static int tests_passed = 0;

#define TEST(name) do { \
    tests_run++; \
    printf("  [%02d] %-50s ", tests_run, name); \
    fflush(stdout); \
} while(0)

#define PASS() do { tests_passed++; printf("PASS\n"); } while(0)
#define FAIL(msg) do { printf("FAIL: %s\n", msg); } while(0)

// ============================================================================
// AES Key Expansion Tests
// ============================================================================

static void test_key_expand_deterministic(void) {
    TEST("key expansion is deterministic");

    uint8_t key[32];
    for (int i = 0; i < 32; i++) key[i] = (uint8_t)i;

    uint8_t expanded1[240], expanded2[240];
    memset(expanded1, 0xAA, 240);
    memset(expanded2, 0xBB, 240);

    aes_key_expand_256(key, expanded1);
    aes_key_expand_256(key, expanded2);

    if (memcmp(expanded1, expanded2, 240) == 0) PASS();
    else FAIL("two expansions of same key differ");
}

static void test_key_expand_first_32(void) {
    TEST("expanded key starts with original key");

    uint8_t key[32];
    for (int i = 0; i < 32; i++) key[i] = (uint8_t)(i * 3 + 7);

    uint8_t expanded[240];
    aes_key_expand_256(key, expanded);

    if (memcmp(expanded, key, 32) == 0) PASS();
    else FAIL("first 32 bytes should be the original key");
}

static void test_key_expand_different_keys(void) {
    TEST("different keys produce different expansions");

    uint8_t key1[32], key2[32];
    memset(key1, 0, 32);
    memset(key2, 0, 32);
    key2[0] = 1;  // one bit difference

    uint8_t exp1[240], exp2[240];
    aes_key_expand_256(key1, exp1);
    aes_key_expand_256(key2, exp2);

    if (memcmp(exp1, exp2, 240) != 0) PASS();
    else FAIL("different keys should give different expansions");
}

// ============================================================================
// AES Block Encryption Tests
// ============================================================================

static void test_encrypt_block_nonzero(void) {
    TEST("encrypted block is non-zero output");

    uint8_t key[32] = {0};
    uint8_t expanded[240];
    aes_key_expand_256(key, expanded);

    uint8_t input[16] = {0};
    uint8_t output[16] = {0};
    aes_encrypt_block(expanded, input, output);

    // Output should not be all zeros (AES of zero with zero key is non-zero)
    int all_zero = 1;
    for (int i = 0; i < 16; i++) {
        if (output[i] != 0) { all_zero = 0; break; }
    }
    if (!all_zero) PASS();
    else FAIL("encrypted output should not be all zeros");
}

static void test_encrypt_block_deterministic(void) {
    TEST("block encryption is deterministic");

    uint8_t key[32];
    for (int i = 0; i < 32; i++) key[i] = (uint8_t)i;
    uint8_t expanded[240];
    aes_key_expand_256(key, expanded);

    uint8_t input[16];
    for (int i = 0; i < 16; i++) input[i] = (uint8_t)(i + 0x10);

    uint8_t out1[16], out2[16];
    aes_encrypt_block(expanded, input, out1);
    aes_encrypt_block(expanded, input, out2);

    if (memcmp(out1, out2, 16) == 0) PASS();
    else FAIL("same input should give same output");
}

static void test_encrypt_block_different_input(void) {
    TEST("different plaintext gives different ciphertext");

    uint8_t key[32] = {0};
    uint8_t expanded[240];
    aes_key_expand_256(key, expanded);

    uint8_t in1[16] = {0};
    uint8_t in2[16] = {0};
    in2[0] = 1;

    uint8_t out1[16], out2[16];
    aes_encrypt_block(expanded, in1, out1);
    aes_encrypt_block(expanded, in2, out2);

    if (memcmp(out1, out2, 16) != 0) PASS();
    else FAIL("different inputs should produce different outputs");
}

// ============================================================================
// AES-CTR Mode Tests
// ============================================================================

static void test_ctr_roundtrip(void) {
    TEST("CTR encrypt then decrypt recovers plaintext");

    uint8_t key[32];
    for (int i = 0; i < 32; i++) key[i] = (uint8_t)(i * 7);
    uint8_t expanded[240];
    aes_key_expand_256(key, expanded);

    uint8_t plaintext[64], ciphertext[64], recovered[64];
    for (int i = 0; i < 64; i++) plaintext[i] = (uint8_t)(i + 100);

    aes_ctr_process(expanded, plaintext, ciphertext, 64, 42);
    aes_ctr_process(expanded, ciphertext, recovered, 64, 42);

    if (memcmp(plaintext, recovered, 64) == 0) PASS();
    else FAIL("CTR decrypt should recover plaintext");
}

static void test_ctr_different_page_ids(void) {
    TEST("different page_ids produce different ciphertext");

    uint8_t key[32] = {1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,
                       17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32};
    uint8_t expanded[240];
    aes_key_expand_256(key, expanded);

    uint8_t plaintext[32] = {0};
    uint8_t ct1[32], ct2[32];

    aes_ctr_process(expanded, plaintext, ct1, 32, 1);
    aes_ctr_process(expanded, plaintext, ct2, 32, 2);

    if (memcmp(ct1, ct2, 32) != 0) PASS();
    else FAIL("different page IDs should give different ciphertext");
}

static void test_ctr_page_size(void) {
    TEST("CTR roundtrip on full page (4096 bytes)");

    uint8_t key[32];
    for (int i = 0; i < 32; i++) key[i] = (uint8_t)(0xAA ^ i);
    uint8_t expanded[240];
    aes_key_expand_256(key, expanded);

    uint8_t *pt = (uint8_t *)page_alloc(1);
    uint8_t *ct = (uint8_t *)page_alloc(1);
    uint8_t *rec = (uint8_t *)page_alloc(1);

    for (int i = 0; i < 4096; i++) pt[i] = (uint8_t)(i & 0xFF);

    aes_ctr_process(expanded, pt, ct, 4096, 99);
    aes_ctr_process(expanded, ct, rec, 4096, 99);

    int ok = (memcmp(pt, rec, 4096) == 0);

    // Also verify ciphertext differs from plaintext
    int differs = (memcmp(pt, ct, 4096) != 0);

    page_free(pt, 1);
    page_free(ct, 1);
    page_free(rec, 1);

    if (ok && differs) PASS();
    else if (!ok) FAIL("roundtrip failed on full page");
    else FAIL("ciphertext should differ from plaintext");
}

// ============================================================================
// Crypto Context Tests
// ============================================================================

static void test_crypto_ctx_lifecycle(void) {
    TEST("crypto context create/destroy");

    void *ctx = crypto_ctx_create();
    if (!ctx) { FAIL("create returned NULL"); return; }

    crypto_ctx_destroy(ctx);
    PASS();
}

static void test_crypto_ctx_set_key(void) {
    TEST("set key and encrypt page");

    void *ctx = crypto_ctx_create();
    if (!ctx) { FAIL("create failed"); return; }

    uint8_t key[32];
    for (int i = 0; i < 32; i++) key[i] = (uint8_t)i;

    int rc = aes_set_key_impl(ctx, key, 32);
    if (rc != 0) { FAIL("set_key failed"); crypto_ctx_destroy(ctx); return; }

    uint8_t *pt = (uint8_t *)page_alloc(1);
    uint8_t *ct = (uint8_t *)page_alloc(1);
    uint8_t *rec = (uint8_t *)page_alloc(1);

    for (int i = 0; i < 4096; i++) pt[i] = (uint8_t)(i * 3);

    rc = aes_page_encrypt(ctx, pt, ct, 7);
    if (rc != 0) { FAIL("encrypt failed"); goto cleanup; }

    rc = aes_page_decrypt(ctx, ct, rec, 7);
    if (rc != 0) { FAIL("decrypt failed"); goto cleanup; }

    if (memcmp(pt, rec, 4096) == 0) PASS();
    else FAIL("roundtrip through context failed");

cleanup:
    page_free(pt, 1);
    page_free(ct, 1);
    page_free(rec, 1);
    crypto_ctx_destroy(ctx);
}

static void test_crypto_ctx_no_key(void) {
    TEST("encrypt without key returns error");

    void *ctx = crypto_ctx_create();
    if (!ctx) { FAIL("create failed"); return; }

    uint8_t pt[16] = {0}, ct[16] = {0};
    int rc = aes_page_encrypt(ctx, pt, ct, 0);

    crypto_ctx_destroy(ctx);

    if (rc != 0) PASS();
    else FAIL("should fail without key set");
}

static void test_crypto_ctx_bad_key_len(void) {
    TEST("set_key with wrong length returns error");

    void *ctx = crypto_ctx_create();
    if (!ctx) { FAIL("create failed"); return; }

    uint8_t key[16] = {0};
    int rc = aes_set_key_impl(ctx, key, 16);

    crypto_ctx_destroy(ctx);

    if (rc != 0) PASS();
    else FAIL("should reject non-32-byte key");
}

// ============================================================================
// No-op Crypto Tests
// ============================================================================

static void test_noop_passthrough(void) {
    TEST("noop crypto copies data unchanged");

    uint8_t *pt = (uint8_t *)page_alloc(1);
    uint8_t *ct = (uint8_t *)page_alloc(1);

    for (int i = 0; i < 4096; i++) pt[i] = (uint8_t)(i & 0xFF);
    memset(ct, 0, 4096);

    int rc = noop_encrypt_page(NULL, pt, ct, 0);

    int ok = (rc == 0 && memcmp(pt, ct, 4096) == 0);

    page_free(pt, 1);
    page_free(ct, 1);

    if (ok) PASS();
    else FAIL("noop should copy data unchanged");
}

// ============================================================================
// AES Known Answer Test (NIST FIPS 197 Appendix C.3)
// ============================================================================

static void test_aes256_known_answer(void) {
    TEST("AES-256 NIST known answer test");

    // NIST FIPS 197 Appendix C.3: AES-256
    // Key: 000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f
    // Plaintext: 00112233445566778899aabbccddeeff
    // Ciphertext: 8ea2b7ca516745bfeafc49904b496089
    uint8_t key[32] = {
        0x00,0x01,0x02,0x03,0x04,0x05,0x06,0x07,
        0x08,0x09,0x0a,0x0b,0x0c,0x0d,0x0e,0x0f,
        0x10,0x11,0x12,0x13,0x14,0x15,0x16,0x17,
        0x18,0x19,0x1a,0x1b,0x1c,0x1d,0x1e,0x1f
    };
    uint8_t plaintext[16] = {
        0x00,0x11,0x22,0x33,0x44,0x55,0x66,0x77,
        0x88,0x99,0xaa,0xbb,0xcc,0xdd,0xee,0xff
    };
    uint8_t expected[16] = {
        0x8e,0xa2,0xb7,0xca,0x51,0x67,0x45,0xbf,
        0xea,0xfc,0x49,0x90,0x4b,0x49,0x60,0x89
    };

    uint8_t expanded[240];
    aes_key_expand_256(key, expanded);

    uint8_t output[16];
    aes_encrypt_block(expanded, plaintext, output);

    if (memcmp(output, expected, 16) == 0) PASS();
    else {
        printf("FAIL: got ");
        for (int i = 0; i < 16; i++) printf("%02x", output[i]);
        printf(" expected ");
        for (int i = 0; i < 16; i++) printf("%02x", expected[i]);
        printf("\n");
    }
}

int main(void) {
    printf("=== AssemblyDB Phase 5: Crypto Tests ===\n\n");

    // Key expansion
    test_key_expand_deterministic();
    test_key_expand_first_32();
    test_key_expand_different_keys();

    // Block encryption
    test_encrypt_block_nonzero();
    test_encrypt_block_deterministic();
    test_encrypt_block_different_input();

    // NIST known answer
    test_aes256_known_answer();

    // CTR mode
    test_ctr_roundtrip();
    test_ctr_different_page_ids();
    test_ctr_page_size();

    // Crypto context
    test_crypto_ctx_lifecycle();
    test_crypto_ctx_set_key();
    test_crypto_ctx_no_key();
    test_crypto_ctx_bad_key_len();

    // No-op
    test_noop_passthrough();

    printf("\n=== Results: %d/%d passed ===\n", tests_passed, tests_run);
    return tests_passed == tests_run ? 0 : 1;
}
