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
// LZ4 Context Tests
// ============================================================================

static void test_lz4_ctx_lifecycle(void) {
    TEST("LZ4 context create/destroy");

    void *ctx = lz4_ctx_create();
    if (!ctx) { FAIL("create returned NULL"); return; }
    lz4_ctx_destroy(ctx);
    PASS();
}

// ============================================================================
// LZ4 Compression Tests
// ============================================================================

static void test_lz4_compress_small(void) {
    TEST("compress small data");

    void *ctx = lz4_ctx_create();
    uint8_t input[] = "Hello, World!";
    uint8_t output[256];

    int64_t clen = lz4_compress(ctx, input, sizeof(input), output, 256);
    lz4_ctx_destroy(ctx);

    if (clen > 0) PASS();
    else FAIL("compress returned error");
}

static void test_lz4_roundtrip_small(void) {
    TEST("compress/decompress roundtrip (small)");

    void *ctx = lz4_ctx_create();
    uint8_t input[] = "The quick brown fox jumps over the lazy dog";
    size_t ilen = sizeof(input);
    uint8_t compressed[256];
    uint8_t decompressed[256];

    int64_t clen = lz4_compress(ctx, input, ilen, compressed, 256);
    lz4_ctx_destroy(ctx);

    if (clen <= 0) { FAIL("compress failed"); return; }

    int64_t dlen = lz4_decompress(compressed, (size_t)clen, decompressed, 256);

    if (dlen == (int64_t)ilen && memcmp(input, decompressed, ilen) == 0)
        PASS();
    else {
        printf("FAIL: dlen=%ld ilen=%zu\n", (long)dlen, ilen);
    }
}

static void test_lz4_roundtrip_repetitive(void) {
    TEST("roundtrip with repetitive data (should compress)");

    void *ctx = lz4_ctx_create();

    // Highly repetitive data - should compress well
    uint8_t input[1024];
    for (int i = 0; i < 1024; i++) input[i] = (uint8_t)(i % 16);

    uint8_t compressed[2048];
    uint8_t decompressed[1024];

    int64_t clen = lz4_compress(ctx, input, 1024, compressed, 2048);
    lz4_ctx_destroy(ctx);

    if (clen <= 0) { FAIL("compress failed"); return; }

    int64_t dlen = lz4_decompress(compressed, (size_t)clen, decompressed, 1024);

    if (dlen == 1024 && memcmp(input, decompressed, 1024) == 0) {
        printf("PASS (ratio: %ld/%d = %.1fx)\n",
               (long)clen, 1024, 1024.0 / (double)clen);
        tests_passed++;
    } else {
        printf("FAIL: dlen=%ld\n", (long)dlen);
    }
}

static void test_lz4_roundtrip_zeros(void) {
    TEST("roundtrip with all-zero data");

    void *ctx = lz4_ctx_create();
    uint8_t input[512];
    memset(input, 0, 512);

    uint8_t compressed[1024];
    uint8_t decompressed[512];

    int64_t clen = lz4_compress(ctx, input, 512, compressed, 1024);
    lz4_ctx_destroy(ctx);

    if (clen <= 0) { FAIL("compress failed"); return; }

    int64_t dlen = lz4_decompress(compressed, (size_t)clen, decompressed, 512);

    if (dlen == 512 && memcmp(input, decompressed, 512) == 0) {
        printf("PASS (ratio: %ld/512 = %.1fx)\n",
               (long)clen, 512.0 / (double)clen);
        tests_passed++;
    } else {
        printf("FAIL: dlen=%ld\n", (long)dlen);
    }
}

static void test_lz4_roundtrip_page(void) {
    TEST("roundtrip on page-sized data (4096)");

    void *ctx = lz4_ctx_create();
    uint8_t *input = (uint8_t *)page_alloc(1);
    uint8_t *compressed = (uint8_t *)page_alloc(2);  // 8KB buffer
    uint8_t *decompressed = (uint8_t *)page_alloc(1);

    // Mixed data: some repetitive, some random-ish
    for (int i = 0; i < 4096; i++) {
        if (i < 2048) input[i] = (uint8_t)(i % 32);
        else input[i] = (uint8_t)(i * 31 + 17);
    }

    int64_t clen = lz4_compress(ctx, input, 4096, compressed, 8192);
    lz4_ctx_destroy(ctx);

    if (clen <= 0) {
        FAIL("compress failed");
        goto cleanup;
    }

    int64_t dlen = lz4_decompress(compressed, (size_t)clen, decompressed, 4096);

    if (dlen == 4096 && memcmp(input, decompressed, 4096) == 0) {
        printf("PASS (ratio: %ld/4096 = %.1fx)\n",
               (long)clen, 4096.0 / (double)clen);
        tests_passed++;
    } else {
        printf("FAIL: dlen=%ld\n", (long)dlen);
    }

cleanup:
    page_free(input, 1);
    page_free(compressed, 2);
    page_free(decompressed, 1);
}

static void test_lz4_random_data(void) {
    TEST("roundtrip with pseudo-random data");

    void *ctx = lz4_ctx_create();

    // Random-like data (poor compression expected)
    uint8_t input[256];
    uint32_t state = 0xDEADBEEF;
    for (int i = 0; i < 256; i++) {
        state = state * 1103515245 + 12345;
        input[i] = (uint8_t)(state >> 16);
    }

    uint8_t compressed[512];
    uint8_t decompressed[256];

    int64_t clen = lz4_compress(ctx, input, 256, compressed, 512);
    lz4_ctx_destroy(ctx);

    if (clen <= 0) { FAIL("compress failed"); return; }

    int64_t dlen = lz4_decompress(compressed, (size_t)clen, decompressed, 256);

    if (dlen == 256 && memcmp(input, decompressed, 256) == 0)
        PASS();
    else {
        printf("FAIL: dlen=%ld\n", (long)dlen);
    }
}

static void test_lz4_empty(void) {
    TEST("compress empty input");

    void *ctx = lz4_ctx_create();
    uint8_t output[16];

    int64_t clen = lz4_compress(ctx, NULL, 0, output, 16);
    lz4_ctx_destroy(ctx);

    // Empty input should produce a small output (just end marker)
    if (clen >= 0) PASS();
    else FAIL("empty compress should not error");
}

static void test_lz4_tiny(void) {
    TEST("roundtrip with 1-byte input");

    void *ctx = lz4_ctx_create();
    uint8_t input[1] = {0x42};
    uint8_t compressed[32];
    uint8_t decompressed[1];

    int64_t clen = lz4_compress(ctx, input, 1, compressed, 32);
    lz4_ctx_destroy(ctx);

    if (clen <= 0) { FAIL("compress failed"); return; }

    int64_t dlen = lz4_decompress(compressed, (size_t)clen, decompressed, 1);

    if (dlen == 1 && decompressed[0] == 0x42) PASS();
    else FAIL("roundtrip mismatch");
}

// ============================================================================
// LZ4 Max Size Test
// ============================================================================

static void test_lz4_max_size(void) {
    TEST("max_compressed_size >= input_len");

    size_t msz = lz4_max_compressed_size(4096);
    if (msz >= 4096) PASS();
    else {
        printf("FAIL: max_size=%zu for 4096 input\n", msz);
    }
}

// ============================================================================
// No-op Compression Tests
// ============================================================================

static void test_noop_compress_passthrough(void) {
    TEST("noop compress copies data unchanged");

    uint8_t input[64], output[64];
    for (int i = 0; i < 64; i++) input[i] = (uint8_t)(i * 5);
    memset(output, 0, 64);

    int64_t len = noop_compress(NULL, input, 64, output, 64);

    if (len == 64 && memcmp(input, output, 64) == 0) PASS();
    else FAIL("noop should copy unchanged");
}

static void test_noop_decompress_passthrough(void) {
    TEST("noop decompress copies data unchanged");

    uint8_t input[64], output[64];
    for (int i = 0; i < 64; i++) input[i] = (uint8_t)(i + 100);
    memset(output, 0, 64);

    int64_t len = noop_decompress(NULL, input, 64, output, 64);

    if (len == 64 && memcmp(input, output, 64) == 0) PASS();
    else FAIL("noop should copy unchanged");
}

// ============================================================================
// LZ4 Stress Test
// ============================================================================

static void test_lz4_multiple_patterns(void) {
    TEST("roundtrip across multiple data patterns");

    void *ctx = lz4_ctx_create();
    int ok = 1;

    uint8_t input[128], compressed[256], decompressed[128];

    // Pattern 1: ascending bytes
    for (int i = 0; i < 128; i++) input[i] = (uint8_t)i;
    int64_t clen = lz4_compress(ctx, input, 128, compressed, 256);
    if (clen <= 0) { ok = 0; goto done; }
    int64_t dlen = lz4_decompress(compressed, (size_t)clen, decompressed, 128);
    if (dlen != 128 || memcmp(input, decompressed, 128) != 0) { ok = 0; goto done; }

    // Pattern 2: all same byte
    memset(input, 0xAA, 128);
    clen = lz4_compress(ctx, input, 128, compressed, 256);
    if (clen <= 0) { ok = 0; goto done; }
    dlen = lz4_decompress(compressed, (size_t)clen, decompressed, 128);
    if (dlen != 128 || memcmp(input, decompressed, 128) != 0) { ok = 0; goto done; }

    // Pattern 3: two-byte repeating
    for (int i = 0; i < 128; i++) input[i] = (i % 2) ? 0xFF : 0x00;
    clen = lz4_compress(ctx, input, 128, compressed, 256);
    if (clen <= 0) { ok = 0; goto done; }
    dlen = lz4_decompress(compressed, (size_t)clen, decompressed, 128);
    if (dlen != 128 || memcmp(input, decompressed, 128) != 0) { ok = 0; goto done; }

done:
    lz4_ctx_destroy(ctx);
    if (ok) PASS();
    else FAIL("one or more patterns failed roundtrip");
}

int main(void) {
    printf("=== AssemblyDB Phase 5: Compression Tests ===\n\n");

    // LZ4 context
    test_lz4_ctx_lifecycle();

    // LZ4 compression
    test_lz4_compress_small();
    test_lz4_roundtrip_small();
    test_lz4_roundtrip_repetitive();
    test_lz4_roundtrip_zeros();
    test_lz4_roundtrip_page();
    test_lz4_random_data();
    test_lz4_empty();
    test_lz4_tiny();
    test_lz4_max_size();

    // No-op
    test_noop_compress_passthrough();
    test_noop_decompress_passthrough();

    // Stress
    test_lz4_multiple_patterns();

    printf("\n=== Results: %d/%d passed ===\n", tests_passed, tests_run);
    return tests_passed == tests_run ? 0 : 1;
}
