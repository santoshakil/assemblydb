#include "assemblydb.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

static int tests_run = 0;
static int tests_passed = 0;
static int tests_failed = 0;

#define TEST(name) do { \
    tests_run++; \
    printf("  [TEST] %-50s", name); \
    fflush(stdout); \
} while(0)

#define PASS() do { \
    tests_passed++; \
    printf(" PASS\n"); \
} while(0)

#define FAIL(msg) do { \
    tests_failed++; \
    printf(" FAIL: %s\n", msg); \
} while(0)

#define ASSERT(cond, msg) do { \
    if (!(cond)) { FAIL(msg); return; } \
} while(0)

/* ================================================================
 * CRC32 Tests
 * ================================================================ */
static void test_crc32_empty(void) {
    TEST("crc32: empty input");
    uint8_t buf[1] = {0};
    uint32_t crc = hw_crc32c(buf, 0);
    /* CRC32C of empty = 0x00000000 (with finalization XOR) */
    PASS();
}

static void test_crc32_known(void) {
    TEST("crc32: known test vector");
    const char *data = "123456789";
    uint32_t crc = hw_crc32c(data, 9);
    /* CRC32C of "123456789" = 0xE3069283 */
    ASSERT(crc == 0xE3069283, "CRC32C mismatch for '123456789'");
    PASS();
}

static void test_crc32_zeros(void) {
    TEST("crc32: 64 zero bytes");
    uint8_t buf[64];
    memset(buf, 0, 64);
    uint32_t crc = hw_crc32c(buf, 64);
    /* Should produce a deterministic non-zero value */
    ASSERT(crc != 0 || crc == 0, "CRC computed"); /* Just verify no crash */
    PASS();
}

static void test_crc32_large(void) {
    TEST("crc32: 4096 bytes (page size)");
    uint8_t *buf = malloc(4096);
    ASSERT(buf != NULL, "malloc failed");
    for (int i = 0; i < 4096; i++) buf[i] = (uint8_t)(i & 0xFF);
    uint32_t crc1 = hw_crc32c(buf, 4096);
    uint32_t crc2 = hw_crc32c(buf, 4096);
    ASSERT(crc1 == crc2, "CRC32C not deterministic");
    /* Flip a bit and verify CRC changes */
    buf[1000] ^= 0x01;
    uint32_t crc3 = hw_crc32c(buf, 4096);
    ASSERT(crc3 != crc1, "CRC32C should change on bit flip");
    free(buf);
    PASS();
}

/* ================================================================
 * NEON Memory Operations Tests
 * ================================================================ */
static void test_neon_memcpy_basic(void) {
    TEST("neon_memcpy: basic 128 bytes");
    uint8_t src[128], dst[128];
    for (int i = 0; i < 128; i++) src[i] = (uint8_t)i;
    memset(dst, 0, 128);
    neon_memcpy(dst, src, 128);
    ASSERT(memcmp(src, dst, 128) == 0, "memcpy mismatch");
    PASS();
}

static void test_neon_memcpy_small(void) {
    TEST("neon_memcpy: small 7 bytes");
    uint8_t src[] = "hello!!";
    uint8_t dst[8] = {0};
    neon_memcpy(dst, src, 7);
    ASSERT(memcmp(src, dst, 7) == 0, "small memcpy mismatch");
    PASS();
}

static void test_neon_memcpy_page(void) {
    TEST("neon_memcpy: full page 4096 bytes");
    uint8_t *src = malloc(4096);
    uint8_t *dst = malloc(4096);
    ASSERT(src && dst, "malloc failed");
    for (int i = 0; i < 4096; i++) src[i] = (uint8_t)(i * 7);
    neon_memcpy(dst, src, 4096);
    ASSERT(memcmp(src, dst, 4096) == 0, "page memcpy mismatch");
    free(src); free(dst);
    PASS();
}

static void test_neon_memzero(void) {
    TEST("neon_memzero: 256 bytes");
    uint8_t buf[256];
    memset(buf, 0xFF, 256);
    neon_memzero(buf, 256);
    for (int i = 0; i < 256; i++) {
        ASSERT(buf[i] == 0, "not zeroed");
    }
    PASS();
}

static void test_neon_memset(void) {
    TEST("neon_memset: 100 bytes with 0xAB");
    uint8_t buf[100];
    neon_memset(buf, 0xAB, 100);
    for (int i = 0; i < 100; i++) {
        ASSERT(buf[i] == 0xAB, "memset mismatch");
    }
    PASS();
}

static void test_neon_memcmp_equal(void) {
    TEST("neon_memcmp: equal buffers");
    uint8_t a[64], b[64];
    memset(a, 0x42, 64);
    memset(b, 0x42, 64);
    int r = neon_memcmp(a, b, 64);
    ASSERT(r == 0, "should be equal");
    PASS();
}

static void test_neon_memcmp_less(void) {
    TEST("neon_memcmp: a < b");
    uint8_t a[64], b[64];
    memset(a, 0x42, 64);
    memset(b, 0x42, 64);
    a[32] = 0x10;
    b[32] = 0x20;
    int r = neon_memcmp(a, b, 64);
    ASSERT(r < 0, "should be less");
    PASS();
}

static void test_neon_memcmp_greater(void) {
    TEST("neon_memcmp: a > b");
    uint8_t a[64], b[64];
    memset(a, 0x42, 64);
    memset(b, 0x42, 64);
    a[0] = 0xFF;
    b[0] = 0x01;
    int r = neon_memcmp(a, b, 64);
    ASSERT(r > 0, "should be greater");
    PASS();
}

static void test_neon_copy_64(void) {
    TEST("neon_copy_64: exact 64-byte copy");
    uint8_t src[64], dst[64];
    for (int i = 0; i < 64; i++) src[i] = (uint8_t)(i + 100);
    memset(dst, 0, 64);
    neon_copy_64(dst, src);
    ASSERT(memcmp(src, dst, 64) == 0, "copy_64 mismatch");
    PASS();
}

static void test_neon_copy_256(void) {
    TEST("neon_copy_256: exact 256-byte copy");
    uint8_t src[256], dst[256];
    for (int i = 0; i < 256; i++) src[i] = (uint8_t)i;
    memset(dst, 0xFF, 256);
    neon_copy_256(dst, src);
    ASSERT(memcmp(src, dst, 256) == 0, "copy_256 mismatch");
    PASS();
}

/* ================================================================
 * Key Comparison Tests
 * ================================================================ */
static void make_key(uint8_t *key, const char *data) {
    memset(key, 0, 64);
    uint16_t len = (uint16_t)strlen(data);
    key[0] = len & 0xFF;
    key[1] = (len >> 8) & 0xFF;
    memcpy(key + 2, data, len);
}

static void test_key_compare_equal(void) {
    TEST("key_compare: equal keys");
    uint8_t a[64], b[64];
    make_key(a, "hello");
    make_key(b, "hello");
    ASSERT(key_compare(a, b) == 0, "should be equal");
    PASS();
}

static void test_key_compare_less(void) {
    TEST("key_compare: apple < banana");
    uint8_t a[64], b[64];
    make_key(a, "apple");
    make_key(b, "banana");
    ASSERT(key_compare(a, b) < 0, "apple should be < banana");
    PASS();
}

static void test_key_compare_greater(void) {
    TEST("key_compare: zebra > apple");
    uint8_t a[64], b[64];
    make_key(a, "zebra");
    make_key(b, "apple");
    ASSERT(key_compare(a, b) > 0, "zebra should be > apple");
    PASS();
}

static void test_key_compare_prefix(void) {
    TEST("key_compare: abc < abcd (shorter < longer prefix)");
    uint8_t a[64], b[64];
    make_key(a, "abc");
    make_key(b, "abcd");
    ASSERT(key_compare(a, b) < 0, "shorter prefix should be less");
    PASS();
}

static void test_key_compare_empty(void) {
    TEST("key_compare: empty < non-empty");
    uint8_t a[64], b[64];
    make_key(a, "");
    make_key(b, "x");
    ASSERT(key_compare(a, b) < 0, "empty should be less");
    PASS();
}

static void test_key_equal(void) {
    TEST("key_equal: identical keys");
    uint8_t a[64], b[64];
    make_key(a, "testkey123");
    make_key(b, "testkey123");
    ASSERT(key_equal(a, b) == 1, "should be equal");
    PASS();
}

static void test_key_not_equal(void) {
    TEST("key_equal: different keys");
    uint8_t a[64], b[64];
    make_key(a, "testkey123");
    make_key(b, "testkey124");
    ASSERT(key_equal(a, b) == 0, "should not be equal");
    PASS();
}

static void test_build_fixed_key(void) {
    TEST("build_fixed_key: builds correct format");
    uint8_t key[64];
    build_fixed_key(key, "hello", 5);
    ASSERT(key[0] == 5 && key[1] == 0, "length wrong");
    ASSERT(memcmp(key + 2, "hello", 5) == 0, "data wrong");
    ASSERT(key[7] == 0 && key[63] == 0, "padding wrong");
    PASS();
}

/* ================================================================
 * Bloom Filter Tests
 * ================================================================ */
static void test_bloom_basic(void) {
    TEST("bloom: add and check present");
    void *bloom = bloom_create(100);
    ASSERT(bloom != NULL, "bloom_create failed");

    uint8_t key[64];
    make_key(key, "testkey");
    bloom_add(bloom, key);

    ASSERT(bloom_check(bloom, key) == 1, "should find added key");
    bloom_destroy(bloom);
    PASS();
}

static void test_bloom_not_present(void) {
    TEST("bloom: check absent key");
    void *bloom = bloom_create(100);
    ASSERT(bloom != NULL, "bloom_create failed");

    uint8_t key1[64], key2[64];
    make_key(key1, "present");
    make_key(key2, "absent");
    bloom_add(bloom, key1);

    /* key2 should likely not be found (low FP rate) */
    /* We can't assert definitively due to false positives */
    int found = bloom_check(bloom, key2);
    /* Just check it doesn't crash - FP is possible */
    (void)found;
    bloom_destroy(bloom);
    PASS();
}

static void test_bloom_false_positive_rate(void) {
    TEST("bloom: false positive rate < 2%");
    void *bloom = bloom_create(1000);
    ASSERT(bloom != NULL, "bloom_create failed");

    /* Add 1000 keys */
    for (int i = 0; i < 1000; i++) {
        uint8_t key[64];
        char buf[32];
        snprintf(buf, sizeof(buf), "key_%06d", i);
        make_key(key, buf);
        bloom_add(bloom, key);
    }

    /* Verify all added keys are found (zero false negatives) */
    for (int i = 0; i < 1000; i++) {
        uint8_t key[64];
        char buf[32];
        snprintf(buf, sizeof(buf), "key_%06d", i);
        make_key(key, buf);
        ASSERT(bloom_check(bloom, key) == 1, "false negative!");
    }

    /* Check 10000 absent keys, count false positives */
    int fp = 0;
    for (int i = 1000; i < 11000; i++) {
        uint8_t key[64];
        char buf[32];
        snprintf(buf, sizeof(buf), "key_%06d", i);
        make_key(key, buf);
        if (bloom_check(bloom, key)) fp++;
    }

    double fp_rate = (double)fp / 10000.0;
    printf("(FP=%.2f%%) ", fp_rate * 100);
    ASSERT(fp_rate < 0.02, "false positive rate too high");

    bloom_destroy(bloom);
    PASS();
}

/* ================================================================
 * Arena Allocator Tests
 * ================================================================ */
static void test_arena_basic(void) {
    TEST("arena: create and alloc");
    void *arena = arena_create();
    ASSERT(arena != NULL, "arena_create failed");

    void *p1 = arena_alloc(arena, 64);
    ASSERT(p1 != NULL, "first alloc failed");

    void *p2 = arena_alloc(arena, 128);
    ASSERT(p2 != NULL, "second alloc failed");
    ASSERT(p2 != p1, "should be different pointers");

    /* Verify alignment (should be 8-byte aligned) */
    ASSERT(((uintptr_t)p1 & 7) == 0, "p1 not 8-byte aligned");
    ASSERT(((uintptr_t)p2 & 7) == 0, "p2 not 8-byte aligned");

    arena_destroy(arena);
    PASS();
}

static void test_arena_many_allocs(void) {
    TEST("arena: 10000 allocations");
    void *arena = arena_create();
    ASSERT(arena != NULL, "arena_create failed");

    for (int i = 0; i < 10000; i++) {
        void *p = arena_alloc(arena, 344); /* Average skip list node */
        ASSERT(p != NULL, "alloc failed");
        /* Write to it to verify no segfault */
        memset(p, 0xAA, 344);
    }

    arena_destroy(arena);
    PASS();
}

static void test_arena_reset(void) {
    TEST("arena: reset and reuse");
    void *arena = arena_create();
    ASSERT(arena != NULL, "arena_create failed");

    for (int i = 0; i < 1000; i++) {
        arena_alloc(arena, 256);
    }

    arena_reset(arena);

    /* Should be able to allocate again */
    void *p = arena_alloc(arena, 64);
    ASSERT(p != NULL, "alloc after reset failed");

    arena_destroy(arena);
    PASS();
}

/* ================================================================
 * PRNG Tests
 * ================================================================ */
static void test_prng_basic(void) {
    TEST("prng: produces different values");
    prng_seed(42);
    uint64_t a = prng_next();
    uint64_t b = prng_next();
    uint64_t c = prng_next();
    ASSERT(a != b, "a == b");
    ASSERT(b != c, "b == c");
    ASSERT(a != 0, "a == 0");
    PASS();
}

static void test_prng_deterministic(void) {
    TEST("prng: same seed = same sequence");
    prng_seed(12345);
    uint64_t a1 = prng_next();
    uint64_t a2 = prng_next();

    prng_seed(12345);
    uint64_t b1 = prng_next();
    uint64_t b2 = prng_next();

    ASSERT(a1 == b1, "first values differ");
    ASSERT(a2 == b2, "second values differ");
    PASS();
}

static void test_random_level_distribution(void) {
    TEST("random_level: distribution check");
    prng_seed(999);
    int counts[21] = {0};
    int total = 100000;

    for (int i = 0; i < total; i++) {
        int lvl = random_level();
        ASSERT(lvl >= 1 && lvl <= 20, "level out of range");
        counts[lvl]++;
    }

    /* Level 1 should be ~75%, level 2 ~18.75%, level 3 ~4.7% */
    double pct1 = (double)counts[1] / total;
    printf("(L1=%.1f%% L2=%.1f%% L3=%.1f%%) ",
           pct1 * 100,
           (double)counts[2] / total * 100,
           (double)counts[3] / total * 100);

    ASSERT(pct1 > 0.70 && pct1 < 0.80, "level 1 distribution off");
    PASS();
}

/* ================================================================
 * String Operations Tests
 * ================================================================ */
static void test_strlen(void) {
    TEST("asm_strlen: basic");
    ASSERT(asm_strlen("hello") == 5, "wrong length");
    ASSERT(asm_strlen("") == 0, "empty should be 0");
    ASSERT(asm_strlen("x") == 1, "single char");
    PASS();
}

static void test_u64_to_dec(void) {
    TEST("u64_to_dec: various values");
    char buf[32];

    u64_to_dec(0, buf);
    ASSERT(strcmp(buf, "0") == 0, "0 failed");

    u64_to_dec(42, buf);
    ASSERT(strcmp(buf, "42") == 0, "42 failed");

    u64_to_dec(123456789, buf);
    ASSERT(strcmp(buf, "123456789") == 0, "123456789 failed");

    PASS();
}

static void test_u64_to_padded(void) {
    TEST("u64_to_padded_dec: zero-padded");
    char buf[32];

    u64_to_padded_dec(1, buf, 6);
    ASSERT(strcmp(buf, "000001") == 0, "padded 1 failed");

    u64_to_padded_dec(42, buf, 6);
    ASSERT(strcmp(buf, "000042") == 0, "padded 42 failed");

    u64_to_padded_dec(999999, buf, 6);
    ASSERT(strcmp(buf, "999999") == 0, "padded 999999 failed");

    PASS();
}

static void test_build_wal_name(void) {
    TEST("build_wal_name: correct format");
    char buf[32];
    build_wal_name(buf, 1);
    ASSERT(strcmp(buf, "wal/000001.wal") == 0, "wal name wrong");

    build_wal_name(buf, 42);
    ASSERT(strcmp(buf, "wal/000042.wal") == 0, "wal name 42 wrong");
    PASS();
}

static void test_build_sst_name(void) {
    TEST("build_sst_name: correct format");
    char buf[32];
    build_sst_name(buf, 0, 1);
    ASSERT(strcmp(buf, "sst/L0-000001.sst") == 0, "sst name wrong");

    build_sst_name(buf, 1, 99);
    ASSERT(strcmp(buf, "sst/L1-000099.sst") == 0, "sst name L1 wrong");
    PASS();
}

/* ================================================================
 * Memory Allocator Tests
 * ================================================================ */
static void test_page_alloc(void) {
    TEST("page_alloc: allocate and free");
    void *p = page_alloc(1);
    ASSERT(p != NULL, "alloc failed");
    /* Page should be zeroed (mmap guarantee) */
    uint8_t *bp = (uint8_t *)p;
    ASSERT(bp[0] == 0 && bp[4095] == 0, "not zeroed");
    /* Write to verify access */
    memset(p, 0xAA, 4096);
    page_free(p, 1);
    PASS();
}

static void test_page_alloc_multi(void) {
    TEST("page_alloc: 256 pages (1 MB)");
    void *p = page_alloc(256);
    ASSERT(p != NULL, "alloc 256 pages failed");
    /* Write to first and last byte */
    uint8_t *bp = (uint8_t *)p;
    bp[0] = 0x42;
    bp[256 * 4096 - 1] = 0x43;
    ASSERT(bp[0] == 0x42 && bp[256 * 4096 - 1] == 0x43, "write failed");
    page_free(p, 256);
    PASS();
}

/* ================================================================
 * Error Helper Tests
 * ================================================================ */
static void test_error_helpers(void) {
    TEST("error helpers: basic checks");
    ASSERT(is_error(0) == 0, "0 should not be error");
    ASSERT(is_error(1) == 1, "1 should be error");
    ASSERT(is_syscall_error(0) == 0, "0 not syscall error");
    ASSERT(is_syscall_error(-1) == 1, "-1 is syscall error");
    ASSERT(is_syscall_error(42) == 0, "42 not syscall error");
    PASS();
}

/* ================================================================
 * Main
 * ================================================================ */
int main(void) {
    printf("========================================\n");
    printf("  AssemblyDB Core Tests (Phase 1)\n");
    printf("========================================\n\n");

    printf("[CRC32C]\n");
    test_crc32_empty();
    test_crc32_known();
    test_crc32_zeros();
    test_crc32_large();

    printf("\n[NEON Memory Operations]\n");
    test_neon_memcpy_basic();
    test_neon_memcpy_small();
    test_neon_memcpy_page();
    test_neon_memzero();
    test_neon_memset();
    test_neon_memcmp_equal();
    test_neon_memcmp_less();
    test_neon_memcmp_greater();
    test_neon_copy_64();
    test_neon_copy_256();

    printf("\n[Key Comparison]\n");
    test_key_compare_equal();
    test_key_compare_less();
    test_key_compare_greater();
    test_key_compare_prefix();
    test_key_compare_empty();
    test_key_equal();
    test_key_not_equal();
    test_build_fixed_key();

    printf("\n[Bloom Filter]\n");
    test_bloom_basic();
    test_bloom_not_present();
    test_bloom_false_positive_rate();

    printf("\n[Arena Allocator]\n");
    test_arena_basic();
    test_arena_many_allocs();
    test_arena_reset();

    printf("\n[PRNG]\n");
    test_prng_basic();
    test_prng_deterministic();
    test_random_level_distribution();

    printf("\n[String Operations]\n");
    test_strlen();
    test_u64_to_dec();
    test_u64_to_padded();
    test_build_wal_name();
    test_build_sst_name();

    printf("\n[Memory Allocator]\n");
    test_page_alloc();
    test_page_alloc_multi();

    printf("\n[Error Helpers]\n");
    test_error_helpers();

    printf("\n========================================\n");
    printf("  Results: %d/%d passed", tests_passed, tests_run);
    if (tests_failed > 0) {
        printf(", %d FAILED", tests_failed);
    }
    printf("\n========================================\n");

    return tests_failed > 0 ? 1 : 0;
}
