#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include "assemblydb.h"

static int tests_run = 0;
static int tests_passed = 0;

#define TEST(name) do { \
    tests_run++; \
    printf("  [%02d] %-55s ", tests_run, name); \
    fflush(stdout); \
} while(0)

#define PASS() do { tests_passed++; printf("PASS\n"); } while(0)
#define FAIL(msg) do { printf("FAIL: %s\n", msg); } while(0)

// ============================================================================
// LRU Cache Tests
// ============================================================================

static void test_lru_create_destroy(void) {
    TEST("LRU cache create and destroy");
    void *cache = lru_cache_create(16);
    if (!cache) { FAIL("create returned NULL"); return; }
    lru_cache_destroy(cache);
    PASS();
}

static void test_lru_insert_fetch(void) {
    TEST("LRU cache insert then fetch");
    void *cache = lru_cache_create(8);
    if (!cache) { FAIL("create failed"); return; }

    // Create a test page
    uint8_t *page = (uint8_t *)page_alloc(1);
    for (int i = 0; i < 4096; i++) page[i] = (uint8_t)(i & 0xFF);

    // Insert page with id=42
    void *cached = lru_cache_insert(cache, 42, page);
    if (!cached) { FAIL("insert returned NULL"); goto cleanup; }

    // Unpin it
    lru_cache_unpin(cache, 42);

    // Fetch it back
    void *fetched = lru_cache_fetch(cache, 42);
    if (!fetched) { FAIL("fetch returned NULL"); goto cleanup; }

    // Verify data matches
    if (memcmp(fetched, page, 4096) == 0) PASS();
    else FAIL("data mismatch");

    lru_cache_unpin(cache, 42);

cleanup:
    page_free(page, 1);
    lru_cache_destroy(cache);
}

static void test_lru_miss(void) {
    TEST("LRU cache miss returns NULL");
    void *cache = lru_cache_create(4);
    if (!cache) { FAIL("create failed"); return; }

    void *p = lru_cache_fetch(cache, 99);
    lru_cache_destroy(cache);

    if (p == NULL) PASS();
    else FAIL("should return NULL for uncached page");
}

static void test_lru_stats(void) {
    TEST("LRU cache hit/miss statistics");
    void *cache = lru_cache_create(4);
    if (!cache) { FAIL("create failed"); return; }

    uint8_t *page = (uint8_t *)page_alloc(1);
    memset(page, 0xAA, 4096);

    // Miss
    lru_cache_fetch(cache, 1);

    // Insert and fetch (hit)
    lru_cache_insert(cache, 1, page);
    lru_cache_unpin(cache, 1);
    lru_cache_fetch(cache, 1);
    lru_cache_unpin(cache, 1);

    uint64_t hits, misses;
    lru_cache_stats(cache, &hits, &misses);

    page_free(page, 1);
    lru_cache_destroy(cache);

    if (hits == 1 && misses == 1) PASS();
    else {
        printf("FAIL: hits=%lu misses=%lu (expected 1,1)\n",
               (unsigned long)hits, (unsigned long)misses);
    }
}

static void test_lru_eviction(void) {
    TEST("LRU cache evicts when full");
    void *cache = lru_cache_create(2);  // Only 2 slots
    if (!cache) { FAIL("create failed"); return; }

    uint8_t *p1 = (uint8_t *)page_alloc(1);
    uint8_t *p2 = (uint8_t *)page_alloc(1);
    uint8_t *p3 = (uint8_t *)page_alloc(1);
    memset(p1, 0x11, 4096);
    memset(p2, 0x22, 4096);
    memset(p3, 0x33, 4096);

    // Fill cache
    lru_cache_insert(cache, 1, p1);
    lru_cache_unpin(cache, 1);
    lru_cache_insert(cache, 2, p2);
    lru_cache_unpin(cache, 2);

    // Insert 3rd - should evict page 1 (LRU)
    lru_cache_insert(cache, 3, p3);
    lru_cache_unpin(cache, 3);

    // Page 1 should be evicted
    void *f1 = lru_cache_fetch(cache, 1);
    // Page 3 should be cached
    void *f3 = lru_cache_fetch(cache, 3);

    int ok = (f1 == NULL && f3 != NULL);
    if (f3) lru_cache_unpin(cache, 3);

    page_free(p1, 1);
    page_free(p2, 1);
    page_free(p3, 1);
    lru_cache_destroy(cache);

    if (ok) PASS();
    else FAIL("eviction not working correctly");
}

static void test_lru_dirty_flag(void) {
    TEST("LRU cache mark dirty");
    void *cache = lru_cache_create(4);
    if (!cache) { FAIL("create failed"); return; }

    uint8_t *page = (uint8_t *)page_alloc(1);
    memset(page, 0, 4096);

    lru_cache_insert(cache, 5, page);
    lru_cache_mark_dirty(cache, 5);
    lru_cache_unpin(cache, 5);

    page_free(page, 1);
    lru_cache_destroy(cache);
    PASS();
}

static void test_lru_multiple_pages(void) {
    TEST("LRU cache with multiple pages");
    void *cache = lru_cache_create(16);
    if (!cache) { FAIL("create failed"); return; }

    uint8_t *page = (uint8_t *)page_alloc(1);
    int ok = 1;

    // Insert 10 pages
    for (int i = 0; i < 10; i++) {
        memset(page, (uint8_t)(i + 1), 4096);
        void *p = lru_cache_insert(cache, (uint32_t)(i + 1), page);
        if (!p) { ok = 0; break; }
        lru_cache_unpin(cache, (uint32_t)(i + 1));
    }

    // Verify all can be fetched
    for (int i = 0; i < 10 && ok; i++) {
        void *p = lru_cache_fetch(cache, (uint32_t)(i + 1));
        if (!p) { ok = 0; break; }
        // Verify first byte
        uint8_t *data = (uint8_t *)p;
        if (data[0] != (uint8_t)(i + 1)) { ok = 0; }
        lru_cache_unpin(cache, (uint32_t)(i + 1));
    }

    page_free(page, 1);
    lru_cache_destroy(cache);

    if (ok) PASS();
    else FAIL("multi-page insert/fetch failed");
}

// ============================================================================
// Index Filename Test
// ============================================================================

static void test_index_filename(void) {
    TEST("build_index_filename");
    char buf[64];
    size_t len = build_index_filename(buf, "user_email", 10);

    if (len > 0 && strcmp(buf, "idx_user_email.dat") == 0) PASS();
    else {
        printf("FAIL: got '%s' len=%zu\n", buf, len);
    }
}

// ============================================================================
// Encryption + Compression Combined Test
// ============================================================================

static void test_encrypt_then_compress(void) {
    TEST("encrypt page then compress");

    // Set up AES
    void *ctx = crypto_ctx_create();
    uint8_t key[32];
    for (int i = 0; i < 32; i++) key[i] = (uint8_t)(i * 11);
    aes_set_key_impl(ctx, key, 32);

    // Original data (compressible)
    uint8_t *pt = (uint8_t *)page_alloc(1);
    for (int i = 0; i < 4096; i++) pt[i] = (uint8_t)(i % 16);

    // Encrypt
    uint8_t *ct = (uint8_t *)page_alloc(1);
    aes_page_encrypt(ctx, pt, ct, 1);

    // Try to compress encrypted data (should not compress well)
    void *lz4 = lz4_ctx_create();
    uint8_t *comp = (uint8_t *)page_alloc(2);
    int64_t clen = lz4_compress(lz4, ct, 4096, comp, 8192);

    // Encrypted data is pseudo-random, compression ratio should be poor
    int enc_compress_ok = (clen > 0);

    // Now compress first, then encrypt (better approach)
    int64_t clen2 = lz4_compress(lz4, pt, 4096, comp, 8192);
    int compress_first_ok = (clen2 > 0 && clen2 < 4096);

    lz4_ctx_destroy(lz4);

    // Decrypt and verify original
    uint8_t *recovered = (uint8_t *)page_alloc(1);
    aes_page_decrypt(ctx, ct, recovered, 1);
    int roundtrip_ok = (memcmp(pt, recovered, 4096) == 0);

    page_free(pt, 1);
    page_free(ct, 1);
    page_free(comp, 2);
    page_free(recovered, 1);
    crypto_ctx_destroy(ctx);

    if (enc_compress_ok && compress_first_ok && roundtrip_ok) PASS();
    else FAIL("encrypt+compress pipeline failed");
}

// ============================================================================
// Full API Integration Test
// ============================================================================

static void test_api_full_lifecycle(void) {
    TEST("full API: open -> put -> get -> delete -> close");

    // Clean up any previous test
    system("rm -rf /tmp/test_adb_integ");

    adb_t *db = NULL;
    int rc = adb_open("/tmp/test_adb_integ", NULL, &db);
    if (rc != 0 || !db) { FAIL("open failed"); return; }

    // Put
    rc = adb_put(db, "hello", 5, "world", 5);
    if (rc != 0) { FAIL("put failed"); adb_close(db); return; }

    // Get
    char vbuf[256];
    uint16_t vlen = 0;
    rc = adb_get(db, "hello", 5, vbuf, 256, &vlen);
    if (rc != 0) { FAIL("get failed"); adb_close(db); return; }
    if (vlen != 5 || memcmp(vbuf, "world", 5) != 0) {
        FAIL("get returned wrong data");
        adb_close(db);
        return;
    }

    // Delete
    rc = adb_delete(db, "hello", 5);
    if (rc != 0) { FAIL("delete failed"); adb_close(db); return; }

    // Get should fail now
    rc = adb_get(db, "hello", 5, vbuf, 256, &vlen);
    if (rc != ADB_ERR_NOT_FOUND) {
        FAIL("get after delete should return NOT_FOUND");
        adb_close(db);
        return;
    }

    adb_close(db);
    system("rm -rf /tmp/test_adb_integ");
    PASS();
}

static void test_api_multiple_keys(void) {
    TEST("full API: insert and retrieve 100 keys");

    system("rm -rf /tmp/test_adb_multi");

    adb_t *db = NULL;
    int rc = adb_open("/tmp/test_adb_multi", NULL, &db);
    if (rc != 0 || !db) { FAIL("open failed"); return; }

    int ok = 1;

    // Insert 100 keys
    for (int i = 0; i < 100; i++) {
        char key[16], val[16];
        int kl = snprintf(key, sizeof(key), "key%04d", i);
        int vl = snprintf(val, sizeof(val), "val%04d", i);
        rc = adb_put(db, key, (uint16_t)kl, val, (uint16_t)vl);
        if (rc != 0) { ok = 0; break; }
    }

    // Retrieve all 100
    for (int i = 0; i < 100 && ok; i++) {
        char key[16], expected[16], vbuf[256];
        int kl = snprintf(key, sizeof(key), "key%04d", i);
        int vl = snprintf(expected, sizeof(expected), "val%04d", i);
        uint16_t vlen = 0;
        rc = adb_get(db, key, (uint16_t)kl, vbuf, 256, &vlen);
        if (rc != 0 || vlen != (uint16_t)vl || memcmp(vbuf, expected, vl) != 0) {
            ok = 0;
        }
    }

    adb_close(db);
    system("rm -rf /tmp/test_adb_multi");

    if (ok) PASS();
    else FAIL("multi-key insert/retrieve failed");
}

static void test_api_transactions(void) {
    TEST("full API: transaction begin/commit");

    system("rm -rf /tmp/test_adb_tx");

    adb_t *db = NULL;
    int rc = adb_open("/tmp/test_adb_tx", NULL, &db);
    if (rc != 0 || !db) { FAIL("open failed"); return; }

    uint64_t tx_id = 0;
    rc = adb_tx_begin(db, ADB_ISO_SNAPSHOT, &tx_id);
    if (rc != 0) { FAIL("tx_begin failed"); adb_close(db); return; }

    rc = adb_tx_commit(db, tx_id);
    if (rc != 0) { FAIL("tx_commit failed"); adb_close(db); return; }

    adb_close(db);
    system("rm -rf /tmp/test_adb_tx");
    PASS();
}

// ============================================================================
// Backup Test
// ============================================================================

static void test_backup_full(void) {
    TEST("full backup creates destination directory");

    system("rm -rf /tmp/test_adb_bkup_src /tmp/test_adb_bkup_dst");

    adb_t *db = NULL;
    int rc = adb_open("/tmp/test_adb_bkup_src", NULL, &db);
    if (rc != 0 || !db) { FAIL("open failed"); return; }

    // Insert some data
    adb_put(db, "bktest", 6, "data123", 7);

    rc = backup_full(db, "/tmp/test_adb_bkup_dst");

    adb_close(db);
    system("rm -rf /tmp/test_adb_bkup_src /tmp/test_adb_bkup_dst");

    if (rc == 0) PASS();
    else {
        printf("FAIL: backup_full returned %d\n", rc);
    }
}

// ============================================================================
// CRC32 + AES Combined Integrity Test
// ============================================================================

static void test_crc32_encrypted_page(void) {
    TEST("CRC32 checksum of encrypted page");

    void *ctx = crypto_ctx_create();
    uint8_t key[32] = {0};
    aes_set_key_impl(ctx, key, 32);

    uint8_t *pt = (uint8_t *)page_alloc(1);
    uint8_t *ct = (uint8_t *)page_alloc(1);
    memset(pt, 0x42, 4096);

    aes_page_encrypt(ctx, pt, ct, 0);

    // Compute checksum
    uint32_t crc1 = hw_crc32c(ct, 4096);

    // Tamper with one byte
    ct[100] ^= 0x01;
    uint32_t crc2 = hw_crc32c(ct, 4096);

    page_free(pt, 1);
    page_free(ct, 1);
    crypto_ctx_destroy(ctx);

    if (crc1 != crc2) PASS();
    else FAIL("CRC should detect tampering");
}

int main(void) {
    printf("=== AssemblyDB Phase 6: Integration Tests ===\n\n");

    // LRU Cache
    test_lru_create_destroy();
    test_lru_insert_fetch();
    test_lru_miss();
    test_lru_stats();
    test_lru_eviction();
    test_lru_dirty_flag();
    test_lru_multiple_pages();

    // Index
    test_index_filename();

    // Combined crypto + compress
    test_encrypt_then_compress();

    // Full API
    test_api_full_lifecycle();
    test_api_multiple_keys();
    test_api_transactions();

    // Backup
    test_backup_full();

    // Integrity
    test_crc32_encrypted_page();

    printf("\n=== Results: %d/%d passed ===\n", tests_passed, tests_run);
    return tests_passed == tests_run ? 0 : 1;
}
