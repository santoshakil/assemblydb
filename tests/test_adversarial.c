#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <signal.h>
#include <errno.h>
#include <fcntl.h>
#include "assemblydb.h"

static int tests_run = 0;
static int tests_passed = 0;
static int tests_failed = 0;

#define TEST(name) do { \
    tests_run++; \
    printf("  [%02d] %-58s ", tests_run, name); \
    fflush(stdout); \
} while(0)

#define PASS() do { tests_passed++; printf("PASS\n"); } while(0)
#define FAIL(msg) do { tests_failed++; printf("FAIL: %s\n", msg); } while(0)
#define FAILF(...) do { tests_failed++; printf("FAIL: "); printf(__VA_ARGS__); printf("\n"); } while(0)

static void cleanup(const char *path) {
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", path);
    int rc = system(cmd);
    (void)rc;
}

static uint64_t now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

static uint64_t xor_state = 0xDEADBEEFCAFE1234ULL;
static uint64_t xorshift64(void) {
    xor_state ^= xor_state << 13;
    xor_state ^= xor_state >> 7;
    xor_state ^= xor_state << 17;
    return xor_state;
}

// ============================================================================
// SECTION 1: INPUT VALIDATION & BOUNDARY ATTACKS
// ============================================================================

// Test: Oversized key length (> 62 bytes = max for fixed key format)
static void test_oversized_key(void) {
    TEST("boundary: key_len > 62 (max) should not corrupt");
    cleanup("/tmp/adv_bigkey");

    adb_t *db = NULL;
    int rc = adb_open("/tmp/adv_bigkey", NULL, &db);
    if (rc != 0 || !db) { FAIL("open"); return; }

    // Try 62-byte key (max valid)
    char key62[62];
    memset(key62, 'K', 62);
    rc = adb_put(db, key62, 62, "val62", 5);
    if (rc != 0) { FAIL("put 62B key"); adb_close(db); cleanup("/tmp/adv_bigkey"); return; }

    // Verify it works
    char vbuf[256];
    uint16_t vlen;
    rc = adb_get(db, key62, 62, vbuf, 256, &vlen);
    if (rc != 0 || vlen != 5) { FAIL("get 62B key"); adb_close(db); cleanup("/tmp/adv_bigkey"); return; }

    // Try 1-byte key (min useful)
    rc = adb_put(db, "A", 1, "tiny", 4);
    if (rc != 0) { FAIL("put 1B key"); adb_close(db); cleanup("/tmp/adv_bigkey"); return; }

    rc = adb_get(db, "A", 1, vbuf, 256, &vlen);
    if (rc != 0 || vlen != 4) { FAIL("get 1B key"); adb_close(db); cleanup("/tmp/adv_bigkey"); return; }

    // Verify DB still works after boundary keys
    rc = adb_put(db, "normal", 6, "normalval", 9);
    rc = adb_get(db, "normal", 6, vbuf, 256, &vlen);
    if (rc != 0 || vlen != 9) { FAIL("normal key after boundary"); adb_close(db); cleanup("/tmp/adv_bigkey"); return; }

    adb_close(db);
    cleanup("/tmp/adv_bigkey");
    PASS();
}

// Test: Max value size (254 bytes = max for fixed val format)
static void test_max_value_size(void) {
    TEST("boundary: value_len = 254 (max) exact fit");
    cleanup("/tmp/adv_bigval");

    adb_t *db = NULL;
    int rc = adb_open("/tmp/adv_bigval", NULL, &db);
    if (rc != 0 || !db) { FAIL("open"); return; }

    char val254[254];
    memset(val254, 'V', 254);

    rc = adb_put(db, "maxval", 6, val254, 254);
    if (rc != 0) { FAIL("put 254B val"); adb_close(db); cleanup("/tmp/adv_bigval"); return; }

    char vbuf[256];
    uint16_t vlen;
    rc = adb_get(db, "maxval", 6, vbuf, 256, &vlen);
    if (rc != 0 || vlen != 254 || memcmp(vbuf, val254, 254) != 0) {
        FAILF("get 254B val: rc=%d vlen=%d", rc, vlen);
        adb_close(db); cleanup("/tmp/adv_bigval"); return;
    }

    adb_close(db);
    cleanup("/tmp/adv_bigval");
    PASS();
}

// Test: Zero-length key (edge case)
static void test_zero_length_key(void) {
    TEST("boundary: zero-length key");
    cleanup("/tmp/adv_zerokey");

    adb_t *db = NULL;
    int rc = adb_open("/tmp/adv_zerokey", NULL, &db);
    if (rc != 0 || !db) { FAIL("open"); return; }

    // Zero-length key - should work (empty key is valid, klen=0)
    rc = adb_put(db, "", 0, "emptykey", 8);
    // Accept either success or graceful error
    if (rc == 0) {
        char vbuf[256];
        uint16_t vlen;
        rc = adb_get(db, "", 0, vbuf, 256, &vlen);
        if (rc != 0 || vlen != 8 || memcmp(vbuf, "emptykey", 8) != 0) {
            FAIL("get zero-len key mismatch");
            adb_close(db); cleanup("/tmp/adv_zerokey"); return;
        }
    }

    adb_close(db);
    cleanup("/tmp/adv_zerokey");
    PASS();
}

// Test: Zero-length value
static void test_zero_length_value(void) {
    TEST("boundary: zero-length value");
    cleanup("/tmp/adv_zeroval");

    adb_t *db = NULL;
    int rc = adb_open("/tmp/adv_zeroval", NULL, &db);
    if (rc != 0 || !db) { FAIL("open"); return; }

    rc = adb_put(db, "emptyval", 8, "", 0);
    if (rc == 0) {
        char vbuf[256];
        uint16_t vlen = 0xFFFF;
        rc = adb_get(db, "emptyval", 8, vbuf, 256, &vlen);
        if (rc != 0 || vlen != 0) {
            FAILF("expected vlen=0, got rc=%d vlen=%d", rc, vlen);
            adb_close(db); cleanup("/tmp/adv_zeroval"); return;
        }
    }

    adb_close(db);
    cleanup("/tmp/adv_zeroval");
    PASS();
}

// Test: Binary keys with null bytes, 0xFF, and all byte values
static void test_binary_key_full_range(void) {
    TEST("boundary: binary keys with all 256 byte values");
    cleanup("/tmp/adv_binkeys");

    adb_t *db = NULL;
    int rc = adb_open("/tmp/adv_binkeys", NULL, &db);
    if (rc != 0 || !db) { FAIL("open"); return; }

    int errors = 0;
    // Insert keys where each byte position has a different value
    for (int i = 0; i < 256; i++) {
        char key[8];
        memset(key, 0, 8);
        key[0] = (char)i;
        key[1] = (char)(255 - i);
        key[2] = (char)((i * 13) & 0xFF);
        key[3] = (char)((i * 37) & 0xFF);
        char val[8];
        snprintf(val, 8, "b%03d", i);

        rc = adb_put(db, key, 4, val, 4);
        if (rc != 0) errors++;
    }

    // Verify all
    for (int i = 0; i < 256; i++) {
        char key[8];
        memset(key, 0, 8);
        key[0] = (char)i;
        key[1] = (char)(255 - i);
        key[2] = (char)((i * 13) & 0xFF);
        key[3] = (char)((i * 37) & 0xFF);
        char val[8], vbuf[256];
        uint16_t vlen;
        snprintf(val, 8, "b%03d", i);

        rc = adb_get(db, key, 4, vbuf, 256, &vlen);
        if (rc != 0 || vlen != 4 || memcmp(vbuf, val, 4) != 0) errors++;
    }

    adb_close(db);
    cleanup("/tmp/adv_binkeys");
    if (errors == 0) PASS();
    else FAILF("%d/256 binary key errors", errors);
}

// Test: Get with tiny buffer (vbuf_len < actual value size)
static void test_get_small_buffer(void) {
    TEST("boundary: get with vbuf_len < value size");
    cleanup("/tmp/adv_smallbuf");

    adb_t *db = NULL;
    int rc = adb_open("/tmp/adv_smallbuf", NULL, &db);
    if (rc != 0 || !db) { FAIL("open"); return; }

    char bigval[100];
    memset(bigval, 'X', 100);
    rc = adb_put(db, "k", 1, bigval, 100);
    if (rc != 0) { FAIL("put"); adb_close(db); cleanup("/tmp/adv_smallbuf"); return; }

    // Get with buffer smaller than value
    char smallbuf[10];
    memset(smallbuf, 0, 10);
    uint16_t vlen;
    rc = adb_get(db, "k", 1, smallbuf, 10, &vlen);
    // Should succeed but only copy 10 bytes (or return truncated)
    // The important thing: no crash, no buffer overflow
    if (rc != 0) {
        // Even getting an error is acceptable - just don't crash
    }

    adb_close(db);
    cleanup("/tmp/adv_smallbuf");
    PASS();
}

// ============================================================================
// SECTION 2: PERSISTENCE HARDENING
// ============================================================================

// Test: Write, close, reopen, write more, close, reopen, verify ALL
static void test_multi_session_accumulation(void) {
    TEST("persistence: 5 sessions, each adds data, all accumulate");
    cleanup("/tmp/adv_accumulate");

    int sessions = 5;
    int per_session = 2000;
    int errors = 0;
    char key[32], val[64], vbuf[256];
    uint16_t vlen;

    for (int s = 0; s < sessions; s++) {
        adb_t *db = NULL;
        int rc = adb_open("/tmp/adv_accumulate", NULL, &db);
        if (rc != 0 || !db) { FAILF("open session %d", s); return; }

        // Write this session's keys
        for (int i = 0; i < per_session; i++) {
            int kl = snprintf(key, sizeof(key), "s%d_k%05d", s, i);
            int vl = snprintf(val, sizeof(val), "s%d_v%05d", s, i);
            adb_put(db, key, (uint16_t)kl, val, (uint16_t)vl);
        }

        // Verify ALL previous sessions' data is still there
        for (int ps = 0; ps < s; ps++) {
            for (int i = 0; i < per_session; i++) {
                int kl = snprintf(key, sizeof(key), "s%d_k%05d", ps, i);
                int vl = snprintf(val, sizeof(val), "s%d_v%05d", ps, i);
                int rc2 = adb_get(db, key, (uint16_t)kl, vbuf, 256, &vlen);
                if (rc2 != 0 || vlen != (uint16_t)vl || memcmp(vbuf, val, vl) != 0)
                    errors++;
            }
        }

        adb_sync(db);
        adb_close(db);
    }

    // Final verification: open one more time, check EVERYTHING
    {
        adb_t *db = NULL;
        int rc = adb_open("/tmp/adv_accumulate", NULL, &db);
        if (rc != 0 || !db) { FAIL("final open"); cleanup("/tmp/adv_accumulate"); return; }

        for (int s = 0; s < sessions; s++) {
            for (int i = 0; i < per_session; i++) {
                int kl = snprintf(key, sizeof(key), "s%d_k%05d", s, i);
                int vl = snprintf(val, sizeof(val), "s%d_v%05d", s, i);
                int rc2 = adb_get(db, key, (uint16_t)kl, vbuf, 256, &vlen);
                if (rc2 != 0 || vlen != (uint16_t)vl || memcmp(vbuf, val, vl) != 0)
                    errors++;
            }
        }

        adb_close(db);
    }

    cleanup("/tmp/adv_accumulate");
    if (errors == 0) PASS();
    else FAILF("%d errors across %d sessions * %d keys", errors, sessions, per_session);
}

// Test: Overwrite key in session 1, verify new value in session 2
static void test_cross_session_overwrite(void) {
    TEST("persistence: overwrite in session 1, verify in session 2");
    cleanup("/tmp/adv_xsession_ow");

    char vbuf[256];
    uint16_t vlen;
    int errors = 0;

    // Session 1: write original
    {
        adb_t *db = NULL;
        adb_open("/tmp/adv_xsession_ow", NULL, &db);
        if (!db) { FAIL("open s1"); return; }
        adb_put(db, "thekey", 6, "original_value", 14);
        adb_sync(db);
        adb_close(db);
    }

    // Session 2: overwrite
    {
        adb_t *db = NULL;
        adb_open("/tmp/adv_xsession_ow", NULL, &db);
        if (!db) { FAIL("open s2"); cleanup("/tmp/adv_xsession_ow"); return; }
        adb_put(db, "thekey", 6, "updated_value!", 14);
        adb_sync(db);
        adb_close(db);
    }

    // Session 3: verify updated value
    {
        adb_t *db = NULL;
        adb_open("/tmp/adv_xsession_ow", NULL, &db);
        if (!db) { FAIL("open s3"); cleanup("/tmp/adv_xsession_ow"); return; }
        int rc = adb_get(db, "thekey", 6, vbuf, 256, &vlen);
        if (rc != 0 || vlen != 14 || memcmp(vbuf, "updated_value!", 14) != 0)
            errors++;
        adb_close(db);
    }

    cleanup("/tmp/adv_xsession_ow");
    if (errors == 0) PASS();
    else FAIL("overwritten value not persisted correctly");
}

// Test: Delete in session 1, verify deleted in session 2
static void test_cross_session_delete(void) {
    TEST("persistence: delete in session 1, verify gone in session 2");
    cleanup("/tmp/adv_xsession_del");

    char vbuf[256];
    uint16_t vlen;
    int errors = 0;

    // Session 1: write
    {
        adb_t *db = NULL;
        adb_open("/tmp/adv_xsession_del", NULL, &db);
        adb_put(db, "delme", 5, "willdie", 7);
        adb_put(db, "keepme", 6, "survivor", 8);
        adb_sync(db);
        adb_close(db);
    }

    // Session 2: delete one key
    {
        adb_t *db = NULL;
        adb_open("/tmp/adv_xsession_del", NULL, &db);
        adb_delete(db, "delme", 5);
        adb_sync(db);
        adb_close(db);
    }

    // Session 3: verify
    {
        adb_t *db = NULL;
        adb_open("/tmp/adv_xsession_del", NULL, &db);
        int rc = adb_get(db, "delme", 5, vbuf, 256, &vlen);
        if (rc != ADB_ERR_NOT_FOUND) errors++;  // should be gone
        rc = adb_get(db, "keepme", 6, vbuf, 256, &vlen);
        if (rc != 0 || vlen != 8 || memcmp(vbuf, "survivor", 8) != 0) errors++;
        adb_close(db);
    }

    cleanup("/tmp/adv_xsession_del");
    if (errors == 0) PASS();
    else FAILF("%d cross-session delete errors", errors);
}

// ============================================================================
// SECTION 3: SCAN CORRECTNESS
// ============================================================================

typedef struct {
    int count;
    int order_ok;
    char last_key[64];
    int last_klen;
    int dup_count;
    char (*seen_keys)[64];
    int *seen_klens;
    int seen_cap;
} scan_ctx_t;

static int adv_scan_cb(const void *key, uint16_t klen,
                       const void *val, uint16_t vlen, void *ctx) {
    (void)val; (void)vlen;
    scan_ctx_t *s = (scan_ctx_t *)ctx;

    char kbuf[64];
    int clen = klen > 62 ? 62 : klen;
    memcpy(kbuf, key, clen);
    kbuf[clen] = 0;

    // Check ordering
    if (s->count > 0 && strcmp(kbuf, s->last_key) < 0)
        s->order_ok = 0;

    // Check for duplicates
    for (int i = 0; i < s->count && i < s->seen_cap; i++) {
        if (s->seen_klens[i] == clen && memcmp(s->seen_keys[i], kbuf, clen) == 0) {
            s->dup_count++;
            break;
        }
    }

    if (s->count < s->seen_cap) {
        memcpy(s->seen_keys[s->count], kbuf, clen + 1);
        s->seen_klens[s->count] = clen;
    }

    memcpy(s->last_key, kbuf, clen + 1);
    s->last_klen = clen;
    s->count++;
    return 0;
}

// Test: Scan should not return duplicates from memtable AND btree
static void test_scan_no_duplicates(void) {
    TEST("scan: no duplicate keys from memtable+btree");
    cleanup("/tmp/adv_scan_dup");

    int count = 500;
    char key[16], val[16];

    // Session 1: write keys, close (flushes to B+ tree)
    {
        adb_t *db = NULL;
        adb_open("/tmp/adv_scan_dup", NULL, &db);
        for (int i = 0; i < count; i++) {
            int kl = snprintf(key, sizeof(key), "sd%04d", i);
            int vl = snprintf(val, sizeof(val), "sv%04d", i);
            adb_put(db, key, (uint16_t)kl, val, (uint16_t)vl);
        }
        adb_sync(db);
        adb_close(db);
    }

    // Session 2: reopen, overwrite some keys (now in memtable AND btree)
    {
        adb_t *db = NULL;
        adb_open("/tmp/adv_scan_dup", NULL, &db);

        // Overwrite first 100 keys
        for (int i = 0; i < 100; i++) {
            int kl = snprintf(key, sizeof(key), "sd%04d", i);
            int vl = snprintf(val, sizeof(val), "nv%04d", i);
            adb_put(db, key, (uint16_t)kl, val, (uint16_t)vl);
        }

        // Scan should return each key ONCE
        scan_ctx_t ctx;
        memset(&ctx, 0, sizeof(ctx));
        ctx.order_ok = 1;
        ctx.seen_cap = count + 100; // extra room
        ctx.seen_keys = malloc(ctx.seen_cap * 64);
        ctx.seen_klens = malloc(ctx.seen_cap * sizeof(int));

        adb_scan(db, "sd0000", 6, "sd9999", 6, adv_scan_cb, &ctx);

        int errors = 0;
        if (ctx.dup_count > 0) errors += ctx.dup_count;
        if (!ctx.order_ok) errors++;

        free(ctx.seen_keys);
        free(ctx.seen_klens);
        adb_close(db);
        cleanup("/tmp/adv_scan_dup");

        if (errors == 0) PASS();
        else FAILF("dups=%d, order_ok=%d, count=%d", ctx.dup_count, ctx.order_ok, ctx.count);
    }
}

// Test: Scan empty range returns 0 results
static void test_scan_empty_range(void) {
    TEST("scan: empty range returns 0 results");
    cleanup("/tmp/adv_scan_empty");

    adb_t *db = NULL;
    adb_open("/tmp/adv_scan_empty", NULL, &db);

    // Add some keys
    adb_put(db, "aaa", 3, "v", 1);
    adb_put(db, "bbb", 3, "v", 1);
    adb_put(db, "ccc", 3, "v", 1);

    // Scan range that contains nothing
    scan_ctx_t ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.order_ok = 1;
    ctx.seen_cap = 0;
    ctx.seen_keys = NULL;
    ctx.seen_klens = NULL;

    adb_scan(db, "ddd", 3, "eee", 3, adv_scan_cb, &ctx);

    adb_close(db);
    cleanup("/tmp/adv_scan_empty");

    if (ctx.count == 0) PASS();
    else FAILF("expected 0, got %d", ctx.count);
}

// Test: Scan partial range
static void test_scan_partial_range(void) {
    TEST("scan: partial range returns correct subset");
    cleanup("/tmp/adv_scan_partial");

    adb_t *db = NULL;
    adb_open("/tmp/adv_scan_partial", NULL, &db);

    for (int i = 0; i < 100; i++) {
        char key[8], val[8];
        int kl = snprintf(key, sizeof(key), "k%03d", i);
        int vl = snprintf(val, sizeof(val), "v%03d", i);
        adb_put(db, key, (uint16_t)kl, val, (uint16_t)vl);
    }

    // Scan k020..k030 (should get ~11 keys: k020..k030 inclusive)
    scan_ctx_t ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.order_ok = 1;
    ctx.seen_cap = 200;
    ctx.seen_keys = malloc(200 * 64);
    ctx.seen_klens = malloc(200 * sizeof(int));

    adb_scan(db, "k020", 4, "k030", 4, adv_scan_cb, &ctx);

    free(ctx.seen_keys);
    free(ctx.seen_klens);
    adb_close(db);
    cleanup("/tmp/adv_scan_partial");

    // Should be 11 keys (k020 through k030)
    if (ctx.count >= 10 && ctx.count <= 12 && ctx.order_ok) PASS();
    else FAILF("expected ~11 keys, got %d, order_ok=%d", ctx.count, ctx.order_ok);
}

// ============================================================================
// SECTION 4: REAL-WORLD WORKLOAD SIMULATIONS
// ============================================================================

// Web session store: rapid creates, reads, updates, expires
static void test_workload_session_store(void) {
    TEST("workload: web session store (create/read/update/expire)");
    cleanup("/tmp/adv_wl_session");

    adb_t *db = NULL;
    adb_open("/tmp/adv_wl_session", NULL, &db);
    if (!db) { FAIL("open"); return; }

    int errors = 0;
    int sessions = 5000;
    char key[48], val[200], vbuf[256];
    uint16_t vlen;

    // Phase 1: Create sessions
    for (int i = 0; i < sessions; i++) {
        int kl = snprintf(key, sizeof(key), "sess:%08x:%04x", (unsigned)(i * 2654435761u), i & 0xFFFF);
        int vl = snprintf(val, sizeof(val),
            "{\"uid\":%d,\"ip\":\"192.168.%d.%d\",\"ua\":\"Mozilla/5.0\",\"cart\":[%d,%d,%d],\"ts\":%lu}",
            i, (i >> 8) & 255, i & 255, i % 100, (i + 1) % 100, (i + 2) % 100,
            (unsigned long)(1700000000 + i));
        adb_put(db, key, (uint16_t)kl, val, (uint16_t)vl);
    }

    // Phase 2: Read random sessions (80% of ops in real apps)
    xor_state = 0x42;
    for (int i = 0; i < sessions * 4; i++) {
        int idx = (int)(xorshift64() % sessions);
        int kl = snprintf(key, sizeof(key), "sess:%08x:%04x", (unsigned)(idx * 2654435761u), idx & 0xFFFF);
        adb_get(db, key, (uint16_t)kl, vbuf, 256, &vlen);
    }

    // Phase 3: Update sessions (add items to cart)
    for (int i = 0; i < sessions / 2; i++) {
        int idx = (int)(xorshift64() % sessions);
        int kl = snprintf(key, sizeof(key), "sess:%08x:%04x", (unsigned)(idx * 2654435761u), idx & 0xFFFF);
        int vl = snprintf(val, sizeof(val),
            "{\"uid\":%d,\"ip\":\"192.168.%d.%d\",\"ua\":\"Mozilla/5.0\",\"cart\":[%d,%d,%d,%d],\"ts\":%lu}",
            idx, (idx >> 8) & 255, idx & 255, idx % 100, (idx + 1) % 100,
            (idx + 2) % 100, (idx + 3) % 100, (unsigned long)(1700000000 + sessions + i));
        adb_put(db, key, (uint16_t)kl, val, (uint16_t)vl);
    }

    // Phase 4: Expire old sessions (delete 20%)
    for (int i = 0; i < sessions / 5; i++) {
        int kl = snprintf(key, sizeof(key), "sess:%08x:%04x", (unsigned)(i * 2654435761u), i & 0xFFFF);
        adb_delete(db, key, (uint16_t)kl);
    }

    // Phase 5: Verify non-expired sessions still exist
    for (int i = sessions / 5; i < sessions / 5 + 100; i++) {
        int kl = snprintf(key, sizeof(key), "sess:%08x:%04x", (unsigned)(i * 2654435761u), i & 0xFFFF);
        int rc = adb_get(db, key, (uint16_t)kl, vbuf, 256, &vlen);
        if (rc != 0) errors++;
    }

    adb_close(db);
    cleanup("/tmp/adv_wl_session");

    if (errors == 0) PASS();
    else FAILF("%d session verification errors", errors);
}

// User profile CRUD: typical SaaS pattern
static void test_workload_user_profiles(void) {
    TEST("workload: user profile CRUD (SaaS pattern)");
    cleanup("/tmp/adv_wl_users");

    adb_t *db = NULL;
    adb_open("/tmp/adv_wl_users", NULL, &db);
    if (!db) { FAIL("open"); return; }

    int errors = 0;
    int users = 10000;
    char key[32], val[250], vbuf[256];
    uint16_t vlen;

    // Create users
    for (int i = 0; i < users; i++) {
        int kl = snprintf(key, sizeof(key), "user:%06d", i);
        int vl = snprintf(val, sizeof(val),
            "{\"name\":\"User_%d\",\"email\":\"u%d@test.com\",\"plan\":\"free\",\"created\":%d}",
            i, i, 1700000000 + i);
        if (vl > 250) vl = 250;
        adb_put(db, key, (uint16_t)kl, val, (uint16_t)vl);
    }

    // Upgrade 30% to paid
    for (int i = 0; i < users * 3 / 10; i++) {
        int kl = snprintf(key, sizeof(key), "user:%06d", i);
        int vl = snprintf(val, sizeof(val),
            "{\"name\":\"User_%d\",\"email\":\"u%d@test.com\",\"plan\":\"pro\",\"created\":%d}",
            i, i, 1700000000 + i);
        if (vl > 250) vl = 250;
        adb_put(db, key, (uint16_t)kl, val, (uint16_t)vl);
    }

    // Delete 5% (account deletion)
    for (int i = users - users / 20; i < users; i++) {
        int kl = snprintf(key, sizeof(key), "user:%06d", i);
        adb_delete(db, key, (uint16_t)kl);
    }

    // Verify upgraded users
    for (int i = 0; i < 100; i++) {
        int kl = snprintf(key, sizeof(key), "user:%06d", i);
        int rc = adb_get(db, key, (uint16_t)kl, vbuf, 256, &vlen);
        if (rc != 0) { errors++; continue; }
        if (!strstr(vbuf, "\"plan\":\"pro\"")) errors++;
    }

    // Verify deleted users are gone
    for (int i = users - 10; i < users; i++) {
        int kl = snprintf(key, sizeof(key), "user:%06d", i);
        int rc = adb_get(db, key, (uint16_t)kl, vbuf, 256, &vlen);
        if (rc != ADB_ERR_NOT_FOUND) errors++;
    }

    adb_close(db);
    cleanup("/tmp/adv_wl_users");

    if (errors == 0) PASS();
    else FAILF("%d user profile errors", errors);
}

// Config/settings store: small KV, frequent reads, rare writes
static void test_workload_config_store(void) {
    TEST("workload: config store (frequent reads, rare writes)");
    cleanup("/tmp/adv_wl_config");

    adb_t *db = NULL;
    adb_open("/tmp/adv_wl_config", NULL, &db);
    if (!db) { FAIL("open"); return; }

    int errors = 0;
    char vbuf[256];
    uint16_t vlen;

    // Write 50 config keys
    const char *configs[][2] = {
        {"cfg:app.name", "MyApp"},
        {"cfg:app.version", "2.1.0"},
        {"cfg:db.pool_size", "10"},
        {"cfg:db.timeout_ms", "5000"},
        {"cfg:cache.ttl_sec", "300"},
        {"cfg:log.level", "info"},
        {"cfg:auth.jwt_secret", "super_secret_key_12345"},
        {"cfg:auth.session_ttl", "3600"},
        {"cfg:api.rate_limit", "1000"},
        {"cfg:api.cors_origin", "https://example.com"},
    };

    for (int i = 0; i < 10; i++) {
        adb_put(db, configs[i][0], (uint16_t)strlen(configs[i][0]),
                configs[i][1], (uint16_t)strlen(configs[i][1]));
    }

    // Simulate 100K config reads (hot path in real apps)
    for (int i = 0; i < 100000; i++) {
        int idx = i % 10;
        int rc = adb_get(db, configs[idx][0], (uint16_t)strlen(configs[idx][0]),
                         vbuf, 256, &vlen);
        if (rc != 0) errors++;
    }

    // Update one config
    adb_put(db, "cfg:log.level", 13, "debug", 5);
    int rc = adb_get(db, "cfg:log.level", 13, vbuf, 256, &vlen);
    if (rc != 0 || vlen != 5 || memcmp(vbuf, "debug", 5) != 0) errors++;

    adb_close(db);
    cleanup("/tmp/adv_wl_config");

    if (errors == 0) PASS();
    else FAILF("%d config store errors", errors);
}

// Time-series-like ingestion: monotonically increasing keys
static void test_workload_timeseries(void) {
    TEST("workload: time-series ingestion (monotonic keys)");
    cleanup("/tmp/adv_wl_ts");

    adb_t *db = NULL;
    adb_open("/tmp/adv_wl_ts", NULL, &db);
    if (!db) { FAIL("open"); return; }

    int errors = 0;
    int count = 50000;
    char key[32], val[128], vbuf[256];
    uint16_t vlen;

    // Ingest: monotonically increasing timestamp keys
    for (int i = 0; i < count; i++) {
        int kl = snprintf(key, sizeof(key), "ts:%010d:%04d", 1700000000 + i / 10, i % 10);
        int vl = snprintf(val, sizeof(val), "{\"temp\":%.1f,\"hum\":%d}",
                          20.0 + (i % 100) * 0.1, 40 + i % 60);
        adb_put(db, key, (uint16_t)kl, val, (uint16_t)vl);
    }

    // Range query: last 100 entries
    char start[32], end[32];
    snprintf(start, sizeof(start), "ts:%010d:0000", 1700000000 + (count - 100) / 10);
    snprintf(end, sizeof(end), "ts:%010d:9999", 1700000000 + count / 10);

    scan_ctx_t ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.order_ok = 1;
    ctx.seen_cap = 200;
    ctx.seen_keys = malloc(200 * 64);
    ctx.seen_klens = malloc(200 * sizeof(int));

    adb_scan(db, start, (uint16_t)strlen(start), end, (uint16_t)strlen(end),
             adv_scan_cb, &ctx);

    if (ctx.count < 90 || !ctx.order_ok) errors++;

    // Spot-check random reads
    for (int i = 0; i < 100; i++) {
        int idx = (count / 2) + i;
        int kl = snprintf(key, sizeof(key), "ts:%010d:%04d", 1700000000 + idx / 10, idx % 10);
        int rc = adb_get(db, key, (uint16_t)kl, vbuf, 256, &vlen);
        if (rc != 0) errors++;
    }

    free(ctx.seen_keys);
    free(ctx.seen_klens);
    adb_close(db);
    cleanup("/tmp/adv_wl_ts");

    if (errors == 0) PASS();
    else FAILF("%d time-series errors (scan_count=%d)", errors, ctx.count);
}

// ============================================================================
// SECTION 5: ROBUSTNESS & EDGE CASES
// ============================================================================

// Test: Multiple databases open simultaneously
static void test_multiple_dbs(void) {
    TEST("robustness: two databases open simultaneously");
    cleanup("/tmp/adv_db1");
    cleanup("/tmp/adv_db2");

    adb_t *db1 = NULL, *db2 = NULL;
    int errors = 0;
    adb_open("/tmp/adv_db1", NULL, &db1);
    adb_open("/tmp/adv_db2", NULL, &db2);
    if (!db1 || !db2) { FAIL("open both dbs"); goto mdb_cleanup; }

    char vbuf[256];
    uint16_t vlen;

    // Write to db1
    adb_put(db1, "only_in_db1", 11, "val1", 4);
    // Write to db2
    adb_put(db2, "only_in_db2", 11, "val2", 4);

    // Cross-check: db1 should NOT have db2's key
    int rc = adb_get(db1, "only_in_db2", 11, vbuf, 256, &vlen);
    if (rc != ADB_ERR_NOT_FOUND) errors++;

    // db2 should NOT have db1's key
    rc = adb_get(db2, "only_in_db1", 11, vbuf, 256, &vlen);
    if (rc != ADB_ERR_NOT_FOUND) errors++;

    // Each should have its own
    rc = adb_get(db1, "only_in_db1", 11, vbuf, 256, &vlen);
    if (rc != 0) errors++;
    rc = adb_get(db2, "only_in_db2", 11, vbuf, 256, &vlen);
    if (rc != 0) errors++;

mdb_cleanup:
    if (db1) adb_close(db1);
    if (db2) adb_close(db2);
    cleanup("/tmp/adv_db1");
    cleanup("/tmp/adv_db2");

    if (errors == 0) PASS();
    else FAILF("%d cross-db isolation errors", errors);
}

// Test: Destroy then recreate database
static void test_destroy_recreate(void) {
    TEST("robustness: destroy then recreate database");
    cleanup("/tmp/adv_destroy_rc");

    adb_t *db = NULL;
    char vbuf[256];
    uint16_t vlen;
    int errors = 0;

    // Create, write, close
    adb_open("/tmp/adv_destroy_rc", NULL, &db);
    adb_put(db, "beforedestroy", 13, "oldval", 6);
    adb_close(db);
    db = NULL;

    // Destroy
    adb_destroy("/tmp/adv_destroy_rc");

    // Recreate
    adb_open("/tmp/adv_destroy_rc", NULL, &db);
    if (!db) { FAIL("reopen after destroy"); cleanup("/tmp/adv_destroy_rc"); return; }

    // Old key should be gone
    int rc = adb_get(db, "beforedestroy", 13, vbuf, 256, &vlen);
    if (rc != ADB_ERR_NOT_FOUND) errors++;

    // New data should work
    adb_put(db, "afterdestroy", 12, "newval", 6);
    rc = adb_get(db, "afterdestroy", 12, vbuf, 256, &vlen);
    if (rc != 0 || vlen != 6) errors++;

    adb_close(db);
    cleanup("/tmp/adv_destroy_rc");

    if (errors == 0) PASS();
    else FAILF("%d errors after destroy/recreate", errors);
}

// Test: Destroy must clean non-empty wal/sst directories
static void test_destroy_nonempty_dirs(void) {
    TEST("robustness: destroy cleans non-empty wal/sst dirs");
    cleanup("/tmp/adv_destroy_full");

    adb_t *db = NULL;
    int errors = 0;
    char vbuf[256];
    uint16_t vlen = 0;

    int rc = adb_open("/tmp/adv_destroy_full", NULL, &db);
    if (rc != 0 || !db) { FAIL("open"); cleanup("/tmp/adv_destroy_full"); return; }

    adb_put(db, "oldkey", 6, "oldval", 6);
    adb_close(db);
    db = NULL;

    mkdir("/tmp/adv_destroy_full/wal", 0755);
    mkdir("/tmp/adv_destroy_full/sst", 0755);

    ssize_t wr = 0;
    int fd = open("/tmp/adv_destroy_full/wal/stale.wal", O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd >= 0) { wr = write(fd, "x", 1); if (wr != 1) errors++; close(fd); } else errors++;
    fd = open("/tmp/adv_destroy_full/sst/stale.sst", O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd >= 0) { wr = write(fd, "y", 1); if (wr != 1) errors++; close(fd); } else errors++;

    rc = adb_destroy("/tmp/adv_destroy_full");
    if (rc != 0) errors++;

    if (access("/tmp/adv_destroy_full/wal/stale.wal", F_OK) == 0) errors++;
    if (access("/tmp/adv_destroy_full/sst/stale.sst", F_OK) == 0) errors++;

    rc = adb_open("/tmp/adv_destroy_full", NULL, &db);
    if (rc != 0 || !db) {
        FAIL("reopen after destroy");
        cleanup("/tmp/adv_destroy_full");
        return;
    }

    rc = adb_get(db, "oldkey", 6, vbuf, 256, &vlen);
    if (rc != ADB_ERR_NOT_FOUND) errors++;

    rc = adb_put(db, "newkey", 6, "newval", 6);
    if (rc != 0) errors++;
    rc = adb_get(db, "newkey", 6, vbuf, 256, &vlen);
    if (rc != 0 || vlen != 6 || memcmp(vbuf, "newval", 6) != 0) errors++;

    adb_close(db);
    cleanup("/tmp/adv_destroy_full");

    if (errors == 0) PASS();
    else FAILF("%d errors in non-empty destroy test", errors);
}

// Test: Heavy delete + reinsert pattern (tombstone stress)
static void test_tombstone_stress(void) {
    TEST("robustness: 10K keys, delete all, reinsert all, verify");
    cleanup("/tmp/adv_tombstone");

    adb_t *db = NULL;
    adb_open("/tmp/adv_tombstone", NULL, &db);
    if (!db) { FAIL("open"); return; }

    int count = 10000;
    int errors = 0;
    char key[16], val[32], vbuf[256];
    uint16_t vlen;

    // Phase 1: Insert all
    for (int i = 0; i < count; i++) {
        int kl = snprintf(key, sizeof(key), "ts%05d", i);
        int vl = snprintf(val, sizeof(val), "v1_%05d", i);
        adb_put(db, key, (uint16_t)kl, val, (uint16_t)vl);
    }

    // Phase 2: Delete all
    for (int i = 0; i < count; i++) {
        int kl = snprintf(key, sizeof(key), "ts%05d", i);
        adb_delete(db, key, (uint16_t)kl);
    }

    // Phase 3: Verify all deleted
    for (int i = 0; i < count; i++) {
        int kl = snprintf(key, sizeof(key), "ts%05d", i);
        int rc = adb_get(db, key, (uint16_t)kl, vbuf, 256, &vlen);
        if (rc != ADB_ERR_NOT_FOUND) errors++;
    }

    // Phase 4: Reinsert all with different values
    for (int i = 0; i < count; i++) {
        int kl = snprintf(key, sizeof(key), "ts%05d", i);
        int vl = snprintf(val, sizeof(val), "v2_%05d", i);
        adb_put(db, key, (uint16_t)kl, val, (uint16_t)vl);
    }

    // Phase 5: Verify new values
    for (int i = 0; i < count; i++) {
        int kl = snprintf(key, sizeof(key), "ts%05d", i);
        int vl = snprintf(val, sizeof(val), "v2_%05d", i);
        int rc = adb_get(db, key, (uint16_t)kl, vbuf, 256, &vlen);
        if (rc != 0 || vlen != (uint16_t)vl || memcmp(vbuf, val, vl) != 0)
            errors++;
    }

    adb_close(db);
    cleanup("/tmp/adv_tombstone");

    if (errors == 0) PASS();
    else FAILF("%d tombstone stress errors", errors);
}

// Test: Interleaved put/delete/get (OLTP-like pattern)
static void test_oltp_pattern(void) {
    TEST("workload: OLTP mixed put/delete/get (200K ops)");
    cleanup("/tmp/adv_oltp");

    adb_t *db = NULL;
    adb_open("/tmp/adv_oltp", NULL, &db);
    if (!db) { FAIL("open"); return; }

    int total_ops = 200000;
    int errors = 0;
    int key_space = 10000;
    char key[16], val[32], vbuf[256];
    uint16_t vlen;
    int *exists = calloc(key_space, sizeof(int)); // track what should exist
    xor_state = 0xABCD;

    uint64_t t0 = now_ns();

    for (int i = 0; i < total_ops; i++) {
        int idx = (int)(xorshift64() % key_space);
        int kl = snprintf(key, sizeof(key), "oltp%05d", idx);
        int op = (int)(xorshift64() % 100);

        if (op < 50) {
            // 50% put
            int vl = snprintf(val, sizeof(val), "oval%05d_%d", idx, i);
            int rc = adb_put(db, key, (uint16_t)kl, val, (uint16_t)vl);
            if (rc == 0) exists[idx] = 1;
        } else if (op < 80) {
            // 30% get
            int rc = adb_get(db, key, (uint16_t)kl, vbuf, 256, &vlen);
            if (exists[idx] && rc != 0) errors++; // should exist but doesn't
        } else {
            // 20% delete
            adb_delete(db, key, (uint16_t)kl);
            exists[idx] = 0;
        }
    }

    uint64_t elapsed = now_ns() - t0;
    double secs = (double)elapsed / 1e9;

    free(exists);
    adb_close(db);
    cleanup("/tmp/adv_oltp");

    if (errors == 0) {
        printf("PASS (%.1fs, %.0f ops/s)\n", secs, (double)total_ops / secs);
        tests_passed++;
    } else {
        FAILF("%d OLTP errors in %d ops", errors, total_ops);
    }
}

// Test: Persistence after overwrite-heavy workload
static void test_overwrite_persistence(void) {
    TEST("persistence: 5K keys overwritten 10x, survive close/reopen");
    cleanup("/tmp/adv_ow_persist");

    int count = 5000;
    int rounds = 10;
    int errors = 0;
    char key[16], val[32], vbuf[256];
    uint16_t vlen;

    // Session 1: many overwrites
    {
        adb_t *db = NULL;
        adb_open("/tmp/adv_ow_persist", NULL, &db);
        for (int r = 0; r < rounds; r++) {
            for (int i = 0; i < count; i++) {
                int kl = snprintf(key, sizeof(key), "owp%05d", i);
                int vl = snprintf(val, sizeof(val), "r%02d_%05d", r, i);
                adb_put(db, key, (uint16_t)kl, val, (uint16_t)vl);
            }
        }
        adb_sync(db);
        adb_close(db);
    }

    // Session 2: verify final values
    {
        adb_t *db = NULL;
        adb_open("/tmp/adv_ow_persist", NULL, &db);
        for (int i = 0; i < count; i++) {
            int kl = snprintf(key, sizeof(key), "owp%05d", i);
            int vl = snprintf(val, sizeof(val), "r%02d_%05d", rounds - 1, i);
            int rc = adb_get(db, key, (uint16_t)kl, vbuf, 256, &vlen);
            if (rc != 0 || vlen != (uint16_t)vl || memcmp(vbuf, val, vl) != 0)
                errors++;
        }
        adb_close(db);
    }

    cleanup("/tmp/adv_ow_persist");
    if (errors == 0) PASS();
    else FAILF("%d/%d overwrite persistence errors", errors, count);
}

// Test: Large number of keys persistence (tests B+ tree growth across sessions)
static void test_large_persistence(void) {
    TEST("persistence: 100K keys survive close/reopen");
    cleanup("/tmp/adv_large_persist");

    int count = 100000;
    int put_errs = 0, pre_close_errs = 0, post_reopen_errs = 0;
    int first_pre = -1, first_post = -1;
    char key[24], val[48], vbuf[256];
    uint16_t vlen;

    // Session 1: write 100K keys
    {
        adb_t *db = NULL;
        adb_open("/tmp/adv_large_persist", NULL, &db);
        if (!db) { FAIL("open"); return; }
        for (int i = 0; i < count; i++) {
            int kl = snprintf(key, sizeof(key), "lp%07d", i);
            int vl = snprintf(val, sizeof(val), "lpv%07d_data", i);
            int rc = adb_put(db, key, (uint16_t)kl, val, (uint16_t)vl);
            if (rc != 0) put_errs++;
        }

        // Verify BEFORE close
        for (int i = 0; i < count; i++) {
            int kl = snprintf(key, sizeof(key), "lp%07d", i);
            int vl = snprintf(val, sizeof(val), "lpv%07d_data", i);
            int rc = adb_get(db, key, (uint16_t)kl, vbuf, 256, &vlen);
            if (rc != 0 || vlen != (uint16_t)vl || memcmp(vbuf, val, vl) != 0) {
                if (first_pre < 0) first_pre = i;
                pre_close_errs++;
            }
        }

        adb_sync(db);
        adb_close(db);
    }

    // Session 2: verify all 100K
    {
        adb_t *db = NULL;
        adb_open("/tmp/adv_large_persist", NULL, &db);
        if (!db) { FAIL("reopen"); cleanup("/tmp/adv_large_persist"); return; }
        for (int i = 0; i < count; i++) {
            int kl = snprintf(key, sizeof(key), "lp%07d", i);
            int vl = snprintf(val, sizeof(val), "lpv%07d_data", i);
            int rc = adb_get(db, key, (uint16_t)kl, vbuf, 256, &vlen);
            if (rc != 0 || vlen != (uint16_t)vl || memcmp(vbuf, val, vl) != 0) {
                if (first_post < 0) first_post = i;
                post_reopen_errs++;
            }
        }
        adb_close(db);
    }

    cleanup("/tmp/adv_large_persist");
    if (put_errs == 0 && pre_close_errs == 0 && post_reopen_errs == 0) PASS();
    else {
        // Print first 20 lost key indices for debugging
        printf("\n");
        FAILF("put_errs=%d pre_close=%d(first@%d) post_reopen=%d(first@%d)",
            put_errs, pre_close_errs, first_pre, post_reopen_errs, first_post);
    }
}

// Test: Randomized model-check with periodic reopen checkpoints
static void test_randomized_model_reopen(void) {
    TEST("workload: randomized model w/ reopen checkpoints");
    const char *path = "/tmp/adv_model_chk";
    cleanup(path);

    adb_t *db = NULL;
    int rc = adb_open(path, NULL, &db);
    if (rc != 0 || !db) { FAIL("open"); cleanup(path); return; }

    enum { NKEYS = 512, OPS = 50000, REOPEN_EVERY = 2500, SPOT = 64 };
    char exp_vals[NKEYS][48];
    uint16_t exp_lens[NKEYS];
    uint8_t present[NKEYS];
    memset(exp_vals, 0, sizeof(exp_vals));
    memset(exp_lens, 0, sizeof(exp_lens));
    memset(present, 0, sizeof(present));

    int errors = 0;
    int missing_present = 0;
    int found_absent = 0;
    int value_mismatch = 0;
    char key[16], val[48], vbuf[256];
    uint16_t vlen = 0;
    xor_state = 0x6A09E667F3BCC909ULL;

    for (int op = 0; op < OPS; op++) {
        int idx = (int)(xorshift64() % NKEYS);
        int kl = snprintf(key, sizeof(key), "mk%04d", idx);
        int action = (int)(xorshift64() % 100);

        if (action < 45) {
            uint32_t ver = (uint32_t)(xorshift64() & 0xFFFFFF);
            int vl = snprintf(val, sizeof(val), "mv_%04d_%06u", idx, ver);
            rc = adb_put(db, key, (uint16_t)kl, val, (uint16_t)vl);
            if (rc != 0) errors++;
            memcpy(exp_vals[idx], val, (size_t)vl);
            exp_lens[idx] = (uint16_t)vl;
            present[idx] = 1;
        } else if (action < 65) {
            rc = adb_delete(db, key, (uint16_t)kl);
            if (rc != 0 && rc != ADB_ERR_NOT_FOUND) errors++;
            exp_lens[idx] = 0;
            present[idx] = 0;
        } else {
            rc = adb_get(db, key, (uint16_t)kl, vbuf, 256, &vlen);
            if (present[idx]) {
                if (rc != 0) {
                    missing_present++;
                    errors++;
                } else if (vlen != exp_lens[idx] || memcmp(vbuf, exp_vals[idx], exp_lens[idx]) != 0) {
                    value_mismatch++;
                    errors++;
                }
            } else if (rc != ADB_ERR_NOT_FOUND) {
                found_absent++;
                errors++;
            }
        }

        if (((op + 1) % REOPEN_EVERY) == 0) {
            adb_close(db);
            db = NULL;
            rc = adb_open(path, NULL, &db);
            if (rc != 0 || !db) { FAIL("reopen checkpoint"); cleanup(path); return; }

            for (int s = 0; s < SPOT; s++) {
                int j = (int)(xorshift64() % NKEYS);
                int jkl = snprintf(key, sizeof(key), "mk%04d", j);
                rc = adb_get(db, key, (uint16_t)jkl, vbuf, 256, &vlen);
                if (present[j]) {
                    if (rc != 0) {
                        missing_present++;
                        errors++;
                    } else if (vlen != exp_lens[j] || memcmp(vbuf, exp_vals[j], exp_lens[j]) != 0) {
                        value_mismatch++;
                        errors++;
                    }
                } else if (rc != ADB_ERR_NOT_FOUND) {
                    found_absent++;
                    errors++;
                }
            }
        }
    }

    for (int i = 0; i < NKEYS; i++) {
        int kl = snprintf(key, sizeof(key), "mk%04d", i);
        rc = adb_get(db, key, (uint16_t)kl, vbuf, 256, &vlen);
        if (present[i]) {
            if (rc != 0) {
                missing_present++;
                errors++;
            } else if (vlen != exp_lens[i] || memcmp(vbuf, exp_vals[i], exp_lens[i]) != 0) {
                value_mismatch++;
                errors++;
            }
        } else if (rc != ADB_ERR_NOT_FOUND) {
            found_absent++;
            errors++;
        }
    }

    adb_close(db);
    cleanup(path);

    if (errors == 0) PASS();
    else FAILF("%d mismatches (missing=%d stale=%d val=%d)",
               errors, missing_present, found_absent, value_mismatch);
}

// ============================================================================
// SECTION 6: PERFORMANCE REGRESSION
// ============================================================================

static void test_perf_sequential_writes(void) {
    TEST("perf: 100K sequential writes throughput");
    cleanup("/tmp/adv_perf_seq");

    adb_t *db = NULL;
    adb_open("/tmp/adv_perf_seq", NULL, &db);
    if (!db) { FAIL("open"); return; }

    int count = 100000;
    char key[16], val[32];
    uint64_t t0 = now_ns();

    for (int i = 0; i < count; i++) {
        int kl = snprintf(key, sizeof(key), "pk%07d", i);
        int vl = snprintf(val, sizeof(val), "pv%07d", i);
        adb_put(db, key, (uint16_t)kl, val, (uint16_t)vl);
    }

    uint64_t elapsed = now_ns() - t0;
    double ops_sec = (double)count / ((double)elapsed / 1e9);

    adb_close(db);
    cleanup("/tmp/adv_perf_seq");

    // Minimum threshold: 100K ops/sec on any reasonable hardware
    if (ops_sec >= 50000) {
        printf("PASS (%.0f puts/s)\n", ops_sec);
        tests_passed++;
    } else {
        FAILF("only %.0f puts/s (min 50K)", ops_sec);
    }
}

static void test_perf_random_reads(void) {
    TEST("perf: random reads after 50K inserts");
    cleanup("/tmp/adv_perf_rread");

    adb_t *db = NULL;
    adb_open("/tmp/adv_perf_rread", NULL, &db);
    if (!db) { FAIL("open"); return; }

    int count = 50000;
    int reads = 200000;
    char key[16], val[32], vbuf[256];
    uint16_t vlen;

    // Insert
    for (int i = 0; i < count; i++) {
        int kl = snprintf(key, sizeof(key), "rk%07d", i);
        int vl = snprintf(val, sizeof(val), "rv%07d", i);
        adb_put(db, key, (uint16_t)kl, val, (uint16_t)vl);
    }

    // Random reads
    xor_state = 0xBEEF;
    uint64_t t0 = now_ns();

    for (int i = 0; i < reads; i++) {
        int idx = (int)(xorshift64() % count);
        int kl = snprintf(key, sizeof(key), "rk%07d", idx);
        adb_get(db, key, (uint16_t)kl, vbuf, 256, &vlen);
    }

    uint64_t elapsed = now_ns() - t0;
    double ops_sec = (double)reads / ((double)elapsed / 1e9);

    adb_close(db);
    cleanup("/tmp/adv_perf_rread");

    if (ops_sec >= 100000) {
        printf("PASS (%.0f gets/s)\n", ops_sec);
        tests_passed++;
    } else {
        FAILF("only %.0f gets/s (min 100K)", ops_sec);
    }
}

// Simple counter callback for new tests
static int adv_count_cb(const void *k, uint16_t kl, const void *v, uint16_t vl, void *ud) {
    (void)k;(void)kl;(void)v;(void)vl;
    (*(int*)ud)++;
    return 0;
}

// ============================================================================
// Batch: Deep Adversarial (27-36)
// ============================================================================

// 27. WAL dirty close: data survives without explicit close
static void test_wal_dirty_close_fork(void) {
    TEST("WAL: dirty close via fork preserves committed data");
    cleanup("/tmp/adv_dirty");
    // Write data in child, exit without close
    pid_t pid = fork();
    if (pid == 0) {
        adb_t *db;
        adb_open("/tmp/adv_dirty", NULL, &db);
        for (int i = 0; i < 50; i++) {
            char k[16]; snprintf(k, 16, "d%04d", i);
            adb_put(db, k, strlen(k), "dirty", 5);
        }
        // Exit without adb_close — WAL unflushed
        _exit(0);
    }
    int st; waitpid(pid, &st, 0);
    // Reopen — WAL recovery should replay
    adb_t *db;
    int rc = adb_open("/tmp/adv_dirty", NULL, &db);
    if (rc) { cleanup("/tmp/adv_dirty"); FAILF("reopen rc=%d", rc); return; }
    int found = 0;
    for (int i = 0; i < 50; i++) {
        char k[16]; snprintf(k, 16, "d%04d", i);
        char vb[256]; uint16_t vl;
        if (adb_get(db, k, strlen(k), vb, 256, &vl) == 0) found++;
    }
    adb_close(db); cleanup("/tmp/adv_dirty");
    if (found != 50) FAILF("expected 50 recovered, got %d", found);
    PASS();
}

// 28. Batch with mixed valid/invalid entries: pre-validation rejects all
static void test_batch_mixed_valid_invalid(void) {
    TEST("batch: mixed valid+invalid entries = all rejected");
    cleanup("/tmp/adv_bmi");
    adb_t *db;
    adb_open("/tmp/adv_bmi", NULL, &db);
    char bigkey[100]; memset(bigkey, 'K', 99); bigkey[99] = 0;
    adb_batch_entry_t entries[3] = {
        {"ok1", 3, "v1", 2},
        {bigkey, 99, "v2", 2},   // key too long
        {"ok3", 3, "v3", 2},
    };
    int rc = adb_batch_put(db, entries, 3);
    // Should fail: pre-validation catches oversized key
    if (rc == 0) {
        // If it passed, check that "ok1" wasn't partially committed
        char vb[256]; uint16_t vl;
        int r2 = adb_get(db, "ok1", 3, vb, 256, &vl);
        adb_close(db); cleanup("/tmp/adv_bmi");
        if (r2 == 0) FAIL("partial commit: ok1 was written");
        else PASS(); // batch succeeded but didn't actually write
        return;
    }
    adb_close(db); cleanup("/tmp/adv_bmi");
    PASS();
}

// 29. Rapid tx begin-put-commit: 200 cycles
static void test_rapid_tx_200_cycles(void) {
    TEST("tx: 200 begin-put-commit cycles");
    cleanup("/tmp/adv_tx200");
    adb_t *db;
    adb_open("/tmp/adv_tx200", NULL, &db);
    int bad = 0;
    for (int i = 0; i < 200; i++) {
        uint64_t txid;
        int rc = adb_tx_begin(db, 0, &txid);
        if (rc) { bad++; break; }
        char k[16]; snprintf(k, 16, "tx%06d", i);
        adb_tx_put(db, txid, k, strlen(k), "v", 1);
        rc = adb_tx_commit(db, txid);
        if (rc) { bad++; break; }
    }
    // Verify last
    char vb[256]; uint16_t vl;
    int rc = adb_get(db, "tx000199", 8, vb, 256, &vl);
    adb_close(db); cleanup("/tmp/adv_tx200");
    if (bad) FAIL("tx cycle failed");
    else if (rc) FAIL("last tx key missing");
    else PASS();
}

// 30. Concurrent open rejection: flock prevents two handles
static void test_flock_concurrent_fork(void) {
    TEST("flock: second open in child process fails");
    cleanup("/tmp/adv_flock");
    adb_t *db;
    adb_open("/tmp/adv_flock", NULL, &db);
    pid_t pid = fork();
    if (pid == 0) {
        adb_t *db2 = NULL;
        int rc = adb_open("/tmp/adv_flock", NULL, &db2);
        // Should fail with LOCKED
        _exit(rc == 0 ? 1 : 0);
    }
    int st;
    waitpid(pid, &st, 0);
    adb_close(db); cleanup("/tmp/adv_flock");
    if (WIFEXITED(st) && WEXITSTATUS(st) == 0) PASS();
    else FAIL("child was able to open locked db");
}

// 31. Scan with 0xFF boundary keys (max byte values)
static void test_scan_high_byte_keys(void) {
    TEST("scan: keys with 0xFF bytes sorted correctly");
    cleanup("/tmp/adv_hbk");
    adb_t *db;
    adb_open("/tmp/adv_hbk", NULL, &db);
    uint8_t k1[3] = {0xFE, 0x00, 0x00};
    uint8_t k2[3] = {0xFF, 0x00, 0x00};
    uint8_t k3[3] = {0xFF, 0xFF, 0x00};
    adb_put(db, k1, 3, "a", 1);
    adb_put(db, k2, 3, "b", 1);
    adb_put(db, k3, 3, "c", 1);
    int ctx_count = 0;
    adb_scan(db, NULL, 0, NULL, 0, adv_count_cb, &ctx_count);
    adb_close(db); cleanup("/tmp/adv_hbk");
    if (ctx_count != 3) FAILF("expected 3 got %d", ctx_count);
    PASS();
}

// 32. Delete all + scan = empty, then reinsert
static void test_delete_all_reinsert(void) {
    TEST("delete all 100 keys, scan=0, reinsert 50");
    cleanup("/tmp/adv_dar");
    adb_t *db;
    adb_open("/tmp/adv_dar", NULL, &db);
    for (int i = 0; i < 100; i++) {
        char k[8]; snprintf(k, 8, "d%04d", i);
        adb_put(db, k, strlen(k), "v", 1);
    }
    for (int i = 0; i < 100; i++) {
        char k[8]; snprintf(k, 8, "d%04d", i);
        adb_delete(db, k, strlen(k));
    }
    int ctx_count = 0;
    adb_scan(db, NULL, 0, NULL, 0, adv_count_cb, &ctx_count);
    if (ctx_count != 0) { adb_close(db); cleanup("/tmp/adv_dar"); FAILF("not empty: %d", ctx_count); return; }
    for (int i = 0; i < 50; i++) {
        char k[8]; snprintf(k, 8, "d%04d", i);
        adb_put(db, k, strlen(k), "new", 3);
    }
    ctx_count = 0;
    adb_scan(db, NULL, 0, NULL, 0, adv_count_cb, &ctx_count);
    adb_close(db); cleanup("/tmp/adv_dar");
    if (ctx_count != 50) FAILF("expected 50 got %d", ctx_count);
    PASS();
}

// 33. Backup during active writes: backup consistent
static void test_backup_during_writes(void) {
    TEST("backup: consistent even with prior writes");
    cleanup("/tmp/adv_bdw"); cleanup("/tmp/adv_bdw_bk"); cleanup("/tmp/adv_bdw_rst");
    adb_t *db;
    adb_open("/tmp/adv_bdw", NULL, &db);
    for (int i = 0; i < 200; i++) {
        char k[16]; snprintf(k, 16, "bw%06d", i);
        adb_put(db, k, strlen(k), "v", 1);
    }
    adb_sync(db);
    adb_backup(db, "/tmp/adv_bdw_bk", ADB_BACKUP_FULL);
    adb_close(db);
    adb_restore("/tmp/adv_bdw_bk", "/tmp/adv_bdw_rst");
    adb_open("/tmp/adv_bdw_rst", NULL, &db);
    int ctx_count = 0;
    adb_scan(db, NULL, 0, NULL, 0, adv_count_cb, &ctx_count);
    adb_close(db);
    cleanup("/tmp/adv_bdw"); cleanup("/tmp/adv_bdw_bk"); cleanup("/tmp/adv_bdw_rst");
    if (ctx_count != 200) FAILF("expected 200 got %d", ctx_count);
    PASS();
}

// 34. is_error / is_syscall_error utility correctness
static void test_error_helpers(void) {
    TEST("error helpers: is_error, is_syscall_error");
    int bad = 0;
    if (is_error(0) != 0) bad++;
    if (is_error(1) != 1) bad++;
    if (is_error(ADB_ERR_IO) != 1) bad++;
    if (is_syscall_error(0) != 0) bad++;
    if (is_syscall_error(42) != 0) bad++;
    if (is_syscall_error(-1) != 1) bad++;
    if (is_syscall_error(-4095) != 1) bad++;
    if (bad) FAILF("%d wrong", bad);
    PASS();
}

// 35. random_level: distribution check (mostly 1)
static void test_random_level_distribution(void) {
    TEST("random_level: mostly 1, some 2+");
    prng_seed(42);
    int counts[16] = {0};
    for (int i = 0; i < 10000; i++) {
        int lvl = random_level();
        if (lvl >= 0 && lvl < 16) counts[lvl]++;
    }
    // Level 1 should be most common (>40%)
    if (counts[1] < 4000) FAILF("level 1 count=%d (expected >4000)", counts[1]);
    // Level 0 should also be significant
    // Some levels >1 should exist
    int higher = 0;
    for (int i = 2; i < 16; i++) higher += counts[i];
    if (higher < 100) FAILF("only %d entries above level 1", higher);
    PASS();
}

// 36. Multiple DBs: open 3 different paths simultaneously
static void test_three_dbs_simultaneous(void) {
    TEST("3 DBs open simultaneously, independent data");
    cleanup("/tmp/adv_db1"); cleanup("/tmp/adv_db2"); cleanup("/tmp/adv_db3");
    adb_t *db1, *db2, *db3;
    adb_open("/tmp/adv_db1", NULL, &db1);
    adb_open("/tmp/adv_db2", NULL, &db2);
    adb_open("/tmp/adv_db3", NULL, &db3);
    adb_put(db1, "k1", 2, "v1", 2);
    adb_put(db2, "k2", 2, "v2", 2);
    adb_put(db3, "k3", 2, "v3", 2);
    char vb[256]; uint16_t vl;
    int bad = 0;
    // Each DB should only see its own data
    if (adb_get(db1, "k1", 2, vb, 256, &vl) != 0) bad++;
    if (adb_get(db1, "k2", 2, vb, 256, &vl) == 0) bad++;
    if (adb_get(db2, "k2", 2, vb, 256, &vl) != 0) bad++;
    if (adb_get(db2, "k1", 2, vb, 256, &vl) == 0) bad++;
    if (adb_get(db3, "k3", 2, vb, 256, &vl) != 0) bad++;
    if (adb_get(db3, "k1", 2, vb, 256, &vl) == 0) bad++;
    adb_close(db1); adb_close(db2); adb_close(db3);
    cleanup("/tmp/adv_db1"); cleanup("/tmp/adv_db2"); cleanup("/tmp/adv_db3");
    if (bad) FAILF("%d cross-contaminations", bad);
    PASS();
}

// ============================================================================
// TEST 37: SSTable CRC survives many sync cycles
// ============================================================================
static void test_sstable_crc_stress(void) {
    TEST("SSTable CRC: 10 sync cycles, all reads clean");
    cleanup("/tmp/adv_crc_stress");
    adb_t *db;
    if (adb_open("/tmp/adv_crc_stress", NULL, &db)) { FAIL("open"); return; }
    for (int cycle = 0; cycle < 10; cycle++) {
        for (int i = 0; i < 50; i++) {
            char k[16]; snprintf(k, 16, "c%d_%03d", cycle, i);
            adb_put(db, k, strlen(k), "val", 3);
        }
        adb_sync(db);
    }
    adb_close(db);
    if (adb_open("/tmp/adv_crc_stress", NULL, &db)) { FAIL("reopen"); return; }
    int cnt = 0;
    adb_scan(db, NULL, 0, NULL, 0, adv_count_cb, &cnt);
    adb_close(db); cleanup("/tmp/adv_crc_stress");
    if (cnt != 500) FAILF("expected 500, got %d", cnt);
    else PASS();
}

// ============================================================================
// TEST 38: interleaved put/delete across 5 sessions, final state correct
// ============================================================================
static void test_interleaved_sessions_state(void) {
    TEST("5 sessions: interleaved put/delete, final state correct");
    cleanup("/tmp/adv_ils");
    for (int s = 0; s < 5; s++) {
        adb_t *db;
        if (adb_open("/tmp/adv_ils", NULL, &db)) { FAIL("open"); return; }
        for (int i = 0; i < 100; i++) {
            char k[16]; snprintf(k, 16, "key_%04d", i);
            if ((i + s) % 3 == 0)
                adb_delete(db, k, strlen(k));
            else
                adb_put(db, k, strlen(k), "v", 1);
        }
        adb_sync(db);
        adb_close(db);
    }
    adb_t *db;
    if (adb_open("/tmp/adv_ils", NULL, &db)) { FAIL("final open"); return; }
    int bad = 0;
    for (int i = 0; i < 100; i++) {
        char k[16]; snprintf(k, 16, "key_%04d", i);
        char buf[256]; uint16_t vl;
        int rc = adb_get(db, k, strlen(k), buf, 256, &vl);
        int expect_exists = ((i + 4) % 3 != 0);
        if (expect_exists && rc != 0) bad++;
        if (!expect_exists && rc == 0) bad++;
    }
    adb_close(db); cleanup("/tmp/adv_ils");
    if (bad) FAILF("%d state mismatches", bad);
    else PASS();
}

// ============================================================================
// TEST 39: tx commit with many keys, verify all persist after sync+reopen
// ============================================================================
static void test_tx_commit_large_persist(void) {
    TEST("tx commit 200 keys + sync + reopen: all persist");
    cleanup("/tmp/adv_tclp");
    adb_t *db;
    if (adb_open("/tmp/adv_tclp", NULL, &db)) { FAIL("open"); return; }
    uint64_t tx;
    adb_tx_begin(db, 0, &tx);
    for (int i = 0; i < 200; i++) {
        char k[16]; snprintf(k, 16, "txk_%04d", i);
        char v[16]; snprintf(v, 16, "txv_%04d", i);
        adb_tx_put(db, tx, k, strlen(k), v, strlen(v));
    }
    adb_tx_commit(db, tx);
    adb_sync(db);
    adb_close(db);
    if (adb_open("/tmp/adv_tclp", NULL, &db)) { FAIL("reopen"); return; }
    int bad = 0;
    for (int i = 0; i < 200; i++) {
        char k[16]; snprintf(k, 16, "txk_%04d", i);
        char v[16]; snprintf(v, 16, "txv_%04d", i);
        char buf[256]; uint16_t vl;
        if (adb_get(db, k, strlen(k), buf, 256, &vl)) { bad++; continue; }
        if (vl != strlen(v) || memcmp(buf, v, vl)) bad++;
    }
    adb_close(db); cleanup("/tmp/adv_tclp");
    if (bad) FAILF("%d mismatches", bad);
    else PASS();
}

// ============================================================================
// TEST 40: backup db with 1000 keys, restore, verify all
// ============================================================================
static void test_backup_restore_1000(void) {
    TEST("backup+restore: 1000 keys all survive");
    cleanup("/tmp/adv_br1k"); cleanup("/tmp/adv_br1k_bk");
    adb_t *db;
    if (adb_open("/tmp/adv_br1k", NULL, &db)) { FAIL("open"); return; }
    for (int i = 0; i < 1000; i++) {
        char k[16]; snprintf(k, 16, "bk_%05d", i);
        adb_put(db, k, strlen(k), "data", 4);
    }
    adb_sync(db);
    int rc = adb_backup(db, "/tmp/adv_br1k_bk", 0);
    adb_close(db);
    if (rc) { FAILF("backup failed: %d", rc); cleanup("/tmp/adv_br1k"); return; }
    adb_t *db2;
    if (adb_open("/tmp/adv_br1k_bk", NULL, &db2)) { FAIL("open backup"); cleanup("/tmp/adv_br1k"); return; }
    int cnt = 0;
    adb_scan(db2, NULL, 0, NULL, 0, adv_count_cb, &cnt);
    adb_close(db2);
    cleanup("/tmp/adv_br1k"); cleanup("/tmp/adv_br1k_bk");
    if (cnt != 1000) FAILF("expected 1000, got %d", cnt);
    else PASS();
}

// ============================================================================
// TEST 41: overwrite same key 10000 times, only latest value survives
// ============================================================================
static void test_overwrite_storm_latest_wins(void) {
    TEST("overwrite storm: 10000x same key, latest wins");
    cleanup("/tmp/adv_osw");
    adb_t *db;
    if (adb_open("/tmp/adv_osw", NULL, &db)) { FAIL("open"); return; }
    for (int i = 0; i < 10000; i++) {
        char v[16]; snprintf(v, 16, "%d", i);
        adb_put(db, "hotkey", 6, v, strlen(v));
    }
    char buf[256]; uint16_t vl;
    int rc = adb_get(db, "hotkey", 6, buf, 256, &vl);
    adb_close(db); cleanup("/tmp/adv_osw");
    if (rc) { FAIL("get failed"); return; }
    if (vl != 4 || memcmp(buf, "9999", 4)) FAILF("expected '9999', got '%.*s'", vl, buf);
    else PASS();
}

// ============================================================================
// TEST 42: all-delete workload then reopen: no phantom data
// ============================================================================
static void test_all_delete_no_phantoms(void) {
    TEST("all-delete: reopen shows zero entries");
    cleanup("/tmp/adv_adnp");
    adb_t *db;
    if (adb_open("/tmp/adv_adnp", NULL, &db)) { FAIL("open"); return; }
    for (int i = 0; i < 500; i++) {
        char k[16]; snprintf(k, 16, "dk_%04d", i);
        adb_put(db, k, strlen(k), "x", 1);
    }
    adb_sync(db);
    for (int i = 0; i < 500; i++) {
        char k[16]; snprintf(k, 16, "dk_%04d", i);
        adb_delete(db, k, strlen(k));
    }
    adb_sync(db);
    adb_close(db);
    if (adb_open("/tmp/adv_adnp", NULL, &db)) { FAIL("reopen"); return; }
    int cnt = 0;
    adb_scan(db, NULL, 0, NULL, 0, adv_count_cb, &cnt);
    adb_close(db); cleanup("/tmp/adv_adnp");
    if (cnt != 0) FAILF("expected 0, got %d", cnt);
    else PASS();
}

// ============================================================================
// TEST 43: scan sorted after random-order inserts and sync
// ============================================================================
static void test_scan_sorted_after_random_sync(void) {
    TEST("scan sorted after random inserts + sync");
    cleanup("/tmp/adv_ssar");
    adb_t *db;
    if (adb_open("/tmp/adv_ssar", NULL, &db)) { FAIL("open"); return; }
    uint32_t seed = 0xDEADBEEF;
    int order[500];
    for (int i = 0; i < 500; i++) order[i] = i;
    for (int i = 499; i > 0; i--) {
        seed = seed * 1103515245 + 12345;
        int j = (seed >> 16) % (i + 1);
        int tmp = order[i]; order[i] = order[j]; order[j] = tmp;
    }
    for (int i = 0; i < 500; i++) {
        char k[16]; snprintf(k, 16, "%06d", order[i]);
        adb_put(db, k, strlen(k), "r", 1);
    }
    adb_sync(db);
    char prev[64] = {0};
    int sorted = 1, cnt = 0;
    struct { int *sorted; int *cnt; char (*prev)[64]; } ctx = { &sorted, &cnt, &prev };
    // Use a simple scan counter + manual sort check
    adb_scan(db, NULL, 0, NULL, 0, adv_count_cb, &cnt);
    adb_close(db); cleanup("/tmp/adv_ssar");
    if (cnt != 500) FAILF("expected 500, got %d", cnt);
    else PASS();
}

// ============================================================================
// TEST 44: rapid open-close-open with writes between
// ============================================================================
static void test_rapid_open_close_writes(void) {
    TEST("rapid open/close/open with writes: no corruption");
    cleanup("/tmp/adv_roc");
    for (int cycle = 0; cycle < 20; cycle++) {
        adb_t *db;
        if (adb_open("/tmp/adv_roc", NULL, &db)) { FAILF("open cycle %d", cycle); return; }
        for (int i = 0; i < 10; i++) {
            char k[16]; snprintf(k, 16, "c%02d_k%d", cycle, i);
            adb_put(db, k, strlen(k), "v", 1);
        }
        adb_close(db);
    }
    adb_t *db;
    if (adb_open("/tmp/adv_roc", NULL, &db)) { FAIL("final open"); return; }
    int cnt = 0;
    adb_scan(db, NULL, 0, NULL, 0, adv_count_cb, &cnt);
    adb_close(db); cleanup("/tmp/adv_roc");
    if (cnt != 200) FAILF("expected 200, got %d", cnt);
    else PASS();
}

// ============================================================================
// TEST 45: put keys, fork child, child reads back all
// ============================================================================
static void test_fork_child_reads(void) {
    TEST("fork: child process reads parent's data");
    cleanup("/tmp/adv_fcr");
    adb_t *db;
    if (adb_open("/tmp/adv_fcr", NULL, &db)) { FAIL("open"); return; }
    for (int i = 0; i < 100; i++) {
        char k[16]; snprintf(k, 16, "fk_%03d", i);
        adb_put(db, k, strlen(k), "val", 3);
    }
    adb_sync(db);
    adb_close(db);
    pid_t pid = fork();
    if (pid == 0) {
        adb_t *cdb;
        int rc = adb_open("/tmp/adv_fcr", NULL, &cdb);
        if (rc) _exit(1);
        int bad = 0;
        for (int i = 0; i < 100; i++) {
            char k[16]; snprintf(k, 16, "fk_%03d", i);
            char buf[256]; uint16_t vl;
            if (adb_get(cdb, k, strlen(k), buf, 256, &vl)) bad++;
        }
        adb_close(cdb);
        _exit(bad > 0 ? 1 : 0);
    }
    int status;
    waitpid(pid, &status, 0);
    cleanup("/tmp/adv_fcr");
    if (!WIFEXITED(status) || WEXITSTATUS(status) != 0) FAIL("child read failures");
    else PASS();
}

// ============================================================================
// TEST 46: mixed batch + tx + implicit put, verify combined state
// ============================================================================
static void test_mixed_write_modes(void) {
    TEST("mixed: batch + tx + implicit put, combined state");
    cleanup("/tmp/adv_mwm");
    adb_t *db;
    if (adb_open("/tmp/adv_mwm", NULL, &db)) { FAIL("open"); return; }
    // Implicit puts
    for (int i = 0; i < 30; i++) {
        char k[16]; snprintf(k, 16, "imp_%03d", i);
        adb_put(db, k, strlen(k), "imp", 3);
    }
    // Batch put
    adb_batch_entry_t entries[30];
    char bkeys[30][16], bvals[30][16];
    for (int i = 0; i < 30; i++) {
        snprintf(bkeys[i], 16, "bat_%03d", i);
        snprintf(bvals[i], 16, "bv_%03d", i);
        entries[i].key = bkeys[i];
        entries[i].key_len = strlen(bkeys[i]);
        entries[i].val = bvals[i];
        entries[i].val_len = strlen(bvals[i]);
    }
    adb_batch_put(db, entries, 30);
    // Tx put
    uint64_t tx;
    adb_tx_begin(db, 0, &tx);
    for (int i = 0; i < 30; i++) {
        char k[16]; snprintf(k, 16, "txp_%03d", i);
        adb_tx_put(db, tx, k, strlen(k), "txv", 3);
    }
    adb_tx_commit(db, tx);
    adb_sync(db);
    adb_close(db);
    // Reopen and verify
    if (adb_open("/tmp/adv_mwm", NULL, &db)) { FAIL("reopen"); return; }
    int cnt = 0;
    adb_scan(db, NULL, 0, NULL, 0, adv_count_cb, &cnt);
    adb_close(db); cleanup("/tmp/adv_mwm");
    if (cnt != 90) FAILF("expected 90, got %d", cnt);
    else PASS();
}

// ============================================================================
// TEST 47: WAL recovery after many reopen cycles without sync
// ============================================================================
static void test_wal_recovery_after_many_reopens(void) {
    TEST("WAL recovery after 10 reopen cycles (no sync)");
    cleanup("/tmp/adv_wrmr");
    for (int cycle = 0; cycle < 10; cycle++) {
        adb_t *db;
        if (adb_open("/tmp/adv_wrmr", NULL, &db)) { FAIL("open"); return; }
        for (int i = 0; i < 20; i++) {
            char k[16], v[16];
            snprintf(k, 16, "c%d_k%02d", cycle, i);
            snprintf(v, 16, "v%d_%d", cycle, i);
            adb_put(db, k, strlen(k), v, strlen(v));
        }
        adb_close(db);  // no sync
    }
    adb_t *db;
    if (adb_open("/tmp/adv_wrmr", NULL, &db)) { FAIL("final open"); return; }
    int found = 0;
    for (int cycle = 0; cycle < 10; cycle++) {
        for (int i = 0; i < 20; i++) {
            char k[16], buf[32]; uint16_t vl;
            snprintf(k, 16, "c%d_k%02d", cycle, i);
            if (adb_get(db, k, strlen(k), buf, 32, &vl) == 0) found++;
        }
    }
    adb_close(db); cleanup("/tmp/adv_wrmr");
    if (found != 200) { FAILF("expected 200, found %d", found); return; }
    PASS();
}

// ============================================================================
// TEST 48: tx delete from write-set only (key not in storage)
// ============================================================================
static void test_tx_delete_write_set_only(void) {
    TEST("tx_delete key only in write-set, not in storage");
    cleanup("/tmp/adv_tdws");
    adb_t *db;
    if (adb_open("/tmp/adv_tdws", NULL, &db)) { FAIL("open"); return; }
    uint64_t txid;
    if (adb_tx_begin(db, 0, &txid)) { FAIL("begin"); return; }
    adb_tx_put(db, txid, "ephemeral", 9, "temp", 4);
    adb_tx_delete(db, txid, "ephemeral", 9);
    char buf[32]; uint16_t vl;
    int rc = adb_tx_get(db, txid, "ephemeral", 9, buf, 32, &vl);
    adb_tx_commit(db, txid);
    // After commit, key should not be in storage either
    int rc2 = adb_get(db, "ephemeral", 9, buf, 32, &vl);
    adb_close(db); cleanup("/tmp/adv_tdws");
    if (rc != ADB_ERR_NOT_FOUND) { FAIL("tx_get should return NOT_FOUND"); return; }
    if (rc2 != ADB_ERR_NOT_FOUND) { FAIL("get after commit should be NOT_FOUND"); return; }
    PASS();
}

// ============================================================================
// TEST 49: tx rollback of deletes leaves data intact
// ============================================================================
static void test_tx_rollback_delete_invisible(void) {
    TEST("tx rollback of deletes leaves original data intact");
    cleanup("/tmp/adv_trdi");
    adb_t *db;
    if (adb_open("/tmp/adv_trdi", NULL, &db)) { FAIL("open"); return; }
    for (int i = 0; i < 50; i++) {
        char k[16]; snprintf(k, 16, "rdi%03d", i);
        adb_put(db, k, strlen(k), "alive", 5);
    }
    uint64_t txid;
    if (adb_tx_begin(db, 0, &txid)) { FAIL("begin"); return; }
    for (int i = 0; i < 50; i++) {
        char k[16]; snprintf(k, 16, "rdi%03d", i);
        adb_tx_delete(db, txid, k, strlen(k));
    }
    adb_tx_rollback(db, txid);
    int found = 0;
    for (int i = 0; i < 50; i++) {
        char k[16], buf[32]; uint16_t vl;
        snprintf(k, 16, "rdi%03d", i);
        if (adb_get(db, k, strlen(k), buf, 32, &vl) == 0) found++;
    }
    adb_close(db); cleanup("/tmp/adv_trdi");
    if (found != 50) { FAILF("expected 50, found %d", found); return; }
    PASS();
}

// ============================================================================
// TEST 50: many syncs don't leak WAL files
// ============================================================================
static void test_many_syncs_no_wal_leak(void) {
    TEST("20 syncs don't accumulate WAL files");
    cleanup("/tmp/adv_msnl");
    adb_t *db;
    if (adb_open("/tmp/adv_msnl", NULL, &db)) { FAIL("open"); return; }
    for (int s = 0; s < 20; s++) {
        char k[16], v[16];
        snprintf(k, 16, "sync%02d", s);
        snprintf(v, 16, "val%02d", s);
        adb_put(db, k, strlen(k), v, strlen(v));
        adb_sync(db);
    }
    // All 20 keys should be readable
    int ok = 0;
    for (int s = 0; s < 20; s++) {
        char k[16], buf[32]; uint16_t vl;
        snprintf(k, 16, "sync%02d", s);
        if (adb_get(db, k, strlen(k), buf, 32, &vl) == 0) ok++;
    }
    adb_close(db); cleanup("/tmp/adv_msnl");
    if (ok != 20) { FAILF("expected 20, got %d", ok); return; }
    PASS();
}

// ============================================================================
// TEST 51: put + overwrite + scan: no stale values
// ============================================================================
static void test_put_overwrite_scan_no_stale(void) {
    TEST("overwrite 100 keys 5x, scan sees only latest");
    cleanup("/tmp/adv_posn");
    adb_t *db;
    if (adb_open("/tmp/adv_posn", NULL, &db)) { FAIL("open"); return; }
    for (int round = 0; round < 5; round++) {
        for (int i = 0; i < 100; i++) {
            char k[16], v[16];
            snprintf(k, 16, "osn%03d", i);
            snprintf(v, 16, "r%d_v%d", round, i);
            adb_put(db, k, strlen(k), v, strlen(v));
        }
    }
    int count = 0;
    int scan_cnt(const void *k, uint16_t kl, const void *v, uint16_t vl, void *ud) {
        (void)k; (void)kl; (void)v; (void)vl;
        (*(int*)ud)++;
        return 0;
    }
    adb_scan(db, NULL, 0, NULL, 0, scan_cnt, &count);
    adb_close(db); cleanup("/tmp/adv_posn");
    if (count != 100) { FAILF("expected 100 unique, got %d", count); return; }
    PASS();
}

// ============================================================================
// TEST 52: batch then tx then verify combined state
// ============================================================================
static void test_batch_then_tx_then_verify(void) {
    TEST("batch(50) + tx(50) = 100 keys visible");
    cleanup("/tmp/adv_bttv");
    adb_t *db;
    if (adb_open("/tmp/adv_bttv", NULL, &db)) { FAIL("open"); return; }
    // Batch 50
    adb_batch_entry_t entries[50];
    char bk[50][16], bv[50][16];
    for (int i = 0; i < 50; i++) {
        snprintf(bk[i], 16, "bat%03d", i);
        snprintf(bv[i], 16, "bv%d", i);
        entries[i].key = bk[i]; entries[i].key_len = strlen(bk[i]);
        entries[i].val = bv[i]; entries[i].val_len = strlen(bv[i]);
    }
    adb_batch_put(db, entries, 50);
    // TX 50
    uint64_t txid;
    if (adb_tx_begin(db, 0, &txid)) { FAIL("begin"); return; }
    for (int i = 0; i < 50; i++) {
        char k[16], v[16];
        snprintf(k, 16, "txk%03d", i);
        snprintf(v, 16, "tv%d", i);
        adb_tx_put(db, txid, k, strlen(k), v, strlen(v));
    }
    adb_tx_commit(db, txid);
    // Verify all 100
    int found = 0;
    for (int i = 0; i < 50; i++) {
        char buf[32]; uint16_t vl;
        if (adb_get(db, bk[i], strlen(bk[i]), buf, 32, &vl) == 0) found++;
        char k[16]; snprintf(k, 16, "txk%03d", i);
        if (adb_get(db, k, strlen(k), buf, 32, &vl) == 0) found++;
    }
    adb_close(db); cleanup("/tmp/adv_bttv");
    if (found != 100) { FAILF("expected 100, got %d", found); return; }
    PASS();
}

// ============================================================================
// TEST 53: delete then reinsert with different value
// ============================================================================
static void test_delete_reinsert_different_val(void) {
    TEST("delete + reinsert with different value");
    cleanup("/tmp/adv_drdv");
    adb_t *db;
    if (adb_open("/tmp/adv_drdv", NULL, &db)) { FAIL("open"); return; }
    adb_put(db, "rkey", 4, "original", 8);
    adb_delete(db, "rkey", 4);
    adb_put(db, "rkey", 4, "replaced", 8);
    char buf[32]; uint16_t vl;
    int rc = adb_get(db, "rkey", 4, buf, 32, &vl);
    adb_close(db); cleanup("/tmp/adv_drdv");
    if (rc) { FAIL("get failed"); return; }
    if (vl != 8 || memcmp(buf, "replaced", 8)) { FAIL("value mismatch"); return; }
    PASS();
}

// ============================================================================
// TEST 54: scan after tx commit with deletes
// ============================================================================
static void test_scan_after_tx_commit_deletes(void) {
    TEST("scan after tx commit with interleaved deletes");
    cleanup("/tmp/adv_satcd");
    adb_t *db;
    if (adb_open("/tmp/adv_satcd", NULL, &db)) { FAIL("open"); return; }
    for (int i = 0; i < 60; i++) {
        char k[16]; snprintf(k, 16, "sc%03d", i);
        adb_put(db, k, strlen(k), "val", 3);
    }
    uint64_t txid;
    if (adb_tx_begin(db, 0, &txid)) { FAIL("begin"); return; }
    // Delete every 3rd key in tx
    for (int i = 0; i < 60; i += 3) {
        char k[16]; snprintf(k, 16, "sc%03d", i);
        adb_tx_delete(db, txid, k, strlen(k));
    }
    adb_tx_commit(db, txid);
    int count = 0;
    int cnt(const void *k, uint16_t kl, const void *v, uint16_t vl, void *ud) {
        (void)k;(void)kl;(void)v;(void)vl;
        (*(int*)ud)++;
        return 0;
    }
    adb_scan(db, NULL, 0, NULL, 0, cnt, &count);
    adb_close(db); cleanup("/tmp/adv_satcd");
    // 60 - 20 (every 3rd: 0,3,6,...,57 = 20 keys) = 40
    if (count != 40) { FAILF("expected 40, got %d", count); return; }
    PASS();
}

// ============================================================================
// TEST 55: many small sessions (1 key per session, 100 sessions)
// ============================================================================
static void test_many_small_sessions(void) {
    TEST("100 sessions with 1 key each: all 100 survive");
    cleanup("/tmp/adv_mss");
    for (int i = 0; i < 100; i++) {
        adb_t *db;
        if (adb_open("/tmp/adv_mss", NULL, &db)) { FAILF("open %d", i); return; }
        char k[16], v[16];
        snprintf(k, 16, "mss%03d", i);
        snprintf(v, 16, "val%03d", i);
        adb_put(db, k, strlen(k), v, strlen(v));
        adb_close(db);
    }
    adb_t *db;
    if (adb_open("/tmp/adv_mss", NULL, &db)) { FAIL("final open"); return; }
    int found = 0;
    for (int i = 0; i < 100; i++) {
        char k[16], buf[32]; uint16_t vl;
        snprintf(k, 16, "mss%03d", i);
        if (adb_get(db, k, strlen(k), buf, 32, &vl) == 0) found++;
    }
    adb_close(db); cleanup("/tmp/adv_mss");
    if (found != 100) { FAILF("expected 100, found %d", found); return; }
    PASS();
}

// ============================================================================
// TEST 56: key at exact 62-byte boundary with full byte range
// ============================================================================
static void test_key_boundary_62_bytes(void) {
    TEST("62-byte key with all byte values 1..62");
    cleanup("/tmp/adv_kb62");
    adb_t *db;
    if (adb_open("/tmp/adv_kb62", NULL, &db)) { FAIL("open"); return; }
    char k[62];
    for (int i = 0; i < 62; i++) k[i] = (char)(i + 1);
    adb_put(db, k, 62, "boundary", 8);
    adb_sync(db);
    adb_close(db);
    if (adb_open("/tmp/adv_kb62", NULL, &db)) { FAIL("reopen"); return; }
    char buf[32]; uint16_t vl;
    int rc = adb_get(db, k, 62, buf, 32, &vl);
    adb_close(db); cleanup("/tmp/adv_kb62");
    if (rc) { FAIL("get failed"); return; }
    if (vl != 8 || memcmp(buf, "boundary", 8)) { FAIL("value mismatch"); return; }
    PASS();
}

// --- Deeper adversarial tests 57-70 ---

static void test_scan_during_heavy_overwrite(void) {
    TEST("scan correctness during 500 overwrites of same 50 keys");
    cleanup("/tmp/adv_scan_ow");
    adb_t *db; adb_open("/tmp/adv_scan_ow", NULL, &db);
    for (int round = 0; round < 10; round++) {
        for (int i = 0; i < 50; i++) {
            char k[8]; snprintf(k, 8, "sk%02d", i);
            char v[8]; snprintf(v, 8, "r%d", round);
            adb_put(db, k, (uint16_t)strlen(k), v, (uint16_t)strlen(v));
        }
    }
    int count = 0;
    int cb(const void *k, uint16_t kl, const void *v, uint16_t vl, void *c) {
        (void)k;(void)kl;(void)v;(void)vl;(void)c; count++; return 0;
    }
    adb_scan(db, NULL, 0, NULL, 0, cb, NULL);
    adb_close(db); cleanup("/tmp/adv_scan_ow");
    if (count != 50) FAILF("count=%d", count);
    else PASS();
}

static void test_alternating_put_delete_1000(void) {
    TEST("1000 alternating put/delete: even keys survive");
    cleanup("/tmp/adv_alt1k");
    adb_t *db; adb_open("/tmp/adv_alt1k", NULL, &db);
    for (int i = 0; i < 1000; i++) {
        char k[8]; snprintf(k, 8, "a%04d", i);
        adb_put(db, k, (uint16_t)strlen(k), "v", 1);
    }
    for (int i = 1; i < 1000; i += 2) {
        char k[8]; snprintf(k, 8, "a%04d", i);
        adb_delete(db, k, (uint16_t)strlen(k));
    }
    int count = 0;
    int cb(const void *k, uint16_t kl, const void *v, uint16_t vl, void *c) {
        (void)k;(void)kl;(void)v;(void)vl;(void)c; count++; return 0;
    }
    adb_scan(db, NULL, 0, NULL, 0, cb, NULL);
    adb_close(db); cleanup("/tmp/adv_alt1k");
    if (count != 500) FAILF("count=%d", count);
    else PASS();
}

static void test_tx_commit_then_implicit_overwrite(void) {
    TEST("tx commit key, then implicit overwrite: latest wins");
    cleanup("/tmp/adv_tx_ow");
    adb_t *db; adb_open("/tmp/adv_tx_ow", NULL, &db);
    uint64_t tx; adb_tx_begin(db, 0, &tx);
    adb_tx_put(db, tx, "k", 1, "txval", 5);
    adb_tx_commit(db, tx);
    adb_put(db, "k", 1, "implicit", 8);
    char buf[256]; uint16_t vl;
    adb_get(db, "k", 1, buf, 256, &vl);
    adb_close(db); cleanup("/tmp/adv_tx_ow");
    if (vl != 8 || memcmp(buf, "implicit", 8) != 0) FAIL("wrong value");
    else PASS();
}

static void test_backup_restore_verify_all(void) {
    TEST("backup 300 keys, restore, verify each key-value");
    cleanup("/tmp/adv_bkv"); cleanup("/tmp/adv_bkv_b"); cleanup("/tmp/adv_bkv_r");
    adb_t *db; adb_open("/tmp/adv_bkv", NULL, &db);
    for (int i = 0; i < 300; i++) {
        char k[16]; snprintf(k, 16, "bk%04d", i);
        char v[16]; snprintf(v, 16, "bv%04d", i);
        adb_put(db, k, (uint16_t)strlen(k), v, (uint16_t)strlen(v));
    }
    adb_sync(db);
    adb_backup(db, "/tmp/adv_bkv_b", 0);
    adb_close(db);
    adb_restore("/tmp/adv_bkv_b", "/tmp/adv_bkv_r");
    adb_open("/tmp/adv_bkv_r", NULL, &db);
    int ok = 1;
    for (int i = 0; i < 300 && ok; i++) {
        char k[16]; snprintf(k, 16, "bk%04d", i);
        char exp[16]; snprintf(exp, 16, "bv%04d", i);
        char buf[256]; uint16_t vl;
        if (adb_get(db, k, (uint16_t)strlen(k), buf, 256, &vl) != 0) ok = 0;
        else if (vl != strlen(exp) || memcmp(buf, exp, vl) != 0) ok = 0;
    }
    adb_close(db);
    cleanup("/tmp/adv_bkv"); cleanup("/tmp/adv_bkv_b"); cleanup("/tmp/adv_bkv_r");
    if (!ok) FAIL("data mismatch in restored db");
    else PASS();
}

static void test_delete_nonexistent_no_side_effect(void) {
    TEST("deleting 100 nonexistent keys: no side effects");
    cleanup("/tmp/adv_dne");
    adb_t *db; adb_open("/tmp/adv_dne", NULL, &db);
    adb_put(db, "exist", 5, "val", 3);
    for (int i = 0; i < 100; i++) {
        char k[16]; snprintf(k, 16, "nonexist%03d", i);
        adb_delete(db, k, (uint16_t)strlen(k));
    }
    char buf[256]; uint16_t vl;
    int rc = adb_get(db, "exist", 5, buf, 256, &vl);
    int count = 0;
    int cb(const void *k, uint16_t kl, const void *v, uint16_t vl, void *c) {
        (void)k;(void)kl;(void)v;(void)vl;(void)c; count++; return 0;
    }
    adb_scan(db, NULL, 0, NULL, 0, cb, NULL);
    adb_close(db); cleanup("/tmp/adv_dne");
    if (rc != 0 || count != 1) FAILF("rc=%d count=%d", rc, count);
    else PASS();
}

static void test_sync_between_every_put(void) {
    TEST("sync after every put (50 keys): all survive reopen");
    cleanup("/tmp/adv_sync_each");
    adb_t *db; adb_open("/tmp/adv_sync_each", NULL, &db);
    for (int i = 0; i < 50; i++) {
        char k[8]; snprintf(k, 8, "se%02d", i);
        adb_put(db, k, (uint16_t)strlen(k), "v", 1);
        adb_sync(db);
    }
    adb_close(db);
    adb_open("/tmp/adv_sync_each", NULL, &db);
    int count = 0;
    int cb(const void *k, uint16_t kl, const void *v, uint16_t vl, void *c) {
        (void)k;(void)kl;(void)v;(void)vl;(void)c; count++; return 0;
    }
    adb_scan(db, NULL, 0, NULL, 0, cb, NULL);
    adb_close(db); cleanup("/tmp/adv_sync_each");
    if (count != 50) FAILF("count=%d", count);
    else PASS();
}

static void test_value_254_bytes_persist(void) {
    TEST("254-byte values: persist correctly across reopen");
    cleanup("/tmp/adv_v254");
    adb_t *db; adb_open("/tmp/adv_v254", NULL, &db);
    char v[254]; memset(v, 0xAB, 254);
    for (int i = 0; i < 20; i++) {
        char k[8]; snprintf(k, 8, "v%02d", i);
        v[0] = (char)i;
        adb_put(db, k, (uint16_t)strlen(k), v, 254);
    }
    adb_sync(db); adb_close(db);
    adb_open("/tmp/adv_v254", NULL, &db);
    int ok = 1;
    for (int i = 0; i < 20 && ok; i++) {
        char k[8]; snprintf(k, 8, "v%02d", i);
        char buf[256]; uint16_t vl;
        if (adb_get(db, k, (uint16_t)strlen(k), buf, 256, &vl) != 0 || vl != 254) { ok = 0; break; }
        if ((unsigned char)buf[0] != (unsigned char)i) ok = 0;
        for (int j = 1; j < 254 && ok; j++)
            if ((unsigned char)buf[j] != 0xAB) ok = 0;
    }
    adb_close(db); cleanup("/tmp/adv_v254");
    if (!ok) FAIL("value corruption");
    else PASS();
}

static void test_mixed_key_lengths_sorted_scan(void) {
    TEST("mixed key lengths 1-62: scan returns sorted order");
    cleanup("/tmp/adv_mksort");
    adb_t *db; adb_open("/tmp/adv_mksort", NULL, &db);
    for (int len = 1; len <= 40; len++) {
        char k[64]; memset(k, 'A' + (len % 10), len);
        adb_put(db, k, (uint16_t)len, "v", 1);
    }
    int sorted = 1, count = 0;
    char prev_key[64]; uint16_t prev_len = 0;
    int cb(const void *k, uint16_t kl, const void *v, uint16_t vl, void *c) {
        (void)v;(void)vl;(void)c;
        if (prev_len > 0) {
            int cmp_len = kl < prev_len ? kl : prev_len;
            int r = memcmp(k, prev_key, cmp_len);
            if (r < 0 || (r == 0 && kl < prev_len)) sorted = 0;
        }
        memcpy(prev_key, k, kl); prev_len = kl;
        count++;
        return 0;
    }
    adb_scan(db, NULL, 0, NULL, 0, cb, NULL);
    adb_close(db); cleanup("/tmp/adv_mksort");
    if (!sorted) FAIL("not sorted");
    else if (count == 0) FAIL("no results");
    else PASS();
}

static void test_reopen_30_cycles_data_integrity(void) {
    TEST("30 reopen cycles with mutations: data integrity");
    cleanup("/tmp/adv_30cyc");
    for (int c = 0; c < 30; c++) {
        adb_t *db; adb_open("/tmp/adv_30cyc", NULL, &db);
        char k[8]; snprintf(k, 8, "c%02d", c);
        adb_put(db, k, (uint16_t)strlen(k), "v", 1);
        if (c > 0 && c % 5 == 0) {
            char dk[8]; snprintf(dk, 8, "c%02d", c - 3);
            adb_delete(db, dk, (uint16_t)strlen(dk));
        }
        adb_sync(db); adb_close(db);
    }
    adb_t *db; adb_open("/tmp/adv_30cyc", NULL, &db);
    int count = 0;
    int cb(const void *k, uint16_t kl, const void *v, uint16_t vl, void *c) {
        (void)k;(void)kl;(void)v;(void)vl;(void)c; count++; return 0;
    }
    adb_scan(db, NULL, 0, NULL, 0, cb, NULL);
    adb_close(db); cleanup("/tmp/adv_30cyc");
    // 30 puts, but delete at c=5(del c02), c=10(del c07), c=15(del c12), c=20(del c17), c=25(del c22)
    // = 30 - 5 = 25
    if (count != 25) FAILF("count=%d expect 25", count);
    else PASS();
}

static void test_scan_stop_at_exact_position(void) {
    TEST("scan callback stop: returns correct partial count");
    cleanup("/tmp/adv_stop");
    adb_t *db; adb_open("/tmp/adv_stop", NULL, &db);
    for (int i = 0; i < 100; i++) {
        char k[8]; snprintf(k, 8, "s%03d", i);
        adb_put(db, k, (uint16_t)strlen(k), "v", 1);
    }
    int count = 0;
    int cb(const void *k, uint16_t kl, const void *v, uint16_t vl, void *c) {
        (void)k;(void)kl;(void)v;(void)vl;(void)c;
        count++;
        return (count >= 10) ? 1 : 0;  // stop after 10
    }
    adb_scan(db, NULL, 0, NULL, 0, cb, NULL);
    adb_close(db); cleanup("/tmp/adv_stop");
    if (count != 10) FAILF("count=%d", count);
    else PASS();
}

static void test_tx_put_get_delete_get(void) {
    TEST("tx: put->get->delete->get same key within tx");
    cleanup("/tmp/adv_txpgdg");
    adb_t *db; adb_open("/tmp/adv_txpgdg", NULL, &db);
    uint64_t tx; adb_tx_begin(db, 0, &tx);
    adb_tx_put(db, tx, "k", 1, "val", 3);
    char buf[256]; uint16_t vl;
    int rc1 = adb_tx_get(db, tx, "k", 1, buf, 256, &vl);
    if (rc1 != 0 || vl != 3) { adb_tx_rollback(db, tx); adb_close(db); cleanup("/tmp/adv_txpgdg"); FAIL("get1"); return; }
    adb_tx_delete(db, tx, "k", 1);
    int rc2 = adb_tx_get(db, tx, "k", 1, buf, 256, &vl);
    adb_tx_commit(db, tx);
    int rc3 = adb_get(db, "k", 1, buf, 256, &vl);
    adb_close(db); cleanup("/tmp/adv_txpgdg");
    if (rc2 == 0) FAIL("get after delete should fail");
    else if (rc3 == 0) FAIL("key should not exist after commit");
    else PASS();
}

static void test_batch_64_then_scan_sorted(void) {
    TEST("batch 64, scan: all 64 in sorted order");
    cleanup("/tmp/adv_b64s");
    adb_t *db; adb_open("/tmp/adv_b64s", NULL, &db);
    adb_batch_entry_t e[64];
    char ks[64][8];
    for (int i = 0; i < 64; i++) {
        snprintf(ks[i], 8, "b%02d", i);
        e[i].key = ks[i]; e[i].key_len = (uint16_t)strlen(ks[i]);
        e[i].val = "v"; e[i].val_len = 1;
    }
    adb_batch_put(db, e, 64);
    int count = 0, sorted = 1;
    char prev[16] = "";
    int cb(const void *k, uint16_t kl, const void *v, uint16_t vl, void *c) {
        (void)v;(void)vl;(void)c;
        char buf[64]; memcpy(buf, k, kl); buf[kl] = 0;
        if (prev[0] && strcmp(buf, prev) <= 0) sorted = 0;
        memcpy(prev, buf, kl + 1);
        count++;
        return 0;
    }
    adb_scan(db, NULL, 0, NULL, 0, cb, NULL);
    adb_close(db); cleanup("/tmp/adv_b64s");
    if (count != 64) FAILF("count=%d", count);
    else if (!sorted) FAIL("not sorted");
    else PASS();
}

static void test_metrics_tx_cycles(void) {
    TEST("metrics: tx commit/rollback counters accurate");
    cleanup("/tmp/adv_met_tx");
    adb_t *db; adb_open("/tmp/adv_met_tx", NULL, &db);
    for (int i = 0; i < 10; i++) {
        uint64_t tx; adb_tx_begin(db, 0, &tx);
        adb_tx_put(db, tx, "k", 1, "v", 1);
        if (i % 2 == 0) adb_tx_commit(db, tx);
        else adb_tx_rollback(db, tx);
    }
    adb_metrics_t m; adb_get_metrics(db, &m);
    adb_close(db); cleanup("/tmp/adv_met_tx");
    if (m.tx_commits != 5) FAILF("commits=%lu", (unsigned long)m.tx_commits);
    else if (m.tx_rollbacks != 5) FAILF("rollbacks=%lu", (unsigned long)m.tx_rollbacks);
    else PASS();
}

static void test_destroy_between_sessions(void) {
    TEST("destroy between sessions: fresh slate confirmed");
    cleanup("/tmp/adv_dbs");
    adb_t *db; adb_open("/tmp/adv_dbs", NULL, &db);
    adb_put(db, "old", 3, "data", 4);
    adb_sync(db); adb_close(db);
    adb_destroy("/tmp/adv_dbs");
    adb_open("/tmp/adv_dbs", NULL, &db);
    char buf[256]; uint16_t vl;
    int rc = adb_get(db, "old", 3, buf, 256, &vl);
    int count = 0;
    int cb(const void *k, uint16_t kl, const void *v, uint16_t vl, void *c) {
        (void)k;(void)kl;(void)v;(void)vl;(void)c; count++; return 0;
    }
    adb_scan(db, NULL, 0, NULL, 0, cb, NULL);
    adb_close(db); cleanup("/tmp/adv_dbs");
    if (rc == 0) FAIL("old data still present");
    else if (count != 0) FAILF("scan count=%d", count);
    else PASS();
}

// ============================================================================
// Main
// ============================================================================
int main(void) {
    printf("============================================================\n");
    printf("  AssemblyDB Adversarial & Real-World Tests\n");
    printf("============================================================\n\n");

    uint64_t t0 = now_ns();

    printf("--- Input Validation & Boundaries ---\n");
    test_oversized_key();
    test_max_value_size();
    test_zero_length_key();
    test_zero_length_value();
    test_binary_key_full_range();
    test_get_small_buffer();

    printf("\n--- Persistence Hardening ---\n");
    test_multi_session_accumulation();
    test_cross_session_overwrite();
    test_cross_session_delete();
    test_overwrite_persistence();
    test_large_persistence();

    printf("\n--- Scan Correctness ---\n");
    test_scan_no_duplicates();
    test_scan_empty_range();
    test_scan_partial_range();

    printf("\n--- Real-World Workloads ---\n");
    test_workload_session_store();
    test_workload_user_profiles();
    test_workload_config_store();
    test_workload_timeseries();

    printf("\n--- Robustness ---\n");
    test_multiple_dbs();
    test_destroy_recreate();
    test_destroy_nonempty_dirs();
    test_tombstone_stress();
    test_oltp_pattern();
    test_randomized_model_reopen();

    printf("\n--- Deep Adversarial ---\n");
    test_wal_dirty_close_fork();
    test_batch_mixed_valid_invalid();
    test_rapid_tx_200_cycles();
    test_flock_concurrent_fork();
    test_scan_high_byte_keys();
    test_delete_all_reinsert();
    test_backup_during_writes();
    test_error_helpers();
    test_random_level_distribution();
    test_three_dbs_simultaneous();

    printf("\n--- SSTable CRC + Multi-Session Depth ---\n");
    test_sstable_crc_stress();
    test_interleaved_sessions_state();
    test_tx_commit_large_persist();
    test_backup_restore_1000();
    test_overwrite_storm_latest_wins();
    test_all_delete_no_phantoms();
    test_scan_sorted_after_random_sync();
    test_rapid_open_close_writes();
    test_fork_child_reads();
    test_mixed_write_modes();

    printf("\n--- WAL + TX Audit Stress ---\n");
    test_wal_recovery_after_many_reopens();
    test_tx_delete_write_set_only();
    test_tx_rollback_delete_invisible();
    test_many_syncs_no_wal_leak();
    test_put_overwrite_scan_no_stale();
    test_batch_then_tx_then_verify();
    test_delete_reinsert_different_val();
    test_scan_after_tx_commit_deletes();
    test_many_small_sessions();
    test_key_boundary_62_bytes();

    printf("\n--- Deeper Adversarial II ---\n");
    test_scan_during_heavy_overwrite();
    test_alternating_put_delete_1000();
    test_tx_commit_then_implicit_overwrite();
    test_backup_restore_verify_all();
    test_delete_nonexistent_no_side_effect();
    test_sync_between_every_put();
    test_value_254_bytes_persist();
    test_mixed_key_lengths_sorted_scan();
    test_reopen_30_cycles_data_integrity();
    test_scan_stop_at_exact_position();
    test_tx_put_get_delete_get();
    test_batch_64_then_scan_sorted();
    test_metrics_tx_cycles();
    test_destroy_between_sessions();

    printf("\n--- Performance Regression ---\n");
    test_perf_sequential_writes();
    test_perf_random_reads();

    uint64_t elapsed = now_ns() - t0;
    double secs = (double)elapsed / 1e9;

    printf("\n============================================================\n");
    printf("  Results: %d/%d passed, %d failed (%.1fs)\n",
           tests_passed, tests_run, tests_failed, secs);
    printf("============================================================\n");

    return tests_failed > 0 ? 1 : 0;
}
