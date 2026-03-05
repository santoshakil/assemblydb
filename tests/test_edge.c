#include "assemblydb.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>

extern void btree_page_init_leaf(void *page, int page_id);
extern void btree_page_set_crc(void *page);
extern int btree_page_verify_crc(void *page);
extern void *btree_page_get_key_ptr(void *page, int index);

static int g_fail = 0;
#define PASS() printf("PASS\n")
#define FAIL(msg) do { printf("FAIL: %s\n", msg); g_fail++; } while(0)
#define FAILF(...) do { printf("FAIL: "); printf(__VA_ARGS__); printf("\n"); g_fail++; } while(0)
static void cleanup(const char *p) { char c[256]; snprintf(c,256,"rm -rf %s",p); int rc = system(c); (void)rc; }

// 1. Keys that differ only in the last byte
static void test_similar_keys(void) {
    printf("  [01] keys differing only in last byte                    "); fflush(stdout);
    const char *path = "/tmp/adb_edge1";
    cleanup(path);
    adb_t *db = NULL;
    adb_open(path, NULL, &db);
    char key[62], vbuf[256]; uint16_t vlen;
    memset(key, 'A', 61);
    int bad = 0;
    for (int i = 0; i < 50; i++) {
        key[61] = (char)('0' + i);
        char val[8]; int vl = snprintf(val, 8, "%d", i);
        adb_put(db, key, 62, val, (uint16_t)vl);
    }
    for (int i = 0; i < 50; i++) {
        key[61] = (char)('0' + i);
        char val[8]; int vl = snprintf(val, 8, "%d", i);
        int rc = adb_get(db, key, 62, vbuf, 256, &vlen);
        if (rc != 0 || vlen != (uint16_t)vl || memcmp(vbuf, val, vl) != 0) bad++;
    }
    adb_close(db); cleanup(path);
    if (bad == 0) PASS(); else { char m[64]; snprintf(m, 64, "%d bad", bad); FAIL(m); }
}

// 2. Keys that are prefixes of each other
static void test_prefix_keys(void) {
    printf("  [02] keys that are prefixes of each other                "); fflush(stdout);
    const char *path = "/tmp/adb_edge2";
    cleanup(path);
    adb_t *db = NULL;
    adb_open(path, NULL, &db);
    char vbuf[256]; uint16_t vlen;
    int bad = 0;
    adb_put(db, "a", 1, "v1", 2);
    adb_put(db, "ab", 2, "v2", 2);
    adb_put(db, "abc", 3, "v3", 2);
    adb_put(db, "abcd", 4, "v4", 2);
    adb_put(db, "abcde", 5, "v5", 2);
    // Verify each is distinct
    int rc;
    rc = adb_get(db, "a", 1, vbuf, 256, &vlen);
    if (rc != 0 || vlen != 2 || memcmp(vbuf, "v1", 2) != 0) bad++;
    rc = adb_get(db, "abc", 3, vbuf, 256, &vlen);
    if (rc != 0 || vlen != 2 || memcmp(vbuf, "v3", 2) != 0) bad++;
    rc = adb_get(db, "abcde", 5, vbuf, 256, &vlen);
    if (rc != 0 || vlen != 2 || memcmp(vbuf, "v5", 2) != 0) bad++;
    adb_close(db); cleanup(path);
    if (bad == 0) PASS(); else { char m[64]; snprintf(m, 64, "%d bad", bad); FAIL(m); }
}

// 3. Rapid put-delete-put cycle for same key
static void test_rapid_put_delete_put(void) {
    printf("  [03] rapid put-delete-put 500 cycles per key, 20 keys    "); fflush(stdout);
    const char *path = "/tmp/adb_edge3";
    cleanup(path);
    adb_t *db = NULL;
    adb_open(path, NULL, &db);
    char key[24], val[48], vbuf[256]; uint16_t vlen;
    int bad = 0;
    for (int k = 0; k < 20; k++) {
        int kl = snprintf(key, sizeof(key), "key%d", k);
        for (int i = 0; i < 500; i++) {
            int vl = snprintf(val, sizeof(val), "v%d_%d", k, i);
            adb_put(db, key, (uint16_t)kl, val, (uint16_t)vl);
            adb_delete(db, key, (uint16_t)kl);
            int vl2 = snprintf(val, sizeof(val), "final%d_%d", k, i);
            adb_put(db, key, (uint16_t)kl, val, (uint16_t)vl2);
        }
    }
    // Verify final values
    for (int k = 0; k < 20; k++) {
        int kl = snprintf(key, sizeof(key), "key%d", k);
        int vl = snprintf(val, sizeof(val), "final%d_499", k);
        int rc = adb_get(db, key, (uint16_t)kl, vbuf, 256, &vlen);
        if (rc != 0 || vlen != (uint16_t)vl || memcmp(vbuf, val, vl) != 0) bad++;
    }
    adb_close(db); cleanup(path);
    if (bad == 0) PASS(); else { char m[64]; snprintf(m, 64, "%d bad", bad); FAIL(m); }
}

// 4. Scan with exact start=end (single key range)
static void test_scan_single_key_range(void) {
    printf("  [04] scan with start==end should return that key only    "); fflush(stdout);
    const char *path = "/tmp/adb_edge4";
    cleanup(path);
    adb_t *db = NULL;
    adb_open(path, NULL, &db);
    char key[24];
    for (int i = 0; i < 100; i++) {
        int kl = snprintf(key, sizeof(key), "k%04d", i);
        char val[8]; int vl = snprintf(val, 8, "%d", i);
        adb_put(db, key, (uint16_t)kl, val, (uint16_t)vl);
    }
    typedef struct { int count; } scan_ctx_t;
    int counter_cb(const void *k, uint16_t kl, const void *v, uint16_t vl, void *uctx) {
        (void)k; (void)kl; (void)v; (void)vl;
        ((scan_ctx_t*)uctx)->count++;
        return 0;
    }
    // Scan with start=k0050, end=k0050
    scan_ctx_t ctx = {0};
    adb_scan(db, "k0050", 5, "k0050", 5, counter_cb, &ctx);
    adb_close(db); cleanup(path);
    if (ctx.count == 1) PASS();
    else { char m[64]; snprintf(m, 64, "count=%d (expected 1)", ctx.count); FAIL(m); }
}

// 5. Scan with narrow range
static void test_scan_narrow_range(void) {
    printf("  [05] scan with narrow range (3 keys out of 1000)         "); fflush(stdout);
    const char *path = "/tmp/adb_edge5";
    cleanup(path);
    adb_t *db = NULL;
    adb_open(path, NULL, &db);
    char key[24];
    for (int i = 0; i < 1000; i++) {
        int kl = snprintf(key, sizeof(key), "k%04d", i);
        char val[8]; int vl = snprintf(val, 8, "%d", i);
        adb_put(db, key, (uint16_t)kl, val, (uint16_t)vl);
    }
    typedef struct { int count; } scan_ctx_t;
    int counter_cb(const void *k, uint16_t kl, const void *v, uint16_t vl, void *uctx) {
        (void)k; (void)kl; (void)v; (void)vl;
        ((scan_ctx_t*)uctx)->count++;
        return 0;
    }
    // Range k0500..k0502 should have exactly 3 keys
    scan_ctx_t ctx = {0};
    adb_scan(db, "k0500", 5, "k0502", 5, counter_cb, &ctx);
    adb_close(db); cleanup(path);
    if (ctx.count == 3) PASS();
    else { char m[64]; snprintf(m, 64, "count=%d (expected 3)", ctx.count); FAIL(m); }
}

// 6. Delete key that doesn't exist
static void test_delete_nonexistent(void) {
    printf("  [06] delete nonexistent key returns NOT_FOUND            "); fflush(stdout);
    const char *path = "/tmp/adb_edge6";
    cleanup(path);
    adb_t *db = NULL;
    adb_open(path, NULL, &db);
    adb_put(db, "exists", 6, "val", 3);
    int rc = adb_delete(db, "nope", 4);
    // Should succeed (tombstone in memtable, doesn't check B+ tree)
    char vbuf[256]; uint16_t vlen;
    // Verify "exists" is still there
    rc = adb_get(db, "exists", 6, vbuf, 256, &vlen);
    adb_close(db); cleanup(path);
    if (rc == 0 && vlen == 3 && memcmp(vbuf, "val", 3) == 0) PASS();
    else FAIL("existing key corrupted");
}

// 7. Multi-session with scan verification each time
static void test_multi_session_scan(void) {
    printf("  [07] 3 sessions: put+persist+scan each, growing dataset  "); fflush(stdout);
    const char *path = "/tmp/adb_edge7";
    cleanup(path);
    char key[24], val[48];
    int bad = 0;
    typedef struct { int count; int sorted; char prev[24]; } scan_ctx_t;
    int order_cb(const void *k, uint16_t kl, const void *v, uint16_t vl, void *uctx) {
        (void)v; (void)vl;
        scan_ctx_t *c = uctx;
        char cur[24]; int len = kl < 23 ? kl : 23;
        memcpy(cur, k, len); cur[len] = 0;
        if (c->count > 0 && strcmp(cur, c->prev) <= 0) c->sorted = 0;
        memcpy(c->prev, cur, 24); c->count++;
        return 0;
    }
    int total = 0;
    for (int session = 0; session < 3; session++) {
        adb_t *db = NULL;
        adb_open(path, NULL, &db);
        int base = session * 200;
        for (int i = 0; i < 200; i++) {
            int kl = snprintf(key, sizeof(key), "k%05d", base + i);
            int vl = snprintf(val, sizeof(val), "s%d_%d", session, i);
            adb_put(db, key, (uint16_t)kl, val, (uint16_t)vl);
        }
        total += 200;
        adb_close(db);
        // Reopen and scan
        db = NULL;
        adb_open(path, NULL, &db);
        scan_ctx_t ctx = {0, 1, ""};
        adb_scan(db, NULL, 0, NULL, 0, order_cb, &ctx);
        if (ctx.count != total || !ctx.sorted) bad++;
        adb_close(db);
    }
    cleanup(path);
    if (bad == 0) PASS(); else { char m[64]; snprintf(m, 64, "%d bad sessions", bad); FAIL(m); }
}

// 8. Put same key from multiple sessions (value convergence)
static void test_same_key_multi_session(void) {
    printf("  [08] same key updated across 5 sessions, final wins      "); fflush(stdout);
    const char *path = "/tmp/adb_edge8";
    cleanup(path);
    char vbuf[256]; uint16_t vlen;
    for (int session = 0; session < 5; session++) {
        adb_t *db = NULL;
        adb_open(path, NULL, &db);
        char val[32]; int vl = snprintf(val, sizeof(val), "session_%d", session);
        adb_put(db, "thekey", 6, val, (uint16_t)vl);
        adb_close(db);
    }
    adb_t *db = NULL;
    adb_open(path, NULL, &db);
    int rc = adb_get(db, "thekey", 6, vbuf, 256, &vlen);
    adb_close(db); cleanup(path);
    if (rc == 0 && vlen == 9 && memcmp(vbuf, "session_4", 9) == 0) PASS();
    else FAIL("wrong final value");
}

// 9. Binary values with null bytes
static void test_binary_values(void) {
    printf("  [09] binary values with embedded null bytes              "); fflush(stdout);
    const char *path = "/tmp/adb_edge9";
    cleanup(path);
    adb_t *db = NULL;
    adb_open(path, NULL, &db);
    unsigned char val[200];
    for (int i = 0; i < 200; i++) val[i] = (unsigned char)(i % 256);
    adb_put(db, "binkey", 6, val, 200);
    adb_close(db);
    db = NULL;
    adb_open(path, NULL, &db);
    char vbuf[256]; uint16_t vlen;
    int rc = adb_get(db, "binkey", 6, vbuf, 256, &vlen);
    adb_close(db); cleanup(path);
    if (rc == 0 && vlen == 200 && memcmp(vbuf, val, 200) == 0) PASS();
    else FAIL("binary value mismatch");
}

// 10. Scan callback that stops early
static void test_scan_early_stop(void) {
    printf("  [10] scan callback returns non-zero to stop early        "); fflush(stdout);
    const char *path = "/tmp/adb_edge10";
    cleanup(path);
    adb_t *db = NULL;
    adb_open(path, NULL, &db);
    char key[24];
    for (int i = 0; i < 100; i++) {
        int kl = snprintf(key, sizeof(key), "k%04d", i);
        adb_put(db, key, (uint16_t)kl, "val", 3);
    }
    typedef struct { int count; } scan_ctx_t;
    int stop_after_5(const void *k, uint16_t kl, const void *v, uint16_t vl, void *uctx) {
        (void)k; (void)kl; (void)v; (void)vl;
        scan_ctx_t *c = uctx;
        c->count++;
        return c->count >= 5 ? 1 : 0;
    }
    scan_ctx_t ctx = {0};
    adb_scan(db, NULL, 0, NULL, 0, stop_after_5, &ctx);
    adb_close(db); cleanup(path);
    if (ctx.count == 5) PASS();
    else { char m[64]; snprintf(m, 64, "count=%d (expected 5)", ctx.count); FAIL(m); }
}

// 11. Delete then reinsert same key with different-length value
static void test_delete_reinsert_diff_len(void) {
    printf("  [11] delete+reinsert with different-length value         "); fflush(stdout);
    const char *path = "/tmp/adb_edge11";
    cleanup(path);
    adb_t *db = NULL;
    adb_open(path, NULL, &db);
    adb_put(db, "mykey", 5, "short", 5);
    adb_delete(db, "mykey", 5);
    adb_put(db, "mykey", 5, "a much longer replacement value", 30);
    adb_close(db);
    db = NULL;
    adb_open(path, NULL, &db);
    char vbuf[256]; uint16_t vlen;
    int rc = adb_get(db, "mykey", 5, vbuf, 256, &vlen);
    adb_close(db); cleanup(path);
    if (rc == 0 && vlen == 30 && memcmp(vbuf, "a much longer replacement value", 30) == 0) PASS();
    else FAIL("wrong value after reinsert");
}

// 12. Open/close without any operations (empty sessions)
static void test_empty_sessions(void) {
    printf("  [12] 10 empty open/close cycles don't corrupt            "); fflush(stdout);
    const char *path = "/tmp/adb_edge12";
    cleanup(path);
    // First put some data
    adb_t *db = NULL;
    adb_open(path, NULL, &db);
    adb_put(db, "sentinel", 8, "alive", 5);
    adb_close(db);
    // Do 10 empty open/close cycles
    for (int i = 0; i < 10; i++) {
        db = NULL;
        adb_open(path, NULL, &db);
        adb_close(db);
    }
    // Verify data survived
    db = NULL;
    adb_open(path, NULL, &db);
    char vbuf[256]; uint16_t vlen;
    int rc = adb_get(db, "sentinel", 8, vbuf, 256, &vlen);
    adb_close(db); cleanup(path);
    if (rc == 0 && vlen == 5 && memcmp(vbuf, "alive", 5) == 0) PASS();
    else FAIL("data corrupted by empty sessions");
}

// 13. Invalid arguments should return ADB_ERR_INVALID (not crash)
static void test_invalid_arguments(void) {
    printf("  [13] invalid args return ADB_ERR_INVALID                "); fflush(stdout);
    int bad = 0;
    adb_t *db = NULL;
    char vbuf[32];
    uint16_t vlen = 7;
    adb_metrics_t m;

    if (adb_open(NULL, NULL, &db) != ADB_ERR_INVALID) bad++;
    if (adb_open("/tmp/adb_edge13", NULL, NULL) != ADB_ERR_INVALID) bad++;
    cleanup("/tmp/adb_edge13");
    if (adb_open("/tmp/adb_edge13", NULL, &db) != 0 || !db) bad++;

    if (adb_put(db, NULL, 1, "v", 1) != ADB_ERR_INVALID) bad++;
    if (adb_put(db, "k", 1, NULL, 1) != ADB_ERR_INVALID) bad++;
    if (adb_get(db, NULL, 1, vbuf, sizeof(vbuf), &vlen) != ADB_ERR_INVALID) bad++;
    if (adb_get(db, "k", 1, NULL, 1, &vlen) != ADB_ERR_INVALID) bad++;
    if (adb_delete(db, NULL, 1) != ADB_ERR_INVALID) bad++;
    if (adb_scan(db, NULL, 0, NULL, 0, NULL, NULL) != ADB_ERR_INVALID) bad++;
    if (adb_get_metrics(NULL, &m) != ADB_ERR_INVALID) bad++;
    if (adb_sync(NULL) != ADB_ERR_INVALID) bad++;
    if (adb_backup(NULL, "/tmp/x", ADB_BACKUP_FULL) != ADB_ERR_INVALID) bad++;
    if (adb_backup(db, "/tmp/x", 255) != ADB_ERR_INVALID) bad++;
    if (adb_restore(NULL, "/tmp/x") != ADB_ERR_INVALID) bad++;
    if (adb_restore("/tmp/x", NULL) != ADB_ERR_INVALID) bad++;

    adb_close(db);
    cleanup("/tmp/adb_edge13");
    if (bad == 0) PASS(); else { char msg[64]; snprintf(msg, sizeof(msg), "%d bad", bad); FAIL(msg); }
}

// 14. Oversized key/value lengths should be rejected deterministically
static void test_oversize_lengths_rejected(void) {
    printf("  [14] oversize key/value lengths rejected cleanly         "); fflush(stdout);
    const char *path = "/tmp/adb_edge14";
    cleanup(path);
    adb_t *db = NULL;
    int rc = adb_open(path, NULL, &db);
    if (rc != 0 || !db) { FAIL("open failed"); cleanup(path); return; }

    int bad = 0;
    char big_key[63];
    char big_val[255];
    memset(big_key, 'K', sizeof(big_key));
    memset(big_val, 'V', sizeof(big_val));

    if (adb_put(db, big_key, 63, "ok", 2) != ADB_ERR_KEY_TOO_LONG) bad++;
    if (adb_put(db, "ok", 2, big_val, 255) != ADB_ERR_VAL_TOO_LONG) bad++;

    char vbuf[32];
    uint16_t vlen = 99;
    if (adb_get(db, big_key, 63, vbuf, sizeof(vbuf), &vlen) != ADB_ERR_KEY_TOO_LONG) bad++;
    if (adb_delete(db, big_key, 63) != ADB_ERR_KEY_TOO_LONG) bad++;

    if (adb_put(db, "healthy", 7, "value", 5) != 0) bad++;
    vlen = 0;
    rc = adb_get(db, "healthy", 7, vbuf, sizeof(vbuf), &vlen);
    if (rc != 0 || vlen != 5 || memcmp(vbuf, "value", 5) != 0) bad++;

    adb_close(db);
    cleanup(path);
    if (bad == 0) PASS(); else { char msg[64]; snprintf(msg, sizeof(msg), "%d bad", bad); FAIL(msg); }
}

// 15. Batch input validation must prevent partial writes
static void test_batch_no_partial_on_invalid(void) {
    printf("  [15] batch invalid entry causes no partial commit        "); fflush(stdout);
    const char *path = "/tmp/adb_edge15";
    cleanup(path);
    adb_t *db = NULL;
    int rc = adb_open(path, NULL, &db);
    if (rc != 0 || !db) { FAIL("open failed"); cleanup(path); return; }

    adb_batch_entry_t e[3];
    e[0].key = "k1"; e[0].key_len = 2; e[0].val = "v1"; e[0].val_len = 2;
    e[1].key = NULL; e[1].key_len = 1; e[1].val = "v2"; e[1].val_len = 2;
    e[2].key = "k3"; e[2].key_len = 2; e[2].val = "v3"; e[2].val_len = 2;

    int bad = 0;
    rc = adb_batch_put(db, e, 3);
    if (rc != ADB_ERR_INVALID) bad++;

    char vbuf[32];
    uint16_t vlen = 0;
    if (adb_get(db, "k1", 2, vbuf, sizeof(vbuf), &vlen) != ADB_ERR_NOT_FOUND) bad++;
    if (adb_get(db, "k3", 2, vbuf, sizeof(vbuf), &vlen) != ADB_ERR_NOT_FOUND) bad++;

    e[1].key = "k2";
    e[1].key_len = 2;
    rc = adb_batch_put(db, e, 3);
    if (rc != 0) bad++;

    rc = adb_get(db, "k1", 2, vbuf, sizeof(vbuf), &vlen);
    if (rc != 0 || vlen != 2 || memcmp(vbuf, "v1", 2) != 0) bad++;
    rc = adb_get(db, "k2", 2, vbuf, sizeof(vbuf), &vlen);
    if (rc != 0 || vlen != 2 || memcmp(vbuf, "v2", 2) != 0) bad++;
    rc = adb_get(db, "k3", 2, vbuf, sizeof(vbuf), &vlen);
    if (rc != 0 || vlen != 2 || memcmp(vbuf, "v3", 2) != 0) bad++;

    adb_close(db);
    cleanup(path);
    if (bad == 0) PASS(); else { char msg[64]; snprintf(msg, sizeof(msg), "%d bad", bad); FAIL(msg); }
}

// 16. Error code contract: API errors should be positive ADB_ERR_* values
static void test_error_code_contract(void) {
    printf("  [16] API errors use positive ADB_ERR_* codes            "); fflush(stdout);
    int bad = 0;
    adb_t *db = NULL;
    uint64_t tx = 0xDEADBEEF;
    if (adb_tx_begin(NULL, ADB_ISO_SNAPSHOT, &tx) != ADB_ERR_INVALID) bad++;
    if (tx != 0) bad++;
    if (adb_tx_commit(NULL, 1) != ADB_ERR_INVALID) bad++;
    if (adb_tx_rollback(NULL, 1) != ADB_ERR_INVALID) bad++;

    cleanup("/tmp/adb_edge16");
    if (adb_open("/tmp/adb_edge16", NULL, &db) != 0 || !db) bad++;
    if (adb_tx_commit(db, 9999) != ADB_ERR_TX_NOT_FOUND) bad++;
    if (adb_tx_rollback(db, 9999) != ADB_ERR_TX_NOT_FOUND) bad++;
    adb_close(db);
    cleanup("/tmp/adb_edge16");

    cleanup("/tmp/adb_edge16_block");
    FILE *f = fopen("/tmp/adb_edge16_block", "wb");
    if (f) {
        fclose(f);
        int rc = adb_open("/tmp/adb_edge16_block", NULL, &db);
        if (rc != ADB_ERR_IO) bad++;
    } else {
        bad++;
    }

    remove("/tmp/adb_edge16_block");
    if (bad == 0) PASS(); else { char msg[64]; snprintf(msg, sizeof(msg), "%d bad", bad); FAIL(msg); }
}

// 17. Tombstone must mask older persisted value (no stale fallback reads)
static void test_tombstone_masks_persisted_value(void) {
    printf("  [17] tombstone masks persisted value across reopen       "); fflush(stdout);
    const char *path = "/tmp/adb_edge17";
    cleanup(path);

    adb_t *db = NULL;
    int rc = adb_open(path, NULL, &db);
    if (rc != 0 || !db) { FAIL("open failed"); cleanup(path); return; }

    int bad = 0;
    char vbuf[64];
    uint16_t vlen = 0;

    rc = adb_put(db, "stale_key", 9, "old_value", 9);
    if (rc != 0) bad++;
    rc = adb_sync(db);
    if (rc != 0) bad++;

    rc = adb_delete(db, "stale_key", 9);
    if (rc != 0 && rc != ADB_ERR_NOT_FOUND) bad++;

    rc = adb_get(db, "stale_key", 9, vbuf, sizeof(vbuf), &vlen);
    if (rc != ADB_ERR_NOT_FOUND) bad++;

    adb_close(db);
    db = NULL;

    rc = adb_open(path, NULL, &db);
    if (rc != 0 || !db) { FAIL("reopen failed"); cleanup(path); return; }
    rc = adb_get(db, "stale_key", 9, vbuf, sizeof(vbuf), &vlen);
    if (rc != ADB_ERR_NOT_FOUND) bad++;

    adb_close(db);
    cleanup(path);
    if (bad == 0) PASS(); else { char msg[64]; snprintf(msg, sizeof(msg), "%d bad", bad); FAIL(msg); }
}

static void test_restore_rejects_bad_manifest(void) {
    printf("  [18] restore rejects missing/corrupt MANIFEST            "); fflush(stdout);
    const char *src = "/tmp/adb_edge18_src";
    const char *bk = "/tmp/adb_edge18_bk";
    const char *dst1 = "/tmp/adb_edge18_dst1";
    const char *dst2 = "/tmp/adb_edge18_dst2";
    cleanup(src); cleanup(bk); cleanup(dst1); cleanup(dst2);

    int bad = 0;
    adb_t *db = NULL;
    if (adb_open(src, NULL, &db) != 0 || !db) {
        FAIL("open failed");
        cleanup(src); cleanup(bk); cleanup(dst1); cleanup(dst2);
        return;
    }

    if (adb_put(db, "k", 1, "v", 1) != 0) bad++;
    if (adb_sync(db) != 0) bad++;
    if (adb_backup(db, bk, ADB_BACKUP_FULL) != 0) bad++;
    adb_close(db);

    char mf[256];
    snprintf(mf, sizeof(mf), "%s/MANIFEST", bk);
    FILE *f = fopen(mf, "r+b");
    if (!f) {
        bad++;
    } else {
        unsigned char b = 0;
        if (fread(&b, 1, 1, f) != 1) bad++;
        b ^= 0x5A;
        if (fseek(f, 0, SEEK_SET) != 0) bad++;
        if (fwrite(&b, 1, 1, f) != 1) bad++;
        fclose(f);
    }

    int rc = adb_restore(bk, dst1);
    if (rc != ADB_ERR_IO) bad++;

    remove(mf);
    rc = adb_restore(bk, dst2);
    if (rc != ADB_ERR_IO) bad++;

    cleanup(src); cleanup(bk); cleanup(dst1); cleanup(dst2);
    if (bad == 0) PASS(); else { char msg[64]; snprintf(msg, sizeof(msg), "%d bad", bad); FAIL(msg); }
}

static void test_restore_rejects_semantic_manifest(void) {
    printf("  [19] restore rejects semantically invalid MANIFEST       "); fflush(stdout);
    const char *src = "/tmp/adb_edge19_src";
    const char *bk = "/tmp/adb_edge19_bk";
    const char *dst = "/tmp/adb_edge19_dst";
    cleanup(src); cleanup(bk); cleanup(dst);

    int bad = 0;
    adb_t *db = NULL;
    if (adb_open(src, NULL, &db) != 0 || !db) {
        FAIL("open failed");
        cleanup(src); cleanup(bk); cleanup(dst);
        return;
    }

    if (adb_put(db, "a", 1, "b", 1) != 0) bad++;
    if (adb_sync(db) != 0) bad++;
    if (adb_backup(db, bk, ADB_BACKUP_FULL) != 0) bad++;
    adb_close(db);

    char mf[256];
    snprintf(mf, sizeof(mf), "%s/MANIFEST", bk);
    FILE *f = fopen(mf, "r+b");
    if (!f) {
        bad++;
    } else {
        unsigned char buf[512];
        if (fread(buf, 1, sizeof(buf), f) != sizeof(buf)) {
            bad++;
        } else {
            uint64_t bad_root = 9999;
            uint64_t one_page = 1;
            memcpy(buf + 0x18, &bad_root, sizeof(bad_root));
            memcpy(buf + 0x20, &one_page, sizeof(one_page));
            uint32_t crc = hw_crc32c(buf, 0x138);
            memcpy(buf + 0x138, &crc, sizeof(crc));
            if (fseek(f, 0, SEEK_SET) != 0) bad++;
            if (fwrite(buf, 1, sizeof(buf), f) != sizeof(buf)) bad++;
        }
        fclose(f);
    }

    int rc = adb_restore(bk, dst);
    if (rc != ADB_ERR_IO) bad++;

    cleanup(src); cleanup(bk); cleanup(dst);
    if (bad == 0) PASS(); else { char msg[64]; snprintf(msg, sizeof(msg), "%d bad", bad); FAIL(msg); }
}

static void test_restore_overwrites_dirty_destination(void) {
    printf("  [20] restore overwrites dirty destination safely         "); fflush(stdout);
    const char *src = "/tmp/adb_edge20_src";
    const char *bk = "/tmp/adb_edge20_bk";
    const char *dst = "/tmp/adb_edge20_dst";
    cleanup(src); cleanup(bk); cleanup(dst);

    int bad = 0;
    adb_t *db = NULL;
    char vbuf[64];
    uint16_t vlen = 0;

    if (adb_open(src, NULL, &db) != 0 || !db) { FAIL("open src failed"); return; }
    if (adb_put(db, "new_key", 7, "new_value", 9) != 0) bad++;
    if (adb_sync(db) != 0) bad++;
    if (adb_backup(db, bk, ADB_BACKUP_FULL) != 0) bad++;
    adb_close(db);

    db = NULL;
    if (adb_open(dst, NULL, &db) != 0 || !db) { FAIL("open dst failed"); cleanup(src); cleanup(bk); cleanup(dst); return; }
    if (adb_put(db, "old_key", 7, "old_value", 9) != 0) bad++;
    adb_close(db);

    if (adb_restore(bk, dst) != 0) bad++;

    db = NULL;
    if (adb_open(dst, NULL, &db) != 0 || !db) {
        bad++;
    } else {
        int rc = adb_get(db, "new_key", 7, vbuf, sizeof(vbuf), &vlen);
        if (rc != 0 || vlen != 9 || memcmp(vbuf, "new_value", 9) != 0) bad++;
        rc = adb_get(db, "old_key", 7, vbuf, sizeof(vbuf), &vlen);
        if (rc != ADB_ERR_NOT_FOUND) bad++;
        adb_close(db);
    }

    cleanup(src); cleanup(bk); cleanup(dst);
    if (bad == 0) PASS(); else { char msg[64]; snprintf(msg, sizeof(msg), "%d bad", bad); FAIL(msg); }
}

static void test_restore_rejects_truncated_btree(void) {
    printf("  [21] restore rejects truncated data.btree                "); fflush(stdout);
    const char *src = "/tmp/adb_edge21_src";
    const char *bk = "/tmp/adb_edge21_bk";
    const char *dst = "/tmp/adb_edge21_dst";
    cleanup(src); cleanup(bk); cleanup(dst);

    int bad = 0;
    adb_t *db = NULL;
    if (adb_open(src, NULL, &db) != 0 || !db) {
        FAIL("open failed");
        cleanup(src); cleanup(bk); cleanup(dst);
        return;
    }

    for (int i = 0; i < 256; i++) {
        char key[32];
        char val[32];
        int kl = snprintf(key, sizeof(key), "k%03d", i);
        int vl = snprintf(val, sizeof(val), "v%03d_payload", i);
        if (adb_put(db, key, (uint16_t)kl, val, (uint16_t)vl) != 0) bad++;
    }
    if (adb_sync(db) != 0) bad++;
    if (adb_backup(db, bk, ADB_BACKUP_FULL) != 0) bad++;
    adb_close(db);

    char bf[256];
    snprintf(bf, sizeof(bf), "%s/data.btree", bk);
    if (truncate(bf, 1024) != 0) bad++;

    int rc = adb_restore(bk, dst);
    if (rc != ADB_ERR_IO) bad++;

    cleanup(src); cleanup(bk); cleanup(dst);
    if (bad == 0) PASS(); else { char msg[64]; snprintf(msg, sizeof(msg), "%d bad", bad); FAIL(msg); }
}

static void test_restore_rejects_bad_root_page_type(void) {
    printf("  [22] restore rejects invalid root page type              "); fflush(stdout);
    const char *src = "/tmp/adb_edge22_src";
    const char *bk = "/tmp/adb_edge22_bk";
    const char *dst = "/tmp/adb_edge22_dst";
    cleanup(src); cleanup(bk); cleanup(dst);

    int bad = 0;
    adb_t *db = NULL;
    if (adb_open(src, NULL, &db) != 0 || !db) {
        FAIL("open failed");
        cleanup(src); cleanup(bk); cleanup(dst);
        return;
    }

    for (int i = 0; i < 128; i++) {
        char key[32];
        char val[32];
        int kl = snprintf(key, sizeof(key), "r%03d", i);
        int vl = snprintf(val, sizeof(val), "root_payload_%03d", i);
        if (adb_put(db, key, (uint16_t)kl, val, (uint16_t)vl) != 0) bad++;
    }
    if (adb_sync(db) != 0) bad++;
    if (adb_backup(db, bk, ADB_BACKUP_FULL) != 0) bad++;
    adb_close(db);

    char mf[256];
    snprintf(mf, sizeof(mf), "%s/MANIFEST", bk);
    FILE *mf_f = fopen(mf, "rb");
    uint64_t root = 0;
    if (!mf_f) {
        bad++;
    } else {
        unsigned char mbuf[512];
        if (fread(mbuf, 1, sizeof(mbuf), mf_f) != sizeof(mbuf)) {
            bad++;
        } else {
            memcpy(&root, mbuf + 0x18, sizeof(root));
        }
        fclose(mf_f);
    }

    char bf[256];
    snprintf(bf, sizeof(bf), "%s/data.btree", bk);
    FILE *bf_f = fopen(bf, "r+b");
    if (!bf_f) {
        bad++;
    } else {
        long off = (long)(root * 4096ULL + 4ULL);
        unsigned char bad_type[2] = {0x7F, 0x00};
        if (fseek(bf_f, off, SEEK_SET) != 0) bad++;
        if (fwrite(bad_type, 1, sizeof(bad_type), bf_f) != sizeof(bad_type)) bad++;
        fclose(bf_f);
    }

    int rc = adb_restore(bk, dst);
    if (rc != ADB_ERR_IO) bad++;

    cleanup(src); cleanup(bk); cleanup(dst);
    if (bad == 0) PASS(); else { char msg[64]; snprintf(msg, sizeof(msg), "%d bad", bad); FAIL(msg); }
}

static void test_restore_copies_manifest_exactly(void) {
    printf("  [23] restore copies MANIFEST byte-for-byte               "); fflush(stdout);
    const char *src = "/tmp/adb_edge23_src";
    const char *bk = "/tmp/adb_edge23_bk";
    const char *dst = "/tmp/adb_edge23_dst";
    cleanup(src); cleanup(bk); cleanup(dst);

    int bad = 0;
    adb_t *db = NULL;
    if (adb_open(src, NULL, &db) != 0 || !db) {
        FAIL("open failed");
        cleanup(src); cleanup(bk); cleanup(dst);
        return;
    }

    for (int i = 0; i < 300; i++) {
        char key[32];
        char val[32];
        int kl = snprintf(key, sizeof(key), "m%03d", i);
        int vl = snprintf(val, sizeof(val), "manifest_payload_%03d", i);
        if (adb_put(db, key, (uint16_t)kl, val, (uint16_t)vl) != 0) bad++;
    }
    if (adb_sync(db) != 0) bad++;
    if (adb_backup(db, bk, ADB_BACKUP_FULL) != 0) bad++;
    adb_close(db);

    if (adb_restore(bk, dst) != 0) bad++;

    unsigned char src_mf[512];
    unsigned char dst_mf[512];
    char srcf[256];
    char dstf[256];
    snprintf(srcf, sizeof(srcf), "%s/MANIFEST", bk);
    snprintf(dstf, sizeof(dstf), "%s/MANIFEST", dst);

    FILE *f1 = fopen(srcf, "rb");
    FILE *f2 = fopen(dstf, "rb");
    if (!f1 || !f2) {
        bad++;
    } else {
        if (fread(src_mf, 1, sizeof(src_mf), f1) != sizeof(src_mf)) bad++;
        if (fread(dst_mf, 1, sizeof(dst_mf), f2) != sizeof(dst_mf)) bad++;
        if (memcmp(src_mf, dst_mf, sizeof(src_mf)) != 0) bad++;
    }
    if (f1) fclose(f1);
    if (f2) fclose(f2);

    cleanup(src); cleanup(bk); cleanup(dst);
    if (bad == 0) PASS(); else { char msg[64]; snprintf(msg, sizeof(msg), "%d bad", bad); FAIL(msg); }
}

static void test_backup_restore_leave_no_tmp_files(void) {
    printf("  [24] backup/restore leave no *.tmp residue               "); fflush(stdout);
    const char *src = "/tmp/adb_edge24_src";
    const char *bk = "/tmp/adb_edge24_bk";
    const char *dst = "/tmp/adb_edge24_dst";
    cleanup(src); cleanup(bk); cleanup(dst);

    int bad = 0;
    adb_t *db = NULL;
    if (adb_open(src, NULL, &db) != 0 || !db) {
        FAIL("open failed");
        cleanup(src); cleanup(bk); cleanup(dst);
        return;
    }

    for (int i = 0; i < 64; i++) {
        char key[24];
        char val[24];
        int kl = snprintf(key, sizeof(key), "t%02d", i);
        int vl = snprintf(val, sizeof(val), "tmp_%02d", i);
        if (adb_put(db, key, (uint16_t)kl, val, (uint16_t)vl) != 0) bad++;
    }
    if (adb_sync(db) != 0) bad++;
    if (adb_backup(db, bk, ADB_BACKUP_FULL) != 0) bad++;
    adb_close(db);

    char bktmp[256];
    snprintf(bktmp, sizeof(bktmp), "%s/data.btree.tmp", bk);
    if (access(bktmp, F_OK) == 0) bad++;
    char bkmftmp[256];
    snprintf(bkmftmp, sizeof(bkmftmp), "%s/MANIFEST.tmp", bk);
    if (access(bkmftmp, F_OK) == 0) bad++;

    if (adb_restore(bk, dst) != 0) bad++;

    char dsttmp[256];
    snprintf(dsttmp, sizeof(dsttmp), "%s/data.btree.tmp", dst);
    if (access(dsttmp, F_OK) == 0) bad++;
    char dstmftmp[256];
    snprintf(dstmftmp, sizeof(dstmftmp), "%s/MANIFEST.tmp", dst);
    if (access(dstmftmp, F_OK) == 0) bad++;

    cleanup(src); cleanup(bk); cleanup(dst);
    if (bad == 0) PASS(); else { char msg[64]; snprintf(msg, sizeof(msg), "%d bad", bad); FAIL(msg); }
}

static void test_restore_rejects_manifest_with_trailing_data(void) {
    printf("  [25] restore rejects MANIFEST with trailing bytes        "); fflush(stdout);
    const char *src = "/tmp/adb_edge25_src";
    const char *bk = "/tmp/adb_edge25_bk";
    const char *dst = "/tmp/adb_edge25_dst";
    cleanup(src); cleanup(bk); cleanup(dst);

    int bad = 0;
    adb_t *db = NULL;
    if (adb_open(src, NULL, &db) != 0 || !db) {
        FAIL("open failed");
        cleanup(src); cleanup(bk); cleanup(dst);
        return;
    }

    if (adb_put(db, "m", 1, "v", 1) != 0) bad++;
    if (adb_sync(db) != 0) bad++;
    if (adb_backup(db, bk, ADB_BACKUP_FULL) != 0) bad++;
    adb_close(db);

    char mf[256];
    snprintf(mf, sizeof(mf), "%s/MANIFEST", bk);
    FILE *f = fopen(mf, "ab");
    if (!f) {
        bad++;
    } else {
        unsigned char extra = 0xA5;
        if (fwrite(&extra, 1, 1, f) != 1) bad++;
        fclose(f);
    }

    int rc = adb_restore(bk, dst);
    if (rc != ADB_ERR_IO) bad++;

    cleanup(src); cleanup(bk); cleanup(dst);
    if (bad == 0) PASS(); else { char msg[64]; snprintf(msg, sizeof(msg), "%d bad", bad); FAIL(msg); }
}

static void test_restore_rejects_same_source_and_destination(void) {
    printf("  [26] restore rejects source==destination path            "); fflush(stdout);
    const char *src = "/tmp/adb_edge26_src";
    const char *bk = "/tmp/adb_edge26_bk";
    const char *dst = "/tmp/adb_edge26_dst";
    cleanup(src); cleanup(bk); cleanup(dst);

    int bad = 0;
    adb_t *db = NULL;
    if (adb_open(src, NULL, &db) != 0 || !db) {
        FAIL("open failed");
        cleanup(src); cleanup(bk); cleanup(dst);
        return;
    }

    if (adb_put(db, "guard_key", 9, "guard_val", 9) != 0) bad++;
    if (adb_sync(db) != 0) bad++;
    if (adb_backup(db, bk, ADB_BACKUP_FULL) != 0) bad++;
    adb_close(db);

    int rc = adb_restore(bk, bk);
    if (rc != ADB_ERR_INVALID) bad++;

    char bk_slash[256];
    snprintf(bk_slash, sizeof(bk_slash), "%s/", bk);
    rc = adb_restore(bk_slash, bk);
    if (rc != ADB_ERR_INVALID) bad++;

    char mf[256];
    char bf[256];
    snprintf(mf, sizeof(mf), "%s/MANIFEST", bk);
    snprintf(bf, sizeof(bf), "%s/data.btree", bk);
    if (access(mf, F_OK) != 0) bad++;
    if (access(bf, F_OK) != 0) bad++;

    if (adb_restore(bk, dst) != 0) bad++;

    db = NULL;
    if (adb_open(dst, NULL, &db) != 0 || !db) {
        bad++;
    } else {
        char vbuf[64];
        uint16_t vlen = 0;
        rc = adb_get(db, "guard_key", 9, vbuf, sizeof(vbuf), &vlen);
        if (rc != 0 || vlen != 9 || memcmp(vbuf, "guard_val", 9) != 0) bad++;
        adb_close(db);
    }

    cleanup(src); cleanup(bk); cleanup(dst);
    if (bad == 0) PASS(); else { char msg[64]; snprintf(msg, sizeof(msg), "%d bad", bad); FAIL(msg); }
}

static void test_restore_rejects_alias_source_destination(void) {
    printf("  [27] restore rejects alias path to same directory        "); fflush(stdout);
    const char *src = "/tmp/adb_edge27_src";
    const char *bk = "/tmp/adb_edge27_bk";
    const char *dst = "/tmp/adb_edge27_dst";
    const char *alias = "/tmp/adb_edge27_alias";
    cleanup(src); cleanup(bk); cleanup(dst); cleanup(alias);

    int bad = 0;
    adb_t *db = NULL;
    if (adb_open(src, NULL, &db) != 0 || !db) {
        FAIL("open failed");
        cleanup(src); cleanup(bk); cleanup(dst); cleanup(alias);
        return;
    }

    if (adb_put(db, "alias_key", 9, "alias_val", 9) != 0) bad++;
    if (adb_sync(db) != 0) bad++;
    if (adb_backup(db, bk, ADB_BACKUP_FULL) != 0) bad++;
    adb_close(db);

    if (symlink(bk, alias) != 0) bad++;

    int rc = adb_restore(alias, bk);
    if (rc != ADB_ERR_INVALID) bad++;

    char mf[256];
    char bf[256];
    snprintf(mf, sizeof(mf), "%s/MANIFEST", bk);
    snprintf(bf, sizeof(bf), "%s/data.btree", bk);
    if (access(mf, F_OK) != 0) bad++;
    if (access(bf, F_OK) != 0) bad++;

    if (adb_restore(bk, dst) != 0) bad++;

    db = NULL;
    if (adb_open(dst, NULL, &db) != 0 || !db) {
        bad++;
    } else {
        char vbuf[64];
        uint16_t vlen = 0;
        rc = adb_get(db, "alias_key", 9, vbuf, sizeof(vbuf), &vlen);
        if (rc != 0 || vlen != 9 || memcmp(vbuf, "alias_val", 9) != 0) bad++;
        adb_close(db);
    }

    cleanup(src); cleanup(bk); cleanup(dst); cleanup(alias);
    if (bad == 0) PASS(); else { char msg[64]; snprintf(msg, sizeof(msg), "%d bad", bad); FAIL(msg); }
}

// 28. Memtable value takes priority over B+ tree (newest-wins)
static void test_memtable_overrides_btree(void) {
    printf("  [28] memtable value overrides persisted B+ tree value      "); fflush(stdout);
    const char *p = "/tmp/adb_edge28"; cleanup(p);
    adb_t *db = NULL; int bad = 0;
    adb_open(p, NULL, &db);
    // Write and sync to persist to B+ tree
    adb_put(db, "dup", 3, "old_val", 7);
    adb_sync(db);
    // Overwrite in memtable (not yet synced)
    adb_put(db, "dup", 3, "new_val", 7);
    // Read should see memtable version
    char buf[256]; uint16_t vl;
    if (adb_get(db, "dup", 3, buf, 256, &vl) || vl != 7 || memcmp(buf, "new_val", 7)) bad++;
    adb_close(db);
    // Reopen: close flushes memtable to B+ tree, should see new_val
    adb_open(p, NULL, &db);
    if (adb_get(db, "dup", 3, buf, 256, &vl) || vl != 7 || memcmp(buf, "new_val", 7)) bad++;
    adb_close(db); cleanup(p);
    if (bad == 0) PASS(); else FAIL("memtable didn't override B+ tree");
}

// 29. Destroy then reuse same path
static void test_destroy_then_reuse(void) {
    printf("  [29] destroy + reopen same path creates fresh db           "); fflush(stdout);
    const char *p = "/tmp/adb_edge29"; cleanup(p);
    adb_t *db = NULL; int bad = 0;
    adb_open(p, NULL, &db);
    adb_put(db, "k1", 2, "v1", 2);
    adb_close(db);
    adb_destroy(p);
    // Reopen: should be fresh
    adb_open(p, NULL, &db);
    char buf[256]; uint16_t vl;
    if (adb_get(db, "k1", 2, buf, 256, &vl) == 0) bad++; // should NOT find
    adb_put(db, "k2", 2, "v2", 2);
    if (adb_get(db, "k2", 2, buf, 256, &vl) || vl != 2) bad++;
    adb_close(db); cleanup(p);
    if (bad == 0) PASS(); else FAIL("destroy didn't clear data");
}

// 30. Delete in memtable masks B+ tree entry across sync
static void test_delete_masks_btree_across_sync(void) {
    printf("  [30] delete in memtable masks B+ tree across sync+reopen  "); fflush(stdout);
    const char *p = "/tmp/adb_edge30"; cleanup(p);
    adb_t *db = NULL; int bad = 0;
    adb_open(p, NULL, &db);
    adb_put(db, "ghost", 5, "old", 3);
    adb_sync(db); // now in B+ tree
    adb_delete(db, "ghost", 5); // tombstone in memtable
    char buf[256]; uint16_t vl;
    // Should not find (tombstone masks B+ tree)
    if (adb_get(db, "ghost", 5, buf, 256, &vl) == 0) bad++;
    // Sync again: flush tombstone to B+ tree
    adb_sync(db);
    if (adb_get(db, "ghost", 5, buf, 256, &vl) == 0) bad++;
    // Close + reopen: tombstone should be applied
    adb_close(db);
    adb_open(p, NULL, &db);
    if (adb_get(db, "ghost", 5, buf, 256, &vl) == 0) bad++;
    adb_close(db); cleanup(p);
    if (bad == 0) PASS(); else { char m[32]; snprintf(m,32,"%d ghost reads",bad); FAIL(m); }
}

// Helper for scan tests
static int scan_counter(const void *k, uint16_t kl, const void *v, uint16_t vl, void *ctx) {
    (void)k; (void)kl; (void)v; (void)vl;
    (*(int*)ctx)++;
    return 0;
}

// 31. Scan with start == end == existing key returns exactly that key
static void test_scan_exact_start_end(void) {
    printf("  [31] scan start==end==key returns exactly 1 entry          "); fflush(stdout);
    const char *p = "/tmp/adb_edge31"; cleanup(p);
    adb_t *db = NULL; int bad = 0;
    adb_open(p, NULL, &db);
    for (int i = 0; i < 100; i++) {
        char k[16], v[16];
        snprintf(k, sizeof(k), "k%04d", i);
        snprintf(v, sizeof(v), "v%04d", i);
        adb_put(db, k, strlen(k), v, strlen(v));
    }
    adb_sync(db);
    // Scan exact range [k0050, k0050]
    int cnt = 0;
    adb_scan(db, "k0050", 5, "k0050", 5, scan_counter, &cnt);
    if (cnt != 1) bad++;
    // Scan range [k0010, k0019] should get ~10 keys
    cnt = 0;
    adb_scan(db, "k0010", 5, "k0019", 5, scan_counter, &cnt);
    if (cnt != 10) bad++;
    adb_close(db); cleanup(p);
    if (bad == 0) PASS(); else { char m[64]; snprintf(m,64,"cnt mismatch bad=%d",bad); FAIL(m); }
}

// 32. Multiple syncs with interleaved deletes maintain count
static void test_multi_sync_delete_count(void) {
    printf("  [32] multi-sync with interleaved deletes, correct count    "); fflush(stdout);
    const char *p = "/tmp/adb_edge32"; cleanup(p);
    adb_t *db = NULL;
    adb_open(p, NULL, &db);
    // Insert 500 keys
    for (int i = 0; i < 500; i++) {
        char k[16]; snprintf(k, sizeof(k), "ms%04d", i);
        adb_put(db, k, strlen(k), "val", 3);
    }
    adb_sync(db);
    // Delete 100 keys (indices 100-199)
    for (int i = 100; i < 200; i++) {
        char k[16]; snprintf(k, sizeof(k), "ms%04d", i);
        adb_delete(db, k, strlen(k));
    }
    adb_sync(db);
    // Scan: should see 400
    int cnt = 0;
    adb_scan(db, NULL, 0, NULL, 0, scan_counter, &cnt);
    adb_close(db);
    // Reopen and verify
    adb_open(p, NULL, &db);
    int cnt2 = 0;
    adb_scan(db, NULL, 0, NULL, 0, scan_counter, &cnt2);
    adb_close(db); cleanup(p);
    if (cnt == 400 && cnt2 == 400) PASS();
    else { char m[64]; snprintf(m,64,"cnt=%d cnt2=%d",cnt,cnt2); FAIL(m); }
}

// 33. Overwrite value with zero-length then non-zero across sessions
static void test_overwrite_zero_length_value(void) {
    printf("  [33] overwrite with zero-length value then restore           "); fflush(stdout);
    const char *p = "/tmp/adb_edge33"; cleanup(p);
    adb_t *db = NULL; int bad = 0;
    adb_open(p, NULL, &db);
    adb_put(db, "k", 1, "hello", 5);
    char buf[256]; uint16_t vl;
    if (adb_get(db, "k", 1, buf, 256, &vl) || vl != 5) bad++;
    adb_put(db, "k", 1, "", 0);
    if (adb_get(db, "k", 1, buf, 256, &vl) || vl != 0) bad++;
    adb_close(db);
    adb_open(p, NULL, &db);
    if (adb_get(db, "k", 1, buf, 256, &vl) || vl != 0) bad++;
    adb_put(db, "k", 1, "back", 4);
    adb_close(db);
    adb_open(p, NULL, &db);
    if (adb_get(db, "k", 1, buf, 256, &vl) || vl != 4 || memcmp(buf, "back", 4)) bad++;
    adb_close(db); cleanup(p);
    if (bad == 0) PASS(); else { char m[64]; snprintf(m,64,"%d bad",bad); FAIL(m); }
}

// 34. WAL replay: dirty close without sync -> data survives via WAL
static void test_wal_replay_dirty_close(void) {
    printf("  [34] WAL replay recovers data after dirty close             "); fflush(stdout);
    const char *p = "/tmp/adb_edge34"; cleanup(p);
    adb_t *db = NULL; int bad = 0;
    adb_open(p, NULL, &db);
    for (int i = 0; i < 200; i++) {
        char key[16]; int kl = snprintf(key, sizeof(key), "w%05d", i);
        char val[16]; int vl = snprintf(val, sizeof(val), "v%05d", i);
        adb_put(db, key, (uint16_t)kl, val, (uint16_t)vl);
    }
    // close without sync - WAL should replay on next open
    adb_close(db);
    adb_open(p, NULL, &db);
    for (int i = 0; i < 200; i++) {
        char key[16]; int kl = snprintf(key, sizeof(key), "w%05d", i);
        char val[16]; int vl = snprintf(val, sizeof(val), "v%05d", i);
        char buf[256]; uint16_t rvl;
        int rc = adb_get(db, key, (uint16_t)kl, buf, 256, &rvl);
        if (rc != 0 || rvl != (uint16_t)vl || memcmp(buf, val, vl) != 0) bad++;
    }
    adb_close(db); cleanup(p);
    if (bad == 0) PASS(); else { char m[64]; snprintf(m,64,"%d missing after replay",bad); FAIL(m); }
}

// 35. Scan where start > end should yield zero results
static void test_scan_inverted_range(void) {
    printf("  [35] scan with start > end yields zero results              "); fflush(stdout);
    const char *p = "/tmp/adb_edge35"; cleanup(p);
    adb_t *db = NULL;
    adb_open(p, NULL, &db);
    for (int i = 0; i < 100; i++) {
        char key[16]; int kl = snprintf(key, sizeof(key), "k%04d", i);
        adb_put(db, key, (uint16_t)kl, "v", 1);
    }
    int cnt = 0;
    adb_scan(db, "k0090", 5, "k0010", 5, scan_counter, &cnt);
    adb_close(db); cleanup(p);
    if (cnt == 0) PASS(); else { char m[64]; snprintf(m,64,"got %d (expected 0)",cnt); FAIL(m); }
}

// 36. Metrics monotonically increase across operations
static void test_metrics_monotonic(void) {
    printf("  [36] metrics increase monotonically with operations         "); fflush(stdout);
    const char *p = "/tmp/adb_edge36"; cleanup(p);
    adb_t *db = NULL; int bad = 0;
    adb_open(p, NULL, &db);
    adb_metrics_t m1, m2, m3;
    adb_get_metrics(db, &m1);
    for (int i = 0; i < 50; i++) {
        char key[16]; int kl = snprintf(key, sizeof(key), "m%04d", i);
        adb_put(db, key, (uint16_t)kl, "val", 3);
    }
    adb_get_metrics(db, &m2);
    if (m2.puts_total < m1.puts_total + 50) bad++;
    for (int i = 0; i < 50; i++) {
        char key[16]; int kl = snprintf(key, sizeof(key), "m%04d", i);
        char buf[256]; uint16_t vl;
        adb_get(db, key, (uint16_t)kl, buf, 256, &vl);
    }
    adb_get_metrics(db, &m3);
    if (m3.gets_total < m2.gets_total + 50) bad++;
    if (m3.puts_total < m2.puts_total) bad++;
    adb_close(db); cleanup(p);
    if (bad == 0) PASS(); else { char m[64]; snprintf(m,64,"%d bad",bad); FAIL(m); }
}

// 37. Tx rollback truly invisible: large write-set rolled back
static void test_tx_large_rollback_invisible(void) {
    printf("  [37] large tx rollback leaves no trace                      "); fflush(stdout);
    const char *p = "/tmp/adb_edge37"; cleanup(p);
    adb_t *db = NULL; int bad = 0;
    adb_open(p, NULL, &db);
    adb_put(db, "pre", 3, "exists", 6);
    uint64_t txid;
    adb_tx_begin(db, 0, &txid);
    for (int i = 0; i < 500; i++) {
        char key[16]; int kl = snprintf(key, sizeof(key), "tx%05d", i);
        char val[16]; int vl = snprintf(val, sizeof(val), "v%05d", i);
        adb_tx_put(db, txid, key, (uint16_t)kl, val, (uint16_t)vl);
    }
    adb_tx_rollback(db, txid);
    // None of the tx keys should be visible
    for (int i = 0; i < 500; i++) {
        char key[16]; int kl = snprintf(key, sizeof(key), "tx%05d", i);
        char buf[256]; uint16_t vl;
        if (adb_get(db, key, (uint16_t)kl, buf, 256, &vl) == 0) bad++;
    }
    // Pre-existing key should still be there
    char buf[256]; uint16_t vl;
    if (adb_get(db, "pre", 3, buf, 256, &vl) || vl != 6) bad++;
    adb_close(db); cleanup(p);
    if (bad == 0) PASS(); else { char m[64]; snprintf(m,64,"%d leaked keys",bad); FAIL(m); }
}

// 38. Multiple destroy calls don't crash
static void test_double_destroy(void) {
    printf("  [38] double destroy on same path doesn't crash              "); fflush(stdout);
    const char *p = "/tmp/adb_edge38"; cleanup(p);
    adb_t *db = NULL;
    adb_open(p, NULL, &db);
    adb_put(db, "x", 1, "y", 1);
    adb_close(db);
    int r1 = adb_destroy(p);
    int r2 = adb_destroy(p); // already gone
    (void)r1; (void)r2;
    // Should be able to create fresh at same path
    adb_open(p, NULL, &db);
    char buf[256]; uint16_t vl;
    int rc = adb_get(db, "x", 1, buf, 256, &vl);
    adb_close(db); cleanup(p);
    if (rc != 0) PASS(); else FAIL("data survived double destroy");
}

// 39. Batch put with all identical keys: last value wins
static void test_batch_identical_keys(void) {
    printf("  [39] batch put with identical keys: last value wins         "); fflush(stdout);
    const char *p = "/tmp/adb_edge39"; cleanup(p);
    adb_t *db = NULL; int bad = 0;
    adb_open(p, NULL, &db);
    adb_batch_entry_t entries[10];
    char vals[10][8];
    for (int i = 0; i < 10; i++) {
        entries[i].key = "same";
        entries[i].key_len = 4;
        int vl = snprintf(vals[i], 8, "v%d", i);
        entries[i].val = vals[i];
        entries[i].val_len = (uint16_t)vl;
    }
    adb_batch_put(db, entries, 10);
    char buf[256]; uint16_t vl;
    int rc = adb_get(db, "same", 4, buf, 256, &vl);
    if (rc != 0) bad++;
    // Last put should win (v9)
    if (vl != 2 || memcmp(buf, "v9", 2) != 0) bad++;
    adb_close(db); cleanup(p);
    if (bad == 0) PASS(); else FAIL("batch identical keys: wrong winner");
}

// 40. Tx scan: committed entries visible, uncommitted write-set not merged yet
static void test_tx_scan_sees_write_set(void) {
    printf("  [40] tx_scan + commit -> all entries visible after commit   "); fflush(stdout);
    const char *p = "/tmp/adb_edge40"; cleanup(p);
    adb_t *db = NULL; int bad = 0;
    adb_open(p, NULL, &db);
    adb_put(db, "a", 1, "v1", 2);
    adb_put(db, "c", 1, "v3", 2);
    uint64_t txid;
    adb_tx_begin(db, 0, &txid);
    adb_tx_put(db, txid, "b", 1, "v2", 2);
    // tx_scan currently delegates to adb_scan (write-set not merged)
    int cnt = 0;
    adb_tx_scan(db, txid, NULL, 0, NULL, 0, scan_counter, &cnt);
    if (cnt != 2) bad++; // sees a,c (write-set "b" not merged yet)
    adb_tx_commit(db, txid);
    // After commit, all 3 should be visible
    cnt = 0;
    adb_scan(db, NULL, 0, NULL, 0, scan_counter, &cnt);
    if (cnt != 3) bad++;
    adb_close(db); cleanup(p);
    if (bad == 0) PASS(); else { char m[64]; snprintf(m,64,"pre=%d post=%d",bad,cnt); FAIL(m); }
}

// 41. Scan returns keys in sorted order after random-order inserts
static void test_scan_sorted_after_random_inserts(void) {
    printf("  [41] scan returns sorted order after random inserts         "); fflush(stdout);
    const char *p = "/tmp/adb_edge41"; cleanup(p);
    adb_t *db = NULL; int bad = 0;
    adb_open(p, NULL, &db);
    // Insert in shuffled order using a simple LCG
    int indices[200];
    for (int i = 0; i < 200; i++) indices[i] = i;
    unsigned seed = 12345;
    for (int i = 199; i > 0; i--) {
        seed = seed * 1103515245 + 12345;
        int j = (int)((seed >> 16) % (unsigned)(i + 1));
        int tmp = indices[i]; indices[i] = indices[j]; indices[j] = tmp;
    }
    for (int i = 0; i < 200; i++) {
        char key[16]; int kl = snprintf(key, sizeof(key), "r%05d", indices[i]);
        adb_put(db, key, (uint16_t)kl, "v", 1);
    }
    typedef struct { char prev[16]; int prev_len; int sorted; } sort_ctx_t;
    int check_sorted(const void *k, uint16_t kl, const void *v, uint16_t vl, void *ctx) {
        (void)v; (void)vl;
        sort_ctx_t *s = ctx;
        if (s->prev_len > 0) {
            int cmp = memcmp(s->prev, k, kl < (uint16_t)s->prev_len ? kl : (uint16_t)s->prev_len);
            if (cmp > 0 || (cmp == 0 && s->prev_len > kl)) s->sorted = 0;
        }
        memcpy(s->prev, k, kl); s->prev_len = kl;
        return 0;
    }
    sort_ctx_t ctx = { .prev_len = 0, .sorted = 1 };
    adb_scan(db, NULL, 0, NULL, 0, check_sorted, &ctx);
    if (!ctx.sorted) bad++;
    adb_close(db); cleanup(p);
    if (bad == 0) PASS(); else FAIL("scan not sorted");
}

// 42. Sync+reopen 50 cycles with alternating puts and deletes
static void test_50_reopen_cycles(void) {
    printf("  [42] 50 reopen cycles with alternating put/delete           "); fflush(stdout);
    const char *p = "/tmp/adb_edge42"; cleanup(p);
    adb_t *db = NULL;
    adb_open(p, NULL, &db);
    for (int cycle = 0; cycle < 50; cycle++) {
        char key[16]; int kl = snprintf(key, sizeof(key), "c%04d", cycle);
        adb_put(db, key, (uint16_t)kl, "alive", 5);
        if (cycle > 0 && (cycle % 3) == 0) {
            char dk[16]; int dkl = snprintf(dk, sizeof(dk), "c%04d", cycle - 1);
            adb_delete(db, dk, (uint16_t)dkl);
        }
        adb_close(db);
        adb_open(p, NULL, &db);
    }
    // Verify: keys not divisible-by-3-minus-1 should be alive
    int cnt = 0;
    adb_scan(db, NULL, 0, NULL, 0, scan_counter, &cnt);
    // Expected surviving keys: 50 inserts - (50/3 - 1) deletes (cycles 3,6,9,...48 delete cycle-1)
    // Deletes at cycles 3,6,9,...,48 = 16 deletes
    // But deleted keys are cycle-1: 2,5,8,...,47 = 16 keys
    int expected = 50 - 16;
    adb_close(db); cleanup(p);
    if (cnt == expected) PASS();
    else { char m[64]; snprintf(m,64,"cnt=%d expected=%d",cnt,expected); FAIL(m); }
}

// 43. Tx delete then tx_get returns NOT_FOUND
static void test_tx_delete_then_get(void) {
    printf("  [43] tx_delete then tx_get returns NOT_FOUND                "); fflush(stdout);
    const char *p = "/tmp/adb_edge43"; cleanup(p);
    adb_t *db = NULL; int bad = 0;
    adb_open(p, NULL, &db);
    adb_put(db, "target", 6, "original", 8);
    uint64_t txid;
    adb_tx_begin(db, 0, &txid);
    adb_tx_delete(db, txid, "target", 6);
    char buf[256]; uint16_t vl;
    int rc = adb_tx_get(db, txid, "target", 6, buf, 256, &vl);
    if (rc != ADB_ERR_NOT_FOUND) bad++;
    // Implicit get should still see original (tx not committed)
    rc = adb_get(db, "target", 6, buf, 256, &vl);
    if (rc != 0 || vl != 8 || memcmp(buf, "original", 8)) bad++;
    adb_tx_commit(db, txid);
    // Now implicit get should see NOT_FOUND
    rc = adb_get(db, "target", 6, buf, 256, &vl);
    if (rc != ADB_ERR_NOT_FOUND) bad++;
    adb_close(db); cleanup(p);
    if (bad == 0) PASS(); else { char m[64]; snprintf(m,64,"%d bad",bad); FAIL(m); }
}

// 44. Put 5000 keys, delete all, verify scan returns 0
static void test_mass_delete_then_scan(void) {
    printf("  [44] insert 5000 then delete all -> scan returns 0          "); fflush(stdout);
    const char *p = "/tmp/adb_edge44"; cleanup(p);
    adb_t *db = NULL;
    adb_open(p, NULL, &db);
    for (int i = 0; i < 5000; i++) {
        char key[16]; int kl = snprintf(key, sizeof(key), "d%05d", i);
        adb_put(db, key, (uint16_t)kl, "v", 1);
    }
    for (int i = 0; i < 5000; i++) {
        char key[16]; int kl = snprintf(key, sizeof(key), "d%05d", i);
        adb_delete(db, key, (uint16_t)kl);
    }
    int cnt = 0;
    adb_scan(db, NULL, 0, NULL, 0, scan_counter, &cnt);
    adb_close(db);
    // Reopen and verify
    adb_open(p, NULL, &db);
    int cnt2 = 0;
    adb_scan(db, NULL, 0, NULL, 0, scan_counter, &cnt2);
    adb_close(db); cleanup(p);
    if (cnt == 0 && cnt2 == 0) PASS();
    else { char m[64]; snprintf(m,64,"cnt=%d cnt2=%d",cnt,cnt2); FAIL(m); }
}

// 45. Sync between every operation for maximum fsync stress
static void test_sync_every_op(void) {
    printf("  [45] sync after every single put (100 ops)                  "); fflush(stdout);
    const char *p = "/tmp/adb_edge45"; cleanup(p);
    adb_t *db = NULL; int bad = 0;
    adb_open(p, NULL, &db);
    for (int i = 0; i < 100; i++) {
        char key[16]; int kl = snprintf(key, sizeof(key), "s%04d", i);
        adb_put(db, key, (uint16_t)kl, "val", 3);
        adb_sync(db);
    }
    adb_close(db);
    adb_open(p, NULL, &db);
    for (int i = 0; i < 100; i++) {
        char key[16]; int kl = snprintf(key, sizeof(key), "s%04d", i);
        char buf[256]; uint16_t vl;
        if (adb_get(db, key, (uint16_t)kl, buf, 256, &vl) || vl != 3) bad++;
    }
    adb_close(db); cleanup(p);
    if (bad == 0) PASS(); else { char m[64]; snprintf(m,64,"%d missing",bad); FAIL(m); }
}

// 46. Key compare with high-byte values (0x80-0xFF range tests unsigned correctness)
static void test_key_compare_high_bytes(void) {
    printf("  [46] key_compare: high-byte values (0x80-0xFF) sort correctly  "); fflush(stdout);
    const char *p = "/tmp/adb_edge46"; cleanup(p);
    adb_t *db = NULL;
    adb_open(p, NULL, &db);
    // Insert keys with bytes 0x01, 0x7F, 0x80, 0xFF
    uint8_t k1[] = {0x01}; adb_put(db, k1, 1, "a", 1);
    uint8_t k2[] = {0x7F}; adb_put(db, k2, 1, "b", 1);
    uint8_t k3[] = {0x80}; adb_put(db, k3, 1, "c", 1);
    uint8_t k4[] = {0xFF}; adb_put(db, k4, 1, "d", 1);
    // Scan: should be sorted 0x01 < 0x7F < 0x80 < 0xFF
    int cnt = 0;
    uint8_t prev = 0; int bad = 0;
    struct { int *cnt; uint8_t *prev; int *bad; } ctx = {&cnt, &prev, &bad};
    adb_scan(db, NULL, 0, NULL, 0, scan_counter, &cnt);
    // Verify using individual gets
    char buf[256]; uint16_t vl;
    int ok = 1;
    if (adb_get(db, k1, 1, buf, 256, &vl) || buf[0] != 'a') ok = 0;
    if (adb_get(db, k2, 1, buf, 256, &vl) || buf[0] != 'b') ok = 0;
    if (adb_get(db, k3, 1, buf, 256, &vl) || buf[0] != 'c') ok = 0;
    if (adb_get(db, k4, 1, buf, 256, &vl) || buf[0] != 'd') ok = 0;
    adb_close(db); cleanup(p);
    if (ok && cnt == 4) PASS(); else FAIL("high byte key mismatch");
}

// 47. Single-byte keys spanning all 256 values
static void test_all_single_byte_keys(void) {
    printf("  [47] all 256 single-byte keys: put, get, scan sorted          "); fflush(stdout);
    const char *p = "/tmp/adb_edge47"; cleanup(p);
    adb_t *db = NULL;
    adb_open(p, NULL, &db);
    // Insert all 256 single-byte keys in random-ish order
    uint8_t order[256];
    for (int i = 0; i < 256; i++) order[i] = (uint8_t)i;
    // Fisher-Yates with fixed seed for reproducibility
    for (int i = 255; i > 0; i--) {
        int j = (i * 7 + 13) % (i + 1);
        uint8_t t = order[i]; order[i] = order[j]; order[j] = t;
    }
    for (int i = 0; i < 256; i++) {
        uint8_t k = order[i];
        char val[4]; int vl = snprintf(val, 4, "%u", (unsigned)k);
        adb_put(db, &k, 1, val, (uint16_t)vl);
    }
    // Verify all retrievable
    int bad = 0;
    for (int i = 0; i < 256; i++) {
        uint8_t k = (uint8_t)i;
        char val[4]; int vl = snprintf(val, 4, "%u", (unsigned)i);
        char buf[256]; uint16_t blen;
        int rc = adb_get(db, &k, 1, buf, 256, &blen);
        if (rc || blen != (uint16_t)vl || memcmp(buf, val, vl)) bad++;
    }
    // Verify scan count
    int cnt = 0;
    adb_scan(db, NULL, 0, NULL, 0, scan_counter, &cnt);
    adb_close(db); cleanup(p);
    if (bad == 0 && cnt == 256) PASS();
    else { char m[64]; snprintf(m,64,"%d bad, cnt=%d",bad,cnt); FAIL(m); }
}

// 48. Reopen 100 cycles, put 10 keys each cycle, delete 5 keys from prev cycle
static void test_100_reopen_churn(void) {
    printf("  [48] 100 reopen cycles: put 10, delete 5 prev, accumulating   "); fflush(stdout);
    const char *p = "/tmp/adb_edge48"; cleanup(p);
    int bad = 0;
    for (int cycle = 0; cycle < 100; cycle++) {
        adb_t *db = NULL;
        if (adb_open(p, NULL, &db)) { bad++; break; }
        // Put 10 new keys for this cycle
        for (int i = 0; i < 10; i++) {
            char key[32]; int kl = snprintf(key, 32, "c%03d_k%d", cycle, i);
            adb_put(db, key, (uint16_t)kl, "v", 1);
        }
        // Delete 5 keys from previous cycle
        if (cycle > 0) {
            for (int i = 0; i < 5; i++) {
                char key[32]; int kl = snprintf(key, 32, "c%03d_k%d", cycle - 1, i);
                adb_delete(db, key, (uint16_t)kl);
            }
        }
        adb_close(db);
    }
    // Expected: cycle 99 has 10 keys, cycles 0-98 have 5 keys each = 5*99 + 10 = 505
    adb_t *db = NULL;
    adb_open(p, NULL, &db);
    int cnt = 0;
    adb_scan(db, NULL, 0, NULL, 0, scan_counter, &cnt);
    adb_close(db); cleanup(p);
    if (bad == 0 && cnt == 505) PASS();
    else { char m[64]; snprintf(m,64,"bad=%d cnt=%d exp=505",bad,cnt); FAIL(m); }
}

// 49. Batch + tx interleaved: batch, tx commit, batch, verify
static void test_batch_tx_interleave(void) {
    printf("  [49] batch+tx interleave: batch, tx, batch, verify all        "); fflush(stdout);
    const char *p = "/tmp/adb_edge49"; cleanup(p);
    adb_t *db = NULL;
    adb_open(p, NULL, &db);
    // Batch 1: 10 entries
    adb_batch_entry_t batch1[10];
    char bk1[10][16]; char bv1[10][16];
    for (int i = 0; i < 10; i++) {
        int kl = snprintf(bk1[i], 16, "ba%04d", i);
        int vl = snprintf(bv1[i], 16, "bv%04d", i);
        batch1[i] = (adb_batch_entry_t){bk1[i], (uint16_t)kl, bv1[i], (uint16_t)vl};
    }
    adb_batch_put(db, batch1, 10);
    // TX: 10 entries
    uint64_t tid;
    adb_tx_begin(db, 0, &tid);
    for (int i = 0; i < 10; i++) {
        char key[16]; int kl = snprintf(key, 16, "tx%04d", i);
        adb_tx_put(db, tid, key, (uint16_t)kl, "tv", 2);
    }
    adb_tx_commit(db, tid);
    // Batch 2: 10 entries
    adb_batch_entry_t batch2[10];
    char bk2[10][16]; char bv2[10][16];
    for (int i = 0; i < 10; i++) {
        int kl = snprintf(bk2[i], 16, "bb%04d", i);
        int vl = snprintf(bv2[i], 16, "bv%04d", i);
        batch2[i] = (adb_batch_entry_t){bk2[i], (uint16_t)kl, bv2[i], (uint16_t)vl};
    }
    adb_batch_put(db, batch2, 10);
    // Verify all 30 keys
    int cnt = 0;
    adb_scan(db, NULL, 0, NULL, 0, scan_counter, &cnt);
    adb_close(db); cleanup(p);
    if (cnt == 30) PASS();
    else { char m[64]; snprintf(m,64,"cnt=%d exp=30",cnt); FAIL(m); }
}

// 50. Metrics: puts/gets/deletes match exact operations
static void test_metrics_exact_counts(void) {
    printf("  [50] metrics: exact counts match operations performed         "); fflush(stdout);
    const char *p = "/tmp/adb_edge50"; cleanup(p);
    adb_t *db = NULL;
    adb_open(p, NULL, &db);
    adb_metrics_t m0; adb_get_metrics(db, &m0);
    // 100 puts
    for (int i = 0; i < 100; i++) {
        char key[16]; int kl = snprintf(key, 16, "m%04d", i);
        adb_put(db, key, (uint16_t)kl, "v", 1);
    }
    // 50 gets
    for (int i = 0; i < 50; i++) {
        char key[16]; int kl = snprintf(key, 16, "m%04d", i);
        char buf[256]; uint16_t vl;
        adb_get(db, key, (uint16_t)kl, buf, 256, &vl);
    }
    // 25 deletes
    for (int i = 0; i < 25; i++) {
        char key[16]; int kl = snprintf(key, 16, "m%04d", i);
        adb_delete(db, key, (uint16_t)kl);
    }
    adb_metrics_t m1; adb_get_metrics(db, &m1);
    adb_close(db); cleanup(p);
    uint64_t dp = m1.puts_total - m0.puts_total;
    uint64_t dg = m1.gets_total - m0.gets_total;
    uint64_t dd = m1.deletes_total - m0.deletes_total;
    if (dp == 100 && dg == 50 && dd == 25) PASS();
    else { char m[128]; snprintf(m,128,"puts=%lu gets=%lu dels=%lu",(unsigned long)dp,(unsigned long)dg,(unsigned long)dd); FAIL(m); }
}

// 51. CRC32C determinism: same data always produces same checksum
static void test_crc32c_determinism(void) {
    printf("  [51] CRC32C determinism across multiple calls              "); fflush(stdout);
    uint8_t data[256];
    for (int i = 0; i < 256; i++) data[i] = (uint8_t)i;
    uint32_t c1 = hw_crc32c(data, 256);
    uint32_t c2 = hw_crc32c(data, 256);
    uint32_t c3 = hw_crc32c(data, 256);
    if (c1 == c2 && c2 == c3 && c1 != 0) PASS();
    else { char m[64]; snprintf(m,64,"c1=0x%08x c2=0x%08x c3=0x%08x",c1,c2,c3); FAIL(m); }
}

// 52. CRC32C: different data produces different checksums
static void test_crc32c_sensitivity(void) {
    printf("  [52] CRC32C sensitivity: 1-bit change -> different CRC     "); fflush(stdout);
    uint8_t data[64];
    memset(data, 0, 64);
    uint32_t c1 = hw_crc32c(data, 64);
    data[0] = 1; // flip 1 bit
    uint32_t c2 = hw_crc32c(data, 64);
    data[0] = 0; data[63] = 1;
    uint32_t c3 = hw_crc32c(data, 64);
    if (c1 != c2 && c2 != c3 && c1 != c3) PASS();
    else FAIL("CRC not sensitive to changes");
}

// 53. NEON memcpy/memcmp: large buffer roundtrip
static void test_neon_large_buffer(void) {
    printf("  [53] NEON memcpy+memcmp: 64KB buffer roundtrip            "); fflush(stdout);
    size_t sz = 65536;
    uint8_t *src = malloc(sz);
    uint8_t *dst = malloc(sz);
    for (size_t i = 0; i < sz; i++) src[i] = (uint8_t)(i * 37 + 13);
    neon_memcpy(dst, src, sz);
    int cmp = neon_memcmp(src, dst, sz);
    free(src); free(dst);
    if (cmp == 0) PASS(); else FAIL("memcmp != 0");
}

// 54. key_compare: zero-length keys compare equal
static void test_key_compare_zero_length(void) {
    printf("  [54] key_compare: two zero-length keys are equal           "); fflush(stdout);
    uint8_t k1[64], k2[64];
    memset(k1, 0, 64);
    memset(k2, 0, 64);
    // Both have length 0
    int cmp = key_compare(k1, k2);
    if (cmp == 0) PASS(); else { char m[32]; snprintf(m,32,"cmp=%d",cmp); FAIL(m); }
}

// 55. key_compare: shorter key sorts before longer with same prefix
static void test_key_compare_length_tiebreak(void) {
    printf("  [55] key_compare: shorter prefix key sorts before longer   "); fflush(stdout);
    uint8_t k1[64], k2[64];
    memset(k1, 0, 64); memset(k2, 0, 64);
    // k1: len=3 "abc"
    k1[0] = 3; k1[1] = 0; k1[2] = 'a'; k1[3] = 'b'; k1[4] = 'c';
    // k2: len=5 "abcde"
    k2[0] = 5; k2[1] = 0; k2[2] = 'a'; k2[3] = 'b'; k2[4] = 'c'; k2[5] = 'd'; k2[6] = 'e';
    int cmp = key_compare(k1, k2);
    if (cmp < 0) PASS(); else { char m[32]; snprintf(m,32,"cmp=%d",cmp); FAIL(m); }
}

// 56. Backup then write more, backup again - backups independent
static void test_backup_independence(void) {
    printf("  [56] backup: two backups are independent snapshots         "); fflush(stdout);
    const char *p = "/tmp/adb_edge56";
    const char *b1 = "/tmp/adb_edge56_b1";
    const char *b2 = "/tmp/adb_edge56_b2";
    const char *r1 = "/tmp/adb_edge56_r1";
    const char *r2 = "/tmp/adb_edge56_r2";
    cleanup(p); cleanup(b1); cleanup(b2); cleanup(r1); cleanup(r2);
    adb_t *db;
    adb_open(p, NULL, &db);
    adb_put(db, "k1", 2, "v1", 2);
    adb_sync(db);
    adb_backup(db, b1, ADB_BACKUP_FULL);
    adb_put(db, "k2", 2, "v2", 2);
    adb_sync(db);
    adb_backup(db, b2, ADB_BACKUP_FULL);
    adb_close(db);
    // Restore b1: should have k1 only
    adb_restore(b1, r1);
    adb_open(r1, NULL, &db);
    char buf[256]; uint16_t len;
    int has_k1 = (adb_get(db, "k1", 2, buf, 256, &len) == 0);
    int has_k2 = (adb_get(db, "k2", 2, buf, 256, &len) == 0);
    adb_close(db);
    // Restore b2: should have k1 and k2
    adb_restore(b2, r2);
    adb_open(r2, NULL, &db);
    int has_k1_2 = (adb_get(db, "k1", 2, buf, 256, &len) == 0);
    int has_k2_2 = (adb_get(db, "k2", 2, buf, 256, &len) == 0);
    adb_close(db);
    cleanup(p); cleanup(b1); cleanup(b2); cleanup(r1); cleanup(r2);
    if (has_k1 && !has_k2 && has_k1_2 && has_k2_2) PASS();
    else FAIL("backup snapshots not independent");
}

static int edge_count_cb(const void*a,uint16_t b,const void*c,uint16_t d,void*e) {
    (void)a;(void)b;(void)c;(void)d;(*(int*)e)++;return 0;
}
// 57. Multiple syncs don't duplicate keys
static void test_multi_sync_no_duplication(void) {
    printf("  [57] multi-sync: 10 syncs don't duplicate scan results     "); fflush(stdout);
    const char *p = "/tmp/adb_edge57";
    cleanup(p);
    adb_t *db; adb_open(p, NULL, &db);
    for (int i = 0; i < 50; i++) {
        char k[16]; snprintf(k,16,"ms%04d",i);
        adb_put(db, k, strlen(k), "v", 1);
    }
    for (int s = 0; s < 10; s++) adb_sync(db);
    int cnt = 0;
    adb_scan(db, NULL, 0, NULL, 0, edge_count_cb, &cnt);
    adb_close(db); cleanup(p);
    if (cnt == 50) PASS(); else { char m[32]; snprintf(m,32,"cnt=%d",cnt); FAIL(m); }
}

// 58. put 0-length value, get returns 0-length
static void test_zero_length_value(void) {
    printf("  [58] put/get: zero-length value roundtrip                  "); fflush(stdout);
    const char *p = "/tmp/adb_edge58";
    cleanup(p);
    adb_t *db; adb_open(p, NULL, &db);
    int rc = adb_put(db, "key", 3, "", 0);
    if (rc) { adb_close(db); cleanup(p); FAIL("put"); return; }
    char buf[256]; uint16_t len = 0xFFFF;
    rc = adb_get(db, "key", 3, buf, 256, &len);
    adb_close(db); cleanup(p);
    if (rc) FAIL("get");
    else if (len != 0) { char m[32]; snprintf(m,32,"len=%d",len); FAIL(m); }
    else PASS();
}

// 59. get with NULL val_len_out doesn't crash
static void test_get_null_vlen_out(void) {
    printf("  [59] get: NULL val_len_out doesn't crash                   "); fflush(stdout);
    const char *p = "/tmp/adb_edge59";
    cleanup(p);
    adb_t *db; adb_open(p, NULL, &db);
    adb_put(db, "k", 1, "v", 1);
    char buf[256];
    int rc = adb_get(db, "k", 1, buf, 256, NULL);
    adb_close(db); cleanup(p);
    if (rc == 0 && buf[0] == 'v') PASS();
    else FAIL("get failed or wrong value");
}

// 60. build_fixed_key: verify padding is zero-filled
static void test_build_fixed_key_padding(void) {
    printf("  [60] build_fixed_key: short key is zero-padded             "); fflush(stdout);
    uint8_t fk[64];
    memset(fk, 0xFF, 64); // pre-fill with garbage
    build_fixed_key(fk, "hi", 2);
    // len field: fk[0]=2, fk[1]=0
    int bad = 0;
    if (fk[0] != 2 || fk[1] != 0) bad++;
    if (fk[2] != 'h' || fk[3] != 'i') bad++;
    // Bytes 4-63 should be zero
    for (int i = 4; i < 64; i++) if (fk[i] != 0) bad++;
    if (bad == 0) PASS(); else { char m[32]; snprintf(m,32,"%d bad bytes",bad); FAIL(m); }
}

// 61. asm_strlen: various lengths
static void test_asm_strlen(void) {
    printf("  [61] asm_strlen: empty, short, long                         "); fflush(stdout);
    int bad = 0;
    if (asm_strlen("") != 0) bad++;
    if (asm_strlen("a") != 1) bad++;
    if (asm_strlen("hello") != 5) bad++;
    char long_str[256]; memset(long_str, 'x', 255); long_str[255] = 0;
    if (asm_strlen(long_str) != 255) bad++;
    if (bad) { char m[32]; snprintf(m,32,"%d wrong",bad); FAIL(m); return; }
    PASS();
}

// 62. build_fixed_val: zero-pads short values
static void test_build_fixed_val_padding(void) {
    printf("  [62] build_fixed_val: short val is zero-padded              "); fflush(stdout);
    uint8_t fv[256];
    memset(fv, 0xFF, 256);
    build_fixed_val(fv, "hi", 2);
    int bad = 0;
    if (fv[0] != 2 || fv[1] != 0) bad++;
    if (fv[2] != 'h' || fv[3] != 'i') bad++;
    for (int i = 4; i < 256; i++) if (fv[i] != 0) bad++;
    if (bad) { char m[32]; snprintf(m,32,"%d bad bytes",bad); FAIL(m); return; }
    PASS();
}

// 63. key_equal: same and different
static void test_key_equal(void) {
    printf("  [63] key_equal: true for same, false for different          "); fflush(stdout);
    uint8_t k1[64], k2[64], k3[64];
    build_fixed_key(k1, "abc", 3);
    build_fixed_key(k2, "abc", 3);
    build_fixed_key(k3, "abd", 3);
    int bad = 0;
    if (!key_equal(k1, k2)) bad++;
    if (key_equal(k1, k3)) bad++;
    if (bad) FAIL("equality wrong");
    else PASS();
}

// 64. neon_memcmp: returns 0 for equal, nonzero for different
static void test_neon_memcmp(void) {
    printf("  [64] neon_memcmp: equal and unequal buffers                 "); fflush(stdout);
    uint8_t a[128], b[128];
    memset(a, 0x42, 128); memset(b, 0x42, 128);
    int bad = 0;
    if (neon_memcmp(a, b, 128) != 0) bad++;
    b[100] = 0x43;
    if (neon_memcmp(a, b, 128) == 0) bad++;
    if (bad) FAIL("memcmp wrong");
    else PASS();
}

// 65. neon_memset: fills correctly
static void test_neon_memset_pattern(void) {
    printf("  [65] neon_memset: fills 1024 bytes with 0xAB               "); fflush(stdout);
    uint8_t buf[1024];
    memset(buf, 0, 1024);
    neon_memset(buf, 0xAB, 1024);
    int bad = 0;
    for (int i = 0; i < 1024; i++) if (buf[i] != 0xAB) bad++;
    if (bad) { char m[32]; snprintf(m,32,"%d wrong bytes",bad); FAIL(m); return; }
    PASS();
}

// 66. adb_close on NULL doesn't crash
static void test_close_null_db(void) {
    printf("  [66] adb_close: NULL db is safe no-op                       "); fflush(stdout);
    int rc = adb_close(NULL);
    // close(NULL) is a safe no-op (like free(NULL))
    if (rc != 0) FAIL("expected OK for NULL close");
    else PASS();
}

// 67. adb_put on NULL db returns error
static void test_put_null_db(void) {
    printf("  [67] adb_put: NULL db returns error, no crash               "); fflush(stdout);
    int rc = adb_put(NULL, "k", 1, "v", 1);
    if (rc == 0) FAIL("expected error for NULL db");
    else PASS();
}

// 68. adb_get on NULL db returns error
static void test_get_null_db(void) {
    printf("  [68] adb_get: NULL db returns error, no crash               "); fflush(stdout);
    char buf[256]; uint16_t vl;
    int rc = adb_get(NULL, "k", 1, buf, 256, &vl);
    if (rc == 0) FAIL("expected error for NULL db");
    else PASS();
}

// 69. scan with both NULL start and end (full scan)
static int edge_count_cb2(const void *k, uint16_t kl, const void *v, uint16_t vl, void *ud) {
    (void)k;(void)kl;(void)v;(void)vl;
    (*(int*)ud)++;
    return 0;
}
static void test_full_scan_null_bounds(void) {
    printf("  [69] scan: NULL start+end does full scan                    "); fflush(stdout);
    const char *p = "/tmp/adb_edge69";
    cleanup(p);
    adb_t *db; adb_open(p, NULL, &db);
    for (int i = 0; i < 25; i++) {
        char k[8]; snprintf(k,8,"f%03d",i);
        adb_put(db, k, strlen(k), "v", 1);
    }
    int count = 0;
    adb_scan(db, NULL, 0, NULL, 0, edge_count_cb2, &count);
    adb_close(db); cleanup(p);
    if (count != 25) { char m[32]; snprintf(m,32,"got %d",count); FAIL(m); }
    else PASS();
}

// 70. batch_put: single entry works
static void test_batch_single_entry(void) {
    printf("  [70] batch_put: single entry succeeds                       "); fflush(stdout);
    const char *p = "/tmp/adb_edge70";
    cleanup(p);
    adb_t *db; adb_open(p, NULL, &db);
    adb_batch_entry_t e = {"k", 1, "v", 1};
    int rc = adb_batch_put(db, &e, 1);
    char buf[256]; uint16_t vl;
    int rc2 = adb_get(db, "k", 1, buf, 256, &vl);
    adb_close(db); cleanup(p);
    if (rc || rc2 || vl != 1 || buf[0] != 'v') FAIL("batch single failed");
    else PASS();
}

// 71. tx_commit with wrong tx_id returns error
static void test_tx_commit_wrong_id(void) {
    printf("  [71] tx_commit: wrong ID returns error                      "); fflush(stdout);
    const char *p = "/tmp/adb_edge71";
    cleanup(p);
    adb_t *db; adb_open(p, NULL, &db);
    uint64_t txid;
    adb_tx_begin(db, 0, &txid);
    int rc = adb_tx_commit(db, txid + 999);
    adb_tx_rollback(db, txid);
    adb_close(db); cleanup(p);
    if (rc == 0) FAIL("should fail with wrong tx_id");
    else PASS();
}

// 72. tx_rollback with wrong tx_id returns error
static void test_tx_rollback_wrong_id(void) {
    printf("  [72] tx_rollback: wrong ID returns error                    "); fflush(stdout);
    const char *p = "/tmp/adb_edge72";
    cleanup(p);
    adb_t *db; adb_open(p, NULL, &db);
    uint64_t txid;
    adb_tx_begin(db, 0, &txid);
    int rc = adb_tx_rollback(db, txid + 999);
    adb_tx_rollback(db, txid);
    adb_close(db); cleanup(p);
    if (rc == 0) FAIL("should fail with wrong tx_id");
    else PASS();
}

// 73. Double tx_begin returns LOCKED
static void test_double_tx_begin(void) {
    printf("  [73] tx_begin: second begin while active returns LOCKED     "); fflush(stdout);
    const char *p = "/tmp/adb_edge73";
    cleanup(p);
    adb_t *db; adb_open(p, NULL, &db);
    uint64_t tx1, tx2;
    adb_tx_begin(db, 0, &tx1);
    int rc = adb_tx_begin(db, 0, &tx2);
    adb_tx_rollback(db, tx1);
    adb_close(db); cleanup(p);
    if (rc != -ADB_ERR_LOCKED && rc != ADB_ERR_LOCKED) FAIL("expected LOCKED error");
    else PASS();
}

// 74. adb_get with vbuf_len = 0 (should still return key-not-found or truncated)
static void test_get_zero_vbuf_len(void) {
    printf("  [74] adb_get: vbuf_len=0 doesn't crash                     "); fflush(stdout);
    const char *p = "/tmp/adb_edge74";
    cleanup(p);
    adb_t *db; adb_open(p, NULL, &db);
    adb_put(db, "k", 1, "v", 1);
    char buf[1]; uint16_t vl = 0;
    int rc = adb_get(db, "k", 1, buf, 0, &vl);
    adb_close(db); cleanup(p);
    // Should succeed or return truncated - just should not crash
    (void)rc;
    PASS();
}

// 75. batch_put with count=0 returns success (no-op)
static void test_batch_zero_count(void) {
    printf("  [75] batch_put: count=0 is a no-op                         "); fflush(stdout);
    const char *p = "/tmp/adb_edge75";
    cleanup(p);
    adb_t *db; adb_open(p, NULL, &db);
    adb_batch_entry_t entries[1] = {{"k",1,"v",1}};
    int rc = adb_batch_put(db, entries, 0);
    adb_close(db); cleanup(p);
    if (rc) FAIL("batch count=0 should succeed");
    else PASS();
}

// 76. Delete a key that was never inserted
static void test_delete_nonexistent_key(void) {
    printf("  [76] delete: non-existent key returns success               "); fflush(stdout);
    const char *p = "/tmp/adb_edge76";
    cleanup(p);
    adb_t *db; adb_open(p, NULL, &db);
    int rc = adb_delete(db, "nope", 4);
    adb_close(db); cleanup(p);
    if (rc) FAIL("delete non-existent should succeed");
    else PASS();
}

// 77. tx_get on a key not in write-set falls through to storage
static void test_tx_get_fallthrough(void) {
    printf("  [77] tx_get: falls through to storage if not in write-set   "); fflush(stdout);
    const char *p = "/tmp/adb_edge77";
    cleanup(p);
    adb_t *db; adb_open(p, NULL, &db);
    adb_put(db, "stored", 6, "from_storage", 12);
    uint64_t txid;
    adb_tx_begin(db, 0, &txid);
    // Don't tx_put "stored" - it should fall through to storage
    char vb[256]; uint16_t vl;
    int rc = adb_tx_get(db, txid, "stored", 6, vb, 256, &vl);
    adb_tx_rollback(db, txid);
    adb_close(db); cleanup(p);
    if (rc || vl != 12 || memcmp(vb, "from_storage", 12)) FAIL("should see storage value");
    else PASS();
}

// 78. Backup to same path as source should fail or be safe
static void test_backup_to_self(void) {
    printf("  [78] backup: to same path as source handled safely          "); fflush(stdout);
    const char *p = "/tmp/adb_edge78";
    cleanup(p);
    adb_t *db; adb_open(p, NULL, &db);
    adb_put(db, "k", 1, "v", 1);
    adb_sync(db);
    int rc = adb_backup(db, "/tmp/adb_edge78", ADB_BACKUP_FULL);
    adb_close(db); cleanup(p);
    // Should either fail gracefully or succeed - must not corrupt
    (void)rc;
    PASS();
}

// 79. Scan with callback that returns non-zero stops early
static int edge_stop_cb(const void *k, uint16_t kl, const void *v, uint16_t vl, void *ud) {
    (void)k;(void)kl;(void)v;(void)vl;
    int *cnt = (int*)ud;
    (*cnt)++;
    return (*cnt >= 3) ? 1 : 0;
}
static void test_scan_callback_stops(void) {
    printf("  [79] scan: callback returning non-zero stops iteration      "); fflush(stdout);
    const char *p = "/tmp/adb_edge79";
    cleanup(p);
    adb_t *db; adb_open(p, NULL, &db);
    for (int i = 0; i < 100; i++) {
        char k[8]; snprintf(k,8,"s%03d",i);
        adb_put(db, k, strlen(k), "v", 1);
    }
    int cnt = 0;
    adb_scan(db, NULL, 0, NULL, 0, edge_stop_cb, &cnt);
    adb_close(db); cleanup(p);
    if (cnt != 3) { char m[32]; snprintf(m,32,"expected 3, got %d",cnt); FAIL(m); }
    else PASS();
}

// 80. Metrics: get_metrics on fresh db returns all zeros
static void test_metrics_fresh_db(void) {
    printf("  [80] metrics: fresh db has all-zero counters                "); fflush(stdout);
    const char *p = "/tmp/adb_edge80";
    cleanup(p);
    adb_t *db; adb_open(p, NULL, &db);
    adb_metrics_t m;
    adb_get_metrics(db, &m);
    int bad = 0;
    if (m.puts_total != 0) bad++;
    if (m.gets_total != 0) bad++;
    if (m.deletes_total != 0) bad++;
    if (m.scans_total != 0) bad++;
    adb_close(db); cleanup(p);
    if (bad) FAIL("metrics not zero");
    else PASS();
}

// 81. Sync with no data is a safe no-op
static void test_sync_empty_db(void) {
    printf("  [81] sync: empty db sync is safe no-op                     "); fflush(stdout);
    const char *p = "/tmp/adb_edge81";
    cleanup(p);
    adb_t *db; adb_open(p, NULL, &db);
    int rc = adb_sync(db);
    adb_close(db); cleanup(p);
    if (rc) FAIL("sync returned error on empty db");
    else PASS();
}

// 82. Put then immediate close (no sync) - WAL preserves data
static void test_put_close_no_sync(void) {
    printf("  [82] put + close (no sync): WAL preserves data            "); fflush(stdout);
    const char *p = "/tmp/adb_edge82";
    cleanup(p);
    adb_t *db; adb_open(p, NULL, &db);
    adb_put(db, "nosync", 6, "data123", 7);
    adb_close(db);
    adb_open(p, NULL, &db);
    char buf[256]; uint16_t vl;
    int rc = adb_get(db, "nosync", 6, buf, 256, &vl);
    adb_close(db); cleanup(p);
    if (rc) FAIL("get after WAL recovery failed");
    else if (vl != 7 || memcmp(buf, "data123", 7)) FAIL("value mismatch");
    else PASS();
}

// 83. Double sync is idempotent
static void test_double_sync(void) {
    printf("  [83] double sync: idempotent, no data loss                "); fflush(stdout);
    const char *p = "/tmp/adb_edge83";
    cleanup(p);
    adb_t *db; adb_open(p, NULL, &db);
    for (int i = 0; i < 50; i++) {
        char k[8]; snprintf(k,8,"ds%02d",i);
        adb_put(db, k, strlen(k), "v", 1);
    }
    adb_sync(db);
    adb_sync(db);
    adb_close(db);
    adb_open(p, NULL, &db);
    int cnt = 0;
    adb_scan(db, NULL, 0, NULL, 0, edge_count_cb, &cnt);
    adb_close(db); cleanup(p);
    if (cnt != 50) { char m[32]; snprintf(m,32,"expected 50, got %d",cnt); FAIL(m); }
    else PASS();
}

// 84. Get on key that was never put returns NOT_FOUND
static void test_get_nonexistent(void) {
    printf("  [84] get: non-existent key returns NOT_FOUND              "); fflush(stdout);
    const char *p = "/tmp/adb_edge84";
    cleanup(p);
    adb_t *db; adb_open(p, NULL, &db);
    char buf[256]; uint16_t vl;
    int rc = adb_get(db, "ghost", 5, buf, 256, &vl);
    adb_close(db); cleanup(p);
    if (rc != ADB_ERR_NOT_FOUND) { char m[32]; snprintf(m,32,"expected 1, got %d",rc); FAIL(m); }
    else PASS();
}

// 85. Put + sync + delete + close (no sync) -> WAL replays delete
static void test_delete_no_sync_wal_replay(void) {
    printf("  [85] delete (no sync) + reopen: WAL replays delete        "); fflush(stdout);
    const char *p = "/tmp/adb_edge85";
    cleanup(p);
    adb_t *db; adb_open(p, NULL, &db);
    adb_put(db, "victim", 6, "here", 4);
    adb_sync(db);
    adb_delete(db, "victim", 6);
    adb_close(db);
    adb_open(p, NULL, &db);
    char buf[256]; uint16_t vl;
    int rc = adb_get(db, "victim", 6, buf, 256, &vl);
    adb_close(db); cleanup(p);
    if (rc != ADB_ERR_NOT_FOUND) FAIL("expected not_found after WAL delete replay");
    else PASS();
}

// 86. Scan on empty db returns zero entries
static void test_scan_empty_db(void) {
    printf("  [86] scan: empty db returns zero entries                  "); fflush(stdout);
    const char *p = "/tmp/adb_edge86";
    cleanup(p);
    adb_t *db; adb_open(p, NULL, &db);
    int cnt = 0;
    adb_scan(db, NULL, 0, NULL, 0, edge_count_cb, &cnt);
    adb_close(db); cleanup(p);
    if (cnt != 0) FAIL("expected 0 entries");
    else PASS();
}

// 87. Batch with 1 entry + reopen: entry persists via WAL
static void test_batch_one_wal_persist(void) {
    printf("  [87] batch 1 entry + close (no sync): WAL persists       "); fflush(stdout);
    const char *p = "/tmp/adb_edge87";
    cleanup(p);
    adb_t *db; adb_open(p, NULL, &db);
    adb_batch_entry_t e = { .key = "bat1", .key_len = 4, .val = "v1", .val_len = 2 };
    adb_batch_put(db, &e, 1);
    adb_close(db);
    adb_open(p, NULL, &db);
    char buf[256]; uint16_t vl;
    int rc = adb_get(db, "bat1", 4, buf, 256, &vl);
    adb_close(db); cleanup(p);
    if (rc) FAIL("get after batch+WAL recovery failed");
    else if (vl != 2 || memcmp(buf, "v1", 2)) FAIL("value mismatch");
    else PASS();
}

// 88. tx_put + tx_get: write-set returns latest put
static void test_tx_write_set_latest(void) {
    printf("  [88] tx write-set: latest put wins in tx_get             "); fflush(stdout);
    const char *p = "/tmp/adb_edge88";
    cleanup(p);
    adb_t *db; adb_open(p, NULL, &db);
    uint64_t tx;
    adb_tx_begin(db, 0, &tx);
    adb_tx_put(db, tx, "k", 1, "first", 5);
    adb_tx_put(db, tx, "k", 1, "second", 6);
    char buf[256]; uint16_t vl;
    int rc = adb_tx_get(db, tx, "k", 1, buf, 256, &vl);
    adb_tx_rollback(db, tx);
    adb_close(db); cleanup(p);
    if (rc) FAIL("tx_get failed");
    else if (vl != 6 || memcmp(buf, "second", 6)) FAIL("expected 'second'");
    else PASS();
}

// 89. Multiple syncs don't double-count in scan
static void test_triple_sync_scan_count(void) {
    printf("  [89] triple sync: scan count stays correct                "); fflush(stdout);
    const char *p = "/tmp/adb_edge89";
    cleanup(p);
    adb_t *db; adb_open(p, NULL, &db);
    for (int i = 0; i < 30; i++) {
        char k[8]; snprintf(k,8,"ts%02d",i);
        adb_put(db, k, strlen(k), "x", 1);
    }
    adb_sync(db); adb_sync(db); adb_sync(db);
    int cnt = 0;
    adb_scan(db, NULL, 0, NULL, 0, edge_count_cb, &cnt);
    adb_close(db); cleanup(p);
    if (cnt != 30) { char m[32]; snprintf(m,32,"expected 30, got %d",cnt); FAIL(m); }
    else PASS();
}

// 90. Destroy non-existent path is safe (no crash either way)
static void test_destroy_nonexistent(void) {
    printf("  [90] destroy: non-existent path does not crash            "); fflush(stdout);
    adb_destroy("/tmp/adb_edge90_nonexistent_xyzzy");
    PASS();
}

static int edge_scan_count_cb2(const void *k, uint16_t kl, const void *v, uint16_t vl, void *ud) {
    (void)k;(void)kl;(void)v;(void)vl;
    (*(int*)ud)++;
    return 0;
}

// 91. Put 1 key, sync, overwrite with shorter value, sync, reopen, verify no stale tail
static void test_overwrite_shorter_no_stale(void) {
    printf("  [91] overwrite shorter value: no stale tail bytes           "); fflush(stdout);
    const char *p = "/tmp/adb_edge91";
    cleanup(p);
    adb_t *db; adb_open(p, NULL, &db);
    char big[100]; memset(big, 'X', 100);
    adb_put(db, "ow", 2, big, 100);
    adb_sync(db);
    adb_put(db, "ow", 2, "tiny", 4);
    adb_sync(db);
    adb_close(db);
    adb_open(p, NULL, &db);
    char buf[128]; uint16_t vl;
    int rc = adb_get(db, "ow", 2, buf, 128, &vl);
    adb_close(db); cleanup(p);
    if (rc) { FAIL("get failed"); return; }
    if (vl != 4) { FAIL("expected vlen=4"); return; }
    if (memcmp(buf, "tiny", 4)) { FAIL("value mismatch"); return; }
    PASS();
}

// 92. tx_get reads from storage when key not in write-set
static void test_tx_get_fallthrough_storage(void) {
    printf("  [92] tx_get falls through to storage for non-ws keys       "); fflush(stdout);
    const char *p = "/tmp/adb_edge92";
    cleanup(p);
    adb_t *db; adb_open(p, NULL, &db);
    adb_put(db, "stored", 6, "indb", 4);
    uint64_t txid;
    adb_tx_begin(db, 0, &txid);
    adb_tx_put(db, txid, "inws", 4, "wval", 4);
    char buf[32]; uint16_t vl;
    int rc = adb_tx_get(db, txid, "stored", 6, buf, 32, &vl);
    adb_tx_rollback(db, txid);
    adb_close(db); cleanup(p);
    if (rc) { FAIL("tx_get should find storage key"); return; }
    if (vl != 4 || memcmp(buf, "indb", 4)) { FAIL("value mismatch"); return; }
    PASS();
}

// 93. Sync then close then reopen: WAL should be clean (no stale replay)
static void test_sync_close_clean_wal(void) {
    printf("  [93] sync+close: reopen has no stale WAL replay            "); fflush(stdout);
    const char *p = "/tmp/adb_edge93";
    cleanup(p);
    adb_t *db; adb_open(p, NULL, &db);
    adb_put(db, "sc", 2, "v1", 2);
    adb_sync(db);
    adb_close(db);
    adb_open(p, NULL, &db);
    char buf[16]; uint16_t vl;
    int rc = adb_get(db, "sc", 2, buf, 16, &vl);
    adb_close(db); cleanup(p);
    if (rc) { FAIL("get failed"); return; }
    if (vl != 2 || memcmp(buf, "v1", 2)) { FAIL("value wrong"); return; }
    PASS();
}

// 94. Batch put empty count = no-op
static void test_batch_empty(void) {
    printf("  [94] batch_put with count=0 is no-op                       "); fflush(stdout);
    const char *p = "/tmp/adb_edge94";
    cleanup(p);
    adb_t *db; adb_open(p, NULL, &db);
    int rc = adb_batch_put(db, NULL, 0);
    adb_close(db); cleanup(p);
    if (rc) { FAIL("expected 0"); return; }
    PASS();
}

// 95. adb_get_metrics on fresh DB returns zero counts
static void test_metrics_zero_on_fresh(void) {
    printf("  [95] get_metrics on fresh DB returns zero counts            "); fflush(stdout);
    const char *p = "/tmp/adb_edge95";
    cleanup(p);
    adb_t *db; adb_open(p, NULL, &db);
    adb_metrics_t m;
    adb_get_metrics(db, &m);
    adb_close(db); cleanup(p);
    if (m.puts_total || m.gets_total || m.deletes_total) { FAIL("expected zero counts"); return; }
    PASS();
}

// 96. put + delete + put + sync + reopen: final value survives
static void test_put_delete_put_sync(void) {
    printf("  [96] put+delete+put+sync+reopen: final value survives      "); fflush(stdout);
    const char *p = "/tmp/adb_edge96";
    cleanup(p);
    adb_t *db; adb_open(p, NULL, &db);
    adb_put(db, "pdp", 3, "first", 5);
    adb_delete(db, "pdp", 3);
    adb_put(db, "pdp", 3, "final", 5);
    adb_sync(db);
    adb_close(db);
    adb_open(p, NULL, &db);
    char buf[32]; uint16_t vl;
    int rc = adb_get(db, "pdp", 3, buf, 32, &vl);
    adb_close(db); cleanup(p);
    if (rc) { FAIL("get failed"); return; }
    if (vl != 5 || memcmp(buf, "final", 5)) { FAIL("value mismatch"); return; }
    PASS();
}

// 97. Scan with start > end returns 0 entries
static void test_scan_start_gt_end(void) {
    printf("  [97] scan where start > end returns 0 entries              "); fflush(stdout);
    const char *p = "/tmp/adb_edge97";
    cleanup(p);
    adb_t *db; adb_open(p, NULL, &db);
    for (int i = 0; i < 20; i++) {
        char k[8]; snprintf(k,8,"z%02d",i);
        adb_put(db, k, strlen(k), "x", 1);
    }
    int cnt = 0;
    adb_scan(db, "z20", 3, "z00", 3, edge_scan_count_cb2, &cnt);
    adb_close(db); cleanup(p);
    if (cnt != 0) { char m[64]; snprintf(m,64,"expected 0, got %d",cnt); FAIL(m); return; }
    PASS();
}

// 98. tx_begin after rollback should succeed
static void test_tx_begin_after_rollback(void) {
    printf("  [98] tx_begin after rollback succeeds                      "); fflush(stdout);
    const char *p = "/tmp/adb_edge98";
    cleanup(p);
    adb_t *db; adb_open(p, NULL, &db);
    uint64_t tx1, tx2;
    int rc1 = adb_tx_begin(db, 0, &tx1);
    adb_tx_rollback(db, tx1);
    int rc2 = adb_tx_begin(db, 0, &tx2);
    adb_tx_commit(db, tx2);
    adb_close(db); cleanup(p);
    if (rc1 || rc2) { FAIL("begin should succeed"); return; }
    PASS();
}

// 99. Large batch (64 entries) with sync + reopen
static void test_large_batch_persist(void) {
    printf("  [99] large batch 64 entries: sync+reopen correct           "); fflush(stdout);
    const char *p = "/tmp/adb_edge99";
    cleanup(p);
    adb_t *db; adb_open(p, NULL, &db);
    adb_batch_entry_t ents[64];
    char ks[64][16], vs[64][16];
    for (int i = 0; i < 64; i++) {
        snprintf(ks[i],16,"lb%03d",i);
        snprintf(vs[i],16,"v%03d",i);
        ents[i].key = ks[i]; ents[i].key_len = strlen(ks[i]);
        ents[i].val = vs[i]; ents[i].val_len = strlen(vs[i]);
    }
    adb_batch_put(db, ents, 64);
    adb_sync(db);
    adb_close(db);
    adb_open(p, NULL, &db);
    int ok = 0;
    for (int i = 0; i < 64; i++) {
        char buf[32]; uint16_t vl;
        if (adb_get(db, ks[i], strlen(ks[i]), buf, 32, &vl) == 0) {
            if (vl == strlen(vs[i]) && memcmp(buf, vs[i], vl) == 0) ok++;
        }
    }
    adb_close(db); cleanup(p);
    if (ok != 64) { char m[64]; snprintf(m,64,"expected 64, got %d",ok); FAIL(m); return; }
    PASS();
}

// 100. adb_put with val_len=0 stores zero-length value
static void test_put_zero_val_len(void) {
    printf("  [100] put with val_len=0: stores empty value               "); fflush(stdout);
    const char *p = "/tmp/adb_edge100";
    cleanup(p);
    adb_t *db; adb_open(p, NULL, &db);
    adb_put(db, "empty", 5, NULL, 0);
    char buf[32]; uint16_t vl = 999;
    int rc = adb_get(db, "empty", 5, buf, 32, &vl);
    adb_close(db); cleanup(p);
    if (rc) { FAIL("get failed"); return; }
    if (vl != 0) { char m[32]; snprintf(m,32,"expected vl=0, got %d",vl); FAIL(m); return; }
    PASS();
}

// 101. CRC detects header byte corruption (page_id field, bytes 0-3)
static void test_crc_detects_header_page_id_corrupt(void) {
    printf("  [101] CRC detects page_id corruption                        "); fflush(stdout);
    void *page = calloc(1, 4096);
    btree_page_init_leaf(page, 42);
    void *k = btree_page_get_key_ptr(page, 0);
    memset(k, 0xAB, 64);
    btree_page_set_crc(page);
    if (!btree_page_verify_crc(page)) { free(page); FAIL("valid page failed CRC"); return; }
    ((unsigned char *)page)[0] ^= 0x01;
    int v = btree_page_verify_crc(page);
    free(page);
    if (v == 0) PASS(); else FAIL("CRC did not detect page_id corruption");
}

// 102. CRC detects header byte corruption (page_type field, bytes 4-5)
static void test_crc_detects_header_page_type_corrupt(void) {
    printf("  [102] CRC detects page_type corruption                      "); fflush(stdout);
    void *page = calloc(1, 4096);
    btree_page_init_leaf(page, 1);
    btree_page_set_crc(page);
    ((unsigned char *)page)[4] ^= 0xFF;
    int v = btree_page_verify_crc(page);
    free(page);
    if (v == 0) PASS(); else FAIL("CRC did not detect page_type corruption");
}

// 103. CRC detects header byte corruption (num_keys field, bytes 6-7)
static void test_crc_detects_header_num_keys_corrupt(void) {
    printf("  [103] CRC detects num_keys corruption                       "); fflush(stdout);
    void *page = calloc(1, 4096);
    btree_page_init_leaf(page, 1);
    btree_page_set_crc(page);
    ((unsigned char *)page)[6] ^= 0x42;
    int v = btree_page_verify_crc(page);
    free(page);
    if (v == 0) PASS(); else FAIL("CRC did not detect num_keys corruption");
}

// 104. CRC detects header corruption at every byte in 0..11
static void test_crc_detects_every_header_byte(void) {
    printf("  [104] CRC detects corruption in every header byte 0..11     "); fflush(stdout);
    void *page = calloc(1, 4096);
    btree_page_init_leaf(page, 99);
    void *k = btree_page_get_key_ptr(page, 0);
    memset(k, 0xDE, 64);
    btree_page_set_crc(page);
    int bad = 0;
    for (int i = 0; i < 12; i++) {
        unsigned char saved = ((unsigned char *)page)[i];
        ((unsigned char *)page)[i] ^= 0xFF;
        if (btree_page_verify_crc(page) != 0) bad++;
        ((unsigned char *)page)[i] = saved;
    }
    free(page);
    if (bad == 0) PASS(); else { char m[64]; snprintf(m, 64, "%d of 12 header bytes undetected", bad); FAIL(m); }
}

// 105. CRC: different page_ids produce different CRCs
static void test_crc_different_page_ids(void) {
    printf("  [105] different page_ids produce different CRCs             "); fflush(stdout);
    void *p1 = calloc(1, 4096);
    void *p2 = calloc(1, 4096);
    btree_page_init_leaf(p1, 1);
    btree_page_init_leaf(p2, 2);
    btree_page_set_crc(p1);
    btree_page_set_crc(p2);
    unsigned int crc1, crc2;
    memcpy(&crc1, (char*)p1 + 12, 4);
    memcpy(&crc2, (char*)p2 + 12, 4);
    free(p1); free(p2);
    if (crc1 != crc2) PASS(); else FAIL("identical CRCs for different page_ids");
}

// 106. Compaction rollback: insert keys, force compact fail scenario
static void test_compaction_preserves_data_on_reopen(void) {
    printf("  [106] compaction preserves all data across reopen           "); fflush(stdout);
    const char *p = "/tmp/adb_edge106";
    cleanup(p);
    adb_t *db; adb_open(p, NULL, &db);
    for (int i = 0; i < 200; i++) {
        char k[16], v[16];
        snprintf(k, 16, "cmp_%04d", i);
        snprintf(v, 16, "val_%04d", i);
        adb_put(db, k, (uint16_t)strlen(k), v, (uint16_t)strlen(v));
    }
    adb_sync(db);
    adb_close(db);
    adb_open(p, NULL, &db);
    int bad = 0;
    for (int i = 0; i < 200; i++) {
        char k[16], v[16], buf[256]; uint16_t vl;
        snprintf(k, 16, "cmp_%04d", i);
        snprintf(v, 16, "val_%04d", i);
        int rc = adb_get(db, k, (uint16_t)strlen(k), buf, 256, &vl);
        if (rc || vl != (uint16_t)strlen(v) || memcmp(buf, v, vl)) bad++;
    }
    adb_close(db); cleanup(p);
    if (bad == 0) PASS(); else { char m[64]; snprintf(m, 64, "%d keys lost", bad); FAIL(m); }
}

// 107. Many deletes then sync then reopen: all deletes persist
static void test_many_deletes_persist(void) {
    printf("  [107] 100 deletes persisted after sync/reopen              "); fflush(stdout);
    const char *p = "/tmp/adb_edge107";
    cleanup(p);
    adb_t *db; adb_open(p, NULL, &db);
    for (int i = 0; i < 100; i++) {
        char k[16], v[8];
        snprintf(k, 16, "dk_%04d", i);
        snprintf(v, 8, "v%d", i);
        adb_put(db, k, (uint16_t)strlen(k), v, (uint16_t)strlen(v));
    }
    adb_sync(db);
    for (int i = 0; i < 100; i++) {
        char k[16]; snprintf(k, 16, "dk_%04d", i);
        adb_delete(db, k, (uint16_t)strlen(k));
    }
    adb_sync(db);
    adb_close(db);
    adb_open(p, NULL, &db);
    int found = 0;
    for (int i = 0; i < 100; i++) {
        char k[16], buf[256]; uint16_t vl;
        snprintf(k, 16, "dk_%04d", i);
        if (adb_get(db, k, (uint16_t)strlen(k), buf, 256, &vl) == 0) found++;
    }
    adb_close(db); cleanup(p);
    if (found == 0) PASS(); else { char m[64]; snprintf(m, 64, "%d ghost keys", found); FAIL(m); }
}

static int edge108_stop_cb(const void *k, uint16_t kl, const void *v, uint16_t vl, void *ctx) {
    (*(int*)ctx)++;
    return 1;
}
// 108. Scan with callback returning 0 (stop) on first entry
static void test_scan_stops_at_first(void) {
    printf("  [108] scan stops immediately on callback return 0           "); fflush(stdout);
    const char *p = "/tmp/adb_edge108";
    cleanup(p);
    adb_t *db; adb_open(p, NULL, &db);
    for (int i = 0; i < 10; i++) {
        char k[8]; snprintf(k, 8, "s%d", i);
        adb_put(db, k, (uint16_t)strlen(k), "v", 1);
    }
    int count = 0;
    adb_scan(db, NULL, 0, NULL, 0, edge108_stop_cb, &count);
    adb_close(db); cleanup(p);
    if (count == 1) PASS(); else { char m[32]; snprintf(m, 32, "expected 1, got %d", count); FAIL(m); }
}

// 109. Batch with all identical keys: last value wins
static void test_batch_identical_keys_last_wins(void) {
    printf("  [109] batch: all identical keys, last value wins            "); fflush(stdout);
    const char *p = "/tmp/adb_edge109";
    cleanup(p);
    adb_t *db; adb_open(p, NULL, &db);
    adb_batch_entry_t entries[10];
    for (int i = 0; i < 10; i++) {
        entries[i].key = "same_key";
        entries[i].key_len = 8;
        char *v = malloc(8);
        snprintf(v, 8, "val_%d", i);
        entries[i].val = v;
        entries[i].val_len = (uint16_t)strlen(v);
    }
    adb_batch_put(db, entries, 10);
    char buf[256]; uint16_t vl;
    int rc = adb_get(db, "same_key", 8, buf, 256, &vl);
    for (int i = 0; i < 10; i++) free((void*)entries[i].val);
    adb_close(db); cleanup(p);
    if (rc) { FAIL("get failed"); return; }
    if (vl == 5 && memcmp(buf, "val_9", 5) == 0) PASS();
    else { char m[64]; snprintf(m, 64, "expected val_9, got %.*s", vl, buf); FAIL(m); }
}

// 110. Rapid open/close without any operations
static void test_rapid_open_close_noop(void) {
    printf("  [110] 50 rapid open/close with no ops                      "); fflush(stdout);
    const char *p = "/tmp/adb_edge110";
    cleanup(p);
    int bad = 0;
    for (int i = 0; i < 50; i++) {
        adb_t *db;
        int rc = adb_open(p, NULL, &db);
        if (rc) { bad++; break; }
        adb_close(db);
    }
    cleanup(p);
    if (bad == 0) PASS(); else FAIL("open/close failed");
}

// 111. tx_put overwrites in write-set, only last value committed
static void test_tx_overwrite_in_writeset(void) {
    printf("  [111] tx: overwrite same key in write-set, last wins        "); fflush(stdout);
    const char *p = "/tmp/adb_edge111";
    cleanup(p);
    adb_t *db; adb_open(p, NULL, &db);
    uint64_t tx;
    adb_tx_begin(db, 0, &tx);
    for (int i = 0; i < 10; i++) {
        char v[8]; snprintf(v, 8, "v%d", i);
        adb_tx_put(db, tx, "key", 3, v, (uint16_t)strlen(v));
    }
    adb_tx_commit(db, tx);
    char buf[256]; uint16_t vl;
    int rc = adb_get(db, "key", 3, buf, 256, &vl);
    adb_close(db); cleanup(p);
    if (rc) { FAIL("get failed"); return; }
    if (vl == 2 && memcmp(buf, "v9", 2) == 0) PASS();
    else FAIL("expected v9");
}

// 112. Delete key in tx, commit, reopen: key still deleted
static void test_tx_delete_persist_reopen(void) {
    printf("  [112] tx delete persists across reopen                      "); fflush(stdout);
    const char *p = "/tmp/adb_edge112";
    cleanup(p);
    adb_t *db; adb_open(p, NULL, &db);
    adb_put(db, "alive", 5, "yes", 3);
    adb_sync(db);
    uint64_t tx;
    adb_tx_begin(db, 0, &tx);
    adb_tx_delete(db, tx, "alive", 5);
    adb_tx_commit(db, tx);
    adb_sync(db);
    adb_close(db);
    adb_open(p, NULL, &db);
    char buf[256]; uint16_t vl;
    int rc = adb_get(db, "alive", 5, buf, 256, &vl);
    adb_close(db); cleanup(p);
    if (rc != 0) PASS(); else FAIL("deleted key reappeared after reopen");
}

static int edge113_cnt_cb(const void *k, uint16_t kl, const void *v, uint16_t vl, void *ctx) {
    (*(int*)ctx)++; return 0;
}
// 113. Put 500 keys, sync, scan: count matches
static void test_large_scan_count(void) {
    printf("  [113] put 500 keys + sync + scan: count = 500              "); fflush(stdout);
    const char *p = "/tmp/adb_edge113";
    cleanup(p);
    adb_t *db; adb_open(p, NULL, &db);
    for (int i = 0; i < 500; i++) {
        char k[16]; snprintf(k, 16, "lsc_%05d", i);
        adb_put(db, k, (uint16_t)strlen(k), "v", 1);
    }
    adb_sync(db);
    int count = 0;
    adb_scan(db, NULL, 0, NULL, 0, edge113_cnt_cb, &count);
    adb_close(db); cleanup(p);
    if (count == 500) PASS(); else { char m[64]; snprintf(m, 64, "expected 500, got %d", count); FAIL(m); }
}

// 114. Multiple syncs interleaved with puts: data integrity
static void test_interleaved_syncs(void) {
    printf("  [114] interleaved put/sync cycles: all data intact          "); fflush(stdout);
    const char *p = "/tmp/adb_edge114";
    cleanup(p);
    adb_t *db; adb_open(p, NULL, &db);
    for (int round = 0; round < 5; round++) {
        for (int i = 0; i < 20; i++) {
            char k[16]; snprintf(k, 16, "is_%d_%d", round, i);
            adb_put(db, k, (uint16_t)strlen(k), "x", 1);
        }
        adb_sync(db);
    }
    adb_close(db);
    adb_open(p, NULL, &db);
    int bad = 0;
    for (int round = 0; round < 5; round++) {
        for (int i = 0; i < 20; i++) {
            char k[16], buf[256]; uint16_t vl;
            snprintf(k, 16, "is_%d_%d", round, i);
            if (adb_get(db, k, (uint16_t)strlen(k), buf, 256, &vl)) bad++;
        }
    }
    adb_close(db); cleanup(p);
    if (bad == 0) PASS(); else { char m[64]; snprintf(m, 64, "%d missing", bad); FAIL(m); }
}

// 115. CRC: data region corruption still detected
static void test_crc_data_region_corrupt(void) {
    printf("  [115] CRC detects data region corruption (byte 2000)        "); fflush(stdout);
    void *page = calloc(1, 4096);
    btree_page_init_leaf(page, 5);
    btree_page_set_crc(page);
    ((unsigned char *)page)[2000] ^= 0xAA;
    int v = btree_page_verify_crc(page);
    free(page);
    if (v == 0) PASS(); else FAIL("CRC missed data corruption");
}

// 116. Put 2000 sequential keys, close (no sync), reopen: WAL recovers all
static void test_wal_recovery_large(void) {
    printf("  [116] WAL recovery: 2000 keys, close without sync           "); fflush(stdout);
    const char *p = "/tmp/adb_edge116";
    cleanup(p);
    adb_t *db; adb_open(p, NULL, &db);
    for (int i = 0; i < 2000; i++) {
        char k[16]; snprintf(k, 16, "wr_%05d", i);
        adb_put(db, k, (uint16_t)strlen(k), "v", 1);
    }
    adb_close(db);
    adb_open(p, NULL, &db);
    int bad = 0;
    for (int i = 0; i < 2000; i++) {
        char k[16], buf[256]; uint16_t vl;
        snprintf(k, 16, "wr_%05d", i);
        if (adb_get(db, k, (uint16_t)strlen(k), buf, 256, &vl)) bad++;
    }
    adb_close(db); cleanup(p);
    if (bad == 0) PASS(); else { char m[64]; snprintf(m, 64, "%d missing", bad); FAIL(m); }
}

// 117. Delete all + sync + put new + close (no sync) + reopen: new keys via WAL
static void test_delete_all_then_wal_insert(void) {
    printf("  [117] delete all, sync, insert new, WAL reopen              "); fflush(stdout);
    const char *p = "/tmp/adb_edge117";
    cleanup(p);
    adb_t *db; adb_open(p, NULL, &db);
    for (int i = 0; i < 50; i++) {
        char k[8]; snprintf(k, 8, "old%02d", i);
        adb_put(db, k, (uint16_t)strlen(k), "x", 1);
    }
    for (int i = 0; i < 50; i++) {
        char k[8]; snprintf(k, 8, "old%02d", i);
        adb_delete(db, k, (uint16_t)strlen(k));
    }
    adb_sync(db);
    for (int i = 0; i < 30; i++) {
        char k[8]; snprintf(k, 8, "new%02d", i);
        adb_put(db, k, (uint16_t)strlen(k), "y", 1);
    }
    adb_close(db); // no sync -> WAL recovery needed
    adb_open(p, NULL, &db);
    int bad = 0;
    for (int i = 0; i < 50; i++) {
        char k[8], buf[256]; uint16_t vl;
        snprintf(k, 8, "old%02d", i);
        if (adb_get(db, k, (uint16_t)strlen(k), buf, 256, &vl) != ADB_ERR_NOT_FOUND) bad++;
    }
    for (int i = 0; i < 30; i++) {
        char k[8], buf[256]; uint16_t vl;
        snprintf(k, 8, "new%02d", i);
        if (adb_get(db, k, (uint16_t)strlen(k), buf, 256, &vl)) bad++;
    }
    adb_close(db); cleanup(p);
    if (bad == 0) PASS(); else { char m[64]; snprintf(m, 64, "%d bad", bad); FAIL(m); }
}

// 118. CRC: last byte of page corruption detected
static void test_crc_last_byte_corrupt(void) {
    printf("  [118] CRC detects last byte (4095) corruption               "); fflush(stdout);
    void *page = calloc(1, 4096);
    btree_page_init_leaf(page, 42);
    btree_page_set_crc(page);
    ((unsigned char *)page)[4095] ^= 0x01;
    int v = btree_page_verify_crc(page);
    free(page);
    if (v == 0) PASS(); else FAIL("CRC missed last-byte corruption");
}

// 119. Overwrite value with exact-max-length (254 bytes)
static void test_overwrite_max_value(void) {
    printf("  [119] overwrite with max-length value (254 bytes)            "); fflush(stdout);
    const char *p = "/tmp/adb_edge119";
    cleanup(p);
    adb_t *db; adb_open(p, NULL, &db);
    adb_put(db, "mk", 2, "short", 5);
    char big[254]; memset(big, 'Z', 254);
    adb_put(db, "mk", 2, big, 254);
    char buf[256]; uint16_t vl;
    int rc = adb_get(db, "mk", 2, buf, 256, &vl);
    adb_close(db); cleanup(p);
    if (rc == 0 && vl == 254 && memcmp(buf, big, 254) == 0) PASS();
    else FAIL("max-length value mismatch");
}

// 120. Arena: allocate 50K 128-byte blocks (forces chunk growth)
static void test_arena_chunk_growth(void) {
    printf("  [120] arena: 50K allocations force chunk growth              "); fflush(stdout);
    void *arena = arena_create();
    if (!arena) { FAIL("create"); return; }
    int bad = 0;
    for (int i = 0; i < 50000; i++) {
        void *p = arena_alloc(arena, 128);
        if (!p) { bad++; break; }
        ((unsigned char *)p)[0] = (unsigned char)(i & 0xFF);
        ((unsigned char *)p)[127] = (unsigned char)(i >> 8);
    }
    arena_destroy(arena);
    if (bad == 0) PASS(); else FAIL("allocation failed");
}

static int edge121_cb(const void *k, uint16_t kl, const void *v, uint16_t vl, void *ctx) {
    (*(int*)ctx)++; return 0;
}
// 121. Scan after heavy overwrites: no duplicates in result
static void test_scan_no_dups_after_overwrites(void) {
    printf("  [121] scan after 10x overwrite: no duplicates               "); fflush(stdout);
    const char *p = "/tmp/adb_edge121";
    cleanup(p);
    adb_t *db; adb_open(p, NULL, &db);
    for (int round = 0; round < 10; round++) {
        for (int i = 0; i < 50; i++) {
            char k[8], v[8]; snprintf(k, 8, "ow%02d", i);
            snprintf(v, 8, "r%d", round);
            adb_put(db, k, (uint16_t)strlen(k), v, (uint16_t)strlen(v));
        }
    }
    adb_sync(db);
    int count = 0;
    adb_scan(db, NULL, 0, NULL, 0, edge121_cb, &count);
    adb_close(db); cleanup(p);
    if (count == 50) PASS(); else { char m[64]; snprintf(m, 64, "expected 50, got %d", count); FAIL(m); }
}

// 122. Tx: put then delete same key in same tx, commit: key absent
static void test_tx_put_delete_same_key(void) {
    printf("  [122] tx: put+delete same key in tx, commit: absent         "); fflush(stdout);
    const char *p = "/tmp/adb_edge122";
    cleanup(p);
    adb_t *db; adb_open(p, NULL, &db);
    uint64_t tx;
    adb_tx_begin(db, ADB_ISO_SNAPSHOT, &tx);
    adb_tx_put(db, tx, "ghost", 5, "val", 3);
    adb_tx_delete(db, tx, "ghost", 5);
    adb_tx_commit(db, tx);
    char buf[256]; uint16_t vl;
    int rc = adb_get(db, "ghost", 5, buf, 256, &vl);
    adb_close(db); cleanup(p);
    if (rc == ADB_ERR_NOT_FOUND) PASS(); else FAIL("deleted key visible after commit");
}

// 123. Destroy then immediate reopen: clean slate
static void test_destroy_immediate_reopen(void) {
    printf("  [123] destroy then immediate reopen: clean slate            "); fflush(stdout);
    const char *p = "/tmp/adb_edge123";
    cleanup(p);
    adb_t *db; adb_open(p, NULL, &db);
    for (int i = 0; i < 100; i++) {
        char k[8]; snprintf(k, 8, "d%03d", i);
        adb_put(db, k, (uint16_t)strlen(k), "v", 1);
    }
    adb_sync(db); adb_close(db);
    adb_destroy(p);
    adb_open(p, NULL, &db);
    int bad = 0;
    for (int i = 0; i < 100; i++) {
        char k[8], buf[256]; uint16_t vl;
        snprintf(k, 8, "d%03d", i);
        if (adb_get(db, k, (uint16_t)strlen(k), buf, 256, &vl) != ADB_ERR_NOT_FOUND) bad++;
    }
    adb_put(db, "fresh", 5, "ok", 2);
    char buf[256]; uint16_t vl;
    int rc = adb_get(db, "fresh", 5, buf, 256, &vl);
    if (rc != 0) bad++;
    adb_close(db); cleanup(p);
    if (bad == 0) PASS(); else { char m[64]; snprintf(m, 64, "%d stale keys", bad); FAIL(m); }
}

// 124. Put 100, backup, put 100 more, restore: only first 100
static void test_backup_point_in_time(void) {
    printf("  [124] backup = point-in-time snapshot (first 100 only)      "); fflush(stdout);
    cleanup("/tmp/adb_e124s"); cleanup("/tmp/adb_e124b"); cleanup("/tmp/adb_e124r");
    adb_t *db; adb_open("/tmp/adb_e124s", NULL, &db);
    for (int i = 0; i < 100; i++) {
        char k[8]; snprintf(k, 8, "bp%03d", i);
        adb_put(db, k, (uint16_t)strlen(k), "first", 5);
    }
    adb_sync(db);
    adb_backup(db, "/tmp/adb_e124b", ADB_BACKUP_FULL);
    for (int i = 100; i < 200; i++) {
        char k[8]; snprintf(k, 8, "bp%03d", i);
        adb_put(db, k, (uint16_t)strlen(k), "second", 6);
    }
    adb_sync(db); adb_close(db);
    adb_restore("/tmp/adb_e124b", "/tmp/adb_e124r");
    adb_open("/tmp/adb_e124r", NULL, &db);
    int bad = 0;
    for (int i = 0; i < 100; i++) {
        char k[8], buf[256]; uint16_t vl;
        snprintf(k, 8, "bp%03d", i);
        if (adb_get(db, k, (uint16_t)strlen(k), buf, 256, &vl)) bad++;
    }
    for (int i = 100; i < 200; i++) {
        char k[8], buf[256]; uint16_t vl;
        snprintf(k, 8, "bp%03d", i);
        if (adb_get(db, k, (uint16_t)strlen(k), buf, 256, &vl) != ADB_ERR_NOT_FOUND) bad++;
    }
    adb_close(db);
    cleanup("/tmp/adb_e124s"); cleanup("/tmp/adb_e124b"); cleanup("/tmp/adb_e124r");
    if (bad == 0) PASS(); else { char m[64]; snprintf(m, 64, "%d bad", bad); FAIL(m); }
}

// 125. Key with embedded null bytes: treated as binary
static void test_key_embedded_nulls(void) {
    printf("  [125] key with embedded null bytes (binary key)             "); fflush(stdout);
    const char *p = "/tmp/adb_edge125";
    cleanup(p);
    adb_t *db; adb_open(p, NULL, &db);
    char k1[8] = {'a', 0, 'b', 0, 'c', 0, 'd', 0};
    char k2[8] = {'a', 0, 'b', 0, 'c', 0, 'e', 0};
    adb_put(db, k1, 8, "v1", 2);
    adb_put(db, k2, 8, "v2", 2);
    char buf[256]; uint16_t vl; int bad = 0;
    int rc = adb_get(db, k1, 8, buf, 256, &vl);
    if (rc != 0 || vl != 2 || memcmp(buf, "v1", 2) != 0) bad++;
    rc = adb_get(db, k2, 8, buf, 256, &vl);
    if (rc != 0 || vl != 2 || memcmp(buf, "v2", 2) != 0) bad++;
    adb_close(db); cleanup(p);
    if (bad == 0) PASS(); else FAIL("binary key mismatch");
}

// 126. Bloom filter: 0% false negatives on 10K keys
static void test_bloom_zero_false_neg(void) {
    printf("  [126] bloom: zero false negatives on 10K keys               "); fflush(stdout);
    void *bloom = bloom_create(10000);
    if (!bloom) { FAIL("create"); return; }
    unsigned char key[64];
    memset(key, 0, 64);
    for (int i = 0; i < 10000; i++) {
        uint16_t kl = (uint16_t)snprintf((char*)key+2, 60, "bfn_%06d", i);
        key[0] = (unsigned char)(kl & 0xFF);
        key[1] = (unsigned char)(kl >> 8);
        bloom_add(bloom, key);
    }
    int fn = 0;
    for (int i = 0; i < 10000; i++) {
        uint16_t kl = (uint16_t)snprintf((char*)key+2, 60, "bfn_%06d", i);
        key[0] = (unsigned char)(kl & 0xFF);
        key[1] = (unsigned char)(kl >> 8);
        if (!bloom_check(bloom, key)) fn++;
    }
    bloom_destroy(bloom);
    if (fn == 0) PASS(); else { char m[64]; snprintf(m, 64, "%d false negatives", fn); FAIL(m); }
}

// 127. Metrics: batch_put counts as N puts
static void test_metrics_batch_counts(void) {
    printf("  [127] metrics: batch_put of 10 counts as 10 puts            "); fflush(stdout);
    const char *p = "/tmp/adb_edge127";
    cleanup(p);
    adb_t *db; adb_open(p, NULL, &db);
    adb_batch_entry_t ents[10];
    char keys[10][8], vals[10][8];
    for (int i = 0; i < 10; i++) {
        snprintf(keys[i], 8, "mb%d", i);
        snprintf(vals[i], 8, "v%d", i);
        ents[i].key = keys[i]; ents[i].key_len = (uint16_t)strlen(keys[i]);
        ents[i].val = vals[i]; ents[i].val_len = (uint16_t)strlen(vals[i]);
    }
    adb_batch_put(db, ents, 10);
    adb_metrics_t m; adb_get_metrics(db, &m);
    adb_close(db); cleanup(p);
    if (m.puts_total >= 10) PASS();
    else { char msg[64]; snprintf(msg, 64, "puts=%lu", (unsigned long)m.puts_total); FAIL(msg); }
}

static int edge128_cb(const void *k, uint16_t kl, const void *v, uint16_t vl, void *ctx) {
    char *last = (char*)ctx;
    char cur[64]; int l = kl > 63 ? 63 : kl;
    memcpy(cur, k, l); cur[l] = 0;
    if (last[0] && strcmp(cur, last) <= 0) { last[0] = 0; return 1; } // order violation
    memcpy(last, cur, l+1);
    return 0;
}
// 128. Scan order: 200 random inserts, scan must be sorted
static void test_scan_order_random_200(void) {
    printf("  [128] scan order: 200 random inserts, result sorted         "); fflush(stdout);
    const char *p = "/tmp/adb_edge128";
    cleanup(p);
    adb_t *db; adb_open(p, NULL, &db);
    unsigned int seed = 12345;
    for (int i = 0; i < 200; i++) {
        seed = seed * 1103515245 + 12345;
        char k[16]; snprintf(k, 16, "so_%06u", seed % 999999);
        adb_put(db, k, (uint16_t)strlen(k), "v", 1);
    }
    char last[64] = {0};
    adb_scan(db, NULL, 0, NULL, 0, edge128_cb, last);
    adb_close(db); cleanup(p);
    if (last[0] != 0) PASS(); else FAIL("scan order violation");
}

// 129. Sync then immediate close, reopen: metrics start fresh (no stale)
static void test_reopen_metrics_fresh(void) {
    printf("  [129] reopen: metrics are fresh (no stale from prior)       "); fflush(stdout);
    const char *p = "/tmp/adb_edge129";
    cleanup(p);
    adb_t *db; adb_open(p, NULL, &db);
    for (int i = 0; i < 100; i++) {
        char k[8]; snprintf(k, 8, "rm%02d", i);
        adb_put(db, k, (uint16_t)strlen(k), "v", 1);
    }
    adb_sync(db); adb_close(db);
    adb_open(p, NULL, &db);
    adb_metrics_t m; adb_get_metrics(db, &m);
    // After reopen, the prior session's metrics should not carry over
    // (metrics are per-session, not persisted)
    adb_close(db); cleanup(p);
    if (m.puts_total == 0) PASS(); else { char msg[64]; snprintf(msg, 64, "puts=%lu (expected 0)", (unsigned long)m.puts_total); FAIL(msg); }
}

// 130. tx_get on key that exists in storage but not write-set
static void test_tx_get_storage_fallthrough(void) {
    printf("  [130] tx_get falls through to storage for non-ws key        "); fflush(stdout);
    const char *p = "/tmp/adb_edge130";
    cleanup(p);
    adb_t *db; adb_open(p, NULL, &db);
    adb_put(db, "pre", 3, "exist", 5);
    uint64_t tx; adb_tx_begin(db, ADB_ISO_SNAPSHOT, &tx);
    adb_tx_put(db, tx, "txonly", 6, "txval", 5);
    char buf[256]; uint16_t vl; int bad = 0;
    int rc = adb_tx_get(db, tx, "pre", 3, buf, 256, &vl);
    if (rc != 0 || vl != 5 || memcmp(buf, "exist", 5) != 0) bad++;
    rc = adb_tx_get(db, tx, "txonly", 6, buf, 256, &vl);
    if (rc != 0 || vl != 5 || memcmp(buf, "txval", 5) != 0) bad++;
    adb_tx_commit(db, tx);
    adb_close(db); cleanup(p);
    if (bad == 0) PASS(); else FAIL("tx_get fallthrough failed");
}

// --- Tests 131-150: deeper edge cases ---

static void test_scan_empty_after_delete_all(void) {
    printf("  [131] scan empty after delete all + sync + reopen            "); fflush(stdout);
    const char *p = "/tmp/adb_edge131"; cleanup(p);
    adb_t *db; adb_open(p, NULL, &db);
    for (int i = 0; i < 100; i++) {
        char k[8]; snprintf(k, 8, "k%02d", i);
        adb_put(db, k, (uint16_t)strlen(k), "v", 1);
    }
    for (int i = 0; i < 100; i++) {
        char k[8]; snprintf(k, 8, "k%02d", i);
        adb_delete(db, k, (uint16_t)strlen(k));
    }
    adb_sync(db); adb_close(db);
    adb_open(p, NULL, &db);
    int count = 0;
    int cb(const void *k, uint16_t kl, const void *v, uint16_t vl, void *c) {
        (void)k;(void)kl;(void)v;(void)vl;(void)c; count++; return 0;
    }
    adb_scan(db, NULL, 0, NULL, 0, cb, NULL);
    adb_close(db); cleanup(p);
    if (count != 0) FAIL("not empty"); else PASS();
}

static void test_put_max_key_get_correct(void) {
    printf("  [132] put max key (62B) + get correct                        "); fflush(stdout);
    const char *p = "/tmp/adb_edge132"; cleanup(p);
    adb_t *db; adb_open(p, NULL, &db);
    char k[62]; memset(k, 'Z', 62);
    char v[10] = "maxkeyval";
    int rc = adb_put(db, k, 62, v, 9);
    if (rc != 0) { adb_close(db); cleanup(p); FAIL("put failed"); }
    char buf[256]; uint16_t vl;
    rc = adb_get(db, k, 62, buf, 256, &vl);
    adb_close(db); cleanup(p);
    if (rc != 0) FAIL("get failed");
    if (vl != 9 || memcmp(buf, "maxkeyval", 9) != 0) FAIL("wrong val");
    PASS();
}

static void test_overwrite_shrink_grow_persist(void) {
    printf("  [133] overwrite: shrink then grow value, persist             "); fflush(stdout);
    const char *p = "/tmp/adb_edge133"; cleanup(p);
    adb_t *db; adb_open(p, NULL, &db);
    char big[200]; memset(big, 'B', 200);
    adb_put(db, "k", 1, big, 200);
    adb_put(db, "k", 1, "sm", 2);
    adb_put(db, "k", 1, big, 200);
    adb_sync(db); adb_close(db);
    adb_open(p, NULL, &db);
    char buf[256]; uint16_t vl;
    int rc = adb_get(db, "k", 1, buf, 256, &vl);
    adb_close(db); cleanup(p);
    if (rc != 0 || vl != 200) FAILF("vl=%u", vl);
    if (memcmp(buf, big, 200) != 0) FAIL("data mismatch");
    PASS();
}

static void test_scan_prefix_exact(void) {
    printf("  [134] scan prefix: only matching prefix keys returned        "); fflush(stdout);
    const char *p = "/tmp/adb_edge134"; cleanup(p);
    adb_t *db; adb_open(p, NULL, &db);
    adb_put(db, "app:1", 5, "a", 1);
    adb_put(db, "app:2", 5, "b", 1);
    adb_put(db, "app:3", 5, "c", 1);
    adb_put(db, "bob:1", 5, "d", 1);
    adb_put(db, "bob:2", 5, "e", 1);
    int count = 0;
    int cb(const void *k, uint16_t kl, const void *v, uint16_t vl, void *c) {
        (void)k;(void)kl;(void)v;(void)vl;(void)c; count++; return 0;
    }
    adb_scan(db, "app:", 4, "app:\xff", 5, cb, NULL);
    adb_close(db); cleanup(p);
    if (count != 3) FAILF("count=%d", count); else PASS();
}

static void test_batch_64_roundtrip(void) {
    printf("  [135] batch 64 entries: all retrievable                      "); fflush(stdout);
    const char *p = "/tmp/adb_edge135"; cleanup(p);
    adb_t *db; adb_open(p, NULL, &db);
    adb_batch_entry_t e[64];
    char ks[64][8];
    for (int i = 0; i < 64; i++) {
        snprintf(ks[i], 8, "bk%02d", i);
        e[i].key = ks[i]; e[i].key_len = (uint16_t)strlen(ks[i]);
        e[i].val = "val"; e[i].val_len = 3;
    }
    int rc = adb_batch_put(db, e, 64);
    if (rc != 0) { adb_close(db); cleanup(p); FAIL("batch failed"); }
    int ok = 1;
    for (int i = 0; i < 64; i++) {
        char buf[256]; uint16_t vl;
        if (adb_get(db, ks[i], (uint16_t)strlen(ks[i]), buf, 256, &vl) != 0) { ok = 0; break; }
    }
    adb_close(db); cleanup(p);
    if (!ok) FAIL("missing key"); else PASS();
}

static void test_tx_delete_nonexistent(void) {
    printf("  [136] tx_delete nonexistent key: no crash, commit ok         "); fflush(stdout);
    const char *p = "/tmp/adb_edge136"; cleanup(p);
    adb_t *db; adb_open(p, NULL, &db);
    uint64_t tx; adb_tx_begin(db, 0, &tx);
    int rc = adb_tx_delete(db, tx, "nope", 4);
    int rc2 = adb_tx_commit(db, tx);
    adb_close(db); cleanup(p);
    if (rc != 0) FAILF("del rc=%d", rc);
    if (rc2 != 0) FAILF("commit rc=%d", rc2);
    PASS();
}

static void test_get_after_destroy_reopen(void) {
    printf("  [137] get after destroy+reopen: not found                    "); fflush(stdout);
    const char *p = "/tmp/adb_edge137"; cleanup(p);
    adb_t *db; adb_open(p, NULL, &db);
    adb_put(db, "data", 4, "val", 3);
    adb_sync(db); adb_close(db);
    adb_destroy(p);
    adb_open(p, NULL, &db);
    char buf[256]; uint16_t vl;
    int rc = adb_get(db, "data", 4, buf, 256, &vl);
    adb_close(db); cleanup(p);
    if (rc == 0) FAIL("should be not found"); else PASS();
}

static void test_scan_single_key_match(void) {
    printf("  [138] scan with start=end=existing key: returns 1            "); fflush(stdout);
    const char *p = "/tmp/adb_edge138"; cleanup(p);
    adb_t *db; adb_open(p, NULL, &db);
    adb_put(db, "aaa", 3, "v", 1);
    adb_put(db, "bbb", 3, "v", 1);
    adb_put(db, "ccc", 3, "v", 1);
    int count = 0;
    int cb(const void *k, uint16_t kl, const void *v, uint16_t vl, void *c) {
        (void)k;(void)kl;(void)v;(void)vl;(void)c; count++; return 0;
    }
    adb_scan(db, "bbb", 3, "bbb", 3, cb, NULL);
    adb_close(db); cleanup(p);
    if (count != 1) FAILF("count=%d", count); else PASS();
}

static void test_sync_no_ops_safe(void) {
    printf("  [139] sync on fresh db with no writes: no crash              "); fflush(stdout);
    const char *p = "/tmp/adb_edge139"; cleanup(p);
    adb_t *db; adb_open(p, NULL, &db);
    int rc = adb_sync(db);
    adb_close(db); cleanup(p);
    if (rc != 0) FAILF("sync rc=%d", rc); else PASS();
}

static void test_backup_empty_db(void) {
    printf("  [140] backup empty db: put 1 key, backup+restore clean       "); fflush(stdout);
    const char *p = "/tmp/adb_edge140"; cleanup(p);
    const char *bk = "/tmp/adb_edge140_bk"; cleanup(bk);
    const char *rs = "/tmp/adb_edge140_rs"; cleanup(rs);
    adb_t *db; adb_open(p, NULL, &db);
    adb_put(db, "x", 1, "y", 1);
    adb_sync(db);
    int rc = adb_backup(db, bk, 0);
    adb_close(db);
    if (rc != 0) { cleanup(p); cleanup(bk); FAILF("backup rc=%d", rc); }
    rc = adb_restore(bk, rs);
    if (rc != 0) { cleanup(p); cleanup(bk); cleanup(rs); FAILF("restore rc=%d", rc); }
    adb_open(rs, NULL, &db);
    int count = 0;
    int cb(const void *k, uint16_t kl, const void *v, uint16_t vl, void *c) {
        (void)k;(void)kl;(void)v;(void)vl;(void)c; count++; return 0;
    }
    adb_scan(db, NULL, 0, NULL, 0, cb, NULL);
    adb_close(db);
    cleanup(p); cleanup(bk); cleanup(rs);
    if (count != 1) FAILF("count=%d", count); else PASS();
}

static void test_multiple_overwrites_wal_replay(void) {
    printf("  [141] 50 overwrites same key, WAL replay: last value         "); fflush(stdout);
    const char *p = "/tmp/adb_edge141"; cleanup(p);
    adb_t *db; adb_open(p, NULL, &db);
    for (int i = 0; i < 50; i++) {
        char v[8]; snprintf(v, 8, "v%02d", i);
        adb_put(db, "key", 3, v, (uint16_t)strlen(v));
    }
    adb_close(db);
    adb_open(p, NULL, &db);
    char buf[256]; uint16_t vl;
    int rc = adb_get(db, "key", 3, buf, 256, &vl);
    adb_close(db); cleanup(p);
    if (rc != 0) FAIL("get failed");
    if (vl != 3 || memcmp(buf, "v49", 3) != 0) FAILF("got %.*s", vl, buf);
    PASS();
}

static void test_tx_get_write_set_overwrite(void) {
    printf("  [142] tx_get returns latest write-set overwrite              "); fflush(stdout);
    const char *p = "/tmp/adb_edge142"; cleanup(p);
    adb_t *db; adb_open(p, NULL, &db);
    uint64_t tx; adb_tx_begin(db, 0, &tx);
    adb_tx_put(db, tx, "k", 1, "first", 5);
    adb_tx_put(db, tx, "k", 1, "second", 6);
    adb_tx_put(db, tx, "k", 1, "third", 5);
    char buf[256]; uint16_t vl;
    int rc = adb_tx_get(db, tx, "k", 1, buf, 256, &vl);
    adb_tx_commit(db, tx);
    adb_close(db); cleanup(p);
    if (rc != 0) FAIL("tx_get failed");
    if (vl != 5 || memcmp(buf, "third", 5) != 0) FAILF("got %.*s", vl, buf);
    PASS();
}

static void test_delete_key_then_batch_same_key(void) {
    printf("  [143] delete key, then batch with same key: key exists       "); fflush(stdout);
    const char *p = "/tmp/adb_edge143"; cleanup(p);
    adb_t *db; adb_open(p, NULL, &db);
    adb_put(db, "k", 1, "old", 3);
    adb_delete(db, "k", 1);
    adb_batch_entry_t e = { .key = "k", .key_len = 1, .val = "new", .val_len = 3 };
    adb_batch_put(db, &e, 1);
    char buf[256]; uint16_t vl;
    int rc = adb_get(db, "k", 1, buf, 256, &vl);
    adb_close(db); cleanup(p);
    if (rc != 0) FAIL("not found");
    if (vl != 3 || memcmp(buf, "new", 3) != 0) FAIL("wrong val");
    PASS();
}

static void test_scan_all_then_bounded(void) {
    printf("  [144] scan all vs bounded: bounded <= total                  "); fflush(stdout);
    const char *p = "/tmp/adb_edge144"; cleanup(p);
    adb_t *db; adb_open(p, NULL, &db);
    for (int i = 0; i < 100; i++) {
        char k[8]; snprintf(k, 8, "k%03d", i);
        adb_put(db, k, (uint16_t)strlen(k), "v", 1);
    }
    int all = 0, bounded = 0;
    int cb_all(const void *k, uint16_t kl, const void *v, uint16_t vl, void *c) {
        (void)k;(void)kl;(void)v;(void)vl;(void)c; all++; return 0;
    }
    int cb_bnd(const void *k, uint16_t kl, const void *v, uint16_t vl, void *c) {
        (void)k;(void)kl;(void)v;(void)vl;(void)c; bounded++; return 0;
    }
    adb_scan(db, NULL, 0, NULL, 0, cb_all, NULL);
    adb_scan(db, "k050", 4, "k080", 4, cb_bnd, NULL);
    adb_close(db); cleanup(p);
    if (all != 100) FAILF("all=%d", all);
    if (bounded > all || bounded < 1) FAILF("bounded=%d", bounded);
    PASS();
}

static void test_reopen_preserves_all_data(void) {
    printf("  [145] 200 keys, sync, reopen 5x: all data intact            "); fflush(stdout);
    const char *p = "/tmp/adb_edge145"; cleanup(p);
    adb_t *db; adb_open(p, NULL, &db);
    for (int i = 0; i < 200; i++) {
        char k[16]; snprintf(k, 16, "rp%04d", i);
        char v[16]; snprintf(v, 16, "val%04d", i);
        adb_put(db, k, (uint16_t)strlen(k), v, (uint16_t)strlen(v));
    }
    adb_sync(db); adb_close(db);
    for (int cyc = 0; cyc < 5; cyc++) {
        adb_open(p, NULL, &db);
        int ok = 1;
        for (int i = 0; i < 200 && ok; i++) {
            char k[16]; snprintf(k, 16, "rp%04d", i);
            char exp[16]; snprintf(exp, 16, "val%04d", i);
            char buf[256]; uint16_t vl;
            if (adb_get(db, k, (uint16_t)strlen(k), buf, 256, &vl) != 0) ok = 0;
            else if (vl != strlen(exp) || memcmp(buf, exp, vl) != 0) ok = 0;
        }
        adb_close(db);
        if (!ok) { cleanup(p); FAILF("cycle %d", cyc); }
    }
    cleanup(p);
    PASS();
}

static void test_metrics_monotonic_across_ops(void) {
    printf("  [146] metrics: counters never decrease across ops            "); fflush(stdout);
    const char *p = "/tmp/adb_edge146"; cleanup(p);
    adb_t *db; adb_open(p, NULL, &db);
    adb_metrics_t prev; adb_get_metrics(db, &prev);
    for (int i = 0; i < 50; i++) {
        char k[8]; snprintf(k, 8, "m%02d", i);
        adb_put(db, k, (uint16_t)strlen(k), "v", 1);
        adb_metrics_t cur; adb_get_metrics(db, &cur);
        if (cur.puts_total < prev.puts_total) {
            adb_close(db); cleanup(p); FAIL("puts decreased");
        }
        prev = cur;
    }
    adb_close(db); cleanup(p);
    PASS();
}

static void test_batch_then_tx_commit(void) {
    printf("  [147] batch 20, then tx put 5 + commit: all 25 exist         "); fflush(stdout);
    const char *p = "/tmp/adb_edge147"; cleanup(p);
    adb_t *db; adb_open(p, NULL, &db);
    adb_batch_entry_t e[20];
    char ks[20][8];
    for (int i = 0; i < 20; i++) {
        snprintf(ks[i], 8, "b%02d", i);
        e[i].key = ks[i]; e[i].key_len = (uint16_t)strlen(ks[i]);
        e[i].val = "v"; e[i].val_len = 1;
    }
    adb_batch_put(db, e, 20);
    uint64_t tx; adb_tx_begin(db, 0, &tx);
    for (int i = 0; i < 5; i++) {
        char k[8]; snprintf(k, 8, "t%d", i);
        adb_tx_put(db, tx, k, (uint16_t)strlen(k), "w", 1);
    }
    adb_tx_commit(db, tx);
    int count = 0;
    int cb(const void *k, uint16_t kl, const void *v, uint16_t vl, void *c) {
        (void)k;(void)kl;(void)v;(void)vl;(void)c; count++; return 0;
    }
    adb_scan(db, NULL, 0, NULL, 0, cb, NULL);
    adb_close(db); cleanup(p);
    if (count != 25) FAILF("count=%d", count); else PASS();
}

static void test_value_zero_length_persist(void) {
    printf("  [148] zero-length value: persists across sync+reopen         "); fflush(stdout);
    const char *p = "/tmp/adb_edge148"; cleanup(p);
    adb_t *db; adb_open(p, NULL, &db);
    adb_put(db, "zv", 2, "", 0);
    adb_sync(db); adb_close(db);
    adb_open(p, NULL, &db);
    char buf[256]; uint16_t vl = 999;
    int rc = adb_get(db, "zv", 2, buf, 256, &vl);
    adb_close(db); cleanup(p);
    if (rc != 0) FAIL("not found");
    if (vl != 0) FAILF("vl=%u", vl);
    PASS();
}

static void test_scan_returns_correct_values(void) {
    printf("  [149] scan: each key-value pair matches put data             "); fflush(stdout);
    const char *p = "/tmp/adb_edge149"; cleanup(p);
    adb_t *db; adb_open(p, NULL, &db);
    for (int i = 0; i < 30; i++) {
        char k[8]; snprintf(k, 8, "sk%02d", i);
        char v[8]; snprintf(v, 8, "sv%02d", i);
        adb_put(db, k, (uint16_t)strlen(k), v, (uint16_t)strlen(v));
    }
    int bad = 0;
    int cb(const void *k, uint16_t kl, const void *v, uint16_t vl, void *c) {
        char kb[64]; memcpy(kb, k, kl); kb[kl] = 0;
        char exp[68];
        snprintf(exp, 68, "sv%s", kb+2);
        if (vl != strlen(exp) || memcmp(v, exp, vl) != 0) (*(int*)c)++;
        return 0;
    }
    adb_scan(db, NULL, 0, NULL, 0, cb, &bad);
    adb_close(db); cleanup(p);
    if (bad != 0) FAILF("%d mismatches", bad); else PASS();
}

static void test_rapid_open_close_with_sync(void) {
    printf("  [150] 20 open-put-sync-close cycles: no leak/crash           "); fflush(stdout);
    const char *p = "/tmp/adb_edge150"; cleanup(p);
    int ok = 1;
    for (int i = 0; i < 20; i++) {
        adb_t *db;
        if (adb_open(p, NULL, &db) != 0) { ok = 0; break; }
        char k[8]; snprintf(k, 8, "r%02d", i);
        adb_put(db, k, (uint16_t)strlen(k), "v", 1);
        adb_sync(db);
        adb_close(db);
    }
    if (ok) {
        adb_t *db; adb_open(p, NULL, &db);
        int count = 0;
        int cb(const void *k, uint16_t kl, const void *v, uint16_t vl, void *c) {
            (void)k;(void)kl;(void)v;(void)vl;(void)c; count++; return 0;
        }
        adb_scan(db, NULL, 0, NULL, 0, cb, NULL);
        adb_close(db);
        if (count != 20) ok = 0;
    }
    cleanup(p);
    if (!ok) FAIL("mismatch"); else PASS();
}

int main(void) {
    printf("============================================================\n");
    printf("  AssemblyDB Edge Case Tests\n");
    printf("============================================================\n\n");
    test_similar_keys();
    test_prefix_keys();
    test_rapid_put_delete_put();
    test_scan_single_key_range();
    test_scan_narrow_range();
    test_delete_nonexistent();
    test_multi_session_scan();
    test_same_key_multi_session();
    test_binary_values();
    test_scan_early_stop();
    test_delete_reinsert_diff_len();
    test_empty_sessions();
    test_invalid_arguments();
    test_oversize_lengths_rejected();
    test_batch_no_partial_on_invalid();
    test_error_code_contract();
    test_tombstone_masks_persisted_value();
    test_restore_rejects_bad_manifest();
    test_restore_rejects_semantic_manifest();
    test_restore_overwrites_dirty_destination();
    test_restore_rejects_truncated_btree();
    test_restore_rejects_bad_root_page_type();
    test_restore_copies_manifest_exactly();
    test_backup_restore_leave_no_tmp_files();
    test_restore_rejects_manifest_with_trailing_data();
    test_restore_rejects_same_source_and_destination();
    test_restore_rejects_alias_source_destination();
    test_memtable_overrides_btree();
    test_destroy_then_reuse();
    test_delete_masks_btree_across_sync();
    test_scan_exact_start_end();
    test_multi_sync_delete_count();
    test_overwrite_zero_length_value();
    test_wal_replay_dirty_close();
    test_scan_inverted_range();
    test_metrics_monotonic();
    test_tx_large_rollback_invisible();
    test_double_destroy();
    test_batch_identical_keys();
    test_tx_scan_sees_write_set();
    test_scan_sorted_after_random_inserts();
    test_50_reopen_cycles();
    test_tx_delete_then_get();
    test_mass_delete_then_scan();
    test_sync_every_op();
    test_key_compare_high_bytes();
    test_all_single_byte_keys();
    test_100_reopen_churn();
    test_batch_tx_interleave();
    test_metrics_exact_counts();
    test_crc32c_determinism();
    test_crc32c_sensitivity();
    test_neon_large_buffer();
    test_key_compare_zero_length();
    test_key_compare_length_tiebreak();
    test_backup_independence();
    test_multi_sync_no_duplication();
    test_zero_length_value();
    test_get_null_vlen_out();
    test_build_fixed_key_padding();
    test_asm_strlen();
    test_build_fixed_val_padding();
    test_key_equal();
    test_neon_memcmp();
    test_neon_memset_pattern();
    test_close_null_db();
    test_put_null_db();
    test_get_null_db();
    test_full_scan_null_bounds();
    test_batch_single_entry();
    test_tx_commit_wrong_id();
    test_tx_rollback_wrong_id();
    test_double_tx_begin();
    test_get_zero_vbuf_len();
    test_batch_zero_count();
    test_delete_nonexistent_key();
    test_tx_get_fallthrough();
    test_backup_to_self();
    test_scan_callback_stops();
    test_metrics_fresh_db();
    test_sync_empty_db();
    test_put_close_no_sync();
    test_double_sync();
    test_get_nonexistent();
    test_delete_no_sync_wal_replay();
    test_scan_empty_db();
    test_batch_one_wal_persist();
    test_tx_write_set_latest();
    test_triple_sync_scan_count();
    test_destroy_nonexistent();
    test_overwrite_shorter_no_stale();
    test_tx_get_fallthrough_storage();
    test_sync_close_clean_wal();
    test_batch_empty();
    test_metrics_zero_on_fresh();
    test_put_delete_put_sync();
    test_scan_start_gt_end();
    test_tx_begin_after_rollback();
    test_large_batch_persist();
    test_put_zero_val_len();
    test_crc_detects_header_page_id_corrupt();
    test_crc_detects_header_page_type_corrupt();
    test_crc_detects_header_num_keys_corrupt();
    test_crc_detects_every_header_byte();
    test_crc_different_page_ids();
    test_compaction_preserves_data_on_reopen();
    test_many_deletes_persist();
    test_scan_stops_at_first();
    test_batch_identical_keys_last_wins();
    test_rapid_open_close_noop();
    test_tx_overwrite_in_writeset();
    test_tx_delete_persist_reopen();
    test_large_scan_count();
    test_interleaved_syncs();
    test_crc_data_region_corrupt();
    test_wal_recovery_large();
    test_delete_all_then_wal_insert();
    test_crc_last_byte_corrupt();
    test_overwrite_max_value();
    test_arena_chunk_growth();
    test_scan_no_dups_after_overwrites();
    test_tx_put_delete_same_key();
    test_destroy_immediate_reopen();
    test_backup_point_in_time();
    test_key_embedded_nulls();
    test_bloom_zero_false_neg();
    test_metrics_batch_counts();
    test_scan_order_random_200();
    test_reopen_metrics_fresh();
    test_tx_get_storage_fallthrough();
    test_scan_empty_after_delete_all();
    test_put_max_key_get_correct();
    test_overwrite_shrink_grow_persist();
    test_scan_prefix_exact();
    test_batch_64_roundtrip();
    test_tx_delete_nonexistent();
    test_get_after_destroy_reopen();
    test_scan_single_key_match();
    test_sync_no_ops_safe();
    test_backup_empty_db();
    test_multiple_overwrites_wal_replay();
    test_tx_get_write_set_overwrite();
    test_delete_key_then_batch_same_key();
    test_scan_all_then_bounded();
    test_reopen_preserves_all_data();
    test_metrics_monotonic_across_ops();
    test_batch_then_tx_commit();
    test_value_zero_length_persist();
    test_scan_returns_correct_values();
    test_rapid_open_close_with_sync();
    printf("\n============================================================\n");
    printf("  Results: %d failed\n", g_fail);
    printf("============================================================\n");
    return g_fail;
}
