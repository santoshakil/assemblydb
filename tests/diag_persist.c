#include "assemblydb.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>

static void cleanup(const char *path) {
    char cmd[128];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", path);
    int rc = system(cmd);
    (void)rc;
}

static double now_sec(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec / 1e9;
}

static int g_fail = 0;
#define PASS() printf("PASS\n")
#define FAIL(msg) do { printf("FAIL: %s\n", msg); g_fail++; } while(0)

// Test 1: 50K delete half + persist
static void test_delete_persist(void) {
    printf("  [01] delete 50%% of 10K, persist, verify survivors       "); fflush(stdout);
    const char *path = "/tmp/diag_dp";
    cleanup(path);
    const int N = 10000;
    char key[24], val[48], vbuf[256]; uint16_t vlen;
    adb_t *db = NULL;
    adb_open(path, NULL, &db);
    for (int i = 0; i < N; i++) {
        int kl = snprintf(key, sizeof(key), "k%07d", i);
        int vl = snprintf(val, sizeof(val), "v%07d", i);
        adb_put(db, key, (uint16_t)kl, val, (uint16_t)vl);
    }
    for (int i = 0; i < N; i += 2) {
        int kl = snprintf(key, sizeof(key), "k%07d", i);
        adb_delete(db, key, (uint16_t)kl);
    }
    adb_close(db);
    db = NULL;
    adb_open(path, NULL, &db);
    int bad = 0;
    for (int i = 0; i < N; i++) {
        int kl = snprintf(key, sizeof(key), "k%07d", i);
        int vl = snprintf(val, sizeof(val), "v%07d", i);
        int rc = adb_get(db, key, (uint16_t)kl, vbuf, 256, &vlen);
        if (i % 2 == 0) { if (rc == 0) bad++; }
        else { if (rc != 0 || vlen != (uint16_t)vl || memcmp(vbuf, val, vl) != 0) bad++; }
    }
    adb_close(db); cleanup(path);
    if (bad == 0) PASS(); else { char m[64]; snprintf(m, 64, "%d bad", bad); FAIL(m); }
}

// Test 2: Overwrite persistence
static void test_overwrite_persist(void) {
    printf("  [02] overwrite 5K keys 5 rounds, persist final values    "); fflush(stdout);
    const char *path = "/tmp/diag_op";
    cleanup(path);
    const int N = 5000;
    char key[24], val[48], vbuf[256]; uint16_t vlen;
    adb_t *db = NULL;
    adb_open(path, NULL, &db);
    for (int round = 0; round < 5; round++)
        for (int i = 0; i < N; i++) {
            int kl = snprintf(key, sizeof(key), "k%07d", i);
            int vl = snprintf(val, sizeof(val), "r%d_v%07d", round, i);
            adb_put(db, key, (uint16_t)kl, val, (uint16_t)vl);
        }
    adb_close(db);
    db = NULL;
    adb_open(path, NULL, &db);
    int bad = 0;
    for (int i = 0; i < N; i++) {
        int kl = snprintf(key, sizeof(key), "k%07d", i);
        int vl = snprintf(val, sizeof(val), "r4_v%07d", i);
        int rc = adb_get(db, key, (uint16_t)kl, vbuf, 256, &vlen);
        if (rc != 0 || vlen != (uint16_t)vl || memcmp(vbuf, val, vl) != 0) bad++;
    }
    adb_close(db); cleanup(path);
    if (bad == 0) PASS(); else { char m[64]; snprintf(m, 64, "%d bad", bad); FAIL(m); }
}

// Test 3: Scan after persist+delete
static void test_scan_after_persist(void) {
    printf("  [03] scan after persist+delete: no ghosts, sorted        "); fflush(stdout);
    const char *path = "/tmp/diag_sp";
    cleanup(path);
    const int N = 5000;
    char key[24], val[48];
    adb_t *db = NULL;
    adb_open(path, NULL, &db);
    for (int i = 0; i < N; i++) {
        int kl = snprintf(key, sizeof(key), "k%07d", i);
        int vl = snprintf(val, sizeof(val), "v%07d", i);
        adb_put(db, key, (uint16_t)kl, val, (uint16_t)vl);
    }
    for (int i = 0; i < N; i += 2) {
        int kl = snprintf(key, sizeof(key), "k%07d", i);
        adb_delete(db, key, (uint16_t)kl);
    }
    adb_close(db);
    db = NULL;
    adb_open(path, NULL, &db);
    typedef struct { int count; int order_ok; char prev[24]; } scan_ctx;
    scan_ctx ctx = {0, 1, ""};
    int scan_cb(const void *k, uint16_t kl, const void *v, uint16_t vl, void *uctx) {
        (void)v; (void)vl;
        scan_ctx *c = uctx;
        char cur[24]; int len = kl < 23 ? kl : 23;
        memcpy(cur, k, len); cur[len] = 0;
        if (c->count > 0 && strcmp(cur, c->prev) <= 0) c->order_ok = 0;
        memcpy(c->prev, cur, 24); c->count++;
        return 0;
    }
    adb_scan(db, NULL, 0, NULL, 0, scan_cb, &ctx);
    adb_close(db); cleanup(path);
    int expected = N / 2;
    if (ctx.count == expected && ctx.order_ok) PASS();
    else { char m[128]; snprintf(m, 128, "count=%d(exp %d) order=%s", ctx.count, expected, ctx.order_ok?"ok":"BAD"); FAIL(m); }
}

// Test 4: 10 incremental sessions
static void test_incremental_sessions(void) {
    printf("  [04] 5 sessions: add 500 + delete 100 each, verify       "); fflush(stdout);
    const char *path = "/tmp/diag_is";
    cleanup(path);
    char key[24], val[48], vbuf[256]; uint16_t vlen;
    const int SESS = 5, PER_SESS = 500, DEL_PER = 100;
    for (int session = 0; session < SESS; session++) {
        adb_t *db = NULL;
        adb_open(path, NULL, &db);
        int base = session * PER_SESS;
        for (int i = 0; i < PER_SESS; i++) {
            int kl = snprintf(key, sizeof(key), "k%07d", base + i);
            int vl = snprintf(val, sizeof(val), "s%d_v%d", session, i);
            adb_put(db, key, (uint16_t)kl, val, (uint16_t)vl);
        }
        if (session > 0) {
            int del_base = (session - 1) * PER_SESS;
            for (int i = 0; i < DEL_PER; i++) {
                int kl = snprintf(key, sizeof(key), "k%07d", del_base + i);
                adb_delete(db, key, (uint16_t)kl);
            }
        }
        adb_close(db);
    }
    adb_t *db = NULL;
    adb_open(path, NULL, &db);
    int bad = 0;
    for (int s = 0; s < SESS; s++) {
        int base = s * PER_SESS;
        for (int i = 0; i < PER_SESS; i++) {
            int kl = snprintf(key, sizeof(key), "k%07d", base + i);
            int rc = adb_get(db, key, (uint16_t)kl, vbuf, 256, &vlen);
            int should_exist = !(s < (SESS-1) && i < DEL_PER);
            if (should_exist && rc != 0) bad++;
            if (!should_exist && rc == 0) bad++;
        }
    }
    adb_close(db); cleanup(path);
    if (bad == 0) PASS(); else { char m[64]; snprintf(m, 64, "%d bad", bad); FAIL(m); }
}

// Test 5: Delete all, scan empty
static void test_delete_all_scan_empty(void) {
    printf("  [05] insert 3K, delete all, persist, scan = 0            "); fflush(stdout);
    const char *path = "/tmp/diag_da";
    cleanup(path);
    const int N = 3000;
    char key[24], val[48];
    adb_t *db = NULL;
    adb_open(path, NULL, &db);
    for (int i = 0; i < N; i++) {
        int kl = snprintf(key, sizeof(key), "k%07d", i);
        int vl = snprintf(val, sizeof(val), "v%07d", i);
        adb_put(db, key, (uint16_t)kl, val, (uint16_t)vl);
    }
    for (int i = 0; i < N; i++) {
        int kl = snprintf(key, sizeof(key), "k%07d", i);
        adb_delete(db, key, (uint16_t)kl);
    }
    adb_close(db);
    db = NULL;
    adb_open(path, NULL, &db);
    int count = 0;
    int scan_cb(const void *k, uint16_t kl, const void *v, uint16_t vl, void *uctx) {
        (void)k; (void)kl; (void)v; (void)vl; (*(int*)uctx)++; return 0;
    }
    adb_scan(db, NULL, 0, NULL, 0, scan_cb, &count);
    adb_close(db); cleanup(path);
    if (count == 0) PASS();
    else { char m[64]; snprintf(m, 64, "scan returned %d (expected 0)", count); FAIL(m); }
}

// Test 6: Reinsert with different values
static void test_reinsert_different_value(void) {
    printf("  [06] insert+delete+reinsert 5K with new values, persist  "); fflush(stdout);
    const char *path = "/tmp/diag_ri";
    cleanup(path);
    const int N = 5000;
    char key[24], val[48], vbuf[256]; uint16_t vlen;
    adb_t *db = NULL;
    adb_open(path, NULL, &db);
    for (int i = 0; i < N; i++) {
        int kl = snprintf(key, sizeof(key), "k%07d", i);
        adb_put(db, key, (uint16_t)kl, "original", 8);
    }
    for (int i = 0; i < N; i++) {
        int kl = snprintf(key, sizeof(key), "k%07d", i);
        adb_delete(db, key, (uint16_t)kl);
    }
    for (int i = 0; i < N; i++) {
        int kl = snprintf(key, sizeof(key), "k%07d", i);
        int vl = snprintf(val, sizeof(val), "new_%d", i);
        adb_put(db, key, (uint16_t)kl, val, (uint16_t)vl);
    }
    adb_close(db);
    db = NULL;
    adb_open(path, NULL, &db);
    int bad = 0;
    for (int i = 0; i < N; i++) {
        int kl = snprintf(key, sizeof(key), "k%07d", i);
        int vl = snprintf(val, sizeof(val), "new_%d", i);
        int rc = adb_get(db, key, (uint16_t)kl, vbuf, 256, &vlen);
        if (rc != 0 || vlen != (uint16_t)vl || memcmp(vbuf, val, vl) != 0) bad++;
    }
    adb_close(db); cleanup(path);
    if (bad == 0) PASS(); else { char m[64]; snprintf(m, 64, "%d bad", bad); FAIL(m); }
}

// Test 7: Max-size values
static void test_large_values_persist(void) {
    printf("  [07] 5K keys with max-size (254B) values persist         "); fflush(stdout);
    const char *path = "/tmp/diag_lv";
    cleanup(path);
    const int N = 5000;
    char key[24], vbuf[256]; unsigned char val[254]; uint16_t vlen;
    adb_t *db = NULL;
    adb_open(path, NULL, &db);
    for (int i = 0; i < N; i++) {
        int kl = snprintf(key, sizeof(key), "k%07d", i);
        for (int j = 0; j < 254; j++) val[j] = (unsigned char)((i * 7 + j * 3) & 0xFF);
        adb_put(db, key, (uint16_t)kl, val, 254);
    }
    adb_close(db);
    db = NULL;
    adb_open(path, NULL, &db);
    int bad = 0;
    for (int i = 0; i < N; i++) {
        int kl = snprintf(key, sizeof(key), "k%07d", i);
        for (int j = 0; j < 254; j++) val[j] = (unsigned char)((i * 7 + j * 3) & 0xFF);
        int rc = adb_get(db, key, (uint16_t)kl, vbuf, 256, &vlen);
        if (rc != 0 || vlen != 254 || memcmp(vbuf, val, 254) != 0) bad++;
    }
    adb_close(db); cleanup(path);
    if (bad == 0) PASS(); else { char m[64]; snprintf(m, 64, "%d/%d bad", bad, N); FAIL(m); }
}

// Test 8: Single key stress
static void test_single_key_stress(void) {
    printf("  [08] same key overwritten 1000x, persist last value      "); fflush(stdout);
    const char *path = "/tmp/diag_sk";
    cleanup(path);
    char vbuf[256]; uint16_t vlen;
    adb_t *db = NULL;
    adb_open(path, NULL, &db);
    for (int i = 0; i < 1000; i++) {
        char val[48]; int vl = snprintf(val, sizeof(val), "iteration_%d", i);
        adb_put(db, "testkey", 7, val, (uint16_t)vl);
    }
    adb_close(db);
    db = NULL;
    adb_open(path, NULL, &db);
    int rc = adb_get(db, "testkey", 7, vbuf, 256, &vlen);
    adb_close(db); cleanup(path);
    char expected[48]; int elen = snprintf(expected, sizeof(expected), "iteration_999");
    if (rc == 0 && vlen == (uint16_t)elen && memcmp(vbuf, expected, elen) == 0) PASS();
    else FAIL("wrong value");
}

int main(void) {
    double t0 = now_sec();
    printf("============================================================\n");
    printf("  AssemblyDB Deep Persistence Tests\n");
    printf("============================================================\n\n"); fflush(stdout);
    test_delete_persist();
    test_overwrite_persist();
    test_scan_after_persist();
    test_incremental_sessions();
    test_delete_all_scan_empty();
    test_reinsert_different_value();
    test_large_values_persist();
    test_single_key_stress();
    double t1 = now_sec();
    printf("\n============================================================\n");
    printf("  Results: %d failed (%.1fs)\n", g_fail, t1-t0);
    printf("============================================================\n");
    return g_fail;
}
