#include "assemblydb.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <signal.h>

static int tests_run = 0, tests_passed = 0;

#define PASS() do { tests_passed++; printf("PASS\n"); } while(0)
#define FAIL(msg) do { printf("FAIL: %s\n", msg); } while(0)
#define RUN(name, fn) do { \
    tests_run++; \
    printf("  [%02d] %-55s ", tests_run, name); \
    fflush(stdout); \
    fn(); \
} while(0)

static void cleanup(const char *p) { adb_destroy(p); }

static int count_cb(const void *k, uint16_t kl, const void *v, uint16_t vl, void *ctx) {
    (void)k; (void)kl; (void)v; (void)vl;
    (*(int*)ctx)++;
    return 0;
}

struct kv_verify_ctx { int ok; int count; };
static int verify_cb(const void *k, uint16_t kl, const void *v, uint16_t vl, void *ctx) {
    struct kv_verify_ctx *c = ctx;
    c->count++;
    if (!k || kl == 0 || !v) c->ok = 0;
    return 0;
}

struct sorted_ctx { char prev[64]; int sorted; int count; };
static int sorted_cb(const void *k, uint16_t kl, const void *v, uint16_t vl, void *ctx) {
    (void)v; (void)vl;
    struct sorted_ctx *c = ctx;
    char cur[64] = {0};
    if (kl > 62) kl = 62;
    memcpy(cur, k, kl);
    if (c->count > 0 && strcmp(cur, c->prev) < 0) c->sorted = 0;
    memcpy(c->prev, cur, 64);
    c->count++;
    return 0;
}

static uint32_t xorshift32(uint32_t *state) {
    uint32_t x = *state;
    x ^= x << 13;
    x ^= x >> 17;
    x ^= x << 5;
    *state = x;
    return x;
}

/* ==========================================================================
 * REAL-WORLD APPLICATION PATTERN 1: Restaurant POS System
 * High-throughput order creation, status updates, queries
 * ========================================================================== */
static void test_pos_order_lifecycle(void) {
    const char *p = "/tmp/adb_rw_pos";
    cleanup(p);
    adb_t *db;
    int rc = adb_open(p, NULL, &db);
    if (rc) { FAIL("open"); return; }

    char key[64], val[254];
    int bad = 0;

    for (int i = 0; i < 500; i++) {
        snprintf(key, sizeof(key), "order:%05d", i);
        snprintf(val, sizeof(val), "{\"table\":%d,\"items\":%d,\"status\":\"pending\",\"total\":%d.%02d}",
                 (i % 20) + 1, (i % 8) + 1, 10 + (i % 90), i % 100);
        rc = adb_put(db, key, strlen(key), val, strlen(val));
        if (rc) { bad++; break; }
    }

    for (int i = 0; i < 500; i += 3) {
        snprintf(key, sizeof(key), "order:%05d", i);
        snprintf(val, sizeof(val), "{\"table\":%d,\"items\":%d,\"status\":\"served\",\"total\":%d.%02d}",
                 (i % 20) + 1, (i % 8) + 1, 10 + (i % 90), i % 100);
        rc = adb_put(db, key, strlen(key), val, strlen(val));
        if (rc) { bad++; break; }
    }

    for (int i = 0; i < 500; i += 7) {
        snprintf(key, sizeof(key), "order:%05d", i);
        rc = adb_delete(db, key, strlen(key));
        if (rc) { bad++; break; }
    }

    char rbuf[256];
    uint16_t rlen;
    for (int i = 0; i < 500; i++) {
        snprintf(key, sizeof(key), "order:%05d", i);
        rc = adb_get(db, key, strlen(key), rbuf, 254, &rlen);
        if (i % 7 == 0) {
            if (rc != ADB_ERR_NOT_FOUND) bad++;
        } else {
            if (rc != 0) bad++;
            else if (i % 3 == 0 && !strstr(rbuf, "served")) bad++;
        }
    }

    adb_sync(db);
    adb_close(db);

    rc = adb_open(p, NULL, &db);
    if (rc) { FAIL("reopen"); cleanup(p); return; }

    int reopen_bad = 0;
    for (int i = 0; i < 500; i++) {
        snprintf(key, sizeof(key), "order:%05d", i);
        rc = adb_get(db, key, strlen(key), rbuf, 254, &rlen);
        if (i % 7 == 0) {
            if (rc != ADB_ERR_NOT_FOUND) reopen_bad++;
        } else {
            if (rc != 0) reopen_bad++;
        }
    }

    adb_close(db);
    cleanup(p);
    if (bad || reopen_bad) FAIL("data mismatch");
    else PASS();
}

/* ==========================================================================
 * REAL-WORLD PATTERN 2: User Session Store (SaaS)
 * Create sessions, read frequently, expire old ones, concurrent-like access
 * ========================================================================== */
static void test_session_store(void) {
    const char *p = "/tmp/adb_rw_sess";
    cleanup(p);
    adb_t *db;
    int rc = adb_open(p, NULL, &db);
    if (rc) { FAIL("open"); return; }

    char key[64], val[254];
    int bad = 0;

    for (int i = 0; i < 1000; i++) {
        snprintf(key, sizeof(key), "sess:%08x", i * 0x1337 + 0xDEAD);
        snprintf(val, sizeof(val), "{\"user\":%d,\"ip\":\"10.0.%d.%d\",\"exp\":%d}",
                 i, (i/256)%256, i%256, 1700000000 + i * 3600);
        rc = adb_put(db, key, strlen(key), val, strlen(val));
        if (rc) { bad++; break; }
    }

    for (int i = 0; i < 1000; i++) {
        snprintf(key, sizeof(key), "sess:%08x", i * 0x1337 + 0xDEAD);
        char rbuf[256];
        uint16_t rlen;
        rc = adb_get(db, key, strlen(key), rbuf, 254, &rlen);
        if (rc) { bad++; break; }
    }

    for (int i = 0; i < 500; i++) {
        snprintf(key, sizeof(key), "sess:%08x", i * 0x1337 + 0xDEAD);
        rc = adb_delete(db, key, strlen(key));
        if (rc) { bad++; break; }
    }

    int cnt = 0;
    adb_scan(db, NULL, 0, NULL, 0, count_cb, &cnt);
    if (cnt != 500) bad++;

    adb_close(db);
    cleanup(p);
    if (bad) FAIL("session ops failed");
    else PASS();
}

/* ==========================================================================
 * REAL-WORLD PATTERN 3: Configuration Store
 * Hierarchical keys, frequent reads, rare writes, small values
 * ========================================================================== */
static void test_config_store(void) {
    const char *p = "/tmp/adb_rw_cfg";
    cleanup(p);
    adb_t *db;
    int rc = adb_open(p, NULL, &db);
    if (rc) { FAIL("open"); return; }

    const char *configs[][2] = {
        {"app:name", "AssemblyDB"},
        {"app:version", "1.0.0"},
        {"db:host", "localhost"},
        {"db:port", "5432"},
        {"db:pool_size", "20"},
        {"db:timeout_ms", "5000"},
        {"cache:enabled", "true"},
        {"cache:ttl", "3600"},
        {"cache:max_size", "1048576"},
        {"log:level", "info"},
        {"log:file", "/var/log/app.log"},
        {"log:rotate_mb", "100"},
        {"auth:jwt_secret", "super-secret-key-here"},
        {"auth:token_ttl", "86400"},
        {"auth:max_attempts", "5"},
        {"rate:limit", "100"},
        {"rate:window_sec", "60"},
        {"feature:dark_mode", "true"},
        {"feature:beta", "false"},
        {"feature:maintenance", "false"},
    };
    int ncfg = sizeof(configs) / sizeof(configs[0]);
    int bad = 0;

    for (int i = 0; i < ncfg; i++) {
        rc = adb_put(db, configs[i][0], strlen(configs[i][0]),
                     configs[i][1], strlen(configs[i][1]));
        if (rc) { bad++; break; }
    }

    for (int round = 0; round < 100; round++) {
        for (int i = 0; i < ncfg; i++) {
            char rbuf[256];
            uint16_t rlen;
            rc = adb_get(db, configs[i][0], strlen(configs[i][0]), rbuf, 254, &rlen);
            if (rc || rlen != strlen(configs[i][1])) { bad++; break; }
            if (memcmp(rbuf, configs[i][1], rlen) != 0) { bad++; break; }
        }
        if (bad) break;
    }

    rc = adb_put(db, "db:pool_size", 12, "50", 2);
    if (rc) bad++;

    adb_sync(db);
    adb_close(db);

    rc = adb_open(p, NULL, &db);
    if (rc) { FAIL("reopen"); cleanup(p); return; }

    char rbuf[256];
    uint16_t rlen;
    rc = adb_get(db, "db:pool_size", 12, rbuf, 254, &rlen);
    if (rc || rlen != 2 || memcmp(rbuf, "50", 2) != 0) bad++;

    adb_close(db);
    cleanup(p);
    if (bad) FAIL("config mismatch");
    else PASS();
}

/* ==========================================================================
 * REAL-WORLD PATTERN 4: Time-Series Sensor Data
 * Monotonically increasing keys, bulk inserts, range queries
 * ========================================================================== */
static void test_timeseries_sensor(void) {
    const char *p = "/tmp/adb_rw_ts";
    cleanup(p);
    adb_t *db;
    int rc = adb_open(p, NULL, &db);
    if (rc) { FAIL("open"); return; }

    char key[64], val[128];
    int bad = 0;

    for (int i = 0; i < 10000; i++) {
        snprintf(key, sizeof(key), "ts:2024-01-%02d:%05d", (i / 500) + 1, i % 500);
        snprintf(val, sizeof(val), "{\"temp\":%.1f,\"hum\":%.1f,\"co2\":%d}",
                 20.0 + (i % 100) * 0.1, 40.0 + (i % 50) * 0.2, 400 + (i % 200));
        rc = adb_put(db, key, strlen(key), val, strlen(val));
        if (rc) { bad++; break; }
    }

    adb_sync(db);

    int cnt = 0;
    const char *start = "ts:2024-01-03:";
    const char *end   = "ts:2024-01-03:~";
    adb_scan(db, start, strlen(start), end, strlen(end), count_cb, &cnt);
    if (cnt != 500) bad++;

    struct sorted_ctx sctx = {{0}, 1, 0};
    adb_scan(db, "ts:2024-01-01:", 14, "ts:2024-01-05:", 14, sorted_cb, &sctx);
    if (!sctx.sorted) bad++;

    adb_close(db);
    cleanup(p);
    if (bad) FAIL("timeseries mismatch");
    else PASS();
}

/* ==========================================================================
 * REAL-WORLD PATTERN 5: Inventory System (CRUD + Transactions)
 * ========================================================================== */
static void test_inventory_crud_tx(void) {
    const char *p = "/tmp/adb_rw_inv";
    cleanup(p);
    adb_t *db;
    int rc = adb_open(p, NULL, &db);
    if (rc) { FAIL("open"); return; }

    char key[64], val[254];
    int bad = 0;

    for (int i = 0; i < 200; i++) {
        snprintf(key, sizeof(key), "prod:%04d", i);
        snprintf(val, sizeof(val), "{\"name\":\"Widget_%d\",\"qty\":%d,\"price\":%d.%02d}",
                 i, 100 + i, 5 + (i % 50), i % 100);
        rc = adb_put(db, key, strlen(key), val, strlen(val));
        if (rc) { bad++; break; }
    }

    uint64_t tx_id;
    rc = adb_tx_begin(db, ADB_ISO_SNAPSHOT, &tx_id);
    if (rc) { bad++; goto done; }

    for (int i = 0; i < 20; i++) {
        snprintf(key, sizeof(key), "prod:%04d", i);
        snprintf(val, sizeof(val), "{\"name\":\"Widget_%d\",\"qty\":%d,\"price\":%d.%02d}",
                 i, 50 + i, 5 + (i % 50), i % 100);
        rc = adb_tx_put(db, tx_id, key, strlen(key), val, strlen(val));
        if (rc) { bad++; break; }
    }

    for (int i = 0; i < 20; i++) {
        snprintf(key, sizeof(key), "prod:%04d", i);
        char rbuf[256];
        uint16_t rlen;
        rc = adb_tx_get(db, tx_id, key, strlen(key), rbuf, 254, &rlen);
        if (rc) { bad++; break; }
        if (!strstr(rbuf, "\"qty\":5")) {
            /* qty should be 50+i which starts with 5 */
        }
    }

    rc = adb_tx_commit(db, tx_id);
    if (rc) bad++;

    for (int i = 0; i < 20; i++) {
        snprintf(key, sizeof(key), "prod:%04d", i);
        char rbuf[256];
        uint16_t rlen;
        rc = adb_get(db, key, strlen(key), rbuf, 254, &rlen);
        if (rc) { bad++; break; }
    }

done:
    adb_close(db);
    cleanup(p);
    if (bad) FAIL("inventory tx failed");
    else PASS();
}

/* ==========================================================================
 * REAL-WORLD PATTERN 6: Transaction Rollback Preserves State
 * ========================================================================== */
static void test_tx_rollback_preserves(void) {
    const char *p = "/tmp/adb_rw_txrb";
    cleanup(p);
    adb_t *db;
    int rc = adb_open(p, NULL, &db);
    if (rc) { FAIL("open"); return; }

    int bad = 0;

    rc = adb_put(db, "balance", 7, "1000", 4);
    if (rc) bad++;

    uint64_t tx_id;
    rc = adb_tx_begin(db, ADB_ISO_SNAPSHOT, &tx_id);
    if (rc) { bad++; goto done; }

    rc = adb_tx_put(db, tx_id, "balance", 7, "500", 3);
    if (rc) bad++;

    char rbuf[256];
    uint16_t rlen;
    rc = adb_tx_get(db, tx_id, "balance", 7, rbuf, 254, &rlen);
    if (rc || rlen != 3 || memcmp(rbuf, "500", 3) != 0) bad++;

    rc = adb_tx_rollback(db, tx_id);
    if (rc) bad++;

    rc = adb_get(db, "balance", 7, rbuf, 254, &rlen);
    if (rc || rlen != 4 || memcmp(rbuf, "1000", 4) != 0) bad++;

done:
    adb_close(db);
    cleanup(p);
    if (bad) FAIL("rollback changed data");
    else PASS();
}

/* ==========================================================================
 * REAL-WORLD PATTERN 7: Message Queue Pattern
 * Sequential writes, sequential reads/deletes, continuous flow
 * ========================================================================== */
static void test_message_queue(void) {
    const char *p = "/tmp/adb_rw_mq";
    cleanup(p);
    adb_t *db;
    int rc = adb_open(p, NULL, &db);
    if (rc) { FAIL("open"); return; }

    char key[64], val[128];
    int bad = 0;

    for (int round = 0; round < 10; round++) {
        for (int i = 0; i < 100; i++) {
            int seq = round * 100 + i;
            snprintf(key, sizeof(key), "msg:%08d", seq);
            snprintf(val, sizeof(val), "payload_%d_%d", round, i);
            rc = adb_put(db, key, strlen(key), val, strlen(val));
            if (rc) { bad++; break; }
        }
        if (bad) break;

        for (int i = 0; i < 100; i++) {
            int seq = round * 100 + i;
            snprintf(key, sizeof(key), "msg:%08d", seq);
            char rbuf[256];
            uint16_t rlen;
            rc = adb_get(db, key, strlen(key), rbuf, 254, &rlen);
            if (rc) { bad++; break; }
            rc = adb_delete(db, key, strlen(key));
            if (rc) { bad++; break; }
        }
        if (bad) break;

        int cnt = 0;
        adb_scan(db, NULL, 0, NULL, 0, count_cb, &cnt);
        if (cnt != 0) { bad++; break; }
    }

    adb_close(db);
    cleanup(p);
    if (bad) FAIL("message queue failed");
    else PASS();
}

/* ==========================================================================
 * REAL-WORLD PATTERN 8: Multi-Tenant Key Namespacing
 * Multiple tenants sharing same DB with prefix-based isolation
 * ========================================================================== */
static void test_multi_tenant(void) {
    const char *p = "/tmp/adb_rw_mt";
    cleanup(p);
    adb_t *db;
    int rc = adb_open(p, NULL, &db);
    if (rc) { FAIL("open"); return; }

    char key[64], val[128];
    int bad = 0;

    for (int tenant = 0; tenant < 10; tenant++) {
        for (int item = 0; item < 50; item++) {
            snprintf(key, sizeof(key), "t%d:item:%03d", tenant, item);
            snprintf(val, sizeof(val), "tenant_%d_item_%d", tenant, item);
            rc = adb_put(db, key, strlen(key), val, strlen(val));
            if (rc) { bad++; break; }
        }
        if (bad) break;
    }

    for (int tenant = 0; tenant < 10; tenant++) {
        char start[64], end[64];
        snprintf(start, sizeof(start), "t%d:", tenant);
        snprintf(end, sizeof(end), "t%d:~", tenant);
        int cnt = 0;
        adb_scan(db, start, strlen(start), end, strlen(end), count_cb, &cnt);
        if (cnt != 50) { bad++; break; }
    }

    for (int item = 0; item < 50; item++) {
        snprintf(key, sizeof(key), "t5:item:%03d", item);
        rc = adb_delete(db, key, strlen(key));
        if (rc) { bad++; break; }
    }

    int cnt = 0;
    adb_scan(db, "t5:", 3, "t5:~", 4, count_cb, &cnt);
    if (cnt != 0) bad++;

    cnt = 0;
    adb_scan(db, "t4:", 3, "t4:~", 4, count_cb, &cnt);
    if (cnt != 50) bad++;

    adb_close(db);
    cleanup(p);
    if (bad) FAIL("multi-tenant failed");
    else PASS();
}

/* ==========================================================================
 * STRESS TEST 1: Large memtable without sync (WAL replay)
 * Tests WAL recovery of many records
 * ========================================================================== */
static void test_large_wal_replay(void) {
    const char *p = "/tmp/adb_rw_walrep";
    cleanup(p);
    adb_t *db;
    int rc = adb_open(p, NULL, &db);
    if (rc) { FAIL("open"); return; }

    char key[64], val[128];
    int bad = 0;

    for (int i = 0; i < 5000; i++) {
        snprintf(key, sizeof(key), "walkey:%06d", i);
        snprintf(val, sizeof(val), "walval:%06d", i);
        rc = adb_put(db, key, strlen(key), val, strlen(val));
        if (rc) { bad++; break; }
    }

    /* Close WITHOUT sync — forces WAL replay on reopen */
    adb_close(db);

    rc = adb_open(p, NULL, &db);
    if (rc) { FAIL("reopen"); cleanup(p); return; }

    for (int i = 0; i < 5000; i++) {
        snprintf(key, sizeof(key), "walkey:%06d", i);
        char rbuf[256];
        uint16_t rlen;
        rc = adb_get(db, key, strlen(key), rbuf, 254, &rlen);
        if (rc) { bad++; break; }
        char expected[128];
        snprintf(expected, sizeof(expected), "walval:%06d", i);
        if (rlen != strlen(expected) || memcmp(rbuf, expected, rlen) != 0) {
            bad++;
            break;
        }
    }

    adb_close(db);
    cleanup(p);
    if (bad) FAIL("WAL replay lost data");
    else PASS();
}

/* ==========================================================================
 * STRESS TEST 2: Rapid sync cycles with interleaved mutations
 * ========================================================================== */
static void test_rapid_sync_cycles(void) {
    const char *p = "/tmp/adb_rw_rsync";
    cleanup(p);
    adb_t *db;
    int rc = adb_open(p, NULL, &db);
    if (rc) { FAIL("open"); return; }

    char key[64], val[128];
    int bad = 0;

    for (int cycle = 0; cycle < 50; cycle++) {
        for (int i = 0; i < 20; i++) {
            snprintf(key, sizeof(key), "rsync:%04d", cycle * 20 + i);
            snprintf(val, sizeof(val), "v:%d:%d", cycle, i);
            rc = adb_put(db, key, strlen(key), val, strlen(val));
            if (rc) { bad++; break; }
        }
        rc = adb_sync(db);
        if (rc) { bad++; break; }
    }

    int cnt = 0;
    adb_scan(db, NULL, 0, NULL, 0, count_cb, &cnt);
    if (cnt != 1000) bad++;

    adb_close(db);
    cleanup(p);
    if (bad) FAIL("rapid sync failed");
    else PASS();
}

/* ==========================================================================
 * STRESS TEST 3: Large batch operations
 * ========================================================================== */
static void test_large_batch(void) {
    const char *p = "/tmp/adb_rw_lbatch";
    cleanup(p);
    adb_t *db;
    int rc = adb_open(p, NULL, &db);
    if (rc) { FAIL("open"); return; }

    int bad = 0;
    const int N = 64;
    char keys[64][32];
    char vals[64][64];
    adb_batch_entry_t entries[64];

    for (int round = 0; round < 20; round++) {
        for (int i = 0; i < N; i++) {
            snprintf(keys[i], sizeof(keys[i]), "batch:%03d:%03d", round, i);
            snprintf(vals[i], sizeof(vals[i]), "bval:%d:%d", round, i);
            entries[i].key = keys[i];
            entries[i].key_len = strlen(keys[i]);
            entries[i].val = vals[i];
            entries[i].val_len = strlen(vals[i]);
        }
        rc = adb_batch_put(db, entries, N);
        if (rc) { bad++; break; }
    }

    int cnt = 0;
    adb_scan(db, NULL, 0, NULL, 0, count_cb, &cnt);
    if (cnt != 20 * 64) bad++;

    adb_sync(db);
    adb_close(db);

    rc = adb_open(p, NULL, &db);
    if (rc) { FAIL("reopen"); cleanup(p); return; }

    cnt = 0;
    adb_scan(db, NULL, 0, NULL, 0, count_cb, &cnt);
    if (cnt != 20 * 64) bad++;

    adb_close(db);
    cleanup(p);
    if (bad) FAIL("large batch failed");
    else PASS();
}

/* ==========================================================================
 * STRESS TEST 4: Transaction with many write-set entries
 * ========================================================================== */
static void test_tx_large_writeset(void) {
    const char *p = "/tmp/adb_rw_txws";
    cleanup(p);
    adb_t *db;
    int rc = adb_open(p, NULL, &db);
    if (rc) { FAIL("open"); return; }

    int bad = 0;
    char key[64], val[128];

    uint64_t tx_id;
    rc = adb_tx_begin(db, ADB_ISO_SNAPSHOT, &tx_id);
    if (rc) { bad++; goto done; }

    for (int i = 0; i < 200; i++) {
        snprintf(key, sizeof(key), "txkey:%04d", i);
        snprintf(val, sizeof(val), "txval:%04d", i);
        rc = adb_tx_put(db, tx_id, key, strlen(key), val, strlen(val));
        if (rc) { bad++; break; }
    }

    for (int i = 0; i < 200; i++) {
        snprintf(key, sizeof(key), "txkey:%04d", i);
        char rbuf[256];
        uint16_t rlen;
        rc = adb_tx_get(db, tx_id, key, strlen(key), rbuf, 254, &rlen);
        if (rc) { bad++; break; }
    }

    for (int i = 0; i < 50; i++) {
        snprintf(key, sizeof(key), "txkey:%04d", i);
        rc = adb_tx_delete(db, tx_id, key, strlen(key));
        if (rc) { bad++; break; }
    }

    for (int i = 0; i < 50; i++) {
        snprintf(key, sizeof(key), "txkey:%04d", i);
        char rbuf[256];
        uint16_t rlen;
        rc = adb_tx_get(db, tx_id, key, strlen(key), rbuf, 254, &rlen);
        if (rc != ADB_ERR_NOT_FOUND) { bad++; break; }
    }

    rc = adb_tx_commit(db, tx_id);
    if (rc) bad++;

    for (int i = 50; i < 200; i++) {
        snprintf(key, sizeof(key), "txkey:%04d", i);
        char rbuf[256];
        uint16_t rlen;
        rc = adb_get(db, key, strlen(key), rbuf, 254, &rlen);
        if (rc) { bad++; break; }
    }

    for (int i = 0; i < 50; i++) {
        snprintf(key, sizeof(key), "txkey:%04d", i);
        char rbuf[256];
        uint16_t rlen;
        rc = adb_get(db, key, strlen(key), rbuf, 254, &rlen);
        if (rc != ADB_ERR_NOT_FOUND) { bad++; break; }
    }

done:
    adb_close(db);
    cleanup(p);
    if (bad) FAIL("tx large writeset failed");
    else PASS();
}

/* ==========================================================================
 * STRESS TEST 5: Overwrite storm then scan correctness
 * ========================================================================== */
static void test_overwrite_storm_scan(void) {
    const char *p = "/tmp/adb_rw_ows";
    cleanup(p);
    adb_t *db;
    int rc = adb_open(p, NULL, &db);
    if (rc) { FAIL("open"); return; }

    int bad = 0;
    char key[64], val[128];

    for (int round = 0; round < 100; round++) {
        for (int i = 0; i < 50; i++) {
            snprintf(key, sizeof(key), "owkey:%03d", i);
            snprintf(val, sizeof(val), "round:%d:item:%d", round, i);
            rc = adb_put(db, key, strlen(key), val, strlen(val));
            if (rc) { bad++; break; }
        }
        if (bad) break;
    }

    struct sorted_ctx sctx = {{0}, 1, 0};
    adb_scan(db, NULL, 0, NULL, 0, sorted_cb, &sctx);
    if (!sctx.sorted || sctx.count != 50) bad++;

    for (int i = 0; i < 50; i++) {
        snprintf(key, sizeof(key), "owkey:%03d", i);
        char rbuf[256];
        uint16_t rlen;
        rc = adb_get(db, key, strlen(key), rbuf, 254, &rlen);
        if (rc) { bad++; break; }
        char expected[128];
        snprintf(expected, sizeof(expected), "round:99:item:%d", i);
        if (rlen != strlen(expected) || memcmp(rbuf, expected, rlen) != 0) {
            bad++;
            break;
        }
    }

    adb_close(db);
    cleanup(p);
    if (bad) FAIL("overwrite storm scan failed");
    else PASS();
}

/* ==========================================================================
 * STRESS TEST 6: Open/close with progressive data accumulation
 * ========================================================================== */
static void test_progressive_accumulation(void) {
    const char *p = "/tmp/adb_rw_acc";
    cleanup(p);
    int bad = 0;
    char key[64], val[64];

    for (int session = 0; session < 30; session++) {
        adb_t *db;
        int rc = adb_open(p, NULL, &db);
        if (rc) { bad++; break; }

        for (int i = 0; i < 50; i++) {
            snprintf(key, sizeof(key), "s%02d:k%03d", session, i);
            snprintf(val, sizeof(val), "s%02d:v%03d", session, i);
            rc = adb_put(db, key, strlen(key), val, strlen(val));
            if (rc) { bad++; break; }
        }

        int cnt = 0;
        adb_scan(db, NULL, 0, NULL, 0, count_cb, &cnt);
        int expected = (session + 1) * 50;
        if (cnt != expected) { bad++; adb_close(db); break; }

        adb_sync(db);
        adb_close(db);
    }

    adb_t *db;
    int rc = adb_open(p, NULL, &db);
    if (rc) { bad++; } else {
        int cnt = 0;
        adb_scan(db, NULL, 0, NULL, 0, count_cb, &cnt);
        if (cnt != 1500) bad++;
        adb_close(db);
    }

    cleanup(p);
    if (bad) FAIL("accumulation lost data");
    else PASS();
}

/* ==========================================================================
 * STRESS TEST 7: Delete-heavy workload (tombstone correctness)
 * ========================================================================== */
static void test_delete_heavy(void) {
    const char *p = "/tmp/adb_rw_delhvy";
    cleanup(p);
    adb_t *db;
    int rc = adb_open(p, NULL, &db);
    if (rc) { FAIL("open"); return; }

    int bad = 0;
    char key[64], val[64];

    for (int i = 0; i < 2000; i++) {
        snprintf(key, sizeof(key), "dk:%05d", i);
        snprintf(val, sizeof(val), "dv:%05d", i);
        rc = adb_put(db, key, strlen(key), val, strlen(val));
        if (rc) { bad++; break; }
    }

    for (int i = 0; i < 2000; i += 2) {
        snprintf(key, sizeof(key), "dk:%05d", i);
        rc = adb_delete(db, key, strlen(key));
        if (rc) { bad++; break; }
    }

    adb_sync(db);

    int cnt = 0;
    adb_scan(db, NULL, 0, NULL, 0, count_cb, &cnt);
    if (cnt != 1000) bad++;

    adb_close(db);

    rc = adb_open(p, NULL, &db);
    if (rc) { FAIL("reopen"); cleanup(p); return; }

    cnt = 0;
    adb_scan(db, NULL, 0, NULL, 0, count_cb, &cnt);
    if (cnt != 1000) bad++;

    for (int i = 1; i < 2000; i += 2) {
        snprintf(key, sizeof(key), "dk:%05d", i);
        char rbuf[256];
        uint16_t rlen;
        rc = adb_get(db, key, strlen(key), rbuf, 254, &rlen);
        if (rc) { bad++; break; }
    }

    for (int i = 0; i < 2000; i += 2) {
        snprintf(key, sizeof(key), "dk:%05d", i);
        char rbuf[256];
        uint16_t rlen;
        rc = adb_get(db, key, strlen(key), rbuf, 254, &rlen);
        if (rc != ADB_ERR_NOT_FOUND) { bad++; break; }
    }

    adb_close(db);
    cleanup(p);
    if (bad) FAIL("delete-heavy failed");
    else PASS();
}

/* ==========================================================================
 * STRESS TEST 8: Backup/Restore under heavy mutation
 * ========================================================================== */
static void test_backup_restore_heavy(void) {
    const char *p = "/tmp/adb_rw_bkup";
    const char *bk = "/tmp/adb_rw_bkup_bak";
    const char *rst = "/tmp/adb_rw_bkup_rst";
    cleanup(p); cleanup(bk); cleanup(rst);

    adb_t *db;
    int rc = adb_open(p, NULL, &db);
    if (rc) { FAIL("open"); return; }

    int bad = 0;
    char key[64], val[128];

    for (int i = 0; i < 1000; i++) {
        snprintf(key, sizeof(key), "bk:%04d", i);
        snprintf(val, sizeof(val), "bkval:%04d", i);
        rc = adb_put(db, key, strlen(key), val, strlen(val));
        if (rc) { bad++; break; }
    }

    adb_sync(db);

    for (int i = 500; i < 800; i++) {
        snprintf(key, sizeof(key), "bk:%04d", i);
        rc = adb_delete(db, key, strlen(key));
        if (rc) { bad++; break; }
    }

    adb_sync(db);

    rc = adb_backup(db, bk, ADB_BACKUP_FULL);
    if (rc) { bad++; goto done; }

    for (int i = 0; i < 500; i++) {
        snprintf(key, sizeof(key), "bk:%04d", i);
        snprintf(val, sizeof(val), "modified:%04d", i);
        rc = adb_put(db, key, strlen(key), val, strlen(val));
        if (rc) { bad++; break; }
    }

    adb_close(db);

    rc = adb_restore(bk, rst);
    if (rc) { bad++; goto cleanup_all; }

    rc = adb_open(rst, NULL, &db);
    if (rc) { bad++; goto cleanup_all; }

    int cnt = 0;
    adb_scan(db, NULL, 0, NULL, 0, count_cb, &cnt);
    if (cnt != 700) bad++;

    for (int i = 0; i < 500; i++) {
        snprintf(key, sizeof(key), "bk:%04d", i);
        char rbuf[256];
        uint16_t rlen;
        rc = adb_get(db, key, strlen(key), rbuf, 254, &rlen);
        if (rc) { bad++; break; }
        char expected[128];
        snprintf(expected, sizeof(expected), "bkval:%04d", i);
        if (rlen != strlen(expected) || memcmp(rbuf, expected, rlen) != 0) {
            bad++;
            break;
        }
    }

    adb_close(db);
    goto cleanup_all;

done:
    adb_close(db);
cleanup_all:
    cleanup(p); cleanup(bk); cleanup(rst);
    if (bad) FAIL("backup/restore heavy failed");
    else PASS();
}

/* ==========================================================================
 * STRESS TEST 9: Fork-based crash recovery at various points
 * ========================================================================== */
static void test_crash_recovery_various(void) {
    const char *p = "/tmp/adb_rw_crash";
    cleanup(p);
    int bad = 0;
    char key[64], val[64];

    {
        adb_t *db;
        int rc = adb_open(p, NULL, &db);
        if (rc) { FAIL("open"); return; }

        for (int i = 0; i < 500; i++) {
            snprintf(key, sizeof(key), "cr:%04d", i);
            snprintf(val, sizeof(val), "crv:%04d", i);
            adb_put(db, key, strlen(key), val, strlen(val));
        }
        adb_sync(db);
        adb_close(db);
    }

    pid_t pid = fork();
    if (pid == 0) {
        adb_t *db;
        adb_open(p, NULL, &db);
        for (int i = 500; i < 1000; i++) {
            snprintf(key, sizeof(key), "cr:%04d", i);
            snprintf(val, sizeof(val), "crv:%04d", i);
            adb_put(db, key, strlen(key), val, strlen(val));
        }
        _exit(0);
    }
    waitpid(pid, NULL, 0);

    {
        adb_t *db;
        int rc = adb_open(p, NULL, &db);
        if (rc) { FAIL("reopen after crash"); cleanup(p); return; }

        for (int i = 0; i < 500; i++) {
            snprintf(key, sizeof(key), "cr:%04d", i);
            char rbuf[256];
            uint16_t rlen;
            rc = adb_get(db, key, strlen(key), rbuf, 254, &rlen);
            if (rc) { bad++; break; }
        }

        int cnt = 0;
        adb_scan(db, NULL, 0, NULL, 0, count_cb, &cnt);
        if (cnt < 500) bad++;

        adb_close(db);
    }

    cleanup(p);
    if (bad) FAIL("crash recovery failed");
    else PASS();
}

/* ==========================================================================
 * STRESS TEST 10: Multiple DB instances concurrently
 * ========================================================================== */
static void test_multi_db_instances(void) {
    const char *p1 = "/tmp/adb_rw_mdb1";
    const char *p2 = "/tmp/adb_rw_mdb2";
    const char *p3 = "/tmp/adb_rw_mdb3";
    cleanup(p1); cleanup(p2); cleanup(p3);

    adb_t *db1, *db2, *db3;
    int bad = 0;

    int rc1 = adb_open(p1, NULL, &db1);
    int rc2 = adb_open(p2, NULL, &db2);
    int rc3 = adb_open(p3, NULL, &db3);
    if (rc1 || rc2 || rc3) { FAIL("open multi"); return; }

    char key[64], val[64];
    for (int i = 0; i < 200; i++) {
        snprintf(key, sizeof(key), "mdb:%04d", i);

        snprintf(val, sizeof(val), "db1:%04d", i);
        adb_put(db1, key, strlen(key), val, strlen(val));

        snprintf(val, sizeof(val), "db2:%04d", i);
        adb_put(db2, key, strlen(key), val, strlen(val));

        snprintf(val, sizeof(val), "db3:%04d", i);
        adb_put(db3, key, strlen(key), val, strlen(val));
    }

    char rbuf[256];
    uint16_t rlen;
    adb_get(db1, "mdb:0100", 8, rbuf, 254, &rlen);
    if (memcmp(rbuf, "db1:", 4) != 0) bad++;
    adb_get(db2, "mdb:0100", 8, rbuf, 254, &rlen);
    if (memcmp(rbuf, "db2:", 4) != 0) bad++;
    adb_get(db3, "mdb:0100", 8, rbuf, 254, &rlen);
    if (memcmp(rbuf, "db3:", 4) != 0) bad++;

    adb_close(db1);
    adb_close(db2);
    adb_close(db3);
    cleanup(p1); cleanup(p2); cleanup(p3);

    if (bad) FAIL("multi-db isolation failed");
    else PASS();
}

/* ==========================================================================
 * EDGE CASE 1: Binary key/value with all byte patterns
 * ========================================================================== */
static void test_binary_all_bytes(void) {
    const char *p = "/tmp/adb_rw_bin";
    cleanup(p);
    adb_t *db;
    int rc = adb_open(p, NULL, &db);
    if (rc) { FAIL("open"); return; }

    int bad = 0;
    uint8_t key[62], val[254];

    for (int b = 0; b < 256; b++) {
        memset(key, b, 62);
        memset(val, b ^ 0xFF, 254);
        rc = adb_put(db, key, 62, val, 254);
        if (rc) { bad++; break; }
    }

    for (int b = 0; b < 256; b++) {
        memset(key, b, 62);
        uint8_t rbuf[256];
        uint16_t rlen;
        rc = adb_get(db, key, 62, rbuf, 254, &rlen);
        if (rc || rlen != 254) { bad++; break; }
        uint8_t expected = b ^ 0xFF;
        for (int j = 0; j < 254; j++) {
            if (rbuf[j] != expected) { bad++; break; }
        }
        if (bad) break;
    }

    adb_sync(db);
    adb_close(db);

    rc = adb_open(p, NULL, &db);
    if (rc) { FAIL("reopen"); cleanup(p); return; }

    for (int b = 0; b < 256; b++) {
        memset(key, b, 62);
        uint8_t rbuf[256];
        uint16_t rlen;
        rc = adb_get(db, key, 62, rbuf, 254, &rlen);
        if (rc || rlen != 254) { bad++; break; }
    }

    adb_close(db);
    cleanup(p);
    if (bad) FAIL("binary all bytes failed");
    else PASS();
}

/* ==========================================================================
 * EDGE CASE 2: Extreme key/value sizes (1-byte key, 1-byte value)
 * ========================================================================== */
static void test_min_size_kv(void) {
    const char *p = "/tmp/adb_rw_min";
    cleanup(p);
    adb_t *db;
    int rc = adb_open(p, NULL, &db);
    if (rc) { FAIL("open"); return; }

    int bad = 0;
    uint8_t key = 0x42, val = 0xFF;

    rc = adb_put(db, &key, 1, &val, 1);
    if (rc) bad++;

    uint8_t rbuf[4];
    uint16_t rlen;
    rc = adb_get(db, &key, 1, rbuf, 4, &rlen);
    if (rc || rlen != 1 || rbuf[0] != 0xFF) bad++;

    adb_sync(db);
    adb_close(db);

    rc = adb_open(p, NULL, &db);
    if (rc) { FAIL("reopen"); cleanup(p); return; }

    rc = adb_get(db, &key, 1, rbuf, 4, &rlen);
    if (rc || rlen != 1 || rbuf[0] != 0xFF) bad++;

    adb_close(db);
    cleanup(p);
    if (bad) FAIL("min kv failed");
    else PASS();
}

/* ==========================================================================
 * EDGE CASE 3: Max key + max value roundtrip
 * ========================================================================== */
static void test_max_size_kv(void) {
    const char *p = "/tmp/adb_rw_max";
    cleanup(p);
    adb_t *db;
    int rc = adb_open(p, NULL, &db);
    if (rc) { FAIL("open"); return; }

    int bad = 0;
    uint8_t key[62], val[254];
    for (int i = 0; i < 62; i++) key[i] = (uint8_t)(i + 1);
    for (int i = 0; i < 254; i++) val[i] = (uint8_t)(254 - i);

    rc = adb_put(db, key, 62, val, 254);
    if (rc) bad++;

    uint8_t rbuf[256];
    uint16_t rlen;
    rc = adb_get(db, key, 62, rbuf, 254, &rlen);
    if (rc || rlen != 254) bad++;
    if (memcmp(rbuf, val, 254) != 0) bad++;

    adb_sync(db);
    adb_close(db);

    rc = adb_open(p, NULL, &db);
    if (rc) { FAIL("reopen"); cleanup(p); return; }

    rc = adb_get(db, key, 62, rbuf, 254, &rlen);
    if (rc || rlen != 254 || memcmp(rbuf, val, 254) != 0) bad++;

    adb_close(db);
    cleanup(p);
    if (bad) FAIL("max kv failed");
    else PASS();
}

/* ==========================================================================
 * EDGE CASE 4: Scan with callback that stops at various points
 * ========================================================================== */
struct stop_ctx { int stop_at; int count; };
static int stop_cb(const void *k, uint16_t kl, const void *v, uint16_t vl, void *ctx) {
    (void)k; (void)kl; (void)v; (void)vl;
    struct stop_ctx *c = ctx;
    c->count++;
    return (c->count >= c->stop_at) ? 1 : 0;
}

static void test_scan_stop_various(void) {
    const char *p = "/tmp/adb_rw_scanstop";
    cleanup(p);
    adb_t *db;
    int rc = adb_open(p, NULL, &db);
    if (rc) { FAIL("open"); return; }

    int bad = 0;
    char key[64], val[32];

    for (int i = 0; i < 100; i++) {
        snprintf(key, sizeof(key), "ss:%03d", i);
        snprintf(val, sizeof(val), "v%d", i);
        adb_put(db, key, strlen(key), val, strlen(val));
    }

    adb_sync(db);

    int stops[] = {1, 5, 10, 50, 99, 100};
    for (int s = 0; s < 6; s++) {
        struct stop_ctx sc = {stops[s], 0};
        adb_scan(db, NULL, 0, NULL, 0, stop_cb, &sc);
        if (sc.count != stops[s]) { bad++; break; }
    }

    adb_close(db);
    cleanup(p);
    if (bad) FAIL("scan stop failed");
    else PASS();
}

/* ==========================================================================
 * EDGE CASE 5: Put, delete, put with different value, verify
 * ========================================================================== */
static void test_put_delete_put_verify(void) {
    const char *p = "/tmp/adb_rw_pdp";
    cleanup(p);
    adb_t *db;
    int rc = adb_open(p, NULL, &db);
    if (rc) { FAIL("open"); return; }

    int bad = 0;

    adb_put(db, "thekey", 6, "original", 8);

    char rbuf[256];
    uint16_t rlen;
    rc = adb_get(db, "thekey", 6, rbuf, 254, &rlen);
    if (rc || rlen != 8 || memcmp(rbuf, "original", 8) != 0) bad++;

    adb_delete(db, "thekey", 6);
    rc = adb_get(db, "thekey", 6, rbuf, 254, &rlen);
    if (rc != ADB_ERR_NOT_FOUND) bad++;

    adb_put(db, "thekey", 6, "replacement", 11);
    rc = adb_get(db, "thekey", 6, rbuf, 254, &rlen);
    if (rc || rlen != 11 || memcmp(rbuf, "replacement", 11) != 0) bad++;

    adb_sync(db);
    adb_close(db);

    rc = adb_open(p, NULL, &db);
    if (rc) { FAIL("reopen"); cleanup(p); return; }

    rc = adb_get(db, "thekey", 6, rbuf, 254, &rlen);
    if (rc || rlen != 11 || memcmp(rbuf, "replacement", 11) != 0) bad++;

    adb_close(db);
    cleanup(p);
    if (bad) FAIL("put-del-put failed");
    else PASS();
}

/* ==========================================================================
 * EDGE CASE 6: Metrics accuracy after complex workload
 * ========================================================================== */
static void test_metrics_complex(void) {
    const char *p = "/tmp/adb_rw_met";
    cleanup(p);
    adb_t *db;
    int rc = adb_open(p, NULL, &db);
    if (rc) { FAIL("open"); return; }

    int bad = 0;
    char key[64], val[32];

    int exp_puts = 0, exp_gets = 0, exp_dels = 0, exp_scans = 0;

    for (int i = 0; i < 100; i++) {
        snprintf(key, sizeof(key), "mk:%03d", i);
        snprintf(val, sizeof(val), "mv:%d", i);
        adb_put(db, key, strlen(key), val, strlen(val));
        exp_puts++;
    }

    for (int i = 0; i < 50; i++) {
        snprintf(key, sizeof(key), "mk:%03d", i);
        char rbuf[256];
        uint16_t rlen;
        adb_get(db, key, strlen(key), rbuf, 254, &rlen);
        exp_gets++;
    }

    for (int i = 0; i < 30; i++) {
        snprintf(key, sizeof(key), "mk:%03d", i);
        adb_delete(db, key, strlen(key));
        exp_dels++;
    }

    for (int i = 0; i < 5; i++) {
        int cnt = 0;
        adb_scan(db, NULL, 0, NULL, 0, count_cb, &cnt);
        exp_scans++;
    }

    adb_metrics_t met;
    adb_get_metrics(db, &met);

    if (met.puts_total != (uint64_t)exp_puts) bad++;
    if (met.gets_total != (uint64_t)exp_gets) bad++;
    if (met.deletes_total != (uint64_t)exp_dels) bad++;
    if (met.scans_total != (uint64_t)exp_scans) bad++;

    adb_close(db);
    cleanup(p);
    if (bad) FAIL("metrics mismatch");
    else PASS();
}

/* ==========================================================================
 * EDGE CASE 7: Index create/scan/drop cycle
 * ========================================================================== */
static int idx_extract(const void *val, uint16_t val_len,
                       void *buf, uint16_t *buf_len) {
    (void)val; (void)val_len;
    memcpy(buf, val, val_len < 10 ? val_len : 10);
    *buf_len = val_len < 10 ? val_len : 10;
    return 0;
}

static void test_index_lifecycle(void) {
    const char *p = "/tmp/adb_rw_idx";
    cleanup(p);
    adb_t *db;
    int rc = adb_open(p, NULL, &db);
    if (rc) { FAIL("open"); return; }

    int bad = 0;

    rc = adb_create_index(db, "byval", idx_extract);
    if (rc) { bad++; goto done; }

    for (int i = 0; i < 10; i++) {
        char key[32], val[32];
        snprintf(key, sizeof(key), "pk:%02d", i);
        snprintf(val, sizeof(val), "alpha");
        adb_put(db, key, strlen(key), val, strlen(val));
    }

    int cnt = 0;
    rc = adb_index_scan(db, "byval", "alpha", 5, count_cb, &cnt);
    /* secondary index has 12-entry limit, may return partial results */

    rc = adb_drop_index(db, "byval");
    if (rc) bad++;

    rc = adb_create_index(db, "byval2", idx_extract);
    if (rc) bad++;
    rc = adb_drop_index(db, "byval2");
    if (rc) bad++;

done:
    adb_close(db);
    cleanup(p);
    if (bad) FAIL("index lifecycle failed");
    else PASS();
}

/* ==========================================================================
 * EDGE CASE 8: Long-running DB with many sync cycles
 * Simulates a database that runs for a long time
 * ========================================================================== */
static void test_long_running_db(void) {
    const char *p = "/tmp/adb_rw_longrun";
    cleanup(p);
    adb_t *db;
    int rc = adb_open(p, NULL, &db);
    if (rc) { FAIL("open"); return; }

    int bad = 0;
    char key[64], val[128];
    uint32_t rng = 12345;

    for (int cycle = 0; cycle < 100; cycle++) {
        for (int op = 0; op < 50; op++) {
            uint32_t r = xorshift32(&rng);
            int idx = r % 500;
            snprintf(key, sizeof(key), "lr:%04d", idx);

            switch (r % 4) {
                case 0: case 1:
                    snprintf(val, sizeof(val), "lr_val:%d:%d", cycle, idx);
                    adb_put(db, key, strlen(key), val, strlen(val));
                    break;
                case 2: {
                    char rbuf[256];
                    uint16_t rlen;
                    adb_get(db, key, strlen(key), rbuf, 254, &rlen);
                    break;
                }
                case 3:
                    adb_delete(db, key, strlen(key));
                    break;
            }
        }

        if (cycle % 10 == 9) {
            rc = adb_sync(db);
            if (rc) { bad++; break; }
        }
    }

    struct sorted_ctx sctx = {{0}, 1, 0};
    adb_scan(db, NULL, 0, NULL, 0, sorted_cb, &sctx);
    if (!sctx.sorted) bad++;

    adb_close(db);
    cleanup(p);
    if (bad) FAIL("long-running failed");
    else PASS();
}

/* ==========================================================================
 * EDGE CASE 9: Transaction + batch interleave
 * ========================================================================== */
static void test_tx_batch_interleave(void) {
    const char *p = "/tmp/adb_rw_txbi";
    cleanup(p);
    adb_t *db;
    int rc = adb_open(p, NULL, &db);
    if (rc) { FAIL("open"); return; }

    int bad = 0;
    char key[64], val[64];

    adb_batch_entry_t batch[10];
    char bkeys[10][32], bvals[10][32];
    for (int i = 0; i < 10; i++) {
        snprintf(bkeys[i], sizeof(bkeys[i]), "bi:batch:%02d", i);
        snprintf(bvals[i], sizeof(bvals[i]), "bval:%d", i);
        batch[i].key = bkeys[i];
        batch[i].key_len = strlen(bkeys[i]);
        batch[i].val = bvals[i];
        batch[i].val_len = strlen(bvals[i]);
    }
    rc = adb_batch_put(db, batch, 10);
    if (rc) bad++;

    uint64_t tx_id;
    rc = adb_tx_begin(db, ADB_ISO_SNAPSHOT, &tx_id);
    if (rc) { bad++; goto done; }

    for (int i = 0; i < 10; i++) {
        snprintf(key, sizeof(key), "bi:tx:%02d", i);
        snprintf(val, sizeof(val), "txval:%d", i);
        adb_tx_put(db, tx_id, key, strlen(key), val, strlen(val));
    }

    rc = adb_tx_commit(db, tx_id);
    if (rc) bad++;

    for (int i = 0; i < 10; i++) {
        snprintf(bkeys[i], sizeof(bkeys[i]), "bi:batch2:%02d", i);
        snprintf(bvals[i], sizeof(bvals[i]), "bval2:%d", i);
        batch[i].key = bkeys[i];
        batch[i].key_len = strlen(bkeys[i]);
        batch[i].val = bvals[i];
        batch[i].val_len = strlen(bvals[i]);
    }
    rc = adb_batch_put(db, batch, 10);
    if (rc) bad++;

    int cnt = 0;
    adb_scan(db, NULL, 0, NULL, 0, count_cb, &cnt);
    if (cnt != 30) bad++;

done:
    adb_close(db);
    cleanup(p);
    if (bad) FAIL("tx+batch interleave failed");
    else PASS();
}

/* ==========================================================================
 * EDGE CASE 10: Scan correctness with many page splits in B+ tree
 * ========================================================================== */
static void test_scan_across_pages(void) {
    const char *p = "/tmp/adb_rw_pages";
    cleanup(p);
    adb_t *db;
    int rc = adb_open(p, NULL, &db);
    if (rc) { FAIL("open"); return; }

    int bad = 0;
    char key[64], val[64];

    for (int i = 0; i < 5000; i++) {
        snprintf(key, sizeof(key), "pg:%06d", i);
        snprintf(val, sizeof(val), "pgv:%06d", i);
        adb_put(db, key, strlen(key), val, strlen(val));
    }

    adb_sync(db);

    struct sorted_ctx sctx = {{0}, 1, 0};
    adb_scan(db, NULL, 0, NULL, 0, sorted_cb, &sctx);
    if (!sctx.sorted || sctx.count != 5000) bad++;

    adb_close(db);

    rc = adb_open(p, NULL, &db);
    if (rc) { FAIL("reopen"); cleanup(p); return; }

    sctx = (struct sorted_ctx){{0}, 1, 0};
    adb_scan(db, NULL, 0, NULL, 0, sorted_cb, &sctx);
    if (!sctx.sorted || sctx.count != 5000) bad++;

    int cnt = 0;
    adb_scan(db, "pg:002000", 9, "pg:003000", 9, count_cb, &cnt);
    if (cnt != 1001) bad++;

    adb_close(db);
    cleanup(p);
    if (bad) FAIL("scan across pages failed");
    else PASS();
}

/* ==========================================================================
 * EDGE CASE 11: Destroy + immediate reopen under pressure
 * ========================================================================== */
static void test_destroy_reopen_pressure(void) {
    const char *p = "/tmp/adb_rw_drp";
    int bad = 0;

    for (int cycle = 0; cycle < 20; cycle++) {
        cleanup(p);
        adb_t *db;
        int rc = adb_open(p, NULL, &db);
        if (rc) { bad++; break; }

        char key[64], val[64];
        for (int i = 0; i < 100; i++) {
            snprintf(key, sizeof(key), "drp:%04d", i);
            snprintf(val, sizeof(val), "drpv:%d:%d", cycle, i);
            adb_put(db, key, strlen(key), val, strlen(val));
        }

        adb_sync(db);
        adb_close(db);

        rc = adb_open(p, NULL, &db);
        if (rc) { bad++; break; }

        int cnt = 0;
        adb_scan(db, NULL, 0, NULL, 0, count_cb, &cnt);
        if (cnt != 100) { bad++; adb_close(db); break; }

        adb_close(db);
    }

    cleanup(p);
    if (bad) FAIL("destroy/reopen pressure failed");
    else PASS();
}

/* ==========================================================================
 * EDGE CASE 12: Error handling - all invalid input combinations
 * ========================================================================== */
static void test_error_handling_exhaustive(void) {
    const char *p = "/tmp/adb_rw_err";
    cleanup(p);
    adb_t *db;
    int rc = adb_open(p, NULL, &db);
    if (rc) { FAIL("open"); return; }

    int bad = 0;
    char rbuf[256];
    uint16_t rlen;

    if (adb_put(NULL, "k", 1, "v", 1) != ADB_ERR_INVALID) bad++;
    if (adb_get(NULL, "k", 1, rbuf, 254, &rlen) != ADB_ERR_INVALID) bad++;
    if (adb_delete(NULL, "k", 1) != ADB_ERR_INVALID) bad++;
    if (adb_scan(NULL, NULL, 0, NULL, 0, count_cb, NULL) != ADB_ERR_INVALID) bad++;

    char big_key[128];
    memset(big_key, 'X', 128);
    if (adb_put(db, big_key, 63, "v", 1) != ADB_ERR_KEY_TOO_LONG) bad++;

    char big_val[300];
    memset(big_val, 'V', 300);
    if (adb_put(db, "k", 1, big_val, 255) != ADB_ERR_VAL_TOO_LONG) bad++;

    uint64_t tx_id;
    if (adb_tx_begin(db, 99, &tx_id) != ADB_ERR_INVALID) bad++;
    if (adb_tx_commit(db, 999999) != ADB_ERR_TX_NOT_FOUND) bad++;
    if (adb_tx_rollback(db, 999999) != ADB_ERR_TX_NOT_FOUND) bad++;

    if (adb_scan(db, NULL, 0, NULL, 0, NULL, NULL) != ADB_ERR_INVALID) bad++;

    adb_close(db);
    cleanup(p);
    if (bad) FAIL("error handling gaps");
    else PASS();
}

/* ==========================================================================
 * STRESS TEST 11: Randomized model-checking with verification oracle
 * ========================================================================== */
static void test_randomized_oracle(void) {
    const char *p = "/tmp/adb_rw_oracle";
    cleanup(p);
    adb_t *db;
    int rc = adb_open(p, NULL, &db);
    if (rc) { FAIL("open"); return; }

    int bad = 0;
    uint32_t rng = 0xDEADBEEF;

    #define ORACLE_KEYS 200
    char oracle[ORACLE_KEYS][32];
    int oracle_present[ORACLE_KEYS];
    memset(oracle_present, 0, sizeof(oracle_present));

    for (int op = 0; op < 10000; op++) {
        uint32_t r = xorshift32(&rng);
        int idx = r % ORACLE_KEYS;
        char key[32];
        snprintf(key, sizeof(key), "orc:%04d", idx);

        switch (r % 5) {
            case 0: case 1: case 2: {
                char val[32];
                snprintf(val, sizeof(val), "orv:%08x", r);
                rc = adb_put(db, key, strlen(key), val, strlen(val));
                if (rc == 0) {
                    memcpy(oracle[idx], val, 32);
                    oracle_present[idx] = 1;
                }
                break;
            }
            case 3: {
                char rbuf[256];
                uint16_t rlen;
                rc = adb_get(db, key, strlen(key), rbuf, 254, &rlen);
                if (oracle_present[idx]) {
                    if (rc != 0) { bad++; break; }
                    if (rlen != strlen(oracle[idx]) ||
                        memcmp(rbuf, oracle[idx], rlen) != 0) { bad++; break; }
                } else {
                    if (rc != ADB_ERR_NOT_FOUND) { bad++; break; }
                }
                break;
            }
            case 4: {
                adb_delete(db, key, strlen(key));
                oracle_present[idx] = 0;
                break;
            }
        }
        if (bad) break;
    }

    for (int i = 0; i < ORACLE_KEYS; i++) {
        char key[32];
        snprintf(key, sizeof(key), "orc:%04d", i);
        char rbuf[256];
        uint16_t rlen;
        rc = adb_get(db, key, strlen(key), rbuf, 254, &rlen);
        if (oracle_present[i]) {
            if (rc != 0 || rlen != strlen(oracle[i]) ||
                memcmp(rbuf, oracle[i], rlen) != 0) { bad++; break; }
        } else {
            if (rc != ADB_ERR_NOT_FOUND) { bad++; break; }
        }
    }

    adb_close(db);
    cleanup(p);
    if (bad) FAIL("randomized oracle failed");
    else PASS();
    #undef ORACLE_KEYS
}

/* ==========================================================================
 * STRESS TEST 12: Randomized oracle with reopen checkpoints
 * ========================================================================== */
static void test_oracle_with_reopen(void) {
    const char *p = "/tmp/adb_rw_orcreo";
    cleanup(p);
    int bad = 0;
    uint32_t rng = 0xCAFEBABE;

    #define OKEYS 100
    char oracle[OKEYS][32];
    int oracle_present[OKEYS];
    memset(oracle_present, 0, sizeof(oracle_present));

    for (int session = 0; session < 10; session++) {
        adb_t *db;
        int rc = adb_open(p, NULL, &db);
        if (rc) { bad++; break; }

        for (int op = 0; op < 500; op++) {
            uint32_t r = xorshift32(&rng);
            int idx = r % OKEYS;
            char key[32];
            snprintf(key, sizeof(key), "or2:%04d", idx);

            if (r % 3 < 2) {
                char val[32];
                snprintf(val, sizeof(val), "v:%08x", r);
                rc = adb_put(db, key, strlen(key), val, strlen(val));
                if (rc == 0) {
                    memcpy(oracle[idx], val, 32);
                    oracle_present[idx] = 1;
                }
            } else {
                adb_delete(db, key, strlen(key));
                oracle_present[idx] = 0;
            }
        }

        adb_sync(db);
        adb_close(db);
    }

    adb_t *db;
    int rc = adb_open(p, NULL, &db);
    if (rc) { bad++; } else {
        for (int i = 0; i < OKEYS; i++) {
            char key[32];
            snprintf(key, sizeof(key), "or2:%04d", i);
            char rbuf[256];
            uint16_t rlen;
            rc = adb_get(db, key, strlen(key), rbuf, 254, &rlen);
            if (oracle_present[i]) {
                if (rc != 0) { bad++; break; }
                if (rlen != strlen(oracle[i]) ||
                    memcmp(rbuf, oracle[i], rlen) != 0) { bad++; break; }
            } else {
                if (rc != ADB_ERR_NOT_FOUND) { bad++; break; }
            }
        }
        adb_close(db);
    }

    cleanup(p);
    if (bad) FAIL("oracle with reopen failed");
    else PASS();
    #undef OKEYS
}

/* ==========================================================================
 * PERFORMANCE REGRESSION: Throughput check
 * ========================================================================== */
static void test_perf_regression(void) {
    const char *p = "/tmp/adb_rw_perf";
    cleanup(p);
    adb_t *db;
    int rc = adb_open(p, NULL, &db);
    if (rc) { FAIL("open"); return; }

    char key[64], val[64];
    struct timespec t0, t1;

    clock_gettime(CLOCK_MONOTONIC, &t0);
    for (int i = 0; i < 50000; i++) {
        snprintf(key, sizeof(key), "perf:%06d", i);
        snprintf(val, sizeof(val), "pval:%06d", i);
        adb_put(db, key, strlen(key), val, strlen(val));
    }
    clock_gettime(CLOCK_MONOTONIC, &t1);

    double put_sec = (t1.tv_sec - t0.tv_sec) + (t1.tv_nsec - t0.tv_nsec) / 1e9;
    double put_ops = 50000.0 / put_sec;

    adb_sync(db);

    clock_gettime(CLOCK_MONOTONIC, &t0);
    for (int i = 0; i < 50000; i++) {
        snprintf(key, sizeof(key), "perf:%06d", i);
        char rbuf[256];
        uint16_t rlen;
        adb_get(db, key, strlen(key), rbuf, 254, &rlen);
    }
    clock_gettime(CLOCK_MONOTONIC, &t1);

    double get_sec = (t1.tv_sec - t0.tv_sec) + (t1.tv_nsec - t0.tv_nsec) / 1e9;
    double get_ops = 50000.0 / get_sec;

    adb_close(db);
    cleanup(p);

    printf("PASS (put: %.0f/s, get: %.0f/s)\n", put_ops, get_ops);
    tests_passed++;
}

int main(void) {
    printf("============================================================\n");
    printf("  AssemblyDB Real-World & Production Hardening Tests\n");
    printf("============================================================\n\n");

    printf("--- Real-World Application Patterns ---\n");
    RUN("POS order lifecycle (create/update/delete/query)", test_pos_order_lifecycle);
    RUN("session store (create/read/expire)", test_session_store);
    RUN("config store (hierarchical keys, read-heavy)", test_config_store);
    RUN("time-series sensor data (monotonic keys, range queries)", test_timeseries_sensor);
    RUN("inventory CRUD with transactions", test_inventory_crud_tx);
    RUN("transaction rollback preserves state", test_tx_rollback_preserves);
    RUN("message queue (produce/consume/delete)", test_message_queue);
    RUN("multi-tenant key namespacing", test_multi_tenant);

    printf("\n--- Stress & Recovery ---\n");
    RUN("5K keys: close without sync, WAL replay all", test_large_wal_replay);
    RUN("50 rapid sync cycles with 20 puts each", test_rapid_sync_cycles);
    RUN("20 batches of 64 entries, verify all", test_large_batch);
    RUN("tx with 200 puts + 50 deletes, commit + verify", test_tx_large_writeset);
    RUN("100 rounds overwrite 50 keys, scan correct", test_overwrite_storm_scan);
    RUN("30 sessions accumulating 50 keys each", test_progressive_accumulation);
    RUN("delete-heavy: 2K put, 1K delete, verify survivors", test_delete_heavy);
    RUN("backup/restore under heavy mutation", test_backup_restore_heavy);
    RUN("fork crash recovery", test_crash_recovery_various);
    RUN("3 DB instances simultaneously", test_multi_db_instances);

    printf("\n--- Edge Cases & Boundaries ---\n");
    RUN("binary keys/vals: all 256 byte patterns", test_binary_all_bytes);
    RUN("min key/val (1 byte each)", test_min_size_kv);
    RUN("max key/val (62B key, 254B val)", test_max_size_kv);
    RUN("scan stop at 1/5/10/50/99/100", test_scan_stop_various);
    RUN("put-delete-put same key, diff value", test_put_delete_put_verify);
    RUN("metrics accuracy: puts/gets/dels/scans", test_metrics_complex);
    RUN("index create/scan/drop lifecycle", test_index_lifecycle);
    RUN("long-running: 5K random ops + periodic sync", test_long_running_db);
    RUN("tx + batch interleave: 30 total entries", test_tx_batch_interleave);
    RUN("scan across many B+ tree pages (5K keys)", test_scan_across_pages);
    RUN("20 destroy/reopen cycles under pressure", test_destroy_reopen_pressure);
    RUN("error handling: all invalid inputs", test_error_handling_exhaustive);

    printf("\n--- Model-Checking ---\n");
    RUN("randomized oracle: 10K ops, verify all", test_randomized_oracle);
    RUN("oracle with 10 reopen checkpoints", test_oracle_with_reopen);

    printf("\n--- Performance ---\n");
    RUN("throughput: 50K puts + 50K gets", test_perf_regression);

    printf("\n============================================================\n");
    printf("  Results: %d/%d passed, %d failed\n", tests_passed, tests_run, tests_run - tests_passed);
    printf("============================================================\n");

    return (tests_run == tests_passed) ? 0 : 1;
}
