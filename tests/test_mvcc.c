#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include "assemblydb.h"

static int tests_run = 0;
static int tests_passed = 0;

#define TEST(name) do { \
    tests_run++; \
    printf("  [%02d] %-50s ", tests_run, name); \
} while(0)

#define PASS() do { tests_passed++; printf("PASS\n"); } while(0)
#define FAIL(msg) do { printf("FAIL: %s\n", msg); } while(0)

/* DB struct offsets */
#define DB_SIZE             0x400
#define DB_NEXT_TX_ID       0x1B8
#define DB_VERSION_STORE    0x1C8
#define DB_WRITER_MUTEX     0x1E8
#define DB_MEMTABLE_RWLOCK  0x1EC

/* External functions */
extern int tx_begin(void *db, int isolation);
extern int tx_commit(void *db, uint64_t tx_id);
extern int tx_rollback(void *db, uint64_t tx_id);

extern int mvcc_is_visible(void *version, uint64_t tx_id);
extern void *mvcc_create_version(void *arena, uint64_t tx_id,
                                  void *key, void *val, int is_delete);

extern int lsm_adapter_init(void *db);
extern int lsm_adapter_close(void *db);

extern int router_put(void *db, void *key, void *val, uint64_t tx_id);
extern int router_get(void *db, void *key, void *val_buf, uint64_t tx_id);
extern int router_delete(void *db, void *key, uint64_t tx_id);

extern void mutex_init(void *mutex);
extern void mutex_lock(void *mutex);
extern void mutex_unlock(void *mutex);
extern int  mutex_trylock(void *mutex);

extern void rwlock_init(void *rwlock);
extern void rwlock_rdlock(void *rwlock);
extern void rwlock_rdunlock(void *rwlock);
extern void rwlock_wrlock(void *rwlock);
extern void rwlock_wrunlock(void *rwlock);

extern int metrics_init(void *db);
extern void metrics_destroy(void *db);
extern void metrics_inc(void *metrics, int offset);
extern void metrics_get(void *db, void *out);

extern void prng_seed(uint64_t seed);

extern int compact_check_needed(void *db);
extern int compact_memtable(void *db);

static void set_u64(void *db, int offset, uint64_t val) {
    *(uint64_t *)((char *)db + offset) = val;
}

static uint64_t get_u64(void *db, int offset) {
    return *(uint64_t *)((char *)db + offset);
}

static void make_key(void *buf, const char *str) {
    memset(buf, 0, 64);
    uint16_t len = strlen(str);
    if (len > 62) len = 62;
    *(uint16_t *)buf = len;
    memcpy((char *)buf + 2, str, len);
}

static void make_val(void *buf, const char *str) {
    memset(buf, 0, 256);
    uint16_t len = strlen(str);
    if (len > 254) len = 254;
    *(uint16_t *)buf = len;
    memcpy((char *)buf + 2, str, len);
}

/* ============ Mutex Tests ============ */

static void test_mutex_basic(void) {
    TEST("mutex init + lock + unlock");

    uint32_t __attribute__((aligned(4))) mutex = 0;
    mutex_init(&mutex);

    if (mutex != 0) { FAIL("init failed"); return; }

    mutex_lock(&mutex);
    if (mutex == 0) { FAIL("lock didn't change state"); return; }

    mutex_unlock(&mutex);
    PASS();
}

static void test_mutex_trylock(void) {
    TEST("mutex trylock");

    uint32_t __attribute__((aligned(4))) mutex = 0;
    mutex_init(&mutex);

    int rc = mutex_trylock(&mutex);
    if (rc != 0) { FAIL("trylock should succeed on free mutex"); return; }

    rc = mutex_trylock(&mutex);
    if (rc == 0) { FAIL("trylock should fail on locked mutex"); mutex_unlock(&mutex); return; }

    mutex_unlock(&mutex);

    rc = mutex_trylock(&mutex);
    if (rc != 0) { FAIL("trylock should succeed after unlock"); return; }

    mutex_unlock(&mutex);
    PASS();
}

/* ============ RWLock Tests ============ */

static void test_rwlock_basic(void) {
    TEST("rwlock basic read/write");

    uint64_t __attribute__((aligned(8))) rwlock = 0;
    rwlock_init(&rwlock);

    rwlock_rdlock(&rwlock);
    rwlock_rdunlock(&rwlock);

    rwlock_wrlock(&rwlock);
    rwlock_wrunlock(&rwlock);

    PASS();
}

static void test_rwlock_multiple_readers(void) {
    TEST("rwlock multiple readers");

    uint64_t __attribute__((aligned(8))) rwlock = 0;
    rwlock_init(&rwlock);

    rwlock_rdlock(&rwlock);
    rwlock_rdlock(&rwlock);
    rwlock_rdlock(&rwlock);

    uint32_t state = *(uint32_t *)&rwlock;
    if (state != 3) {
        char msg[64];
        snprintf(msg, sizeof(msg), "state=%u expected 3", state);
        FAIL(msg);
        rwlock_rdunlock(&rwlock);
        rwlock_rdunlock(&rwlock);
        rwlock_rdunlock(&rwlock);
        return;
    }

    rwlock_rdunlock(&rwlock);
    rwlock_rdunlock(&rwlock);
    rwlock_rdunlock(&rwlock);

    state = *(uint32_t *)&rwlock;
    if (state != 0) { FAIL("rwlock not free after all unlocks"); return; }

    PASS();
}

/* ============ Transaction Tests ============ */

static void test_tx_begin_commit(void) {
    TEST("tx begin + commit");

    uint8_t db[DB_SIZE];
    memset(db, 0, DB_SIZE);
    set_u64(db, DB_NEXT_TX_ID, 1);

    int64_t tx_id = tx_begin(db, 0);
    if (tx_id <= 0) { FAIL("tx_begin failed"); return; }

    int rc = tx_commit(db, (uint64_t)tx_id);
    if (rc != 0) {
        char msg[64];
        snprintf(msg, sizeof(msg), "tx_commit failed rc=%d", rc);
        FAIL(msg);
        return;
    }

    PASS();
}

static void test_tx_begin_rollback(void) {
    TEST("tx begin + rollback");

    uint8_t db[DB_SIZE];
    memset(db, 0, DB_SIZE);
    set_u64(db, DB_NEXT_TX_ID, 1);

    int64_t tx_id = tx_begin(db, 0);
    if (tx_id <= 0) { FAIL("tx_begin failed"); return; }

    int rc = tx_rollback(db, (uint64_t)tx_id);
    if (rc != 0) { FAIL("tx_rollback failed"); return; }

    PASS();
}

static void test_tx_monotonic_ids(void) {
    TEST("tx IDs monotonically increase");

    uint8_t db[DB_SIZE];
    memset(db, 0, DB_SIZE);
    set_u64(db, DB_NEXT_TX_ID, 1);

    int64_t id1 = tx_begin(db, 0);
    tx_commit(db, (uint64_t)id1);

    int64_t id2 = tx_begin(db, 0);
    tx_commit(db, (uint64_t)id2);

    int64_t id3 = tx_begin(db, 0);
    tx_commit(db, (uint64_t)id3);

    if (id1 >= id2 || id2 >= id3) {
        char msg[64];
        snprintf(msg, sizeof(msg), "non-monotonic: %ld %ld %ld", id1, id2, id3);
        FAIL(msg);
        return;
    }

    PASS();
}

/* ============ MVCC Visibility Tests ============ */

static void test_mvcc_visibility(void) {
    TEST("MVCC version visibility");

    void *arena = arena_create();
    if (!arena) { FAIL("arena_create failed"); return; }

    uint8_t key[64], val[256];
    make_key(key, "vis_key");
    make_val(val, "vis_val");

    void *ver = mvcc_create_version(arena, 5, key, val, 0);
    if (!ver) { FAIL("create_version failed"); arena_destroy(arena); return; }

    // tx_id=10 should see version created at tx_id=5 (end_tx=MAX)
    int vis = mvcc_is_visible(ver, 10);
    if (vis != 0) { FAIL("should be visible to tx 10"); arena_destroy(arena); return; }

    // tx_id=3 should NOT see version created at tx_id=5
    vis = mvcc_is_visible(ver, 3);
    if (vis == 0) { FAIL("should NOT be visible to tx 3"); arena_destroy(arena); return; }

    arena_destroy(arena);
    PASS();
}

/* ============ Router Tests ============ */

static void test_router_put_get(void) {
    TEST("router put + get");

    uint8_t db[DB_SIZE];
    memset(db, 0, DB_SIZE);
    prng_seed(42);

    int rc = lsm_adapter_init(db);
    if (rc != 0) { FAIL("lsm init"); return; }

    uint8_t key[64], val[256], val_out[256];
    make_key(key, "route_key");
    make_val(val, "route_val");

    rc = router_put(db, key, val, 0);
    if (rc != 0) { FAIL("router_put failed"); lsm_adapter_close(db); return; }

    memset(val_out, 0, 256);
    rc = router_get(db, key, val_out, 0);
    if (rc != 0) {
        char msg[64];
        snprintf(msg, sizeof(msg), "router_get failed rc=%d", rc);
        FAIL(msg);
        lsm_adapter_close(db);
        return;
    }

    uint16_t vlen = *(uint16_t *)val_out;
    if (vlen != 9 || memcmp(val_out + 2, "route_val", 9) != 0) {
        FAIL("value mismatch");
        lsm_adapter_close(db);
        return;
    }

    lsm_adapter_close(db);
    PASS();
}

static void test_router_delete(void) {
    TEST("router delete");

    uint8_t db[DB_SIZE];
    memset(db, 0, DB_SIZE);
    prng_seed(42);

    lsm_adapter_init(db);

    uint8_t key[64], val[256], val_out[256];
    make_key(key, "del_route");
    make_val(val, "del_val");

    router_put(db, key, val, 0);
    router_delete(db, key, 0);

    int rc = router_get(db, key, val_out, 0);
    if (rc == 0) { FAIL("should not find deleted key"); lsm_adapter_close(db); return; }

    lsm_adapter_close(db);
    PASS();
}

/* ============ Metrics Tests ============ */

static void test_metrics(void) {
    TEST("metrics init + increment + read");

    uint8_t db[DB_SIZE];
    memset(db, 0, DB_SIZE);

    int rc = metrics_init(db);
    if (rc != 0) { FAIL("metrics_init failed"); return; }

    void *mp = (void *)get_u64(db, 0x1F0);  // DB_METRICS_PTR
    if (!mp) { FAIL("metrics_ptr is NULL"); return; }

    // Increment puts counter 3 times
    metrics_inc(mp, 0x00);  // MET_PUTS
    metrics_inc(mp, 0x00);
    metrics_inc(mp, 0x00);

    // Read metrics
    uint8_t out[256];
    memset(out, 0, 256);
    metrics_get(db, out);

    uint64_t puts = *(uint64_t *)out;
    if (puts != 3) {
        char msg[64];
        snprintf(msg, sizeof(msg), "puts=%lu expected 3", puts);
        FAIL(msg);
        metrics_destroy(db);
        return;
    }

    metrics_destroy(db);
    PASS();
}

/* ============ Public API Tests ============ */

static void test_api_open_close(void) {
    TEST("adb_open + adb_close");

    int r = system("rm -rf /tmp/test_adb_api"); (void)r;

    void *db = NULL;
    int rc = adb_open("/tmp/test_adb_api", NULL, (adb_t **)&db);
    if (rc != 0) {
        char msg[64];
        snprintf(msg, sizeof(msg), "adb_open failed rc=%d", rc);
        FAIL(msg);
        return;
    }

    if (!db) { FAIL("db is NULL"); return; }

    rc = adb_close((adb_t *)db);
    if (rc != 0) { FAIL("adb_close failed"); return; }

    r = system("rm -rf /tmp/test_adb_api");
    PASS();
}

static void test_api_put_get(void) {
    TEST("adb_put + adb_get");

    int r = system("rm -rf /tmp/test_adb_api2"); (void)r;

    void *db = NULL;
    int rc = adb_open("/tmp/test_adb_api2", NULL, (adb_t **)&db);
    if (rc != 0) { FAIL("adb_open failed"); return; }

    rc = adb_put((adb_t *)db, "hello", 5, "world", 5);
    if (rc != 0) {
        char msg[64];
        snprintf(msg, sizeof(msg), "adb_put failed rc=%d", rc);
        FAIL(msg);
        adb_close((adb_t *)db);
        r = system("rm -rf /tmp/test_adb_api2");
        return;
    }

    char vbuf[256];
    uint16_t vlen = 0;
    rc = adb_get((adb_t *)db, "hello", 5, vbuf, 256, &vlen);
    if (rc != 0) {
        char msg[64];
        snprintf(msg, sizeof(msg), "adb_get failed rc=%d", rc);
        FAIL(msg);
        adb_close((adb_t *)db);
        r = system("rm -rf /tmp/test_adb_api2");
        return;
    }

    if (vlen != 5 || memcmp(vbuf, "world", 5) != 0) {
        FAIL("value mismatch");
        adb_close((adb_t *)db);
        r = system("rm -rf /tmp/test_adb_api2");
        return;
    }

    adb_close((adb_t *)db);
    r = system("rm -rf /tmp/test_adb_api2");
    PASS();
}

static void test_api_delete(void) {
    TEST("adb_delete");

    int r = system("rm -rf /tmp/test_adb_api3"); (void)r;

    void *db = NULL;
    adb_open("/tmp/test_adb_api3", NULL, (adb_t **)&db);

    adb_put((adb_t *)db, "to_del", 6, "value", 5);

    char vbuf[256];
    uint16_t vlen = 0;
    int rc = adb_get((adb_t *)db, "to_del", 6, vbuf, 256, &vlen);
    if (rc != 0) { FAIL("get before delete failed"); adb_close((adb_t *)db); r = system("rm -rf /tmp/test_adb_api3"); return; }

    adb_delete((adb_t *)db, "to_del", 6);

    rc = adb_get((adb_t *)db, "to_del", 6, vbuf, 256, &vlen);
    if (rc == 0) { FAIL("should not find deleted key"); adb_close((adb_t *)db); r = system("rm -rf /tmp/test_adb_api3"); return; }

    adb_close((adb_t *)db);
    r = system("rm -rf /tmp/test_adb_api3");
    PASS();
}

static void test_api_many_keys(void) {
    TEST("adb_put + adb_get 500 keys");

    int r = system("rm -rf /tmp/test_adb_api4"); (void)r;

    void *db = NULL;
    int rc = adb_open("/tmp/test_adb_api4", NULL, (adb_t **)&db);
    if (rc != 0) { FAIL("adb_open failed"); return; }

    char kbuf[32], vbuf[64], rbuf[256];
    uint16_t vlen;
    int N = 500;

    for (int i = 0; i < N; i++) {
        snprintf(kbuf, sizeof(kbuf), "api_k_%06d", i);
        snprintf(vbuf, sizeof(vbuf), "api_v_%06d", i);
        rc = adb_put((adb_t *)db, kbuf, strlen(kbuf), vbuf, strlen(vbuf));
        if (rc != 0) {
            char msg[64];
            snprintf(msg, sizeof(msg), "put failed at %d rc=%d", i, rc);
            FAIL(msg);
            adb_close((adb_t *)db);
            r = system("rm -rf /tmp/test_adb_api4");
            return;
        }
    }

    int fail_count = 0;
    for (int i = 0; i < N; i++) {
        snprintf(kbuf, sizeof(kbuf), "api_k_%06d", i);
        snprintf(vbuf, sizeof(vbuf), "api_v_%06d", i);
        vlen = 0;
        rc = adb_get((adb_t *)db, kbuf, strlen(kbuf), rbuf, 256, &vlen);
        if (rc != 0) { fail_count++; continue; }
        if (vlen != strlen(vbuf) || memcmp(rbuf, vbuf, vlen) != 0) {
            fail_count++;
        }
    }

    if (fail_count > 0) {
        char msg[64];
        snprintf(msg, sizeof(msg), "%d/%d keys failed", fail_count, N);
        FAIL(msg);
        adb_close((adb_t *)db);
        r = system("rm -rf /tmp/test_adb_api4");
        return;
    }

    adb_close((adb_t *)db);
    r = system("rm -rf /tmp/test_adb_api4");
    PASS();
}

static void test_api_tx_basic(void) {
    TEST("adb_tx_begin + commit");

    int r = system("rm -rf /tmp/test_adb_api5"); (void)r;

    void *db = NULL;
    adb_open("/tmp/test_adb_api5", NULL, (adb_t **)&db);

    uint64_t tx_id = 0;
    int rc = adb_tx_begin((adb_t *)db, 0, &tx_id);
    if (rc != 0) { FAIL("tx_begin failed"); adb_close((adb_t *)db); r = system("rm -rf /tmp/test_adb_api5"); return; }

    if (tx_id == 0) { FAIL("tx_id is 0"); adb_close((adb_t *)db); r = system("rm -rf /tmp/test_adb_api5"); return; }

    rc = adb_tx_commit((adb_t *)db, tx_id);
    if (rc != 0) { FAIL("tx_commit failed"); adb_close((adb_t *)db); r = system("rm -rf /tmp/test_adb_api5"); return; }

    adb_close((adb_t *)db);
    r = system("rm -rf /tmp/test_adb_api5");
    PASS();
}

/* ============ Main ============ */

int main(void) {
    printf("=== AssemblyDB Phase 4 Tests (MVCC + Concurrency) ===\n\n");

    printf("--- Mutex ---\n");
    test_mutex_basic();
    test_mutex_trylock();

    printf("\n--- RWLock ---\n");
    test_rwlock_basic();
    test_rwlock_multiple_readers();

    printf("\n--- Transactions ---\n");
    test_tx_begin_commit();
    test_tx_begin_rollback();
    test_tx_monotonic_ids();

    printf("\n--- MVCC ---\n");
    test_mvcc_visibility();

    printf("\n--- Router ---\n");
    test_router_put_get();
    test_router_delete();

    printf("\n--- Metrics ---\n");
    test_metrics();

    printf("\n--- Public API ---\n");
    test_api_open_close();
    test_api_put_get();
    test_api_delete();
    test_api_many_keys();
    test_api_tx_basic();

    printf("\n=== Results: %d/%d passed ===\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
