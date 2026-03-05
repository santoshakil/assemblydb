#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/stat.h>
#include <errno.h>
#include "assemblydb.h"

static int tests_run = 0;
static int tests_passed = 0;
static int tests_failed = 0;

#define TEST(name) do { \
    tests_run++; \
    printf("  [%02d] %-55s ", tests_run, name); \
    fflush(stdout); \
} while(0)

#define PASS() do { tests_passed++; printf("PASS\n"); } while(0)
#define FAIL(msg) do { tests_failed++; printf("FAIL: %s\n", msg); } while(0)
#define FAILF(...) do { tests_failed++; printf("FAIL: "); printf(__VA_ARGS__); printf("\n"); } while(0)

static void cleanup(const char *path) {
    char cmd[256];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", path);
    int rc = system(cmd);
    (void)rc;
}

static uint64_t xor_state = 0xA5A5A5A5DEADBEEFULL;
static uint64_t xorshift64(void) {
    xor_state ^= xor_state << 13;
    xor_state ^= xor_state >> 7;
    xor_state ^= xor_state << 17;
    return xor_state;
}

static void make_key(char *buf, int id) {
    snprintf(buf, 60, "k-%08d", id);
}

static void make_val(char *buf, int id, int version) {
    snprintf(buf, 250, "v-%08d-ver%04d-pad%.*s", id, version,
             200, "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
             "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
             "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
             "xxxxxxxxxxxxxxxxxxxxxxxxxx");
}

// ============================================================================
// Test 1: Memtable flush pressure - force multiple flushes
// Write enough to trigger memtable_max_bytes, verify data after each flush
// ============================================================================
static void test_memtable_flush_pressure(void) {
    TEST("memtable flush pressure: 5K keys with small memtable");
    const char *path = "/tmp/adb_test_flush_pressure";
    cleanup(path);

    adb_config_t cfg = {0};
    cfg.memtable_max_bytes = 32768;  // 32KB - forces frequent flushes
    adb_t *db = NULL;
    int rc = adb_open(path, &cfg, &db);
    if (rc != ADB_OK) { FAILF("open: %d", rc); return; }

    char key[64], val[256];
    int n = 5000;
    for (int i = 0; i < n; i++) {
        make_key(key, i);
        make_val(val, i, 0);
        rc = adb_put(db, key, strlen(key), val, strlen(val));
        if (rc != ADB_OK) { FAILF("put %d: %d", i, rc); adb_close(db); cleanup(path); return; }
    }

    // Sync to flush everything
    rc = adb_sync(db);
    if (rc != ADB_OK) { FAILF("sync: %d", rc); adb_close(db); cleanup(path); return; }

    // Verify all keys
    uint16_t vlen;
    char vbuf[256];
    int miss = 0;
    for (int i = 0; i < n; i++) {
        make_key(key, i);
        rc = adb_get(db, key, strlen(key), vbuf, sizeof(vbuf), &vlen);
        if (rc != ADB_OK) miss++;
    }

    adb_close(db);
    cleanup(path);
    if (miss > 0) { FAILF("%d keys missing after flush", miss); return; }
    PASS();
}

// ============================================================================
// Test 2: App restart pattern - repeated open/write/close/reopen cycles
// Mimics a mobile app or embedded system restarting
// ============================================================================
static void test_app_restart_cycles(void) {
    TEST("app restart: 20 open/write/close/reopen cycles");
    const char *path = "/tmp/adb_test_restart";
    cleanup(path);

    char key[64], val[256], vbuf[256];
    uint16_t vlen;
    int total_keys = 0;

    for (int cycle = 0; cycle < 20; cycle++) {
        adb_t *db = NULL;
        int rc = adb_open(path, NULL, &db);
        if (rc != ADB_OK) { FAILF("open cycle %d: %d", cycle, rc); cleanup(path); return; }

        // Write 50 keys per cycle
        for (int i = 0; i < 50; i++) {
            make_key(key, total_keys + i);
            make_val(val, total_keys + i, cycle);
            rc = adb_put(db, key, strlen(key), val, strlen(val));
            if (rc != ADB_OK) { FAILF("put cycle %d key %d: %d", cycle, i, rc); adb_close(db); cleanup(path); return; }
        }

        // Sync every other cycle (simulate clean vs dirty shutdown)
        if (cycle % 2 == 0) adb_sync(db);

        // Verify some keys from previous cycles
        if (total_keys > 0) {
            int check_id = total_keys / 2;
            make_key(key, check_id);
            rc = adb_get(db, key, strlen(key), vbuf, sizeof(vbuf), &vlen);
            if (rc != ADB_OK) {
                FAILF("verify cycle %d key %d: %d", cycle, check_id, rc);
                adb_close(db); cleanup(path); return;
            }
        }

        total_keys += 50;
        adb_close(db);
    }

    // Final verification: reopen and check all keys
    adb_t *db = NULL;
    int rc = adb_open(path, NULL, &db);
    if (rc != ADB_OK) { FAILF("final open: %d", rc); cleanup(path); return; }

    int miss = 0;
    for (int i = 0; i < total_keys; i++) {
        make_key(key, i);
        rc = adb_get(db, key, strlen(key), vbuf, sizeof(vbuf), &vlen);
        if (rc != ADB_OK) miss++;
    }

    adb_close(db);
    cleanup(path);
    if (miss > 0) { FAILF("%d/%d keys missing after 20 restarts", miss, total_keys); return; }
    PASS();
}

// ============================================================================
// Test 3: Delete-heavy workload with interleaved reads
// Delete 80% of keys, verify remaining 20% still correct
// ============================================================================
static void test_delete_heavy_workload(void) {
    TEST("delete-heavy: insert 2K, delete 80%, verify rest");
    const char *path = "/tmp/adb_test_del_heavy";
    cleanup(path);

    adb_t *db = NULL;
    int rc = adb_open(path, NULL, &db);
    if (rc != ADB_OK) { FAILF("open: %d", rc); cleanup(path); return; }

    char key[64], val[256], vbuf[256];
    uint16_t vlen;
    int n = 2000;

    // Insert all
    for (int i = 0; i < n; i++) {
        make_key(key, i);
        make_val(val, i, 0);
        rc = adb_put(db, key, strlen(key), val, strlen(val));
        if (rc != ADB_OK) { FAILF("put %d: %d", i, rc); adb_close(db); cleanup(path); return; }
    }

    adb_sync(db);

    // Delete 80% (every key not divisible by 5)
    int deleted = 0;
    for (int i = 0; i < n; i++) {
        if (i % 5 != 0) {
            make_key(key, i);
            rc = adb_delete(db, key, strlen(key));
            if (rc != ADB_OK) { FAILF("delete %d: %d", i, rc); adb_close(db); cleanup(path); return; }
            deleted++;
        }
    }

    adb_sync(db);

    // Verify surviving keys exist and deleted keys are gone
    int wrong = 0;
    for (int i = 0; i < n; i++) {
        make_key(key, i);
        rc = adb_get(db, key, strlen(key), vbuf, sizeof(vbuf), &vlen);
        if (i % 5 == 0) {
            if (rc != ADB_OK) wrong++;
        } else {
            if (rc == ADB_OK) wrong++;
        }
    }

    adb_close(db);
    cleanup(path);
    if (wrong > 0) { FAILF("%d wrong results after delete-heavy", wrong); return; }
    PASS();
}

// ============================================================================
// Test 4: Reopen after delete-heavy verifies tombstone persistence
// ============================================================================
static void test_delete_persistence(void) {
    TEST("delete persistence: delete, close, reopen, verify gone");
    const char *path = "/tmp/adb_test_del_persist";
    cleanup(path);

    adb_t *db = NULL;
    int rc = adb_open(path, NULL, &db);
    if (rc != ADB_OK) { FAILF("open: %d", rc); cleanup(path); return; }

    char key[64], val[256], vbuf[256];
    uint16_t vlen;

    // Insert 500 keys
    for (int i = 0; i < 500; i++) {
        make_key(key, i);
        make_val(val, i, 0);
        adb_put(db, key, strlen(key), val, strlen(val));
    }
    adb_sync(db);

    // Delete half
    for (int i = 0; i < 500; i += 2) {
        make_key(key, i);
        adb_delete(db, key, strlen(key));
    }
    adb_sync(db);
    adb_close(db);

    // Reopen
    rc = adb_open(path, NULL, &db);
    if (rc != ADB_OK) { FAILF("reopen: %d", rc); cleanup(path); return; }

    int wrong = 0;
    for (int i = 0; i < 500; i++) {
        make_key(key, i);
        rc = adb_get(db, key, strlen(key), vbuf, sizeof(vbuf), &vlen);
        if (i % 2 == 0) { if (rc == ADB_OK) wrong++; }
        else { if (rc != ADB_OK) wrong++; }
    }

    adb_close(db);
    cleanup(path);
    if (wrong > 0) { FAILF("%d wrong after reopen", wrong); return; }
    PASS();
}

// ============================================================================
// Test 5: WAL replay without explicit sync
// Write data, close without sync, reopen - WAL should replay
// ============================================================================
static void test_wal_replay_no_sync(void) {
    TEST("WAL replay: write 200 keys, close no sync, reopen");
    const char *path = "/tmp/adb_test_wal_replay";
    cleanup(path);

    adb_t *db = NULL;
    int rc = adb_open(path, NULL, &db);
    if (rc != ADB_OK) { FAILF("open: %d", rc); cleanup(path); return; }

    char key[64], val[256], vbuf[256];
    uint16_t vlen;

    for (int i = 0; i < 200; i++) {
        make_key(key, i);
        make_val(val, i, 0);
        adb_put(db, key, strlen(key), val, strlen(val));
    }

    // Close WITHOUT sync
    adb_close(db);

    // Reopen - WAL should replay
    rc = adb_open(path, NULL, &db);
    if (rc != ADB_OK) { FAILF("reopen: %d", rc); cleanup(path); return; }

    int miss = 0;
    for (int i = 0; i < 200; i++) {
        make_key(key, i);
        rc = adb_get(db, key, strlen(key), vbuf, sizeof(vbuf), &vlen);
        if (rc != ADB_OK) miss++;
    }

    adb_close(db);
    cleanup(path);
    if (miss > 0) { FAILF("%d keys lost after WAL replay", miss); return; }
    PASS();
}

// ============================================================================
// Test 6: Overwrite storm - update same key 1000 times, verify final value
// ============================================================================
static void test_overwrite_storm(void) {
    TEST("overwrite storm: update same key 1000x, verify final");
    const char *path = "/tmp/adb_test_overwrite";
    cleanup(path);

    adb_t *db = NULL;
    int rc = adb_open(path, NULL, &db);
    if (rc != ADB_OK) { FAILF("open: %d", rc); cleanup(path); return; }

    char key[64], val[256], vbuf[256];
    uint16_t vlen;
    snprintf(key, sizeof(key), "hot-key");

    for (int i = 0; i < 1000; i++) {
        snprintf(val, sizeof(val), "value-version-%04d", i);
        rc = adb_put(db, key, strlen(key), val, strlen(val));
        if (rc != ADB_OK) { FAILF("put %d: %d", i, rc); adb_close(db); cleanup(path); return; }
    }

    rc = adb_get(db, key, strlen(key), vbuf, sizeof(vbuf), &vlen);
    if (rc != ADB_OK) { FAILF("get: %d", rc); adb_close(db); cleanup(path); return; }

    vbuf[vlen] = '\0';
    adb_close(db);
    cleanup(path);
    if (strcmp(vbuf, "value-version-0999") != 0) { FAILF("wrong: %s", vbuf); return; }
    PASS();
}

// ============================================================================
// Test 7: Overwrite storm with reopen - verify persistence of latest value
// ============================================================================
static void test_overwrite_storm_persist(void) {
    TEST("overwrite persist: 500 updates, sync, reopen, verify");
    const char *path = "/tmp/adb_test_ow_persist";
    cleanup(path);

    adb_t *db = NULL;
    int rc = adb_open(path, NULL, &db);
    if (rc != ADB_OK) { FAILF("open: %d", rc); cleanup(path); return; }

    char key[64], val[256], vbuf[256];
    uint16_t vlen;
    snprintf(key, sizeof(key), "persistent-key");

    for (int i = 0; i < 500; i++) {
        snprintf(val, sizeof(val), "ver-%04d", i);
        adb_put(db, key, strlen(key), val, strlen(val));
    }
    adb_sync(db);
    adb_close(db);

    rc = adb_open(path, NULL, &db);
    if (rc != ADB_OK) { FAILF("reopen: %d", rc); cleanup(path); return; }

    rc = adb_get(db, key, strlen(key), vbuf, sizeof(vbuf), &vlen);
    vbuf[vlen] = '\0';

    adb_close(db);
    cleanup(path);
    if (rc != ADB_OK || strcmp(vbuf, "ver-0499") != 0) { FAILF("got: %s (rc=%d)", vbuf, rc); return; }
    PASS();
}

// ============================================================================
// Test 8: Scan correctness after mixed insert/delete/reinsert
// ============================================================================
struct scan_ctx { int count; int ordered; char last_key[64]; };

static int scan_order_cb(const void *key, uint16_t key_len,
                         const void *val, uint16_t val_len, void *user_data) {
    (void)val; (void)val_len;
    struct scan_ctx *ctx = (struct scan_ctx *)user_data;
    char kbuf[64] = {0};
    memcpy(kbuf, key, key_len < 63 ? key_len : 63);
    if (ctx->count > 0 && strcmp(kbuf, ctx->last_key) <= 0)
        ctx->ordered = 0;
    memcpy(ctx->last_key, kbuf, 64);
    ctx->count++;
    return 0;
}

static void test_scan_after_churn(void) {
    TEST("scan order after insert/delete/reinsert churn");
    const char *path = "/tmp/adb_test_scan_churn";
    cleanup(path);

    adb_t *db = NULL;
    int rc = adb_open(path, NULL, &db);
    if (rc != ADB_OK) { FAILF("open: %d", rc); cleanup(path); return; }

    char key[64], val[256];

    // Insert 500 keys
    for (int i = 0; i < 500; i++) {
        make_key(key, i);
        make_val(val, i, 0);
        adb_put(db, key, strlen(key), val, strlen(val));
    }
    adb_sync(db);

    // Delete even keys
    for (int i = 0; i < 500; i += 2) {
        make_key(key, i);
        adb_delete(db, key, strlen(key));
    }

    // Reinsert some deleted keys with new values
    for (int i = 0; i < 500; i += 4) {
        make_key(key, i);
        make_val(val, i, 1);
        adb_put(db, key, strlen(key), val, strlen(val));
    }
    adb_sync(db);

    // Scan and verify ordering
    struct scan_ctx ctx = {0, 1, {0}};
    rc = adb_scan(db, NULL, 0, NULL, 0, scan_order_cb, &ctx);

    adb_close(db);
    cleanup(path);

    // Expected: 250 odd keys + 125 reinserted even keys = 375
    if (!ctx.ordered) { FAIL("scan returned out-of-order keys"); return; }
    if (ctx.count < 370 || ctx.count > 380) {
        FAILF("expected ~375 keys, got %d", ctx.count);
        return;
    }
    PASS();
}

// ============================================================================
// Test 9: Transaction isolation - uncommitted writes invisible
// ============================================================================
static void test_tx_isolation(void) {
    TEST("tx isolation: uncommitted writes invisible to get");
    const char *path = "/tmp/adb_test_tx_iso";
    cleanup(path);

    adb_t *db = NULL;
    int rc = adb_open(path, NULL, &db);
    if (rc != ADB_OK) { FAILF("open: %d", rc); cleanup(path); return; }

    char vbuf[256];
    uint16_t vlen;

    // Put a key via regular API
    adb_put(db, "base-key", 8, "base-val", 8);
    adb_sync(db);

    // Start tx, write a new key
    uint64_t tx_id;
    rc = adb_tx_begin(db, ADB_ISO_SNAPSHOT, &tx_id);
    if (rc != ADB_OK) { FAILF("tx_begin: %d", rc); adb_close(db); cleanup(path); return; }

    adb_tx_put(db, tx_id, "tx-key", 6, "tx-val", 6);

    // Regular get should NOT see tx-key
    rc = adb_get(db, "tx-key", 6, vbuf, sizeof(vbuf), &vlen);
    if (rc == ADB_OK) {
        adb_tx_rollback(db, tx_id);
        adb_close(db); cleanup(path);
        FAIL("uncommitted tx write visible to adb_get");
        return;
    }

    // tx_get should see it
    rc = adb_tx_get(db, tx_id, "tx-key", 6, vbuf, sizeof(vbuf), &vlen);
    if (rc != ADB_OK) {
        adb_tx_rollback(db, tx_id);
        adb_close(db); cleanup(path);
        FAILF("tx_get can't see own write: %d", rc);
        return;
    }

    // Commit
    adb_tx_commit(db, tx_id);

    // Now regular get should see it
    rc = adb_get(db, "tx-key", 6, vbuf, sizeof(vbuf), &vlen);

    adb_close(db);
    cleanup(path);
    if (rc != ADB_OK) { FAILF("committed tx write not visible: %d", rc); return; }
    PASS();
}

// ============================================================================
// Test 10: Transaction rollback truly discards writes
// ============================================================================
static void test_tx_rollback_discards(void) {
    TEST("tx rollback: writes fully discarded");
    const char *path = "/tmp/adb_test_tx_rb";
    cleanup(path);

    adb_t *db = NULL;
    int rc = adb_open(path, NULL, &db);
    if (rc != ADB_OK) { FAILF("open: %d", rc); cleanup(path); return; }

    char vbuf[256];
    uint16_t vlen;

    // Existing key
    adb_put(db, "keep", 4, "original", 8);
    adb_sync(db);

    // Start tx
    uint64_t tx_id;
    adb_tx_begin(db, ADB_ISO_SNAPSHOT, &tx_id);

    // Overwrite existing + add new
    adb_tx_put(db, tx_id, "keep", 4, "modified", 8);
    adb_tx_put(db, tx_id, "new-key", 7, "new-val", 7);
    adb_tx_delete(db, tx_id, "keep", 4);

    // Rollback
    adb_tx_rollback(db, tx_id);

    // Original should be untouched
    rc = adb_get(db, "keep", 4, vbuf, sizeof(vbuf), &vlen);
    if (rc != ADB_OK) { FAILF("original key lost: %d", rc); adb_close(db); cleanup(path); return; }
    vbuf[vlen] = '\0';
    if (strcmp(vbuf, "original") != 0) { FAILF("original modified: %s", vbuf); adb_close(db); cleanup(path); return; }

    // New key should not exist
    rc = adb_get(db, "new-key", 7, vbuf, sizeof(vbuf), &vlen);
    adb_close(db);
    cleanup(path);
    if (rc == ADB_OK) { FAIL("rolled-back new key exists"); return; }
    PASS();
}

// ============================================================================
// Test 11: Backup during active writes
// ============================================================================
static void test_backup_during_writes(void) {
    TEST("backup: consistent snapshot during active writes");
    const char *path = "/tmp/adb_test_bk_active";
    const char *bkpath = "/tmp/adb_test_bk_active_bk";
    cleanup(path); cleanup(bkpath);

    adb_t *db = NULL;
    int rc = adb_open(path, NULL, &db);
    if (rc != ADB_OK) { FAILF("open: %d", rc); cleanup(path); return; }

    char key[64], val[256], vbuf[256];
    uint16_t vlen;

    // Write 1000 keys
    for (int i = 0; i < 1000; i++) {
        make_key(key, i);
        make_val(val, i, 0);
        adb_put(db, key, strlen(key), val, strlen(val));
    }
    adb_sync(db);

    // Write 500 more (may be in memtable)
    for (int i = 1000; i < 1500; i++) {
        make_key(key, i);
        make_val(val, i, 0);
        adb_put(db, key, strlen(key), val, strlen(val));
    }

    // Backup
    rc = adb_backup(db, bkpath, ADB_BACKUP_FULL);
    if (rc != ADB_OK) { FAILF("backup: %d", rc); adb_close(db); cleanup(path); cleanup(bkpath); return; }

    adb_close(db);

    // Restore and verify
    const char *restpath = "/tmp/adb_test_bk_restored";
    cleanup(restpath);
    rc = adb_restore(bkpath, restpath);
    if (rc != ADB_OK) { FAILF("restore: %d", rc); cleanup(path); cleanup(bkpath); cleanup(restpath); return; }

    rc = adb_open(restpath, NULL, &db);
    if (rc != ADB_OK) { FAILF("open restored: %d", rc); cleanup(path); cleanup(bkpath); cleanup(restpath); return; }

    // At minimum, the synced 1000 keys should be there
    int found = 0;
    for (int i = 0; i < 1000; i++) {
        make_key(key, i);
        rc = adb_get(db, key, strlen(key), vbuf, sizeof(vbuf), &vlen);
        if (rc == ADB_OK) found++;
    }

    adb_close(db);
    cleanup(path); cleanup(bkpath); cleanup(restpath);
    if (found < 1000) { FAILF("only %d/1000 keys in backup", found); return; }
    PASS();
}

// ============================================================================
// Test 12: Batch put validation - mix of valid and oversized entries
// ============================================================================
static void test_batch_validation(void) {
    TEST("batch validation: reject oversized, keep valid");
    const char *path = "/tmp/adb_test_batch_val";
    cleanup(path);

    adb_t *db = NULL;
    int rc = adb_open(path, NULL, &db);
    if (rc != ADB_OK) { FAILF("open: %d", rc); cleanup(path); return; }

    // Valid batch
    adb_batch_entry_t entries[10];
    char keys[10][32], vals[10][64];
    for (int i = 0; i < 10; i++) {
        snprintf(keys[i], sizeof(keys[i]), "batch-%03d", i);
        snprintf(vals[i], sizeof(vals[i]), "bval-%03d", i);
        entries[i].key = keys[i];
        entries[i].key_len = strlen(keys[i]);
        entries[i].val = vals[i];
        entries[i].val_len = strlen(vals[i]);
    }

    rc = adb_batch_put(db, entries, 10);
    if (rc != ADB_OK) { FAILF("valid batch: %d", rc); adb_close(db); cleanup(path); return; }

    // Verify all 10 exist
    char vbuf[256];
    uint16_t vlen;
    int found = 0;
    for (int i = 0; i < 10; i++) {
        rc = adb_get(db, keys[i], strlen(keys[i]), vbuf, sizeof(vbuf), &vlen);
        if (rc == ADB_OK) found++;
    }

    // Now try batch with oversized key
    char bigkey[256];
    memset(bigkey, 'X', 255);
    bigkey[255] = '\0';
    adb_batch_entry_t bad_entries[1];
    bad_entries[0].key = bigkey;
    bad_entries[0].key_len = 255;
    bad_entries[0].val = "val";
    bad_entries[0].val_len = 3;

    int bad_rc = adb_batch_put(db, bad_entries, 1);

    adb_close(db);
    cleanup(path);
    if (found != 10) { FAILF("batch: only %d/10 found", found); return; }
    if (bad_rc == ADB_OK) { FAIL("oversized batch should fail"); return; }
    PASS();
}

// ============================================================================
// Test 13: Metrics accuracy across operations
// ============================================================================
static void test_metrics_accuracy(void) {
    TEST("metrics accuracy: put/get/delete/scan counts");
    const char *path = "/tmp/adb_test_metrics";
    cleanup(path);

    adb_t *db = NULL;
    int rc = adb_open(path, NULL, &db);
    if (rc != ADB_OK) { FAILF("open: %d", rc); cleanup(path); return; }

    adb_metrics_t m;

    // 100 puts
    char key[64], val[256];
    for (int i = 0; i < 100; i++) {
        make_key(key, i);
        make_val(val, i, 0);
        adb_put(db, key, strlen(key), val, strlen(val));
    }

    // 50 gets
    char vbuf[256];
    uint16_t vlen;
    for (int i = 0; i < 50; i++) {
        make_key(key, i);
        adb_get(db, key, strlen(key), vbuf, sizeof(vbuf), &vlen);
    }

    // 10 deletes
    for (int i = 0; i < 10; i++) {
        make_key(key, i);
        adb_delete(db, key, strlen(key));
    }

    // 1 scan
    adb_scan(db, NULL, 0, NULL, 0, scan_order_cb, &(struct scan_ctx){0, 1, {0}});

    rc = adb_get_metrics(db, &m);
    adb_close(db);
    cleanup(path);

    if (rc != ADB_OK) { FAILF("get_metrics: %d", rc); return; }
    if (m.puts_total < 100) { FAILF("puts: %lu < 100", m.puts_total); return; }
    if (m.gets_total < 50) { FAILF("gets: %lu < 50", m.gets_total); return; }
    if (m.deletes_total < 10) { FAILF("deletes: %lu < 10", m.deletes_total); return; }
    if (m.scans_total < 1) { FAILF("scans: %lu < 1", m.scans_total); return; }
    PASS();
}

// ============================================================================
// Test 14: Zero-length value and minimum key
// ============================================================================
static void test_edge_sizes(void) {
    TEST("edge sizes: 1-byte key, empty val, max key, max val");
    const char *path = "/tmp/adb_test_edge_sz";
    cleanup(path);

    adb_t *db = NULL;
    int rc = adb_open(path, NULL, &db);
    if (rc != ADB_OK) { FAILF("open: %d", rc); cleanup(path); return; }

    char vbuf[256];
    uint16_t vlen;

    // 1-byte key
    rc = adb_put(db, "x", 1, "val1", 4);
    if (rc != ADB_OK) { FAILF("1byte put: %d", rc); adb_close(db); cleanup(path); return; }
    rc = adb_get(db, "x", 1, vbuf, sizeof(vbuf), &vlen);
    if (rc != ADB_OK || vlen != 4) { FAILF("1byte get: rc=%d vlen=%d", rc, vlen); adb_close(db); cleanup(path); return; }

    // Max key (62 bytes)
    char maxkey[62];
    memset(maxkey, 'M', 62);
    rc = adb_put(db, maxkey, 62, "max-key-val", 11);
    if (rc != ADB_OK) { FAILF("maxkey put: %d", rc); adb_close(db); cleanup(path); return; }
    rc = adb_get(db, maxkey, 62, vbuf, sizeof(vbuf), &vlen);
    if (rc != ADB_OK) { FAILF("maxkey get: %d", rc); adb_close(db); cleanup(path); return; }

    // Max value (254 bytes)
    char maxval[254];
    memset(maxval, 'V', 254);
    rc = adb_put(db, "big-val", 7, maxval, 254);
    if (rc != ADB_OK) { FAILF("maxval put: %d", rc); adb_close(db); cleanup(path); return; }
    rc = adb_get(db, "big-val", 7, vbuf, sizeof(vbuf), &vlen);
    if (rc != ADB_OK || vlen != 254) { FAILF("maxval get: rc=%d vlen=%d", rc, vlen); adb_close(db); cleanup(path); return; }

    // Over-max key (63 bytes)
    char overkey[63];
    memset(overkey, 'O', 63);
    rc = adb_put(db, overkey, 63, "val", 3);
    if (rc == ADB_OK) {
        adb_close(db); cleanup(path);
        FAIL("63-byte key should be rejected");
        return;
    }

    adb_close(db);
    cleanup(path);
    PASS();
}

// ============================================================================
// Test 15: Binary keys and values (all byte values 0x00-0xFF)
// ============================================================================
static void test_binary_data(void) {
    TEST("binary data: keys/values with all byte values");
    const char *path = "/tmp/adb_test_binary";
    cleanup(path);

    adb_t *db = NULL;
    int rc = adb_open(path, NULL, &db);
    if (rc != ADB_OK) { FAILF("open: %d", rc); cleanup(path); return; }

    char vbuf[256];
    uint16_t vlen;

    // Write 32 keys with binary data
    for (int i = 0; i < 32; i++) {
        char bkey[8];
        for (int j = 0; j < 8; j++)
            bkey[j] = (char)((i * 8 + j) & 0xFF);

        char bval[32];
        for (int j = 0; j < 32; j++)
            bval[j] = (char)(((i + 1) * (j + 1)) & 0xFF);

        rc = adb_put(db, bkey, 8, bval, 32);
        if (rc != ADB_OK) { FAILF("binary put %d: %d", i, rc); adb_close(db); cleanup(path); return; }
    }

    adb_sync(db);

    // Verify
    int wrong = 0;
    for (int i = 0; i < 32; i++) {
        char bkey[8];
        for (int j = 0; j < 8; j++)
            bkey[j] = (char)((i * 8 + j) & 0xFF);

        char bval[32];
        for (int j = 0; j < 32; j++)
            bval[j] = (char)(((i + 1) * (j + 1)) & 0xFF);

        rc = adb_get(db, bkey, 8, vbuf, sizeof(vbuf), &vlen);
        if (rc != ADB_OK || vlen != 32 || memcmp(vbuf, bval, 32) != 0)
            wrong++;
    }

    adb_close(db);
    cleanup(path);
    if (wrong > 0) { FAILF("%d binary keys incorrect", wrong); return; }
    PASS();
}

// ============================================================================
// Test 16: Concurrent lock exclusion via fork
// ============================================================================
static void test_lock_exclusion(void) {
    TEST("lock exclusion: child can't open while parent holds");
    const char *path = "/tmp/adb_test_lock";
    cleanup(path);

    adb_t *db = NULL;
    int rc = adb_open(path, NULL, &db);
    if (rc != ADB_OK) { FAILF("open: %d", rc); cleanup(path); return; }

    // Write some data
    adb_put(db, "lock-test", 9, "value", 5);

    pid_t pid = fork();
    if (pid == 0) {
        // Child: try to open same db
        adb_t *db2 = NULL;
        int rc2 = adb_open(path, NULL, &db2);
        if (rc2 == ADB_OK) {
            adb_close(db2);
            _exit(0);  // BAD: child could open
        }
        _exit(1);  // GOOD: locked out
    }

    int status;
    waitpid(pid, &status, 0);

    adb_close(db);
    cleanup(path);

    if (WIFEXITED(status) && WEXITSTATUS(status) == 0) {
        FAIL("child opened locked db");
        return;
    }
    PASS();
}

// ============================================================================
// Test 17: Destroy then recreate - clean slate
// ============================================================================
static void test_destroy_recreate(void) {
    TEST("destroy + recreate: clean slate after destroy");
    const char *path = "/tmp/adb_test_destroy";
    cleanup(path);

    adb_t *db = NULL;
    char vbuf[256];
    uint16_t vlen;

    // Create and populate
    adb_open(path, NULL, &db);
    for (int i = 0; i < 100; i++) {
        char key[32], val[32];
        snprintf(key, sizeof(key), "key-%d", i);
        snprintf(val, sizeof(val), "val-%d", i);
        adb_put(db, key, strlen(key), val, strlen(val));
    }
    adb_sync(db);
    adb_close(db);

    // Destroy
    int rc = adb_destroy(path);
    if (rc != ADB_OK) { FAILF("destroy: %d", rc); cleanup(path); return; }

    // Recreate
    rc = adb_open(path, NULL, &db);
    if (rc != ADB_OK) { FAILF("recreate open: %d", rc); cleanup(path); return; }

    // Old keys should not exist
    rc = adb_get(db, "key-50", 6, vbuf, sizeof(vbuf), &vlen);
    adb_close(db);
    cleanup(path);
    if (rc == ADB_OK) { FAIL("old key survived destroy"); return; }
    PASS();
}

// ============================================================================
// Test 18: Large sequential + random access pattern
// Insert 10K sequential, then do 5K random reads
// ============================================================================
static void test_seq_then_random(void) {
    TEST("mixed access: 10K sequential write, 5K random read");
    const char *path = "/tmp/adb_test_seq_rand";
    cleanup(path);

    adb_t *db = NULL;
    int rc = adb_open(path, NULL, &db);
    if (rc != ADB_OK) { FAILF("open: %d", rc); cleanup(path); return; }

    char key[64], val[256], vbuf[256];
    uint16_t vlen;
    int n = 10000;

    for (int i = 0; i < n; i++) {
        make_key(key, i);
        make_val(val, i, 0);
        rc = adb_put(db, key, strlen(key), val, strlen(val));
        if (rc != ADB_OK) { FAILF("put %d: %d", i, rc); adb_close(db); cleanup(path); return; }
    }
    adb_sync(db);

    // Random reads
    int miss = 0;
    for (int i = 0; i < 5000; i++) {
        int id = (int)(xorshift64() % n);
        make_key(key, id);
        rc = adb_get(db, key, strlen(key), vbuf, sizeof(vbuf), &vlen);
        if (rc != ADB_OK) miss++;
    }

    adb_close(db);
    cleanup(path);
    if (miss > 0) { FAILF("%d random reads missed", miss); return; }
    PASS();
}

// ============================================================================
// Test 19: Transaction with large write-set
// ============================================================================
static void test_tx_large_writeset(void) {
    TEST("tx: large write-set (200 keys) commit + verify");
    const char *path = "/tmp/adb_test_tx_large";
    cleanup(path);

    adb_t *db = NULL;
    int rc = adb_open(path, NULL, &db);
    if (rc != ADB_OK) { FAILF("open: %d", rc); cleanup(path); return; }

    uint64_t tx_id;
    rc = adb_tx_begin(db, ADB_ISO_SNAPSHOT, &tx_id);
    if (rc != ADB_OK) { FAILF("tx_begin: %d", rc); adb_close(db); cleanup(path); return; }

    char key[64], val[256], vbuf[256];
    uint16_t vlen;

    for (int i = 0; i < 200; i++) {
        snprintf(key, sizeof(key), "tx-key-%04d", i);
        snprintf(val, sizeof(val), "tx-val-%04d", i);
        rc = adb_tx_put(db, tx_id, key, strlen(key), val, strlen(val));
        if (rc != ADB_OK) {
            FAILF("tx_put %d: %d", i, rc);
            adb_tx_rollback(db, tx_id);
            adb_close(db); cleanup(path); return;
        }
    }

    rc = adb_tx_commit(db, tx_id);
    if (rc != ADB_OK) { FAILF("commit: %d", rc); adb_close(db); cleanup(path); return; }

    // Verify all committed
    int miss = 0;
    for (int i = 0; i < 200; i++) {
        snprintf(key, sizeof(key), "tx-key-%04d", i);
        rc = adb_get(db, key, strlen(key), vbuf, sizeof(vbuf), &vlen);
        if (rc != ADB_OK) miss++;
    }

    adb_close(db);
    cleanup(path);
    if (miss > 0) { FAILF("%d tx keys missing after commit", miss); return; }
    PASS();
}

// ============================================================================
// Test 20: Rapid put/get interleave - models real app behavior
// ============================================================================
static void test_rapid_put_get(void) {
    TEST("rapid put/get interleave: 5K cycles");
    const char *path = "/tmp/adb_test_rapid";
    cleanup(path);

    adb_t *db = NULL;
    int rc = adb_open(path, NULL, &db);
    if (rc != ADB_OK) { FAILF("open: %d", rc); cleanup(path); return; }

    char key[64], val[256], vbuf[256];
    uint16_t vlen;
    int errors = 0;

    for (int i = 0; i < 5000; i++) {
        make_key(key, i);
        make_val(val, i, 0);
        rc = adb_put(db, key, strlen(key), val, strlen(val));
        if (rc != ADB_OK) { errors++; continue; }

        // Immediately read back
        rc = adb_get(db, key, strlen(key), vbuf, sizeof(vbuf), &vlen);
        if (rc != ADB_OK) errors++;
    }

    adb_close(db);
    cleanup(path);
    if (errors > 0) { FAILF("%d put/get errors", errors); return; }
    PASS();
}

// ============================================================================
// Test 21: Scan with bounded range
// ============================================================================
struct range_ctx { int count; };

static int range_cb(const void *key, uint16_t key_len,
                    const void *val, uint16_t val_len, void *user_data) {
    (void)key; (void)key_len; (void)val; (void)val_len;
    struct range_ctx *ctx = (struct range_ctx *)user_data;
    ctx->count++;
    return 0;
}

static void test_bounded_scan(void) {
    TEST("bounded scan: range query [k-00000100, k-00000200)");
    const char *path = "/tmp/adb_test_bscan";
    cleanup(path);

    adb_t *db = NULL;
    int rc = adb_open(path, NULL, &db);
    if (rc != ADB_OK) { FAILF("open: %d", rc); cleanup(path); return; }

    char key[64], val[256];
    for (int i = 0; i < 500; i++) {
        make_key(key, i);
        make_val(val, i, 0);
        adb_put(db, key, strlen(key), val, strlen(val));
    }
    adb_sync(db);

    char start[64], end[64];
    make_key(start, 100);
    make_key(end, 200);

    struct range_ctx ctx = {0};
    adb_scan(db, start, strlen(start), end, strlen(end), range_cb, &ctx);

    adb_close(db);
    cleanup(path);
    // Should be keys 100..199 = 100 keys
    if (ctx.count < 95 || ctx.count > 105) {
        FAILF("expected ~100 in range, got %d", ctx.count);
        return;
    }
    PASS();
}

// ============================================================================
// Test 22: Scan early stop
// ============================================================================
static int stop_at_5(const void *key, uint16_t key_len,
                     const void *val, uint16_t val_len, void *user_data) {
    (void)key; (void)key_len; (void)val; (void)val_len;
    int *count = (int *)user_data;
    (*count)++;
    return (*count >= 5) ? 1 : 0;
}

static void test_scan_early_stop(void) {
    TEST("scan early stop: callback returns non-zero at 5");
    const char *path = "/tmp/adb_test_early_stop";
    cleanup(path);

    adb_t *db = NULL;
    adb_open(path, NULL, &db);

    char key[64], val[256];
    for (int i = 0; i < 100; i++) {
        make_key(key, i);
        make_val(val, i, 0);
        adb_put(db, key, strlen(key), val, strlen(val));
    }
    adb_sync(db);

    int count = 0;
    adb_scan(db, NULL, 0, NULL, 0, stop_at_5, &count);

    adb_close(db);
    cleanup(path);
    if (count != 5) { FAILF("expected 5, scanned %d", count); return; }
    PASS();
}

// ============================================================================
// Test 23: Mixed sync and nosync writes across restarts
// ============================================================================
static void test_mixed_sync_restart(void) {
    TEST("mixed sync: synced keys survive, unsynced via WAL");
    const char *path = "/tmp/adb_test_mixed_sync";
    cleanup(path);

    adb_t *db = NULL;
    char key[64], val[256], vbuf[256];
    uint16_t vlen;

    // Phase 1: write + sync
    adb_open(path, NULL, &db);
    for (int i = 0; i < 100; i++) {
        make_key(key, i);
        make_val(val, i, 0);
        adb_put(db, key, strlen(key), val, strlen(val));
    }
    adb_sync(db);

    // Phase 2: more writes, NO sync
    for (int i = 100; i < 200; i++) {
        make_key(key, i);
        make_val(val, i, 0);
        adb_put(db, key, strlen(key), val, strlen(val));
    }
    adb_close(db);

    // Reopen
    adb_open(path, NULL, &db);

    int miss = 0;
    for (int i = 0; i < 200; i++) {
        make_key(key, i);
        int rc = adb_get(db, key, strlen(key), vbuf, sizeof(vbuf), &vlen);
        if (rc != ADB_OK) miss++;
    }

    adb_close(db);
    cleanup(path);
    if (miss > 0) { FAILF("%d/200 keys missing", miss); return; }
    PASS();
}

// ============================================================================
// Test 24: Scan with all entries deleted — should return 0 results
// ============================================================================
static void test_scan_all_deleted(void) {
    TEST("scan all deleted: 50 keys inserted + deleted = 0 scan");
    const char *path = "/tmp/adb_test_scan_del";
    cleanup(path);

    adb_t *db = NULL;
    adb_open(path, NULL, &db);

    char key[64], val[256];
    for (int i = 0; i < 50; i++) {
        make_key(key, i);
        make_val(val, i, 0);
        adb_put(db, key, strlen(key), val, strlen(val));
    }
    adb_sync(db);

    for (int i = 0; i < 50; i++) {
        make_key(key, i);
        adb_delete(db, key, strlen(key));
    }
    adb_sync(db);

    struct range_ctx ctx = {0};
    adb_scan(db, NULL, 0, NULL, 0, range_cb, &ctx);

    adb_close(db);
    cleanup(path);
    if (ctx.count != 0) { FAILF("expected 0 scan results, got %d", ctx.count); return; }
    PASS();
}

// ============================================================================
// Test 25: Double close safety (close already-closed db pointer)
// ============================================================================
static void test_double_operations(void) {
    TEST("safety: double close, open after destroy");
    const char *path = "/tmp/adb_test_double";
    cleanup(path);

    adb_t *db = NULL;
    adb_open(path, NULL, &db);
    adb_put(db, "key", 3, "val", 3);
    adb_sync(db);
    adb_close(db);

    // Destroy and verify we can re-create
    adb_destroy(path);
    int rc = adb_open(path, NULL, &db);
    if (rc != ADB_OK) { FAILF("open after destroy: %d", rc); cleanup(path); return; }

    char vbuf[256];
    uint16_t vlen;
    rc = adb_get(db, "key", 3, vbuf, sizeof(vbuf), &vlen);
    adb_close(db);
    cleanup(path);
    if (rc == ADB_OK) { FAIL("key found after destroy+reopen"); return; }
    PASS();
}

// ============================================================================
// Test 26: Compaction trigger — enough data to trigger L0 compaction
// ============================================================================
static void test_compaction_trigger(void) {
    TEST("compaction: 8K keys with small memtable forces compaction");
    const char *path = "/tmp/adb_test_compact";
    cleanup(path);

    adb_config_t cfg = {0};
    cfg.memtable_max_bytes = 16384;  // 16KB forces very frequent flushes
    adb_t *db = NULL;
    int rc = adb_open(path, &cfg, &db);
    if (rc != ADB_OK) { FAILF("open: %d", rc); cleanup(path); return; }

    char key[64], val[256], vbuf[256];
    uint16_t vlen;

    for (int i = 0; i < 8000; i++) {
        make_key(key, i);
        make_val(val, i, 0);
        rc = adb_put(db, key, strlen(key), val, strlen(val));
        if (rc != ADB_OK) { FAILF("put %d: %d", i, rc); adb_close(db); cleanup(path); return; }
    }
    adb_sync(db);

    // Verify all readable
    int miss = 0;
    for (int i = 0; i < 8000; i++) {
        make_key(key, i);
        rc = adb_get(db, key, strlen(key), vbuf, sizeof(vbuf), &vlen);
        if (rc != ADB_OK) miss++;
    }

    adb_metrics_t m;
    adb_get_metrics(db, &m);

    adb_close(db);
    cleanup(path);
    if (miss > 0) { FAILF("%d/8000 missing after compaction", miss); return; }
    PASS();
}

// ============================================================================
// Test 27: Compaction + reopen — data survives L0 compaction + restart
// ============================================================================
static void test_compaction_reopen(void) {
    TEST("compaction + reopen: 5K keys survive flush+compact+restart");
    const char *path = "/tmp/adb_test_compact_reopen";
    cleanup(path);

    adb_config_t cfg = {0};
    cfg.memtable_max_bytes = 16384;
    adb_t *db = NULL;
    int rc = adb_open(path, &cfg, &db);
    if (rc != ADB_OK) { FAILF("open: %d", rc); cleanup(path); return; }

    char key[64], val[256], vbuf[256];
    uint16_t vlen;

    for (int i = 0; i < 5000; i++) {
        make_key(key, i);
        make_val(val, i, 0);
        adb_put(db, key, strlen(key), val, strlen(val));
    }
    adb_sync(db);
    adb_close(db);

    // Reopen
    rc = adb_open(path, NULL, &db);
    if (rc != ADB_OK) { FAILF("reopen: %d", rc); cleanup(path); return; }

    int miss = 0;
    for (int i = 0; i < 5000; i++) {
        make_key(key, i);
        rc = adb_get(db, key, strlen(key), vbuf, sizeof(vbuf), &vlen);
        if (rc != ADB_OK) miss++;
    }

    adb_close(db);
    cleanup(path);
    if (miss > 0) { FAILF("%d/5000 missing after compact+reopen", miss); return; }
    PASS();
}

// ============================================================================
// Test 28: Index lifecycle - create, populate, query, drop
// ============================================================================

static void test_index_create_query_drop(void) {
    TEST("index lifecycle: create, populate, scan, drop");
    const char *path = "/tmp/adb_test_idx";
    cleanup(path);

    adb_t *db = NULL;
    int rc = adb_open(path, NULL, &db);
    if (rc != ADB_OK) { FAILF("open: %d", rc); cleanup(path); return; }

    rc = adb_create_index(db, "test_idx", NULL);
    if (rc != ADB_OK) { FAILF("create_index: %d", rc); adb_close(db); cleanup(path); return; }

    // Insert some data
    for (int i = 0; i < 10; i++) {
        char key[32], val[32];
        snprintf(key, sizeof(key), "pk-%03d", i);
        snprintf(val, sizeof(val), "val-%03d", i);
        adb_put(db, key, strlen(key), val, strlen(val));
    }
    adb_sync(db);

    // Drop index
    rc = adb_drop_index(db, "test_idx");
    if (rc != ADB_OK) { FAILF("drop_index: %d", rc); adb_close(db); cleanup(path); return; }

    adb_close(db);
    cleanup(path);
    PASS();
}

// ============================================================================
// Test 29: Stress open/close without writes (no-op cycles)
// ============================================================================
static void test_noop_open_close(void) {
    TEST("no-op: 50 rapid open/close cycles without writes");
    const char *path = "/tmp/adb_test_noop_oc";
    cleanup(path);

    for (int i = 0; i < 50; i++) {
        adb_t *db = NULL;
        int rc = adb_open(path, NULL, &db);
        if (rc != ADB_OK) { FAILF("open cycle %d: %d", i, rc); cleanup(path); return; }
        adb_close(db);
    }

    cleanup(path);
    PASS();
}

// ============================================================================
// Test 30: Full API smoke test in one session
// ============================================================================
static void test_full_api_smoke(void) {
    TEST("full API smoke: all 23 operations in one session");
    const char *path = "/tmp/adb_test_smoke";
    const char *bkpath = "/tmp/adb_test_smoke_bk";
    cleanup(path); cleanup(bkpath);

    adb_t *db = NULL;
    char vbuf[256];
    uint16_t vlen;

    // open
    int rc = adb_open(path, NULL, &db);
    if (rc != ADB_OK) { FAILF("open: %d", rc); return; }

    // put
    rc = adb_put(db, "k1", 2, "v1", 2);
    if (rc != ADB_OK) { FAILF("put: %d", rc); adb_close(db); cleanup(path); return; }

    // get
    rc = adb_get(db, "k1", 2, vbuf, sizeof(vbuf), &vlen);
    if (rc != ADB_OK) { FAILF("get: %d", rc); adb_close(db); cleanup(path); return; }

    // delete
    rc = adb_delete(db, "k1", 2);
    if (rc != ADB_OK) { FAILF("delete: %d", rc); adb_close(db); cleanup(path); return; }

    // batch_put
    adb_batch_entry_t entries[2] = {
        {.key = "b1", .key_len = 2, .val = "bv1", .val_len = 3},
        {.key = "b2", .key_len = 2, .val = "bv2", .val_len = 3},
    };
    rc = adb_batch_put(db, entries, 2);
    if (rc != ADB_OK) { FAILF("batch_put: %d", rc); adb_close(db); cleanup(path); return; }

    // scan
    struct range_ctx sctx = {0};
    adb_scan(db, NULL, 0, NULL, 0, range_cb, &sctx);

    // sync
    adb_sync(db);

    // tx_begin + tx_put + tx_get + tx_delete + tx_commit
    uint64_t tx_id;
    adb_tx_begin(db, ADB_ISO_SNAPSHOT, &tx_id);
    adb_tx_put(db, tx_id, "tk", 2, "tv", 2);
    adb_tx_get(db, tx_id, "tk", 2, vbuf, sizeof(vbuf), &vlen);
    adb_tx_delete(db, tx_id, "tk", 2);
    adb_tx_commit(db, tx_id);

    // tx_begin + tx_rollback
    adb_tx_begin(db, ADB_ISO_SNAPSHOT, &tx_id);
    adb_tx_put(db, tx_id, "rk", 2, "rv", 2);
    adb_tx_rollback(db, tx_id);

    // create_index + drop_index
    adb_create_index(db, "idx1", NULL);
    adb_drop_index(db, "idx1");

    // backup + restore
    adb_backup(db, bkpath, ADB_BACKUP_FULL);

    // get_metrics
    adb_metrics_t m;
    adb_get_metrics(db, &m);

    // close
    adb_close(db);

    // restore
    const char *restpath = "/tmp/adb_test_smoke_rest";
    cleanup(restpath);
    adb_restore(bkpath, restpath);

    // destroy
    adb_destroy(restpath);

    cleanup(path); cleanup(bkpath); cleanup(restpath);
    PASS();
}

// ============================================================================
// Main
// ============================================================================
int main(void) {
    printf("============================================================\n");
    printf("  AssemblyDB Production Tests\n");
    printf("============================================================\n\n");

    test_memtable_flush_pressure();
    test_app_restart_cycles();
    test_delete_heavy_workload();
    test_delete_persistence();
    test_wal_replay_no_sync();
    test_overwrite_storm();
    test_overwrite_storm_persist();
    test_scan_after_churn();
    test_tx_isolation();
    test_tx_rollback_discards();
    test_backup_during_writes();
    test_batch_validation();
    test_metrics_accuracy();
    test_edge_sizes();
    test_binary_data();
    test_lock_exclusion();
    test_destroy_recreate();
    test_seq_then_random();
    test_tx_large_writeset();
    test_rapid_put_get();
    test_bounded_scan();
    test_scan_early_stop();
    test_mixed_sync_restart();
    test_scan_all_deleted();
    test_double_operations();
    test_compaction_trigger();
    test_compaction_reopen();
    test_index_create_query_drop();
    test_noop_open_close();
    test_full_api_smoke();

    printf("\n============================================================\n");
    printf("  Results: %d/%d passed, %d failed\n", tests_passed, tests_run, tests_failed);
    printf("============================================================\n");

    return tests_failed > 0 ? 1 : 0;
}
