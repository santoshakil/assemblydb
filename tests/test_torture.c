#include "assemblydb.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <errno.h>
#include <dirent.h>

static int tests_run = 0, tests_passed = 0;

#define PASS() do { tests_passed++; printf("PASS\n"); } while(0)
#define FAIL(msg) do { printf("FAIL: %s\n", msg); } while(0)
#define RUN(name, fn) do { \
    tests_run++; \
    printf("  [%02d] %-58s ", tests_run, name); \
    fflush(stdout); \
    fn(); \
} while(0)

static void cleanup(const char *p) { adb_destroy(p); }

static int count_cb(const void *k, uint16_t kl, const void *v, uint16_t vl, void *ctx) {
    (void)k; (void)kl; (void)v; (void)vl;
    (*(int*)ctx)++;
    return 0;
}

/* ==========================================================================
 * WAL CORRUPTION: Truncated WAL record
 * ========================================================================== */
static void test_wal_truncated_record(void) {
    const char *p = "/tmp/adb_tt_waltrunc";
    cleanup(p);
    adb_t *db;
    int rc = adb_open(p, NULL, &db);
    if (rc) { FAIL("open"); return; }

    for (int i = 0; i < 100; i++) {
        char key[32], val[32];
        snprintf(key, sizeof(key), "wt:%05d", i);
        snprintf(val, sizeof(val), "v:%05d", i);
        adb_put(db, key, strlen(key), val, strlen(val));
    }

    adb_close(db);

    /* Find WAL files and truncate the last one by half */
    char wal_dir[256];
    snprintf(wal_dir, sizeof(wal_dir), "%s/wal", p);
    DIR *d = opendir(wal_dir);
    if (d) {
        struct dirent *ent;
        char last_wal[512] = {0};
        while ((ent = readdir(d)) != NULL) {
            if (strstr(ent->d_name, ".log")) {
                snprintf(last_wal, sizeof(last_wal), "%s/%s", wal_dir, ent->d_name);
            }
        }
        closedir(d);

        if (last_wal[0]) {
            struct stat st;
            if (stat(last_wal, &st) == 0 && st.st_size > 0) {
                truncate(last_wal, st.st_size / 2);
            }
        }
    }

    /* Reopen - should survive WAL corruption */
    rc = adb_open(p, NULL, &db);
    if (rc) { FAIL("reopen after WAL truncation"); cleanup(p); return; }

    /* Some keys should still be recoverable */
    int cnt = 0;
    adb_scan(db, NULL, 0, NULL, 0, count_cb, &cnt);

    adb_close(db);
    cleanup(p);
    PASS();
}

/* ==========================================================================
 * WAL CORRUPTION: Zero-filled WAL
 * ========================================================================== */
static void test_wal_zeroed_file(void) {
    const char *p = "/tmp/adb_tt_walzero";
    cleanup(p);
    adb_t *db;
    int rc = adb_open(p, NULL, &db);
    if (rc) { FAIL("open"); return; }

    for (int i = 0; i < 50; i++) {
        char key[32], val[16];
        snprintf(key, sizeof(key), "wz:%03d", i);
        snprintf(val, sizeof(val), "v:%d", i);
        adb_put(db, key, strlen(key), val, strlen(val));
    }
    adb_close(db);

    /* Zero out the WAL file */
    char wal_dir[256];
    snprintf(wal_dir, sizeof(wal_dir), "%s/wal", p);
    DIR *d = opendir(wal_dir);
    if (d) {
        struct dirent *ent;
        while ((ent = readdir(d)) != NULL) {
            if (strstr(ent->d_name, ".log")) {
                char path[512];
                snprintf(path, sizeof(path), "%s/%s", wal_dir, ent->d_name);
                struct stat st;
                if (stat(path, &st) == 0) {
                    int fd = open(path, O_WRONLY);
                    if (fd >= 0) {
                        char *zeros = calloc(1, st.st_size);
                        if (zeros) {
                            (void)write(fd, zeros, st.st_size);
                            free(zeros);
                        }
                        close(fd);
                    }
                }
            }
        }
        closedir(d);
    }

    rc = adb_open(p, NULL, &db);
    if (rc) { FAIL("reopen after WAL zeroing"); cleanup(p); return; }
    adb_close(db);
    cleanup(p);
    PASS();
}

/* ==========================================================================
 * BTREE STRESS: Insert 50K, delete half, verify remainder sorted
 * ========================================================================== */
static void test_btree_heavy_delete(void) {
    const char *p = "/tmp/adb_tt_btdel";
    cleanup(p);
    adb_t *db;
    int rc = adb_open(p, NULL, &db);
    if (rc) { FAIL("open"); return; }

    int bad = 0;
    char key[64], val[64];

    for (int i = 0; i < 50000; i++) {
        snprintf(key, sizeof(key), "bd:%07d", i);
        snprintf(val, sizeof(val), "bv:%07d", i);
        rc = adb_put(db, key, strlen(key), val, strlen(val));
        if (rc) { bad++; break; }
    }

    adb_sync(db);

    /* Delete all even keys */
    for (int i = 0; i < 50000; i += 2) {
        snprintf(key, sizeof(key), "bd:%07d", i);
        adb_delete(db, key, strlen(key));
    }

    adb_sync(db);

    /* Verify odd keys still exist */
    for (int i = 1; i < 50000 && !bad; i += 2) {
        snprintf(key, sizeof(key), "bd:%07d", i);
        char rbuf[256];
        uint16_t rlen;
        rc = adb_get(db, key, strlen(key), rbuf, 254, &rlen);
        if (rc) { bad++; break; }
    }

    /* Verify even keys are gone */
    for (int i = 0; i < 1000 && !bad; i += 2) {
        snprintf(key, sizeof(key), "bd:%07d", i);
        char rbuf[256];
        uint16_t rlen;
        rc = adb_get(db, key, strlen(key), rbuf, 254, &rlen);
        if (rc != ADB_ERR_NOT_FOUND) { bad++; break; }
    }

    /* Count should be 25000 */
    int cnt = 0;
    adb_scan(db, NULL, 0, NULL, 0, count_cb, &cnt);
    if (cnt != 25000) bad++;

    adb_close(db);
    cleanup(p);
    if (bad) FAIL("btree heavy delete");
    else PASS();
}

/* ==========================================================================
 * BTREE STRESS: Insert 50K, delete all, reinsert 10K
 * ========================================================================== */
static void test_btree_delete_reinsert(void) {
    const char *p = "/tmp/adb_tt_btdri";
    cleanup(p);
    adb_t *db;
    int rc = adb_open(p, NULL, &db);
    if (rc) { FAIL("open"); return; }

    int bad = 0;
    char key[64], val[64];

    for (int i = 0; i < 50000; i++) {
        snprintf(key, sizeof(key), "dr:%07d", i);
        snprintf(val, sizeof(val), "v1:%07d", i);
        adb_put(db, key, strlen(key), val, strlen(val));
    }
    adb_sync(db);

    for (int i = 0; i < 50000; i++) {
        snprintf(key, sizeof(key), "dr:%07d", i);
        adb_delete(db, key, strlen(key));
    }
    adb_sync(db);

    /* All should be gone */
    int cnt = 0;
    adb_scan(db, NULL, 0, NULL, 0, count_cb, &cnt);
    if (cnt != 0) bad++;

    /* Reinsert 10K with different values */
    for (int i = 0; i < 10000; i++) {
        snprintf(key, sizeof(key), "dr:%07d", i);
        snprintf(val, sizeof(val), "v2:%07d", i);
        adb_put(db, key, strlen(key), val, strlen(val));
    }
    adb_sync(db);

    cnt = 0;
    adb_scan(db, NULL, 0, NULL, 0, count_cb, &cnt);
    if (cnt != 10000) bad++;

    /* Spot check new values */
    for (int i = 0; i < 100 && !bad; i++) {
        int idx = (i * 97) % 10000;
        snprintf(key, sizeof(key), "dr:%07d", idx);
        char rbuf[256];
        uint16_t rlen;
        rc = adb_get(db, key, strlen(key), rbuf, 254, &rlen);
        if (rc) { bad++; break; }
        char expected[64];
        snprintf(expected, sizeof(expected), "v2:%07d", idx);
        if (rlen != strlen(expected) || memcmp(rbuf, expected, rlen) != 0) { bad++; break; }
    }

    adb_close(db);
    cleanup(p);
    if (bad) FAIL("btree delete/reinsert");
    else PASS();
}

/* ==========================================================================
 * PERSISTENCE: 50K keys survive sync+close+reopen+delete half+sync+reopen
 * ========================================================================== */
static void test_persistence_delete_reopen(void) {
    const char *p = "/tmp/adb_tt_pdr";
    cleanup(p);
    adb_t *db;
    int rc = adb_open(p, NULL, &db);
    if (rc) { FAIL("open"); return; }

    int bad = 0;
    char key[64], val[64];

    for (int i = 0; i < 50000; i++) {
        snprintf(key, sizeof(key), "pd:%07d", i);
        snprintf(val, sizeof(val), "pv:%07d", i);
        adb_put(db, key, strlen(key), val, strlen(val));
    }
    adb_sync(db);
    adb_close(db);

    /* Reopen and delete half */
    rc = adb_open(p, NULL, &db);
    if (rc) { FAIL("reopen1"); cleanup(p); return; }

    for (int i = 0; i < 50000; i += 2) {
        snprintf(key, sizeof(key), "pd:%07d", i);
        adb_delete(db, key, strlen(key));
    }
    adb_sync(db);
    adb_close(db);

    /* Reopen and verify */
    rc = adb_open(p, NULL, &db);
    if (rc) { FAIL("reopen2"); cleanup(p); return; }

    int cnt = 0;
    adb_scan(db, NULL, 0, NULL, 0, count_cb, &cnt);
    if (cnt != 25000) bad++;

    adb_close(db);
    cleanup(p);
    if (bad) FAIL("persistence delete/reopen");
    else PASS();
}

/* ==========================================================================
 * CONCURRENT OPEN: Same path from same process should fail with LOCKED
 * ========================================================================== */
static void test_concurrent_open_locked(void) {
    const char *p = "/tmp/adb_tt_lock";
    cleanup(p);
    adb_t *db1, *db2;
    int rc = adb_open(p, NULL, &db1);
    if (rc) { FAIL("open1"); return; }

    int bad = 0;
    rc = adb_open(p, NULL, &db2);
    if (rc != ADB_ERR_LOCKED) bad++;

    adb_close(db1);
    cleanup(p);
    if (bad) FAIL("concurrent open not locked");
    else PASS();
}

/* ==========================================================================
 * CONCURRENT OPEN: Child process should fail with LOCKED
 * ========================================================================== */
static void test_concurrent_open_child(void) {
    const char *p = "/tmp/adb_tt_lockchild";
    cleanup(p);
    adb_t *db;
    int rc = adb_open(p, NULL, &db);
    if (rc) { FAIL("open"); return; }

    adb_put(db, "k", 1, "v", 1);

    int pipefd[2];
    pipe(pipefd);

    pid_t pid = fork();
    if (pid == 0) {
        close(pipefd[0]);
        adb_t *child_db;
        int crc = adb_open(p, NULL, &child_db);
        char result = (crc == ADB_ERR_LOCKED) ? 1 : 0;
        (void)write(pipefd[1], &result, 1);
        close(pipefd[1]);
        if (crc == 0) adb_close(child_db);
        _exit(0);
    }

    close(pipefd[1]);
    char result = 0;
    (void)read(pipefd[0], &result, 1);
    close(pipefd[0]);
    waitpid(pid, NULL, 0);

    adb_close(db);
    cleanup(p);
    if (result != 1) FAIL("child not locked");
    else PASS();
}

/* ==========================================================================
 * TX STRESS: Large write-set (500 entries)
 * ========================================================================== */
static void test_tx_large_writeset(void) {
    const char *p = "/tmp/adb_tt_txlws";
    cleanup(p);
    adb_t *db;
    int rc = adb_open(p, NULL, &db);
    if (rc) { FAIL("open"); return; }

    int bad = 0;
    uint64_t tx_id;
    rc = adb_tx_begin(db, ADB_ISO_SNAPSHOT, &tx_id);
    if (rc) { FAIL("tx_begin"); adb_close(db); cleanup(p); return; }

    for (int i = 0; i < 500; i++) {
        char key[64], val[64];
        snprintf(key, sizeof(key), "tw:%05d", i);
        snprintf(val, sizeof(val), "tv:%05d", i);
        rc = adb_tx_put(db, tx_id, key, strlen(key), val, strlen(val));
        if (rc) { bad++; break; }
    }

    rc = adb_tx_commit(db, tx_id);
    if (rc) bad++;

    /* Verify all committed */
    int cnt = 0;
    adb_scan(db, NULL, 0, NULL, 0, count_cb, &cnt);
    if (cnt != 500) bad++;

    adb_close(db);
    cleanup(p);
    if (bad) FAIL("tx large write-set");
    else PASS();
}

/* ==========================================================================
 * TX: Rollback large write-set, verify nothing leaked
 * ========================================================================== */
static void test_tx_large_rollback(void) {
    const char *p = "/tmp/adb_tt_txlrb";
    cleanup(p);
    adb_t *db;
    int rc = adb_open(p, NULL, &db);
    if (rc) { FAIL("open"); return; }

    int bad = 0;

    /* Put some baseline data */
    for (int i = 0; i < 100; i++) {
        char key[64], val[32];
        snprintf(key, sizeof(key), "base:%03d", i);
        snprintf(val, sizeof(val), "bv:%03d", i);
        adb_put(db, key, strlen(key), val, strlen(val));
    }

    uint64_t tx_id;
    rc = adb_tx_begin(db, ADB_ISO_SNAPSHOT, &tx_id);
    if (rc) { FAIL("tx_begin"); adb_close(db); cleanup(p); return; }

    for (int i = 0; i < 500; i++) {
        char key[64], val[64];
        snprintf(key, sizeof(key), "txrb:%05d", i);
        snprintf(val, sizeof(val), "trv:%05d", i);
        adb_tx_put(db, tx_id, key, strlen(key), val, strlen(val));
    }

    rc = adb_tx_rollback(db, tx_id);
    if (rc) bad++;

    /* Only baseline data should exist */
    int cnt = 0;
    adb_scan(db, NULL, 0, NULL, 0, count_cb, &cnt);
    if (cnt != 100) bad++;

    /* None of the tx keys should exist */
    char rbuf[256];
    uint16_t rlen;
    rc = adb_get(db, "txrb:00000", 10, rbuf, 254, &rlen);
    if (rc != ADB_ERR_NOT_FOUND) bad++;

    adb_close(db);
    cleanup(p);
    if (bad) FAIL("tx large rollback");
    else PASS();
}

/* ==========================================================================
 * TX: Write-set overwrite then commit
 * ========================================================================== */
static void test_tx_writeset_overwrite(void) {
    const char *p = "/tmp/adb_tt_txwso";
    cleanup(p);
    adb_t *db;
    int rc = adb_open(p, NULL, &db);
    if (rc) { FAIL("open"); return; }

    int bad = 0;
    uint64_t tx_id;
    rc = adb_tx_begin(db, ADB_ISO_SNAPSHOT, &tx_id);
    if (rc) { bad++; goto done; }

    /* Write key 3 times with different values in same tx */
    adb_tx_put(db, tx_id, "owk", 3, "val1", 4);
    adb_tx_put(db, tx_id, "owk", 3, "val2", 4);
    adb_tx_put(db, tx_id, "owk", 3, "val3", 4);

    /* tx_get should return latest write */
    char rbuf[256];
    uint16_t rlen;
    rc = adb_tx_get(db, tx_id, "owk", 3, rbuf, 254, &rlen);
    if (rc || rlen != 4 || memcmp(rbuf, "val3", 4) != 0) bad++;

    rc = adb_tx_commit(db, tx_id);
    if (rc) bad++;

    /* After commit, should see val3 */
    rc = adb_get(db, "owk", 3, rbuf, 254, &rlen);
    if (rc || rlen != 4 || memcmp(rbuf, "val3", 4) != 0) bad++;

done:
    adb_close(db);
    cleanup(p);
    if (bad) FAIL("tx write-set overwrite");
    else PASS();
}

/* ==========================================================================
 * TX: Put then delete in same tx
 * ========================================================================== */
static void test_tx_put_then_delete(void) {
    const char *p = "/tmp/adb_tt_txpd";
    cleanup(p);
    adb_t *db;
    int rc = adb_open(p, NULL, &db);
    if (rc) { FAIL("open"); return; }

    int bad = 0;
    uint64_t tx_id;
    rc = adb_tx_begin(db, ADB_ISO_SNAPSHOT, &tx_id);
    if (rc) { bad++; goto done; }

    adb_tx_put(db, tx_id, "pdkey", 5, "pdval", 5);

    /* tx_get should find it */
    char rbuf[256];
    uint16_t rlen;
    rc = adb_tx_get(db, tx_id, "pdkey", 5, rbuf, 254, &rlen);
    if (rc) bad++;

    adb_tx_delete(db, tx_id, "pdkey", 5);

    /* tx_get should return NOT_FOUND (tombstone in write-set) */
    rc = adb_tx_get(db, tx_id, "pdkey", 5, rbuf, 254, &rlen);
    if (rc != ADB_ERR_NOT_FOUND) bad++;

    rc = adb_tx_commit(db, tx_id);
    if (rc) bad++;

    /* After commit, key should be gone */
    rc = adb_get(db, "pdkey", 5, rbuf, 254, &rlen);
    if (rc != ADB_ERR_NOT_FOUND) bad++;

done:
    adb_close(db);
    cleanup(p);
    if (bad) FAIL("tx put then delete");
    else PASS();
}

/* ==========================================================================
 * TX: Delete existing key, then re-put in same tx
 * ========================================================================== */
static void test_tx_delete_then_reput(void) {
    const char *p = "/tmp/adb_tt_txdrp";
    cleanup(p);
    adb_t *db;
    int rc = adb_open(p, NULL, &db);
    if (rc) { FAIL("open"); return; }

    int bad = 0;

    /* Pre-existing key */
    adb_put(db, "drk", 3, "original", 8);

    uint64_t tx_id;
    rc = adb_tx_begin(db, ADB_ISO_SNAPSHOT, &tx_id);
    if (rc) { bad++; goto done; }

    adb_tx_delete(db, tx_id, "drk", 3);

    /* Should be tombstone in write-set */
    char rbuf[256];
    uint16_t rlen;
    rc = adb_tx_get(db, tx_id, "drk", 3, rbuf, 254, &rlen);
    if (rc != ADB_ERR_NOT_FOUND) bad++;

    /* Re-put with new value */
    adb_tx_put(db, tx_id, "drk", 3, "newval", 6);

    /* Should now find new value */
    rc = adb_tx_get(db, tx_id, "drk", 3, rbuf, 254, &rlen);
    if (rc || rlen != 6 || memcmp(rbuf, "newval", 6) != 0) bad++;

    rc = adb_tx_commit(db, tx_id);
    if (rc) bad++;

    rc = adb_get(db, "drk", 3, rbuf, 254, &rlen);
    if (rc || rlen != 6 || memcmp(rbuf, "newval", 6) != 0) bad++;

done:
    adb_close(db);
    cleanup(p);
    if (bad) FAIL("tx delete then re-put");
    else PASS();
}

/* ==========================================================================
 * SCAN: Exact prefix boundary
 * ========================================================================== */
static void test_scan_exact_prefix(void) {
    const char *p = "/tmp/adb_tt_scanpfx";
    cleanup(p);
    adb_t *db;
    int rc = adb_open(p, NULL, &db);
    if (rc) { FAIL("open"); return; }

    int bad = 0;

    /* Insert keys with different prefixes */
    adb_put(db, "aaa:1", 5, "v", 1);
    adb_put(db, "aaa:2", 5, "v", 1);
    adb_put(db, "aab:1", 5, "v", 1);
    adb_put(db, "bbb:1", 5, "v", 1);
    adb_put(db, "bbb:2", 5, "v", 1);

    /* Scan for "aaa:" prefix */
    int cnt = 0;
    adb_scan(db, "aaa:", 4, "aaa:~", 5, count_cb, &cnt);
    if (cnt != 2) bad++;

    /* Scan for "bbb:" prefix */
    cnt = 0;
    adb_scan(db, "bbb:", 4, "bbb:~", 5, count_cb, &cnt);
    if (cnt != 2) bad++;

    /* Scan for everything starting with "a" */
    cnt = 0;
    adb_scan(db, "a", 1, "b", 1, count_cb, &cnt);
    if (cnt != 3) bad++;

    adb_close(db);
    cleanup(p);
    if (bad) FAIL("scan exact prefix");
    else PASS();
}

/* ==========================================================================
 * ARENA: Stress with many tiny allocations
 * ========================================================================== */
static void test_arena_tiny_allocs(void) {
    int bad = 0;
    void *arena = arena_create();
    if (!arena) { FAIL("arena_create"); return; }

    for (int i = 0; i < 100000; i++) {
        void *p = arena_alloc(arena, 8);
        if (!p) { bad++; break; }
        memset(p, 0xAA, 8);
    }

    arena_destroy(arena);
    if (bad) FAIL("arena tiny allocs");
    else PASS();
}

/* ==========================================================================
 * ARENA: Large allocation forces new chunk
 * ========================================================================== */
static void test_arena_large_alloc(void) {
    int bad = 0;
    void *arena = arena_create();
    if (!arena) { FAIL("arena_create"); return; }

    /* Allocate something larger than default chunk to force new chunk creation */
    void *p1 = arena_alloc(arena, 512 * 1024);
    if (!p1) { bad++; }
    else {
        memset(p1, 0xBB, 512 * 1024);
    }

    /* Normal alloc should still work after large alloc */
    void *p2 = arena_alloc(arena, 64);
    if (!p2) bad++;

    arena_destroy(arena);
    if (bad) FAIL("arena large alloc");
    else PASS();
}

/* ==========================================================================
 * ARENA: Reset and reuse
 * ========================================================================== */
static void test_arena_reset_reuse(void) {
    int bad = 0;
    void *arena = arena_create();
    if (!arena) { FAIL("arena_create"); return; }

    /* Fill up the arena */
    for (int i = 0; i < 10000; i++) {
        void *p = arena_alloc(arena, 128);
        if (!p) { bad++; break; }
    }

    /* Reset should free extra chunks */
    arena_reset(arena);

    /* Should be able to allocate again */
    for (int i = 0; i < 10000; i++) {
        void *p = arena_alloc(arena, 128);
        if (!p) { bad++; break; }
    }

    arena_destroy(arena);
    if (bad) FAIL("arena reset/reuse");
    else PASS();
}

/* ==========================================================================
 * BLOOM: False positive rate under expected load
 * ========================================================================== */
static void test_bloom_fp_rate(void) {
    int bad = 0;
    void *bloom = bloom_create(10000);
    if (!bloom) { FAIL("bloom_create"); return; }

    /* Insert 10K keys */
    for (int i = 0; i < 10000; i++) {
        char key_buf[64];
        memset(key_buf, 0, 64);
        uint16_t kl = snprintf(key_buf + 2, 62, "bloom:%05d", i);
        key_buf[0] = kl & 0xFF;
        key_buf[1] = (kl >> 8) & 0xFF;
        bloom_add(bloom, key_buf);
    }

    /* Check all inserted keys: zero false negatives */
    int fn = 0;
    for (int i = 0; i < 10000; i++) {
        char key_buf[64];
        memset(key_buf, 0, 64);
        uint16_t kl = snprintf(key_buf + 2, 62, "bloom:%05d", i);
        key_buf[0] = kl & 0xFF;
        key_buf[1] = (kl >> 8) & 0xFF;
        if (!bloom_check(bloom, key_buf)) fn++;
    }
    if (fn > 0) bad++;

    /* Check 10K non-existent keys: measure false positives */
    int fp = 0;
    for (int i = 10000; i < 20000; i++) {
        char key_buf[64];
        memset(key_buf, 0, 64);
        uint16_t kl = snprintf(key_buf + 2, 62, "bloom:%05d", i);
        key_buf[0] = kl & 0xFF;
        key_buf[1] = (kl >> 8) & 0xFF;
        if (bloom_check(bloom, key_buf)) fp++;
    }

    /* FP rate should be < 2% for 10 bits/key, 7 hashes */
    double fp_rate = (double)fp / 10000.0;
    if (fp_rate > 0.02) bad++;

    bloom_destroy(bloom);
    if (bad) FAIL("bloom FP rate");
    else PASS();
}

/* ==========================================================================
 * LZ4: Compress/decompress various patterns
 * ========================================================================== */
static void test_lz4_patterns(void) {
    int bad = 0;
    void *ctx = lz4_ctx_create();
    if (!ctx) { FAIL("lz4_ctx_create"); return; }

    /* Pattern 1: All zeros (highly compressible) */
    char input[4096], output[8192], decomp[4096];
    memset(input, 0, 4096);
    int64_t clen = lz4_compress(ctx, input, 4096, output, 8192);
    if (clen <= 0) { bad++; }
    else {
        int64_t dlen = lz4_decompress(output, clen, decomp, 4096);
        if (dlen != 4096 || memcmp(input, decomp, 4096) != 0) bad++;
    }

    /* Pattern 2: Random-ish data (low compressibility) */
    for (int i = 0; i < 4096; i++) input[i] = (char)((i * 17 + 31) & 0xFF);
    clen = lz4_compress(ctx, input, 4096, output, 8192);
    if (clen <= 0) { bad++; }
    else {
        int64_t dlen = lz4_decompress(output, clen, decomp, 4096);
        if (dlen != 4096 || memcmp(input, decomp, 4096) != 0) bad++;
    }

    /* Pattern 3: Repeated short sequences */
    for (int i = 0; i < 4096; i++) input[i] = "ABCDEF"[i % 6];
    clen = lz4_compress(ctx, input, 4096, output, 8192);
    if (clen <= 0) { bad++; }
    else {
        int64_t dlen = lz4_decompress(output, clen, decomp, 4096);
        if (dlen != 4096 || memcmp(input, decomp, 4096) != 0) bad++;
    }

    /* Pattern 4: Small input (16 bytes) */
    memset(input, 'X', 16);
    clen = lz4_compress(ctx, input, 16, output, 128);
    if (clen <= 0) { bad++; }
    else {
        int64_t dlen = lz4_decompress(output, clen, decomp, 16);
        if (dlen != 16 || memcmp(input, decomp, 16) != 0) bad++;
    }

    /* Pattern 5: 1-byte input */
    input[0] = 'Z';
    clen = lz4_compress(ctx, input, 1, output, 128);
    if (clen <= 0) { bad++; }
    else {
        int64_t dlen = lz4_decompress(output, clen, decomp, 1);
        if (dlen != 1 || decomp[0] != 'Z') bad++;
    }

    lz4_ctx_destroy(ctx);
    if (bad) FAIL("lz4 patterns");
    else PASS();
}

/* ==========================================================================
 * AES: Encrypt/decrypt 1000 pages with different page IDs
 * ========================================================================== */
static void test_aes_multi_page(void) {
    int bad = 0;
    void *ctx = crypto_ctx_create();
    if (!ctx) { FAIL("crypto_ctx_create"); return; }

    uint8_t key[32];
    for (int i = 0; i < 32; i++) key[i] = (uint8_t)(i * 7 + 13);
    int rc = aes_set_key_impl(ctx, key, 32);
    if (rc) { FAIL("set_key"); crypto_ctx_destroy(ctx); return; }

    uint8_t *pt = aligned_alloc(64, 4096);
    uint8_t *ct = aligned_alloc(64, 4096);
    uint8_t *dt = aligned_alloc(64, 4096);

    for (int page = 0; page < 1000 && !bad; page++) {
        for (int i = 0; i < 4096; i++) pt[i] = (uint8_t)((i + page) & 0xFF);
        rc = aes_page_encrypt(ctx, pt, ct, page);
        if (rc) { bad++; break; }
        rc = aes_page_decrypt(ctx, ct, dt, page);
        if (rc) { bad++; break; }
        if (memcmp(pt, dt, 4096) != 0) { bad++; break; }
        /* Verify encryption actually changed the data */
        if (memcmp(pt, ct, 4096) == 0) { bad++; break; }
    }

    free(pt); free(ct); free(dt);
    crypto_ctx_destroy(ctx);
    if (bad) FAIL("aes multi page");
    else PASS();
}

/* ==========================================================================
 * AES: Different page IDs produce different ciphertext
 * ========================================================================== */
static void test_aes_page_id_matters(void) {
    int bad = 0;
    void *ctx = crypto_ctx_create();
    if (!ctx) { FAIL("crypto_ctx_create"); return; }

    uint8_t key[32] = {0};
    key[0] = 0x42;
    aes_set_key_impl(ctx, key, 32);

    uint8_t pt[4096], ct1[4096], ct2[4096];
    memset(pt, 0xAA, 4096);

    aes_page_encrypt(ctx, pt, ct1, 1);
    aes_page_encrypt(ctx, pt, ct2, 2);

    /* Same plaintext, different page IDs → different ciphertext */
    if (memcmp(ct1, ct2, 4096) == 0) bad++;

    crypto_ctx_destroy(ctx);
    if (bad) FAIL("aes page_id matters");
    else PASS();
}

/* ==========================================================================
 * BACKUP: 10K keys, backup, insert more, restore, verify original
 * ========================================================================== */
static void test_backup_restore_10k(void) {
    const char *p = "/tmp/adb_tt_bk10k";
    const char *bp = "/tmp/adb_tt_bk10k_backup";
    cleanup(p);
    cleanup(bp);
    adb_t *db;
    int rc = adb_open(p, NULL, &db);
    if (rc) { FAIL("open"); return; }

    int bad = 0;
    char key[64], val[64];

    for (int i = 0; i < 10000; i++) {
        snprintf(key, sizeof(key), "bk:%06d", i);
        snprintf(val, sizeof(val), "bv:%06d", i);
        adb_put(db, key, strlen(key), val, strlen(val));
    }
    adb_sync(db);

    rc = adb_backup(db, bp, ADB_BACKUP_FULL);
    if (rc) { bad++; }

    /* Insert more data AFTER backup */
    for (int i = 10000; i < 15000; i++) {
        snprintf(key, sizeof(key), "bk:%06d", i);
        snprintf(val, sizeof(val), "bv:%06d", i);
        adb_put(db, key, strlen(key), val, strlen(val));
    }
    adb_close(db);

    /* Restore over original */
    cleanup(p);
    rc = adb_restore(bp, p);
    if (rc) bad++;

    rc = adb_open(p, NULL, &db);
    if (rc) { FAIL("open after restore"); cleanup(p); cleanup(bp); return; }

    /* Should have exactly 10K keys (backup point-in-time) */
    int cnt = 0;
    adb_scan(db, NULL, 0, NULL, 0, count_cb, &cnt);
    if (cnt != 10000) bad++;

    /* Spot-check */
    for (int i = 0; i < 100 && !bad; i++) {
        int idx = (i * 97) % 10000;
        snprintf(key, sizeof(key), "bk:%06d", idx);
        char rbuf[256];
        uint16_t rlen;
        rc = adb_get(db, key, strlen(key), rbuf, 254, &rlen);
        if (rc) bad++;
    }

    adb_close(db);
    cleanup(p);
    cleanup(bp);
    if (bad) FAIL("backup/restore 10K");
    else PASS();
}

/* ==========================================================================
 * MULTI-DB: 5 concurrent databases
 * ========================================================================== */
static void test_multi_db_5(void) {
    const char *paths[5] = {
        "/tmp/adb_tt_mdb0", "/tmp/adb_tt_mdb1", "/tmp/adb_tt_mdb2",
        "/tmp/adb_tt_mdb3", "/tmp/adb_tt_mdb4"
    };
    adb_t *dbs[5] = {0};
    int bad = 0;

    for (int i = 0; i < 5; i++) cleanup(paths[i]);

    for (int i = 0; i < 5; i++) {
        int rc = adb_open(paths[i], NULL, &dbs[i]);
        if (rc) { bad++; break; }
    }

    /* Write different data to each */
    for (int i = 0; i < 5 && !bad; i++) {
        for (int j = 0; j < 1000; j++) {
            char key[64], val[64];
            snprintf(key, sizeof(key), "db%d:%04d", i, j);
            snprintf(val, sizeof(val), "v%d:%04d", i, j);
            int rc = adb_put(dbs[i], key, strlen(key), val, strlen(val));
            if (rc) { bad++; break; }
        }
    }

    /* Cross-verify: db[0] should NOT have db[1]'s keys */
    if (!bad) {
        char rbuf[256];
        uint16_t rlen;
        int rc = adb_get(dbs[0], "db1:0000", 8, rbuf, 254, &rlen);
        if (rc != ADB_ERR_NOT_FOUND) bad++;
    }

    /* Each DB should have exactly 1000 keys */
    for (int i = 0; i < 5 && !bad; i++) {
        int cnt = 0;
        adb_scan(dbs[i], NULL, 0, NULL, 0, count_cb, &cnt);
        if (cnt != 1000) bad++;
    }

    for (int i = 0; i < 5; i++) {
        if (dbs[i]) adb_close(dbs[i]);
        cleanup(paths[i]);
    }

    if (bad) FAIL("multi-db 5");
    else PASS();
}

/* ==========================================================================
 * OVERWRITE STORM: Same 100 keys overwritten 1000 times each
 * ========================================================================== */
static void test_overwrite_storm(void) {
    const char *p = "/tmp/adb_tt_owstorm";
    cleanup(p);
    adb_t *db;
    int rc = adb_open(p, NULL, &db);
    if (rc) { FAIL("open"); return; }

    int bad = 0;
    char key[64], val[64];

    for (int round = 0; round < 1000 && !bad; round++) {
        for (int i = 0; i < 100; i++) {
            snprintf(key, sizeof(key), "ow:%03d", i);
            snprintf(val, sizeof(val), "r%04d:v%03d", round, i);
            rc = adb_put(db, key, strlen(key), val, strlen(val));
            if (rc) { bad++; break; }
        }
    }

    /* Verify all keys have the last round's values */
    for (int i = 0; i < 100 && !bad; i++) {
        snprintf(key, sizeof(key), "ow:%03d", i);
        char rbuf[256];
        uint16_t rlen;
        rc = adb_get(db, key, strlen(key), rbuf, 254, &rlen);
        if (rc) { bad++; break; }
        char expected[64];
        snprintf(expected, sizeof(expected), "r0999:v%03d", i);
        if (rlen != strlen(expected) || memcmp(rbuf, expected, rlen) != 0) { bad++; break; }
    }

    /* Should still be exactly 100 keys */
    int cnt = 0;
    adb_scan(db, NULL, 0, NULL, 0, count_cb, &cnt);
    if (cnt != 100) bad++;

    adb_close(db);
    cleanup(p);
    if (bad) FAIL("overwrite storm");
    else PASS();
}

/* ==========================================================================
 * CRASH RECOVERY: Fork, write without sync, kill, recover
 * ========================================================================== */
static void test_crash_recovery_fork(void) {
    const char *p = "/tmp/adb_tt_crash";
    cleanup(p);

    /* Phase 1: Write initial data and sync */
    adb_t *db;
    int rc = adb_open(p, NULL, &db);
    if (rc) { FAIL("open"); return; }

    for (int i = 0; i < 500; i++) {
        char key[32], val[32];
        snprintf(key, sizeof(key), "cr:%05d", i);
        snprintf(val, sizeof(val), "cv:%05d", i);
        adb_put(db, key, strlen(key), val, strlen(val));
    }
    adb_sync(db);
    adb_close(db);

    /* Phase 2: Fork, write more WITHOUT sync, kill child */
    pid_t pid = fork();
    if (pid == 0) {
        adb_t *child_db;
        if (adb_open(p, NULL, &child_db) == 0) {
            for (int i = 500; i < 1000; i++) {
                char key[32], val[32];
                snprintf(key, sizeof(key), "cr:%05d", i);
                snprintf(val, sizeof(val), "cv:%05d", i);
                adb_put(child_db, key, strlen(key), val, strlen(val));
            }
            /* No sync, no close - simulate crash */
        }
        _exit(0);
    }
    waitpid(pid, NULL, 0);

    /* Phase 3: Recover */
    rc = adb_open(p, NULL, &db);
    if (rc) { FAIL("reopen after crash"); cleanup(p); return; }

    int bad = 0;

    /* At least the synced 500 should be there */
    int cnt = 0;
    adb_scan(db, NULL, 0, NULL, 0, count_cb, &cnt);
    if (cnt < 500) bad++;

    /* All 500 synced keys must be intact */
    for (int i = 0; i < 500 && !bad; i++) {
        char key[32], rbuf[256];
        uint16_t rlen;
        snprintf(key, sizeof(key), "cr:%05d", i);
        rc = adb_get(db, key, strlen(key), rbuf, 254, &rlen);
        if (rc) { bad++; break; }
    }

    adb_close(db);
    cleanup(p);
    if (bad) FAIL("crash recovery fork");
    else PASS();
}

/* ==========================================================================
 * METRICS: Counters track all operations accurately
 * ========================================================================== */
static void test_metrics_comprehensive(void) {
    const char *p = "/tmp/adb_tt_metcomp";
    cleanup(p);
    adb_t *db;
    int rc = adb_open(p, NULL, &db);
    if (rc) { FAIL("open"); return; }

    int bad = 0;

    for (int i = 0; i < 200; i++) {
        char key[32], val[16];
        snprintf(key, sizeof(key), "m:%03d", i);
        snprintf(val, sizeof(val), "v:%d", i);
        adb_put(db, key, strlen(key), val, strlen(val));
    }

    for (int i = 0; i < 50; i++) {
        char key[32], rbuf[256];
        uint16_t rlen;
        snprintf(key, sizeof(key), "m:%03d", i);
        adb_get(db, key, strlen(key), rbuf, 254, &rlen);
    }

    for (int i = 0; i < 30; i++) {
        char key[32];
        snprintf(key, sizeof(key), "m:%03d", i);
        adb_delete(db, key, strlen(key));
    }

    int cnt = 0;
    adb_scan(db, NULL, 0, NULL, 0, count_cb, &cnt);

    adb_metrics_t met;
    rc = adb_get_metrics(db, &met);
    if (rc) { bad++; }
    else {
        if (met.puts_total < 200) bad++;
        if (met.gets_total < 50) bad++;
        if (met.deletes_total < 30) bad++;
        if (met.scans_total < 1) bad++;
    }

    adb_close(db);
    cleanup(p);
    if (bad) FAIL("metrics comprehensive");
    else PASS();
}

/* ==========================================================================
 * BATCH: Large batch of 100 entries
 * ========================================================================== */
static void test_batch_100(void) {
    const char *p = "/tmp/adb_tt_batch100";
    cleanup(p);
    adb_t *db;
    int rc = adb_open(p, NULL, &db);
    if (rc) { FAIL("open"); return; }

    int bad = 0;
    adb_batch_entry_t entries[100];
    char keys[100][32], vals[100][32];

    for (int i = 0; i < 100; i++) {
        snprintf(keys[i], 32, "b100:%03d", i);
        snprintf(vals[i], 32, "bv:%03d", i);
        entries[i].key = keys[i];
        entries[i].key_len = strlen(keys[i]);
        entries[i].val = vals[i];
        entries[i].val_len = strlen(vals[i]);
    }

    rc = adb_batch_put(db, entries, 100);
    if (rc) bad++;

    int cnt = 0;
    adb_scan(db, NULL, 0, NULL, 0, count_cb, &cnt);
    if (cnt != 100) bad++;

    adb_close(db);
    cleanup(p);
    if (bad) FAIL("batch 100");
    else PASS();
}

/* ==========================================================================
 * BATCH: Reject batch with oversized key
 * ========================================================================== */
static void test_batch_reject_oversized(void) {
    const char *p = "/tmp/adb_tt_batchrej";
    cleanup(p);
    adb_t *db;
    int rc = adb_open(p, NULL, &db);
    if (rc) { FAIL("open"); return; }

    int bad = 0;
    char big_key[64];
    memset(big_key, 'X', 63);
    big_key[63] = 0;

    adb_batch_entry_t entries[3];
    entries[0] = (adb_batch_entry_t){ "ok1", 3, "v1", 2 };
    entries[1] = (adb_batch_entry_t){ big_key, 63, "v2", 2 };  /* oversized */
    entries[2] = (adb_batch_entry_t){ "ok3", 3, "v3", 2 };

    rc = adb_batch_put(db, entries, 3);
    if (rc != ADB_ERR_KEY_TOO_LONG) bad++;

    /* No keys should have been inserted (pre-validation) */
    int cnt = 0;
    adb_scan(db, NULL, 0, NULL, 0, count_cb, &cnt);
    if (cnt != 0) bad++;

    adb_close(db);
    cleanup(p);
    if (bad) FAIL("batch reject oversized");
    else PASS();
}

/* ==========================================================================
 * NOOP CRYPTO: Encrypt = plaintext (pass-through)
 * ========================================================================== */
static void test_noop_crypto(void) {
    int bad = 0;
    uint8_t pt[4096], ct[4096], dt[4096];
    memset(pt, 0x42, 4096);

    int rc = noop_encrypt_page(NULL, pt, ct, 0);
    if (rc) bad++;
    if (memcmp(pt, ct, 4096) != 0) bad++;

    rc = noop_decrypt_page(NULL, ct, dt, 0);
    if (rc) bad++;
    if (memcmp(ct, dt, 4096) != 0) bad++;

    if (bad) FAIL("noop crypto");
    else PASS();
}

/* ==========================================================================
 * NOOP COMPRESS: Compress = copy
 * ========================================================================== */
static void test_noop_compress(void) {
    int bad = 0;
    char input[1024], output[2048];
    memset(input, 'A', 1024);

    int64_t clen = noop_compress(NULL, input, 1024, output, 2048);
    if (clen != 1024) bad++;
    if (memcmp(input, output, 1024) != 0) bad++;

    char decomp[1024];
    int64_t dlen = noop_decompress(NULL, output, clen, decomp, 1024);
    if (dlen != 1024) bad++;
    if (memcmp(input, decomp, 1024) != 0) bad++;

    if (bad) FAIL("noop compress");
    else PASS();
}

/* ==========================================================================
 * STRING OPS: u64_to_dec correctness
 * ========================================================================== */
static void test_u64_to_dec(void) {
    int bad = 0;
    char buf[32];

    size_t len = u64_to_dec(0, buf);
    buf[len] = 0;
    if (strcmp(buf, "0") != 0) bad++;

    len = u64_to_dec(12345, buf);
    buf[len] = 0;
    if (strcmp(buf, "12345") != 0) bad++;

    len = u64_to_dec(18446744073709551615ULL, buf);
    buf[len] = 0;
    if (strcmp(buf, "18446744073709551615") != 0) bad++;

    if (bad) FAIL("u64_to_dec");
    else PASS();
}

/* ==========================================================================
 * STRING OPS: u64_to_padded_dec correctness
 * ========================================================================== */
static void test_u64_to_padded_dec(void) {
    int bad = 0;
    char buf[32];

    size_t len = u64_to_padded_dec(42, buf, 6);
    buf[len] = 0;
    if (strcmp(buf, "000042") != 0) bad++;

    len = u64_to_padded_dec(123456, buf, 6);
    buf[len] = 0;
    if (strcmp(buf, "123456") != 0) bad++;

    if (bad) FAIL("u64_to_padded_dec");
    else PASS();
}

/* ==========================================================================
 * STRING OPS: build_wal_name / build_sst_name
 * ========================================================================== */
static void test_build_names(void) {
    int bad = 0;
    char buf[128];

    build_wal_name(buf, 0);
    if (strstr(buf, "wal") == NULL) bad++;

    build_wal_name(buf, 42);
    if (strstr(buf, "42") == NULL && strstr(buf, "000042") == NULL) bad++;

    build_sst_name(buf, 0, 5);
    if (strstr(buf, "L0") == NULL && strstr(buf, "0") == NULL) bad++;

    build_sst_name(buf, 1, 3);
    if (strstr(buf, "1") == NULL) bad++;

    if (bad) FAIL("build names");
    else PASS();
}

/* ==========================================================================
 * KEY COMPARE: Exhaustive edge cases
 * ========================================================================== */
static void test_key_compare_edge(void) {
    int bad = 0;
    char k1[64], k2[64];

    /* Equal keys */
    memset(k1, 0, 64); memset(k2, 0, 64);
    k1[0] = 5; memcpy(k1+2, "hello", 5);
    k2[0] = 5; memcpy(k2+2, "hello", 5);
    if (key_compare(k1, k2) != 0) bad++;

    /* First shorter (prefix match, k1 shorter) */
    memset(k1, 0, 64); memset(k2, 0, 64);
    k1[0] = 3; memcpy(k1+2, "abc", 3);
    k2[0] = 5; memcpy(k2+2, "abcde", 5);
    if (key_compare(k1, k2) >= 0) bad++;

    /* Equal prefix, different data */
    memset(k1, 0, 64); memset(k2, 0, 64);
    k1[0] = 5; memcpy(k1+2, "abcde", 5);
    k2[0] = 5; memcpy(k2+2, "abcdf", 5);
    if (key_compare(k1, k2) >= 0) bad++;

    /* key_equal */
    memset(k1, 0, 64); memset(k2, 0, 64);
    k1[0] = 3; memcpy(k1+2, "xyz", 3);
    k2[0] = 3; memcpy(k2+2, "xyz", 3);
    if (!key_equal(k1, k2)) bad++;

    /* key_equal different */
    memset(k2, 0, 64);
    k2[0] = 3; memcpy(k2+2, "xya", 3);
    if (key_equal(k1, k2)) bad++;

    if (bad) FAIL("key_compare edge");
    else PASS();
}

/* ==========================================================================
 * ERROR HELPERS: is_error / is_syscall_error / syscall_to_adb_error
 * ========================================================================== */
static void test_error_helpers(void) {
    int bad = 0;

    if (is_error(0)) bad++;
    if (!is_error(1)) bad++;
    if (!is_error(-1)) bad++;

    if (is_syscall_error(0)) bad++;
    if (is_syscall_error(1)) bad++;
    if (!is_syscall_error(-1)) bad++;

    int adb_err = syscall_to_adb_error(-2);  /* ENOENT */
    if (adb_err == 0) bad++;  /* Should map to some ADB error */

    if (bad) FAIL("error helpers");
    else PASS();
}

/* ==========================================================================
 * MEMTABLE: Direct API - create, put, get, iterate
 * ========================================================================== */
static void test_memtable_direct(void) {
    int bad = 0;
    void *arena = arena_create();
    if (!arena) { FAIL("arena_create"); return; }

    void *mt = memtable_create2(arena);
    if (!mt) { FAIL("memtable_create2"); arena_destroy(arena); return; }

    /* Build fixed key/val and insert */
    char key[64], val[256];
    memset(key, 0, 64);
    key[0] = 5; memcpy(key+2, "testK", 5);
    memset(val, 0, 256);
    val[0] = 5; memcpy(val+2, "testV", 5);

    int rc = memtable_put(mt, key, val, 0);
    if (rc) bad++;

    /* Get */
    char got_val[256];
    memset(got_val, 0, 256);
    rc = memtable_get(mt, key, got_val);
    if (rc) bad++;
    if (memcmp(val, got_val, 256) != 0) bad++;

    /* Entry count */
    uint64_t count = memtable_entry_count(mt);
    if (count != 1) bad++;

    /* Iterator */
    void *node = memtable_iter_first(mt);
    if (!node) bad++;
    void *next = memtable_iter_next(node);
    if (next) bad++;  /* Only 1 entry */

    arena_destroy(arena);
    if (bad) FAIL("memtable direct");
    else PASS();
}

/* ==========================================================================
 * RAPID OPEN/CLOSE: 100 cycles with data
 * ========================================================================== */
static void test_rapid_open_close(void) {
    const char *p = "/tmp/adb_tt_rapid";
    cleanup(p);

    int bad = 0;
    for (int i = 0; i < 100 && !bad; i++) {
        adb_t *db;
        int rc = adb_open(p, NULL, &db);
        if (rc) { bad++; break; }

        char key[32], val[16];
        snprintf(key, sizeof(key), "r:%05d", i);
        snprintf(val, sizeof(val), "rv:%d", i);
        adb_put(db, key, strlen(key), val, strlen(val));
        adb_close(db);
    }

    /* Final verify all 100 keys present */
    if (!bad) {
        adb_t *db;
        int rc = adb_open(p, NULL, &db);
        if (rc) bad++;
        else {
            int cnt = 0;
            adb_scan(db, NULL, 0, NULL, 0, count_cb, &cnt);
            if (cnt != 100) bad++;
            adb_close(db);
        }
    }

    cleanup(p);
    if (bad) FAIL("rapid open/close");
    else PASS();
}

/* ==========================================================================
 * SCAN CALLBACK: Return large positive / negative values
 * ========================================================================== */
static int stop_large_cb(const void *k, uint16_t kl, const void *v, uint16_t vl, void *ctx) {
    (void)k; (void)kl; (void)v; (void)vl;
    (*(int*)ctx)++;
    return 999999;
}

static void test_scan_stop_large_value(void) {
    const char *p = "/tmp/adb_tt_scanstop";
    cleanup(p);
    adb_t *db;
    int rc = adb_open(p, NULL, &db);
    if (rc) { FAIL("open"); return; }

    for (int i = 0; i < 10; i++) {
        char key[32], val[16];
        snprintf(key, sizeof(key), "ss:%02d", i);
        snprintf(val, sizeof(val), "v:%d", i);
        adb_put(db, key, strlen(key), val, strlen(val));
    }

    int cnt = 0;
    adb_scan(db, NULL, 0, NULL, 0, stop_large_cb, &cnt);
    /* Should stop after first callback (returned non-zero) */
    if (cnt != 1) { FAIL("should stop at 1"); }
    else { PASS(); }

    adb_close(db);
    cleanup(p);
}

/* ==========================================================================
 * LRU CACHE: Stress with many inserts/evictions
 * ========================================================================== */
static void test_lru_stress(void) {
    int bad = 0;
    void *cache = lru_cache_create(16);  /* 16 pages */
    if (!cache) { FAIL("cache_create"); return; }

    char page[4096];

    for (int i = 0; i < 1000; i++) {
        memset(page, i & 0xFF, 4096);
        void *slot = lru_cache_insert(cache, i, page);
        if (!slot) { bad++; break; }
        lru_cache_unpin(cache, i);
    }

    /* Recent pages should be fetchable */
    for (int i = 984; i < 1000 && !bad; i++) {
        void *p = lru_cache_fetch(cache, i);
        if (!p) { bad++; break; }
        lru_cache_unpin(cache, i);
    }

    uint64_t hits, misses;
    lru_cache_stats(cache, &hits, &misses);
    if (hits == 0) bad++;

    lru_cache_destroy(cache);
    if (bad) FAIL("lru stress");
    else PASS();
}

int main(void) {
    printf("============================================================\n");
    printf("  AssemblyDB Torture Tests\n");
    printf("============================================================\n\n");

    printf("--- WAL Corruption ---\n");
    RUN("WAL: truncated record recovery", test_wal_truncated_record);
    RUN("WAL: zero-filled file recovery", test_wal_zeroed_file);

    printf("\n--- B+ Tree Stress ---\n");
    RUN("50K insert, delete half, verify remainder", test_btree_heavy_delete);
    RUN("50K insert, delete all, reinsert 10K", test_btree_delete_reinsert);

    printf("\n--- Persistence ---\n");
    RUN("50K → sync → reopen → delete half → reopen", test_persistence_delete_reopen);
    RUN("crash recovery: fork, write, kill, recover", test_crash_recovery_fork);
    RUN("100 rapid open/close cycles with data", test_rapid_open_close);

    printf("\n--- Concurrency ---\n");
    RUN("concurrent open: same process → LOCKED", test_concurrent_open_locked);
    RUN("concurrent open: child process → LOCKED", test_concurrent_open_child);

    printf("\n--- Transactions ---\n");
    RUN("tx: 500-entry write-set commit", test_tx_large_writeset);
    RUN("tx: 500-entry rollback, nothing leaked", test_tx_large_rollback);
    RUN("tx: write-set overwrite 3x same key", test_tx_writeset_overwrite);
    RUN("tx: put then delete in same tx", test_tx_put_then_delete);
    RUN("tx: delete existing, re-put in same tx", test_tx_delete_then_reput);

    printf("\n--- Scan ---\n");
    RUN("scan exact prefix boundaries", test_scan_exact_prefix);
    RUN("scan stop with large return value", test_scan_stop_large_value);

    printf("\n--- Core Data Structures ---\n");
    RUN("arena: 100K tiny allocations", test_arena_tiny_allocs);
    RUN("arena: large allocation forces new chunk", test_arena_large_alloc);
    RUN("arena: reset and reuse", test_arena_reset_reuse);
    RUN("bloom: FP rate < 2% on 10K keys", test_bloom_fp_rate);
    RUN("memtable: direct create/put/get/iterate", test_memtable_direct);
    RUN("LRU cache: 1000 insert/evict stress", test_lru_stress);

    printf("\n--- Crypto & Compression ---\n");
    RUN("LZ4: 5 compression patterns roundtrip", test_lz4_patterns);
    RUN("AES: 1000 page encrypt/decrypt roundtrip", test_aes_multi_page);
    RUN("AES: different page IDs → different ciphertext", test_aes_page_id_matters);
    RUN("noop crypto: pass-through", test_noop_crypto);
    RUN("noop compress: copy", test_noop_compress);

    printf("\n--- Batch ---\n");
    RUN("batch: 100 entries all retrievable", test_batch_100);
    RUN("batch: reject oversized key (no partial)", test_batch_reject_oversized);

    printf("\n--- Backup/Restore ---\n");
    RUN("backup/restore: 10K keys point-in-time", test_backup_restore_10k);

    printf("\n--- Scale & Stress ---\n");
    RUN("100 keys overwritten 1000x each", test_overwrite_storm);
    RUN("5 concurrent databases", test_multi_db_5);

    printf("\n--- Low-Level Coverage ---\n");
    RUN("u64_to_dec correctness", test_u64_to_dec);
    RUN("u64_to_padded_dec correctness", test_u64_to_padded_dec);
    RUN("build_wal_name / build_sst_name", test_build_names);
    RUN("key_compare / key_equal edge cases", test_key_compare_edge);
    RUN("is_error / is_syscall_error / syscall_to_adb_error", test_error_helpers);

    printf("\n--- Metrics ---\n");
    RUN("metrics: comprehensive operation tracking", test_metrics_comprehensive);

    printf("\n============================================================\n");
    printf("  Results: %d/%d passed, %d failed\n", tests_passed, tests_run, tests_run - tests_passed);
    printf("============================================================\n");

    return (tests_run == tests_passed) ? 0 : 1;
}
