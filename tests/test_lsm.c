#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "assemblydb.h"

static int tests_run = 0;
static int tests_passed = 0;

#define TEST(name) do { \
    tests_run++; \
    printf("  [%02d] %-50s ", tests_run, name); \
} while(0)

#define PASS() do { tests_passed++; printf("PASS\n"); } while(0)
#define FAIL(msg) do { printf("FAIL: %s\n", msg); } while(0)

/* DB struct offsets from const.s */
#define DB_SIZE             0x400
#define DB_DIR_FD           0x000
#define DB_WAL_PORT         0x028
#define DB_MEMTABLE_PTR     0x078
#define DB_MEMTABLE_SIZE    0x080
#define DB_IMM_MEMTABLE     0x088
#define DB_ARENA_PTR        0x090
#define DB_WAL_FD           0x098
#define DB_WAL_SEQ          0x0A0
#define DB_WAL_OFFSET       0x0A8
#define DB_SST_COUNT_L0     0x0B0
#define DB_SST_COUNT_L1     0x0B4

/* Skip list node offsets */
#define SLN_KEY_LEN         0x000
#define SLN_KEY_DATA        0x002
#define SLN_VAL_LEN         0x040
#define SLN_VAL_DATA        0x042
#define SLN_HEIGHT          0x140
#define SLN_IS_DELETED      0x141
#define SLN_FORWARD         0x148

/* Skip list head offsets */
#define SLH_ENTRY_COUNT     0x000
#define SLH_MAX_HEIGHT      0x008
#define SLH_FORWARD         0x010
#define SLH_ARENA_PTR       0x0B0
#define SLH_DATA_SIZE       0x0B8

/* WAL record offsets */
#define WAL_RECORD_SIZE     338
#define WR_RECORD_LEN       0x000
#define WR_CRC32            0x004
#define WR_SEQUENCE         0x008
#define WR_OP_TYPE          0x010
#define WR_KEY_LEN          0x012
#define WR_KEY_DATA         0x014
#define WR_VAL_LEN          0x052
#define WR_VAL_DATA         0x054

#define WAL_OP_PUT          0x01
#define WAL_OP_DELETE       0x02

/* SSTable descriptor offsets */
#define SSTD_FD             0x000
#define SSTD_FILE_SIZE      0x010
#define SSTD_NUM_ENTRIES    0x018
#define SSTD_INDEX_OFFSET   0x020
#define SSTD_NUM_DATA_BLOCKS 0x028
#define SSTD_SIZE           0x100

/* Use declarations from assemblydb.h, plus extras for PRNG */
extern void prng_seed(uint64_t seed);
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

static const char *test_dir = "/tmp/test_assemblydb_lsm";

static void cleanup_test_dir(void) {
    char cmd[256];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", test_dir);
    int r = system(cmd); (void)r;
}

static int setup_test_dir(void) {
    cleanup_test_dir();
    if (mkdir(test_dir, 0755) < 0) return -1;
    char sub[256];
    snprintf(sub, sizeof(sub), "%s/wal", test_dir);
    mkdir(sub, 0755);
    snprintf(sub, sizeof(sub), "%s/sst", test_dir);
    mkdir(sub, 0755);
    return open(test_dir, O_RDONLY | O_DIRECTORY);
}

/* ============ Memtable Tests ============ */

static void test_memtable_create(void) {
    TEST("memtable create");

    void *arena = arena_create();
    if (!arena) { FAIL("arena_create returned NULL"); return; }

    void *mt = memtable_create2(arena);
    if (!mt) { FAIL("memtable_create2 returned NULL"); arena_destroy(arena); return; }

    uint64_t count = memtable_entry_count(mt);
    if (count != 0) { FAIL("entry count not 0"); arena_destroy(arena); return; }

    uint64_t dsize = memtable_data_size(mt);
    if (dsize != 0) { FAIL("data size not 0"); arena_destroy(arena); return; }

    arena_destroy(arena);
    PASS();
}

static void test_memtable_put_get(void) {
    TEST("memtable put + get single key");

    void *arena = arena_create();
    void *mt = memtable_create2(arena);
    prng_seed(42);

    uint8_t key[64], val[256], val_out[256];
    make_key(key, "hello");
    make_val(val, "world");
    memset(val_out, 0, 256);

    int rc = memtable_put(mt, key, val, 0);
    if (rc != 0) { FAIL("put failed"); arena_destroy(arena); return; }

    uint64_t count = memtable_entry_count(mt);
    if (count != 1) { FAIL("entry count not 1"); arena_destroy(arena); return; }

    rc = memtable_get(mt, key, val_out);
    if (rc != 0) { FAIL("get failed (not found)"); arena_destroy(arena); return; }

    uint16_t vlen = *(uint16_t *)val_out;
    if (vlen != 5 || memcmp(val_out + 2, "world", 5) != 0) {
        FAIL("value mismatch");
        arena_destroy(arena);
        return;
    }

    arena_destroy(arena);
    PASS();
}

static void test_memtable_put_multiple(void) {
    TEST("memtable put + get 100 keys");

    void *arena = arena_create();
    void *mt = memtable_create2(arena);
    prng_seed(123);

    uint8_t key[64], val[256], val_out[256];
    char kbuf[32], vbuf[64];

    for (int i = 0; i < 100; i++) {
        snprintf(kbuf, sizeof(kbuf), "key_%06d", i);
        snprintf(vbuf, sizeof(vbuf), "value_%06d", i);
        make_key(key, kbuf);
        make_val(val, vbuf);
        int rc = memtable_put(mt, key, val, 0);
        if (rc != 0) {
            char msg[64];
            snprintf(msg, sizeof(msg), "put failed at %d", i);
            FAIL(msg);
            arena_destroy(arena);
            return;
        }
    }

    uint64_t count = memtable_entry_count(mt);
    if (count != 100) {
        char msg[64];
        snprintf(msg, sizeof(msg), "count=%lu expected 100", count);
        FAIL(msg);
        arena_destroy(arena);
        return;
    }

    int found = 0;
    for (int i = 0; i < 100; i++) {
        snprintf(kbuf, sizeof(kbuf), "key_%06d", i);
        snprintf(vbuf, sizeof(vbuf), "value_%06d", i);
        make_key(key, kbuf);
        memset(val_out, 0, 256);
        int rc = memtable_get(mt, key, val_out);
        if (rc != 0) {
            char msg[64];
            snprintf(msg, sizeof(msg), "get failed for key_%06d", i);
            FAIL(msg);
            arena_destroy(arena);
            return;
        }
        uint16_t vlen = *(uint16_t *)val_out;
        if (vlen != strlen(vbuf) || memcmp(val_out + 2, vbuf, vlen) != 0) {
            char msg[64];
            snprintf(msg, sizeof(msg), "value mismatch for key_%06d", i);
            FAIL(msg);
            arena_destroy(arena);
            return;
        }
        found++;
    }

    arena_destroy(arena);
    PASS();
}

static void test_memtable_upsert(void) {
    TEST("memtable upsert (update existing)");

    void *arena = arena_create();
    void *mt = memtable_create2(arena);
    prng_seed(42);

    uint8_t key[64], val[256], val_out[256];
    make_key(key, "mykey");
    make_val(val, "original");

    memtable_put(mt, key, val, 0);
    make_val(val, "updated");
    memtable_put(mt, key, val, 0);

    uint64_t count = memtable_entry_count(mt);
    if (count != 1) { FAIL("count should be 1 after upsert"); arena_destroy(arena); return; }

    memset(val_out, 0, 256);
    int rc = memtable_get(mt, key, val_out);
    if (rc != 0) { FAIL("get failed"); arena_destroy(arena); return; }

    uint16_t vlen = *(uint16_t *)val_out;
    if (vlen != 7 || memcmp(val_out + 2, "updated", 7) != 0) {
        FAIL("value not updated");
        arena_destroy(arena);
        return;
    }

    arena_destroy(arena);
    PASS();
}

static void test_memtable_not_found(void) {
    TEST("memtable get non-existent key");

    void *arena = arena_create();
    void *mt = memtable_create2(arena);
    prng_seed(42);

    uint8_t key[64], val_out[256];
    make_key(key, "nonexistent");
    int rc = memtable_get(mt, key, val_out);
    if (rc == 0) { FAIL("should not find key"); arena_destroy(arena); return; }

    arena_destroy(arena);
    PASS();
}

static void test_memtable_delete(void) {
    TEST("memtable delete (tombstone)");

    void *arena = arena_create();
    void *mt = memtable_create2(arena);
    prng_seed(42);

    uint8_t key[64], val[256], val_out[256];
    make_key(key, "to_delete");
    make_val(val, "some_value");

    memtable_put(mt, key, val, 0);
    int rc = memtable_get(mt, key, val_out);
    if (rc != 0) { FAIL("get before delete failed"); arena_destroy(arena); return; }

    memtable_delete2(mt, key);

    rc = memtable_get(mt, key, val_out);
    if (rc == 0) { FAIL("key should be deleted"); arena_destroy(arena); return; }

    arena_destroy(arena);
    PASS();
}

static void test_memtable_iterator(void) {
    TEST("memtable iterator (sorted order)");

    void *arena = arena_create();
    void *mt = memtable_create2(arena);
    prng_seed(999);

    uint8_t key[64], val[256];
    const char *keys[] = {"delta", "alpha", "charlie", "bravo", "echo"};
    int n = 5;

    for (int i = 0; i < n; i++) {
        make_key(key, keys[i]);
        make_val(val, keys[i]);
        memtable_put(mt, key, val, 0);
    }

    const char *expected[] = {"alpha", "bravo", "charlie", "delta", "echo"};
    void *node = memtable_iter_first(mt);
    int idx = 0;

    while (node && idx < n) {
        uint16_t klen = *(uint16_t *)((char *)node + SLN_KEY_LEN);
        char *kdata = (char *)node + SLN_KEY_DATA;

        if (klen != strlen(expected[idx]) ||
            memcmp(kdata, expected[idx], klen) != 0) {
            char msg[128];
            snprintf(msg, sizeof(msg), "at idx %d: expected '%s' got '%.*s'",
                     idx, expected[idx], klen, kdata);
            FAIL(msg);
            arena_destroy(arena);
            return;
        }

        node = memtable_iter_next(node);
        idx++;
    }

    if (idx != n || node != NULL) {
        FAIL("wrong number of entries in iteration");
        arena_destroy(arena);
        return;
    }

    arena_destroy(arena);
    PASS();
}

static void test_memtable_stress(void) {
    TEST("memtable stress (5000 keys)");

    void *arena = arena_create();
    void *mt = memtable_create2(arena);
    prng_seed(777);

    uint8_t key[64], val[256], val_out[256];
    char kbuf[32], vbuf[64];
    int N = 5000;

    for (int i = 0; i < N; i++) {
        snprintf(kbuf, sizeof(kbuf), "sk_%08d", i);
        snprintf(vbuf, sizeof(vbuf), "sv_%08d", i);
        make_key(key, kbuf);
        make_val(val, vbuf);
        int rc = memtable_put(mt, key, val, 0);
        if (rc != 0) {
            char msg[64];
            snprintf(msg, sizeof(msg), "put failed at %d", i);
            FAIL(msg);
            arena_destroy(arena);
            return;
        }
    }

    uint64_t count = memtable_entry_count(mt);
    if ((int)count != N) {
        char msg[64];
        snprintf(msg, sizeof(msg), "count=%lu expected %d", count, N);
        FAIL(msg);
        arena_destroy(arena);
        return;
    }

    int fail_count = 0;
    for (int i = 0; i < N; i++) {
        snprintf(kbuf, sizeof(kbuf), "sk_%08d", i);
        snprintf(vbuf, sizeof(vbuf), "sv_%08d", i);
        make_key(key, kbuf);
        memset(val_out, 0, 256);
        int rc = memtable_get(mt, key, val_out);
        if (rc != 0) { fail_count++; continue; }
        uint16_t vlen = *(uint16_t *)val_out;
        if (vlen != strlen(vbuf) || memcmp(val_out + 2, vbuf, vlen) != 0) {
            fail_count++;
        }
    }

    if (fail_count > 0) {
        char msg[64];
        snprintf(msg, sizeof(msg), "%d keys failed lookup", fail_count);
        FAIL(msg);
        arena_destroy(arena);
        return;
    }

    arena_destroy(arena);
    PASS();
}

/* ============ LSM Adapter Tests ============ */

static void test_lsm_adapter_init_close(void) {
    TEST("lsm adapter init + close");

    uint8_t db[DB_SIZE];
    memset(db, 0, DB_SIZE);

    int rc = lsm_adapter_init(db);
    if (rc != 0) { FAIL("init failed"); return; }

    void *mt = (void *)get_u64(db, DB_MEMTABLE_PTR);
    if (!mt) { FAIL("memtable not created"); lsm_adapter_close(db); return; }

    void *ar = (void *)get_u64(db, DB_ARENA_PTR);
    if (!ar) { FAIL("arena not created"); lsm_adapter_close(db); return; }

    rc = lsm_adapter_close(db);
    if (rc != 0) { FAIL("close failed"); return; }

    PASS();
}

static void test_lsm_put_get(void) {
    TEST("lsm put + get through adapter");

    uint8_t db[DB_SIZE];
    memset(db, 0, DB_SIZE);
    prng_seed(42);

    int rc = lsm_adapter_init(db);
    if (rc != 0) { FAIL("init failed"); return; }

    uint8_t key[64], val[256], val_out[256];
    make_key(key, "test_key");
    make_val(val, "test_value");

    rc = lsm_put(db, key, val, 0);
    if (rc != 0) { FAIL("put failed"); lsm_adapter_close(db); return; }

    memset(val_out, 0, 256);
    rc = lsm_get(db, key, val_out, 0);
    if (rc != 0) {
        char msg[64];
        snprintf(msg, sizeof(msg), "get failed rc=%d", rc);
        FAIL(msg);
        lsm_adapter_close(db);
        return;
    }

    uint16_t vlen = *(uint16_t *)val_out;
    if (vlen != 10 || memcmp(val_out + 2, "test_value", 10) != 0) {
        FAIL("value mismatch");
        lsm_adapter_close(db);
        return;
    }

    lsm_adapter_close(db);
    PASS();
}

static void test_lsm_multiple_keys(void) {
    TEST("lsm put + get 200 keys through adapter");

    uint8_t db[DB_SIZE];
    memset(db, 0, DB_SIZE);
    prng_seed(100);

    int rc = lsm_adapter_init(db);
    if (rc != 0) { FAIL("init failed"); return; }

    uint8_t key[64], val[256], val_out[256];
    char kbuf[32], vbuf[64];
    int N = 200;

    for (int i = 0; i < N; i++) {
        snprintf(kbuf, sizeof(kbuf), "lsm_%06d", i);
        snprintf(vbuf, sizeof(vbuf), "lval_%06d", i);
        make_key(key, kbuf);
        make_val(val, vbuf);
        rc = lsm_put(db, key, val, 0);
        if (rc != 0) {
            char msg[64];
            snprintf(msg, sizeof(msg), "put failed at %d", i);
            FAIL(msg);
            lsm_adapter_close(db);
            return;
        }
    }

    int fail_count = 0;
    for (int i = 0; i < N; i++) {
        snprintf(kbuf, sizeof(kbuf), "lsm_%06d", i);
        snprintf(vbuf, sizeof(vbuf), "lval_%06d", i);
        make_key(key, kbuf);
        memset(val_out, 0, 256);
        rc = lsm_get(db, key, val_out, 0);
        if (rc != 0) { fail_count++; continue; }
        uint16_t vlen = *(uint16_t *)val_out;
        if (vlen != strlen(vbuf) || memcmp(val_out + 2, vbuf, vlen) != 0) {
            fail_count++;
        }
    }

    if (fail_count > 0) {
        char msg[64];
        snprintf(msg, sizeof(msg), "%d keys failed", fail_count);
        FAIL(msg);
        lsm_adapter_close(db);
        return;
    }

    lsm_adapter_close(db);
    PASS();
}

static void test_lsm_delete(void) {
    TEST("lsm delete through adapter");

    uint8_t db[DB_SIZE];
    memset(db, 0, DB_SIZE);
    prng_seed(42);

    int rc = lsm_adapter_init(db);
    if (rc != 0) { FAIL("init failed"); return; }

    uint8_t key[64], val[256], val_out[256];
    make_key(key, "del_key");
    make_val(val, "del_val");

    lsm_put(db, key, val, 0);

    rc = lsm_get(db, key, val_out, 0);
    if (rc != 0) { FAIL("get before delete failed"); lsm_adapter_close(db); return; }

    lsm_delete(db, key, 0);

    rc = lsm_get(db, key, val_out, 0);
    if (rc == 0) { FAIL("should not find deleted key"); lsm_adapter_close(db); return; }

    lsm_adapter_close(db);
    PASS();
}

static void test_lsm_get_after_compact_memtable(void) {
    TEST("lsm get after memtable flush to SST");

    int dir_fd = setup_test_dir();
    if (dir_fd < 0) { FAIL("setup dir"); return; }

    uint8_t db[DB_SIZE];
    memset(db, 0, DB_SIZE);
    set_u64(db, DB_DIR_FD, (uint64_t)(int64_t)dir_fd);
    prng_seed(42);

    int rc = lsm_adapter_init(db);
    if (rc != 0) { FAIL("init failed"); close(dir_fd); cleanup_test_dir(); return; }

    uint8_t key[64], val[256], val_out[256];
    make_key(key, "flush_key");
    make_val(val, "flush_val");

    rc = lsm_put(db, key, val, 0);
    if (rc != 0) { FAIL("put failed"); lsm_adapter_close(db); close(dir_fd); cleanup_test_dir(); return; }

    rc = compact_memtable(db);
    if (rc != 0) {
        char msg[64];
        snprintf(msg, sizeof(msg), "compact_memtable rc=%d", rc);
        FAIL(msg);
        lsm_adapter_close(db);
        close(dir_fd);
        cleanup_test_dir();
        return;
    }

    memset(val_out, 0, 256);
    rc = lsm_get(db, key, val_out, 0);
    if (rc != 0) { FAIL("get after flush failed"); lsm_adapter_close(db); close(dir_fd); cleanup_test_dir(); return; }

    uint16_t vlen = *(uint16_t *)val_out;
    if (vlen != 9 || memcmp(val_out + 2, "flush_val", 9) != 0) {
        FAIL("value mismatch after flush");
        lsm_adapter_close(db);
        close(dir_fd);
        cleanup_test_dir();
        return;
    }

    lsm_adapter_close(db);
    close(dir_fd);
    cleanup_test_dir();
    PASS();
}

static void test_lsm_tombstone_masks_flushed_sstable(void) {
    TEST("lsm tombstone masks older flushed value");

    int dir_fd = setup_test_dir();
    if (dir_fd < 0) { FAIL("setup dir"); return; }

    uint8_t db[DB_SIZE];
    memset(db, 0, DB_SIZE);
    set_u64(db, DB_DIR_FD, (uint64_t)(int64_t)dir_fd);
    prng_seed(42);

    int rc = lsm_adapter_init(db);
    if (rc != 0) { FAIL("init failed"); close(dir_fd); cleanup_test_dir(); return; }

    uint8_t key[64], val[256], val_out[256];
    make_key(key, "shadow_key");
    make_val(val, "old_value");

    rc = lsm_put(db, key, val, 0);
    if (rc != 0) { FAIL("put failed"); lsm_adapter_close(db); close(dir_fd); cleanup_test_dir(); return; }

    rc = compact_memtable(db);
    if (rc != 0) {
        char msg[64];
        snprintf(msg, sizeof(msg), "compact_memtable rc=%d", rc);
        FAIL(msg);
        lsm_adapter_close(db);
        close(dir_fd);
        cleanup_test_dir();
        return;
    }

    rc = lsm_delete(db, key, 0);
    if (rc != 0) { FAIL("delete failed"); lsm_adapter_close(db); close(dir_fd); cleanup_test_dir(); return; }

    memset(val_out, 0, 256);
    rc = lsm_get(db, key, val_out, 0);
    if (rc == 0) {
        FAIL("stale SST value visible despite tombstone");
        lsm_adapter_close(db);
        close(dir_fd);
        cleanup_test_dir();
        return;
    }

    lsm_adapter_close(db);
    close(dir_fd);
    cleanup_test_dir();
    PASS();
}

/* ============ WAL Tests ============ */

static void test_wal_write_sync(void) {
    TEST("WAL write + sync");

    int dir_fd = setup_test_dir();
    if (dir_fd < 0) { FAIL("setup dir"); return; }

    uint8_t db[DB_SIZE];
    memset(db, 0, DB_SIZE);
    set_u64(db, DB_DIR_FD, (uint64_t)(int64_t)dir_fd);
    set_u64(db, DB_WAL_SEQ, 0);
    set_u64(db, DB_WAL_OFFSET, 0);

    int rc = wal_open(db);
    if (rc != 0) {
        char msg[64];
        snprintf(msg, sizeof(msg), "wal_open failed rc=%d", rc);
        FAIL(msg);
        close(dir_fd);
        cleanup_test_dir();
        return;
    }

    uint8_t key[64], val[256];
    make_key(key, "wal_key_1");
    make_val(val, "wal_val_1");

    rc = wal_append(db, WAL_OP_PUT, key, val, 1);
    if (rc != 0) {
        char msg[64];
        snprintf(msg, sizeof(msg), "wal_append failed rc=%d", rc);
        FAIL(msg);
        wal_close(db);
        close(dir_fd);
        cleanup_test_dir();
        return;
    }

    uint64_t offset = get_u64(db, DB_WAL_OFFSET);
    if (offset != WAL_RECORD_SIZE) {
        char msg[64];
        snprintf(msg, sizeof(msg), "offset=%lu expected %d", offset, WAL_RECORD_SIZE);
        FAIL(msg);
        wal_close(db);
        close(dir_fd);
        cleanup_test_dir();
        return;
    }

    rc = wal_sync(db);
    if (rc < 0) { FAIL("wal_sync failed"); wal_close(db); close(dir_fd); cleanup_test_dir(); return; }

    wal_close(db);
    close(dir_fd);
    cleanup_test_dir();
    PASS();
}

static void test_wal_multiple_records(void) {
    TEST("WAL write 10 records");

    int dir_fd = setup_test_dir();
    if (dir_fd < 0) { FAIL("setup dir"); return; }

    uint8_t db[DB_SIZE];
    memset(db, 0, DB_SIZE);
    set_u64(db, DB_DIR_FD, (uint64_t)(int64_t)dir_fd);
    set_u64(db, DB_WAL_SEQ, 0);
    set_u64(db, DB_WAL_OFFSET, 0);

    int rc = wal_open(db);
    if (rc != 0) { FAIL("wal_open failed"); close(dir_fd); cleanup_test_dir(); return; }

    uint8_t key[64], val[256];
    char kbuf[32], vbuf[64];

    for (int i = 0; i < 10; i++) {
        snprintf(kbuf, sizeof(kbuf), "wk_%06d", i);
        snprintf(vbuf, sizeof(vbuf), "wv_%06d", i);
        make_key(key, kbuf);
        make_val(val, vbuf);
        rc = wal_append(db, WAL_OP_PUT, key, val, (uint64_t)i);
        if (rc != 0) {
            char msg[64];
            snprintf(msg, sizeof(msg), "append failed at %d", i);
            FAIL(msg);
            wal_close(db);
            close(dir_fd);
            cleanup_test_dir();
            return;
        }
    }

    uint64_t offset = get_u64(db, DB_WAL_OFFSET);
    if (offset != (uint64_t)(10 * WAL_RECORD_SIZE)) {
        char msg[64];
        snprintf(msg, sizeof(msg), "offset=%lu expected %d", offset, 10 * WAL_RECORD_SIZE);
        FAIL(msg);
        wal_close(db);
        close(dir_fd);
        cleanup_test_dir();
        return;
    }

    wal_sync(db);
    wal_close(db);
    close(dir_fd);
    cleanup_test_dir();
    PASS();
}

/* WAL recovery test data */
struct recovery_ctx {
    int count;
    char keys[20][32];
    int ops[20];
};

static int recovery_callback(int op, void *key_ptr, void *val_ptr __attribute__((unused)), void *ctx_ptr) {
    struct recovery_ctx *ctx = (struct recovery_ctx *)ctx_ptr;
    if (ctx->count < 20) {
        ctx->ops[ctx->count] = op;
        uint16_t klen = *(uint16_t *)key_ptr;
        if (klen > 30) klen = 30;
        memcpy(ctx->keys[ctx->count], (char *)key_ptr + 2, klen);
        ctx->keys[ctx->count][klen] = '\0';
        ctx->count++;
    }
    return 0;
}

static void test_wal_recovery(void) {
    TEST("WAL recovery");

    int dir_fd = setup_test_dir();
    if (dir_fd < 0) { FAIL("setup dir"); return; }

    uint8_t db[DB_SIZE];
    memset(db, 0, DB_SIZE);
    set_u64(db, DB_DIR_FD, (uint64_t)(int64_t)dir_fd);
    set_u64(db, DB_WAL_SEQ, 0);
    set_u64(db, DB_WAL_OFFSET, 0);

    /* Write some records */
    int rc = wal_open(db);
    if (rc != 0) { FAIL("wal_open failed"); close(dir_fd); cleanup_test_dir(); return; }

    uint8_t key[64], val[256];

    make_key(key, "rec_aaa");
    make_val(val, "val_aaa");
    wal_append(db, WAL_OP_PUT, key, val, 1);

    make_key(key, "rec_bbb");
    make_val(val, "val_bbb");
    wal_append(db, WAL_OP_PUT, key, val, 2);

    make_key(key, "rec_ccc");
    make_val(val, "");
    wal_append(db, WAL_OP_DELETE, key, val, 3);

    wal_sync(db);
    wal_close(db);

    /* Re-open for recovery */
    close(dir_fd);
    dir_fd = open(test_dir, O_RDONLY | O_DIRECTORY);

    memset(db, 0, DB_SIZE);
    set_u64(db, DB_DIR_FD, (uint64_t)(int64_t)dir_fd);
    set_u64(db, DB_WAL_SEQ, 0);

    struct recovery_ctx rctx;
    memset(&rctx, 0, sizeof(rctx));

    rc = wal_recover(db, (void *)recovery_callback, &rctx);
    if (rc != 0) {
        char msg[64];
        snprintf(msg, sizeof(msg), "wal_recover failed rc=%d", rc);
        FAIL(msg);
        close(dir_fd);
        cleanup_test_dir();
        return;
    }

    if (rctx.count != 3) {
        char msg[64];
        snprintf(msg, sizeof(msg), "recovered %d records expected 3", rctx.count);
        FAIL(msg);
        close(dir_fd);
        cleanup_test_dir();
        return;
    }

    if (strcmp(rctx.keys[0], "rec_aaa") != 0 ||
        strcmp(rctx.keys[1], "rec_bbb") != 0 ||
        strcmp(rctx.keys[2], "rec_ccc") != 0) {
        FAIL("recovered keys mismatch");
        close(dir_fd);
        cleanup_test_dir();
        return;
    }

    if (rctx.ops[0] != WAL_OP_PUT || rctx.ops[1] != WAL_OP_PUT || rctx.ops[2] != WAL_OP_DELETE) {
        FAIL("recovered ops mismatch");
        close(dir_fd);
        cleanup_test_dir();
        return;
    }

    close(dir_fd);
    cleanup_test_dir();
    PASS();
}

/* ============ SSTable Tests ============ */

static void test_sstable_flush_and_read(void) {
    TEST("SSTable flush + read back");

    int dir_fd = setup_test_dir();
    if (dir_fd < 0) { FAIL("setup dir"); return; }

    uint8_t db[DB_SIZE];
    memset(db, 0, DB_SIZE);
    set_u64(db, DB_DIR_FD, (uint64_t)(int64_t)dir_fd);

    /* Create memtable with some data */
    void *arena = arena_create();
    void *mt = memtable_create2(arena);
    prng_seed(42);

    uint8_t key[64], val[256], val_out[256];
    char kbuf[32], vbuf[64];
    int N = 50;

    for (int i = 0; i < N; i++) {
        snprintf(kbuf, sizeof(kbuf), "sst_%06d", i);
        snprintf(vbuf, sizeof(vbuf), "sv_%06d", i);
        make_key(key, kbuf);
        make_val(val, vbuf);
        memtable_put(mt, key, val, 0);
    }

    /* Flush memtable to SSTable L0, seq 0 */
    int rc = sstable_flush(db, mt, 0, 0);
    if (rc != 0) {
        char msg[64];
        snprintf(msg, sizeof(msg), "sstable_flush failed rc=%d", rc);
        FAIL(msg);
        arena_destroy(arena);
        close(dir_fd);
        cleanup_test_dir();
        return;
    }

    /* Open SSTable and read back */
    uint8_t desc[SSTD_SIZE];
    memset(desc, 0, SSTD_SIZE);

    rc = sstable_open(db, 0, 0, desc);
    if (rc != 0) {
        char msg[64];
        snprintf(msg, sizeof(msg), "sstable_open failed rc=%d", rc);
        FAIL(msg);
        arena_destroy(arena);
        close(dir_fd);
        cleanup_test_dir();
        return;
    }

    /* Verify we can read back each key */
    int found = 0;
    for (int i = 0; i < N; i++) {
        snprintf(kbuf, sizeof(kbuf), "sst_%06d", i);
        make_key(key, kbuf);
        memset(val_out, 0, 256);
        rc = sstable_get(desc, key, val_out);
        if (rc == 0) {
            snprintf(vbuf, sizeof(vbuf), "sv_%06d", i);
            uint16_t vlen = *(uint16_t *)val_out;
            if (vlen == strlen(vbuf) && memcmp(val_out + 2, vbuf, vlen) == 0) {
                found++;
            }
        }
    }

    sstable_close(desc);

    if (found != N) {
        char msg[64];
        snprintf(msg, sizeof(msg), "found %d/%d keys in SSTable", found, N);
        FAIL(msg);
        arena_destroy(arena);
        close(dir_fd);
        cleanup_test_dir();
        return;
    }

    arena_destroy(arena);
    close(dir_fd);
    cleanup_test_dir();
    PASS();
}

static void test_sstable_not_found(void) {
    TEST("SSTable get non-existent key");

    int dir_fd = setup_test_dir();
    if (dir_fd < 0) { FAIL("setup dir"); return; }

    uint8_t db[DB_SIZE];
    memset(db, 0, DB_SIZE);
    set_u64(db, DB_DIR_FD, (uint64_t)(int64_t)dir_fd);

    void *arena = arena_create();
    void *mt = memtable_create2(arena);
    prng_seed(42);

    uint8_t key[64], val[256], val_out[256];
    make_key(key, "only_key");
    make_val(val, "only_val");
    memtable_put(mt, key, val, 0);

    sstable_flush(db, mt, 0, 0);

    uint8_t desc[SSTD_SIZE];
    memset(desc, 0, SSTD_SIZE);
    sstable_open(db, 0, 0, desc);

    make_key(key, "missing_key");
    int rc = sstable_get(desc, key, val_out);
    sstable_close(desc);

    if (rc == 0) {
        FAIL("should not find missing key");
        arena_destroy(arena);
        close(dir_fd);
        cleanup_test_dir();
        return;
    }

    arena_destroy(arena);
    close(dir_fd);
    cleanup_test_dir();
    PASS();
}

/* ============ Main ============ */

int main(void) {
    printf("=== AssemblyDB LSM Engine Tests (Phase 3) ===\n\n");

    printf("--- Memtable ---\n");
    test_memtable_create();
    test_memtable_put_get();
    test_memtable_put_multiple();
    test_memtable_upsert();
    test_memtable_not_found();
    test_memtable_delete();
    test_memtable_iterator();
    test_memtable_stress();

    printf("\n--- LSM Adapter ---\n");
    test_lsm_adapter_init_close();
    test_lsm_put_get();
    test_lsm_multiple_keys();
    test_lsm_delete();
    test_lsm_get_after_compact_memtable();
    test_lsm_tombstone_masks_flushed_sstable();

    printf("\n--- WAL ---\n");
    test_wal_write_sync();
    test_wal_multiple_records();
    test_wal_recovery();

    printf("\n--- SSTable ---\n");
    test_sstable_flush_and_read();
    test_sstable_not_found();

    printf("\n=== Results: %d/%d passed ===\n", tests_passed, tests_run);
    return (tests_passed == tests_run) ? 0 : 1;
}
