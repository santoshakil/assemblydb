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

/* B+ tree internal functions - declared here for direct testing */

/* Page operations */
extern void btree_page_init_leaf(void *page, uint32_t page_id);
extern void btree_page_init_internal(void *page, uint32_t page_id);
extern void *btree_page_get_key_ptr(void *page, uint32_t index);
extern void *btree_leaf_get_val_ptr(void *page, uint32_t index);
extern uint64_t btree_int_get_child(void *page, uint32_t index);
extern void btree_int_set_child(void *page, uint32_t index, uint64_t child);
extern uint64_t btree_leaf_get_txid(void *page, uint32_t index);
extern void btree_leaf_set_txid(void *page, uint32_t index, uint64_t tx_id);
extern uint16_t btree_page_num_keys(void *page);
extern uint16_t btree_page_type(void *page);
extern int btree_page_is_leaf(void *page);
extern void btree_page_set_crc(void *page);
extern int btree_page_verify_crc(void *page);
extern void *btree_page_get_ptr(void *mmap_base, uint32_t page_id);

/* Search */
extern void btree_page_binary_search(void *page, void *key); /* returns w0=idx, w1=found */

/* Insert (high-level) */
extern int btree_insert(void *db, void *key, void *val, uint64_t tx_id);

/* Delete */
extern int btree_delete(void *db, void *key);

/* Search (high-level) */
extern int btree_get(void *mmap_base, uint32_t root_page_id, void *key, void *val_buf);

/* Scan */
typedef int (*scan_callback)(const void *key, uint16_t key_len,
                             const void *val, uint16_t val_len,
                             void *user_data);
extern int btree_scan(void *mmap_base, uint32_t root_page_id,
                      void *start_key, void *end_key,
                      scan_callback cb, void *user_data);
extern uint64_t btree_count(void *mmap_base, uint32_t root_page_id);

/* Adapter */
extern int btree_adapter_init(void *db, int dir_fd);
extern int btree_adapter_close(void *db);

/* We need a db_t-like struct. Offsets from const.s */
#define DB_SIZE 1024

/* Key offsets from const.s */
#define DB_DIR_FD           0x000
#define DB_STORAGE_PORT     0x010
#define DB_BTREE_FD         0x048
#define DB_BTREE_MMAP       0x050
#define DB_BTREE_MMAP_LEN   0x058
#define DB_BTREE_ROOT       0x060
#define DB_BTREE_NUM_PAGES  0x068
#define DB_BTREE_CAPACITY   0x070

static uint64_t get_u64(void *db, int offset) {
    return *(uint64_t *)((char *)db + offset);
}

static uint32_t get_u32(void *db, int offset) {
    return *(uint32_t *)((char *)db + offset);
}

static void make_key(void *dst, const char *str) {
    memset(dst, 0, 64);
    uint16_t len = strlen(str);
    if (len > 62) len = 62;
    *(uint16_t *)dst = len;
    memcpy((char *)dst + 2, str, len);
}

static void make_val(void *dst, const char *str) {
    memset(dst, 0, 256);
    uint16_t len = strlen(str);
    if (len > 254) len = 254;
    *(uint16_t *)dst = len;
    memcpy((char *)dst + 2, str, len);
}

static void make_key_num(void *dst, int n) {
    char buf[64];
    snprintf(buf, sizeof(buf), "key_%06d", n);
    make_key(dst, buf);
}

static void make_val_num(void *dst, int n) {
    char buf[256];
    snprintf(buf, sizeof(buf), "value_%06d_data_padding_for_testing", n);
    make_val(dst, buf);
}

static const char *test_dir = "/tmp/assemblydb_test_btree";

static void cleanup_test_dir(void) {
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", test_dir);
    int rc = system(cmd);
    (void)rc;
}

static int setup_test_dir(void) {
    cleanup_test_dir();
    return mkdir(test_dir, 0755);
}

/* ============================================================ */
/* Tests */
/* ============================================================ */

static void test_page_init_leaf(void) {
    TEST("page_init_leaf");
    void *page = calloc(1, 4096);

    btree_page_init_leaf(page, 42);

    uint32_t page_id = *(uint32_t *)page;
    uint16_t page_type = *(uint16_t *)((char *)page + 4);
    uint16_t num_keys = *(uint16_t *)((char *)page + 6);
    uint32_t parent = *(uint32_t *)((char *)page + 8);
    uint32_t next = *(uint32_t *)((char *)page + 0x18);
    uint32_t prev = *(uint32_t *)((char *)page + 0x20);

    if (page_id == 42 && page_type == 2 && num_keys == 0 &&
        parent == 0xFFFFFFFF && next == 0xFFFFFFFF && prev == 0xFFFFFFFF) {
        PASS();
    } else {
        char msg[256];
        snprintf(msg, sizeof(msg), "id=%u type=%u keys=%u parent=0x%X next=0x%X prev=0x%X",
                 page_id, page_type, num_keys, parent, next, prev);
        FAIL(msg);
    }
    free(page);
}

static void test_page_init_internal(void) {
    TEST("page_init_internal");
    void *page = calloc(1, 4096);

    btree_page_init_internal(page, 7);

    uint32_t page_id = *(uint32_t *)page;
    uint16_t page_type = *(uint16_t *)((char *)page + 4);
    uint32_t parent = *(uint32_t *)((char *)page + 8);

    if (page_id == 7 && page_type == 1 && parent == 0xFFFFFFFF) {
        PASS();
    } else {
        FAIL("wrong header values");
    }
    free(page);
}

static void test_page_key_access(void) {
    TEST("page key/val access helpers");
    void *page = calloc(1, 4096);
    btree_page_init_leaf(page, 0);

    /* Key ptr should be at page + 0x040 + idx * 64 */
    void *k0 = btree_page_get_key_ptr(page, 0);
    void *k1 = btree_page_get_key_ptr(page, 1);

    if ((char *)k0 == (char *)page + 0x040 &&
        (char *)k1 == (char *)page + 0x040 + 64) {
        /* Val ptr should be at page + 0x340 + idx * 256 */
        void *v0 = btree_leaf_get_val_ptr(page, 0);
        void *v1 = btree_leaf_get_val_ptr(page, 1);

        if ((char *)v0 == (char *)page + 0x340 &&
            (char *)v1 == (char *)page + 0x340 + 256) {
            PASS();
        } else {
            FAIL("wrong val ptr");
        }
    } else {
        FAIL("wrong key ptr");
    }
    free(page);
}

static void test_page_is_leaf(void) {
    TEST("page_is_leaf");
    void *page = calloc(1, 4096);

    btree_page_init_leaf(page, 0);
    int leaf = btree_page_is_leaf(page);

    btree_page_init_internal(page, 0);
    int internal = btree_page_is_leaf(page);

    if (leaf == 1 && internal == 0) {
        PASS();
    } else {
        FAIL("wrong is_leaf results");
    }
    free(page);
}

static void test_page_crc(void) {
    TEST("page CRC set/verify");
    void *page = calloc(1, 4096);
    btree_page_init_leaf(page, 1);

    /* Write some data */
    void *k = btree_page_get_key_ptr(page, 0);
    make_key(k, "test_key");

    btree_page_set_crc(page);
    int valid = btree_page_verify_crc(page);

    /* Corrupt a byte */
    ((char *)page)[100] ^= 0xFF;
    int invalid = btree_page_verify_crc(page);

    if (valid == 1 && invalid == 0) {
        PASS();
    } else {
        char msg[64];
        snprintf(msg, sizeof(msg), "valid=%d invalid=%d", valid, invalid);
        FAIL(msg);
    }
    free(page);
}

static void test_internal_children(void) {
    TEST("internal node children get/set");
    void *page = calloc(1, 4096);
    btree_page_init_internal(page, 0);

    btree_int_set_child(page, 0, 100);
    btree_int_set_child(page, 1, 200);
    btree_int_set_child(page, 55, 999);

    uint64_t c0 = btree_int_get_child(page, 0);
    uint64_t c1 = btree_int_get_child(page, 1);
    uint64_t c55 = btree_int_get_child(page, 55);

    if (c0 == 100 && c1 == 200 && c55 == 999) {
        PASS();
    } else {
        FAIL("wrong child values");
    }
    free(page);
}

static void test_leaf_txid(void) {
    TEST("leaf txid get/set");
    void *page = calloc(1, 4096);
    btree_page_init_leaf(page, 0);

    btree_leaf_set_txid(page, 0, 1000);
    btree_leaf_set_txid(page, 11, 2000);

    uint64_t t0 = btree_leaf_get_txid(page, 0);
    uint64_t t11 = btree_leaf_get_txid(page, 11);

    if (t0 == 1000 && t11 == 2000) {
        PASS();
    } else {
        FAIL("wrong txid values");
    }
    free(page);
}

static void test_page_get_ptr(void) {
    TEST("page_get_ptr calculation");

    char *base = (char *)0x10000000;
    void *p0 = btree_page_get_ptr(base, 0);
    void *p1 = btree_page_get_ptr(base, 1);
    void *p10 = btree_page_get_ptr(base, 10);

    if (p0 == base && p1 == base + 4096 && p10 == base + 40960) {
        PASS();
    } else {
        FAIL("wrong pointer calculation");
    }
}

/* ============================================================ */
/* Integration tests with btree_adapter */
/* ============================================================ */

static void test_adapter_init_close(void) {
    TEST("adapter init and close");

    setup_test_dir();

    int dir_fd = open(test_dir, O_RDONLY | O_DIRECTORY);
    if (dir_fd < 0) { FAIL("can't open dir"); return; }

    void *db = calloc(1, DB_SIZE);
    int rc = btree_adapter_init(db, dir_fd);

    if (rc != 0) {
        char msg[64];
        snprintf(msg, sizeof(msg), "init failed: rc=%d", rc);
        FAIL(msg);
        free(db);
        close(dir_fd);
        return;
    }

    /* Verify state */
    uint64_t mmap_ptr = get_u64(db, DB_BTREE_MMAP);
    uint64_t root = get_u64(db, DB_BTREE_ROOT);
    uint64_t storage_port = get_u64(db, DB_STORAGE_PORT);

    if (mmap_ptr == 0 || storage_port == 0) {
        FAIL("mmap or vtable not set");
    } else if (root != 0xFFFFFFFF) {
        char msg[64];
        snprintf(msg, sizeof(msg), "root should be INVALID_PAGE, got 0x%lx", root);
        FAIL(msg);
    } else {
        rc = btree_adapter_close(db);
        if (rc == 0) {
            PASS();
        } else {
            FAIL("close failed");
        }
    }

    free(db);
    close(dir_fd);
    cleanup_test_dir();
}

static void test_insert_and_get_single(void) {
    TEST("insert + get single key");

    setup_test_dir();
    int dir_fd = open(test_dir, O_RDONLY | O_DIRECTORY);
    void *db = calloc(1, DB_SIZE);
    btree_adapter_init(db, dir_fd);

    uint8_t key[64], val[256], val_out[256];
    make_key(key, "hello");
    make_val(val, "world");
    memset(val_out, 0, 256);

    int rc = btree_insert(db, key, val, 0);
    if (rc != 0) {
        char msg[64];
        snprintf(msg, sizeof(msg), "insert failed: rc=%d", rc);
        FAIL(msg);
        goto cleanup1;
    }

    uint64_t mmap_base = get_u64(db, DB_BTREE_MMAP);
    uint32_t root = get_u32(db, DB_BTREE_ROOT);

    rc = btree_get((void *)mmap_base, root, key, val_out);
    if (rc != 0) {
        char msg[64];
        snprintf(msg, sizeof(msg), "get failed: rc=%d", rc);
        FAIL(msg);
        goto cleanup1;
    }

    /* Compare value */
    if (memcmp(val, val_out, 256) == 0) {
        PASS();
    } else {
        FAIL("value mismatch");
    }

cleanup1:
    btree_adapter_close(db);
    free(db);
    close(dir_fd);
    cleanup_test_dir();
}

static void test_insert_multiple(void) {
    TEST("insert 100 keys + get all");

    setup_test_dir();
    int dir_fd = open(test_dir, O_RDONLY | O_DIRECTORY);
    void *db = calloc(1, DB_SIZE);
    btree_adapter_init(db, dir_fd);

    uint8_t key[64], val[256], val_out[256];
    int n = 100;
    int ok = 1;

    for (int i = 0; i < n; i++) {
        make_key_num(key, i);
        make_val_num(val, i);
        int rc = btree_insert(db, key, val, 0);
        if (rc != 0) {
            char msg[64];
            snprintf(msg, sizeof(msg), "insert[%d] failed: rc=%d", i, rc);
            FAIL(msg);
            ok = 0;
            break;
        }
    }

    if (ok) {
        /* Verify all */
        for (int i = 0; i < n; i++) {
            make_key_num(key, i);
            make_val_num(val, i);
            memset(val_out, 0, 256);

            uint64_t mmap_base = get_u64(db, DB_BTREE_MMAP);
            uint32_t root = get_u32(db, DB_BTREE_ROOT);

            int rc = btree_get((void *)mmap_base, root, key, val_out);
            if (rc != 0) {
                char msg[64];
                snprintf(msg, sizeof(msg), "get[%d] failed: rc=%d", i, rc);
                FAIL(msg);
                ok = 0;
                break;
            }
            if (memcmp(val, val_out, 256) != 0) {
                char msg[64];
                snprintf(msg, sizeof(msg), "value mismatch at key %d", i);
                FAIL(msg);
                ok = 0;
                break;
            }
        }
        if (ok) PASS();
    }

    btree_adapter_close(db);
    free(db);
    close(dir_fd);
    cleanup_test_dir();
}

static void test_insert_causes_split(void) {
    TEST("insert triggers leaf split (>12 keys)");

    setup_test_dir();
    int dir_fd = open(test_dir, O_RDONLY | O_DIRECTORY);
    void *db = calloc(1, DB_SIZE);
    btree_adapter_init(db, dir_fd);

    uint8_t key[64], val[256], val_out[256];
    int n = 20; /* More than 12 (BTREE_LEAF_MAX_KEYS) to trigger split */
    int ok = 1;

    for (int i = 0; i < n; i++) {
        make_key_num(key, i);
        make_val_num(val, i);
        int rc = btree_insert(db, key, val, 0);
        if (rc != 0) {
            char msg[64];
            snprintf(msg, sizeof(msg), "insert[%d] failed: rc=%d", i, rc);
            FAIL(msg);
            ok = 0;
            break;
        }
    }

    if (ok) {
        uint64_t num_pages = get_u64(db, DB_BTREE_NUM_PAGES);
        if (num_pages <= 2) {
            char msg[64];
            snprintf(msg, sizeof(msg), "expected split: only %lu pages", num_pages);
            FAIL(msg);
            ok = 0;
        }
    }

    if (ok) {
        /* Verify all still retrievable */
        for (int i = 0; i < n; i++) {
            make_key_num(key, i);
            make_val_num(val, i);
            memset(val_out, 0, 256);

            uint64_t mmap_base = get_u64(db, DB_BTREE_MMAP);
            uint32_t root = get_u32(db, DB_BTREE_ROOT);
            int rc = btree_get((void *)mmap_base, root, key, val_out);
            if (rc != 0 || memcmp(val, val_out, 256) != 0) {
                char msg[64];
                snprintf(msg, sizeof(msg), "verify[%d] failed after split", i);
                FAIL(msg);
                ok = 0;
                break;
            }
        }
        if (ok) PASS();
    }

    btree_adapter_close(db);
    free(db);
    close(dir_fd);
    cleanup_test_dir();
}

static void test_upsert(void) {
    TEST("upsert (update existing key)");

    setup_test_dir();
    int dir_fd = open(test_dir, O_RDONLY | O_DIRECTORY);
    void *db = calloc(1, DB_SIZE);
    btree_adapter_init(db, dir_fd);

    uint8_t key[64], val1[256], val2[256], val_out[256];
    make_key(key, "upsert_key");
    make_val(val1, "original_value");
    make_val(val2, "updated_value");

    btree_insert(db, key, val1, 0);
    btree_insert(db, key, val2, 0);  /* Should update */

    uint64_t mmap_base = get_u64(db, DB_BTREE_MMAP);
    uint32_t root = get_u32(db, DB_BTREE_ROOT);
    memset(val_out, 0, 256);

    int rc = btree_get((void *)mmap_base, root, key, val_out);
    if (rc == 0 && memcmp(val2, val_out, 256) == 0) {
        PASS();
    } else {
        FAIL("upsert did not update value");
    }

    btree_adapter_close(db);
    free(db);
    close(dir_fd);
    cleanup_test_dir();
}

static void test_get_not_found(void) {
    TEST("get non-existent key");

    setup_test_dir();
    int dir_fd = open(test_dir, O_RDONLY | O_DIRECTORY);
    void *db = calloc(1, DB_SIZE);
    btree_adapter_init(db, dir_fd);

    uint8_t key[64], val[256], val_out[256];
    make_key(key, "existing");
    make_val(val, "value");
    btree_insert(db, key, val, 0);

    uint8_t missing_key[64];
    make_key(missing_key, "missing");

    uint64_t mmap_base = get_u64(db, DB_BTREE_MMAP);
    uint32_t root = get_u32(db, DB_BTREE_ROOT);

    int rc = btree_get((void *)mmap_base, root, missing_key, val_out);
    if (rc == 1) {  /* ADB_ERR_NOT_FOUND = 1 */
        PASS();
    } else {
        char msg[64];
        snprintf(msg, sizeof(msg), "expected NOT_FOUND(1), got %d", rc);
        FAIL(msg);
    }

    btree_adapter_close(db);
    free(db);
    close(dir_fd);
    cleanup_test_dir();
}

static void test_delete_single(void) {
    TEST("delete single key");

    setup_test_dir();
    int dir_fd = open(test_dir, O_RDONLY | O_DIRECTORY);
    void *db = calloc(1, DB_SIZE);
    btree_adapter_init(db, dir_fd);

    uint8_t key[64], val[256], val_out[256];
    make_key(key, "del_me");
    make_val(val, "value");
    btree_insert(db, key, val, 0);

    int rc = btree_delete(db, key);
    if (rc != 0) {
        char msg[64];
        snprintf(msg, sizeof(msg), "delete failed: rc=%d", rc);
        FAIL(msg);
        goto cleanup_del;
    }

    uint64_t mmap_base = get_u64(db, DB_BTREE_MMAP);
    uint32_t root = get_u32(db, DB_BTREE_ROOT);

    rc = btree_get((void *)mmap_base, root, key, val_out);
    if (rc == 1) { /* NOT_FOUND */
        PASS();
    } else {
        FAIL("key still found after delete");
    }

cleanup_del:
    btree_adapter_close(db);
    free(db);
    close(dir_fd);
    cleanup_test_dir();
}

static void test_delete_not_found(void) {
    TEST("delete non-existent key");

    setup_test_dir();
    int dir_fd = open(test_dir, O_RDONLY | O_DIRECTORY);
    void *db = calloc(1, DB_SIZE);
    btree_adapter_init(db, dir_fd);

    uint8_t key[64];
    make_key(key, "nope");

    /* Delete from empty tree */
    int rc = btree_delete(db, key);
    if (rc == 1) { /* ADB_ERR_NOT_FOUND */
        PASS();
    } else {
        char msg[64];
        snprintf(msg, sizeof(msg), "expected NOT_FOUND(1), got %d", rc);
        FAIL(msg);
    }

    btree_adapter_close(db);
    free(db);
    close(dir_fd);
    cleanup_test_dir();
}

typedef struct {
    int count;
    int in_order;
    char last_key[64];
} scan_state;

static int scan_cb(const void *key, uint16_t key_len,
                   const void *val, uint16_t val_len,
                   void *user_data) {
    (void)val; (void)val_len;
    scan_state *st = (scan_state *)user_data;

    if (st->count > 0) {
        /* Check ordering */
        char cur[64];
        memset(cur, 0, 64);
        if (key_len > 62) key_len = 62;
        memcpy(cur, key, key_len);

        if (strcmp(cur, st->last_key) <= 0) {
            st->in_order = 0;
        }
    }

    memset(st->last_key, 0, 64);
    if (key_len > 62) key_len = 62;
    memcpy(st->last_key, key, key_len);
    st->count++;
    return 0;
}

static void test_scan_all(void) {
    TEST("scan all entries in order");

    setup_test_dir();
    int dir_fd = open(test_dir, O_RDONLY | O_DIRECTORY);
    void *db = calloc(1, DB_SIZE);
    btree_adapter_init(db, dir_fd);

    uint8_t key[64], val[256];
    int n = 50;

    for (int i = 0; i < n; i++) {
        make_key_num(key, i);
        make_val_num(val, i);
        btree_insert(db, key, val, 0);
    }

    uint64_t mmap_base = get_u64(db, DB_BTREE_MMAP);
    uint32_t root = get_u32(db, DB_BTREE_ROOT);

    scan_state st = {0, 1, {0}};
    int rc = btree_scan((void *)mmap_base, root, NULL, NULL,
                        scan_cb, &st);

    if (rc == 0 && st.count == n && st.in_order) {
        PASS();
    } else {
        char msg[128];
        snprintf(msg, sizeof(msg), "rc=%d count=%d (expected %d) in_order=%d",
                 rc, st.count, n, st.in_order);
        FAIL(msg);
    }

    btree_adapter_close(db);
    free(db);
    close(dir_fd);
    cleanup_test_dir();
}

static void test_count(void) {
    TEST("btree_count");

    setup_test_dir();
    int dir_fd = open(test_dir, O_RDONLY | O_DIRECTORY);
    void *db = calloc(1, DB_SIZE);
    btree_adapter_init(db, dir_fd);

    uint8_t key[64], val[256];
    int n = 30;

    for (int i = 0; i < n; i++) {
        make_key_num(key, i);
        make_val_num(val, i);
        btree_insert(db, key, val, 0);
    }

    uint64_t mmap_base = get_u64(db, DB_BTREE_MMAP);
    uint32_t root = get_u32(db, DB_BTREE_ROOT);

    uint64_t count = btree_count((void *)mmap_base, root);
    if (count == (uint64_t)n) {
        PASS();
    } else {
        char msg[64];
        snprintf(msg, sizeof(msg), "count=%lu expected=%d", count, n);
        FAIL(msg);
    }

    btree_adapter_close(db);
    free(db);
    close(dir_fd);
    cleanup_test_dir();
}

static void test_large_insert(void) {
    TEST("insert 1000 keys (multi-level tree)");

    setup_test_dir();
    int dir_fd = open(test_dir, O_RDONLY | O_DIRECTORY);
    void *db = calloc(1, DB_SIZE);
    btree_adapter_init(db, dir_fd);

    uint8_t key[64], val[256], val_out[256];
    int n = 1000;
    int ok = 1;

    for (int i = 0; i < n; i++) {
        make_key_num(key, i);
        make_val_num(val, i);
        int rc = btree_insert(db, key, val, 0);
        if (rc != 0) {
            char msg[64];
            snprintf(msg, sizeof(msg), "insert[%d] failed: rc=%d", i, rc);
            FAIL(msg);
            ok = 0;
            break;
        }
    }

    if (ok) {
        /* Spot-check some keys */
        int checks[] = {0, 1, 499, 500, 998, 999};
        for (int c = 0; c < 6; c++) {
            int i = checks[c];
            make_key_num(key, i);
            make_val_num(val, i);
            memset(val_out, 0, 256);

            uint64_t mmap_base = get_u64(db, DB_BTREE_MMAP);
            uint32_t root = get_u32(db, DB_BTREE_ROOT);
            int rc = btree_get((void *)mmap_base, root, key, val_out);
            if (rc != 0 || memcmp(val, val_out, 256) != 0) {
                char msg[64];
                snprintf(msg, sizeof(msg), "verify[%d] failed", i);
                FAIL(msg);
                ok = 0;
                break;
            }
        }

        if (ok) {
            uint64_t mmap_base = get_u64(db, DB_BTREE_MMAP);
            uint32_t root = get_u32(db, DB_BTREE_ROOT);
            uint64_t count = btree_count((void *)mmap_base, root);
            if (count == (uint64_t)n) {
                printf("PASS (%lu pages)\n", get_u64(db, DB_BTREE_NUM_PAGES));
                tests_passed++;
            } else {
                char msg[64];
                snprintf(msg, sizeof(msg), "count=%lu expected=%d", count, n);
                FAIL(msg);
            }
        }
    }

    btree_adapter_close(db);
    free(db);
    close(dir_fd);
    cleanup_test_dir();
}

static void test_insert_reverse_order(void) {
    TEST("insert keys in reverse order");

    setup_test_dir();
    int dir_fd = open(test_dir, O_RDONLY | O_DIRECTORY);
    void *db = calloc(1, DB_SIZE);
    btree_adapter_init(db, dir_fd);

    uint8_t key[64], val[256], val_out[256];
    int n = 100;
    int ok = 1;

    /* Insert in reverse order */
    for (int i = n - 1; i >= 0; i--) {
        make_key_num(key, i);
        make_val_num(val, i);
        int rc = btree_insert(db, key, val, 0);
        if (rc != 0) {
            char msg[64];
            snprintf(msg, sizeof(msg), "insert[%d] failed: rc=%d", i, rc);
            FAIL(msg);
            ok = 0;
            break;
        }
    }

    if (ok) {
        for (int i = 0; i < n; i++) {
            make_key_num(key, i);
            make_val_num(val, i);
            memset(val_out, 0, 256);

            uint64_t mmap_base = get_u64(db, DB_BTREE_MMAP);
            uint32_t root = get_u32(db, DB_BTREE_ROOT);
            int rc = btree_get((void *)mmap_base, root, key, val_out);
            if (rc != 0 || memcmp(val, val_out, 256) != 0) {
                char msg[64];
                snprintf(msg, sizeof(msg), "verify[%d] failed", i);
                FAIL(msg);
                ok = 0;
                break;
            }
        }
        if (ok) PASS();
    }

    btree_adapter_close(db);
    free(db);
    close(dir_fd);
    cleanup_test_dir();
}

int main(void) {
    printf("=== AssemblyDB B+ Tree Tests ===\n\n");
    printf("--- Page Operations ---\n");
    test_page_init_leaf();
    test_page_init_internal();
    test_page_key_access();
    test_page_is_leaf();
    test_page_crc();
    test_internal_children();
    test_leaf_txid();
    test_page_get_ptr();

    printf("\n--- Adapter + CRUD ---\n");
    test_adapter_init_close();
    test_insert_and_get_single();
    test_insert_multiple();
    test_insert_causes_split();
    test_upsert();
    test_get_not_found();
    test_delete_single();
    test_delete_not_found();

    printf("\n--- Scan + Count ---\n");
    test_scan_all();
    test_count();

    printf("\n--- Stress ---\n");
    test_large_insert();
    test_insert_reverse_order();

    printf("\n=== Results: %d/%d passed ===\n", tests_passed, tests_run);
    cleanup_test_dir();
    return (tests_passed == tests_run) ? 0 : 1;
}
