#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <time.h>
#include "assemblydb.h"

static uint64_t now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ULL + (uint64_t)ts.tv_nsec;
}

static void fmt_rate(double ops_per_sec, char *buf, size_t len) {
    if (ops_per_sec >= 1e6)
        snprintf(buf, len, "%.2f M ops/sec", ops_per_sec / 1e6);
    else if (ops_per_sec >= 1e3)
        snprintf(buf, len, "%.1f K ops/sec", ops_per_sec / 1e3);
    else
        snprintf(buf, len, "%.0f ops/sec", ops_per_sec);
}

static void fmt_bytes(double bytes_per_sec, char *buf, size_t len) {
    if (bytes_per_sec >= 1e9)
        snprintf(buf, len, "%.2f GB/s", bytes_per_sec / 1e9);
    else if (bytes_per_sec >= 1e6)
        snprintf(buf, len, "%.1f MB/s", bytes_per_sec / 1e6);
    else
        snprintf(buf, len, "%.1f KB/s", bytes_per_sec / 1e3);
}

// ============================================================================
// Core Primitive Benchmarks
// ============================================================================

static void bench_crc32(void) {
    uint8_t *data = (uint8_t *)page_alloc(1);
    memset(data, 0x42, 4096);

    int iters = 1000000;
    uint64_t t0 = now_ns();
    volatile uint32_t crc = 0;
    for (int i = 0; i < iters; i++) {
        crc = hw_crc32c(data, 4096);
    }
    uint64_t elapsed = now_ns() - t0;
    (void)crc;

    double ops = (double)iters / ((double)elapsed / 1e9);
    double throughput = (double)iters * 4096.0 / ((double)elapsed / 1e9);
    char rbuf[64], tbuf[64];
    fmt_rate(ops, rbuf, sizeof(rbuf));
    fmt_bytes(throughput, tbuf, sizeof(tbuf));
    printf("  CRC32C (4KB page)         %s  (%s)\n", rbuf, tbuf);

    page_free(data, 1);
}

static void bench_neon_memcpy(void) {
    uint8_t *src = (uint8_t *)page_alloc(1);
    uint8_t *dst = (uint8_t *)page_alloc(1);
    memset(src, 0xAB, 4096);

    int iters = 2000000;
    uint64_t t0 = now_ns();
    for (int i = 0; i < iters; i++) {
        neon_memcpy(dst, src, 4096);
    }
    uint64_t elapsed = now_ns() - t0;

    double throughput = (double)iters * 4096.0 / ((double)elapsed / 1e9);
    char tbuf[64];
    fmt_bytes(throughput, tbuf, sizeof(tbuf));
    printf("  NEON memcpy (4KB)         %s\n", tbuf);

    page_free(src, 1);
    page_free(dst, 1);
}

static void bench_key_compare(void) {
    uint8_t key_a[64], key_b[64];
    memset(key_a, 0, 64);
    memset(key_b, 0, 64);
    key_a[0] = 10; key_a[1] = 0;
    memcpy(key_a + 2, "benchmark_key_a", 15);
    key_b[0] = 10; key_b[1] = 0;
    memcpy(key_b + 2, "benchmark_key_b", 15);

    int iters = 10000000;
    uint64_t t0 = now_ns();
    volatile int result = 0;
    for (int i = 0; i < iters; i++) {
        result = key_compare(key_a, key_b);
    }
    uint64_t elapsed = now_ns() - t0;
    (void)result;

    double ops = (double)iters / ((double)elapsed / 1e9);
    char rbuf[64];
    fmt_rate(ops, rbuf, sizeof(rbuf));
    printf("  key_compare               %s\n", rbuf);
}

static void bench_bloom_filter(void) {
    void *bloom = bloom_create(10000);

    uint8_t key[64];
    memset(key, 0, 64);

    int iters = 10000;
    uint64_t t0 = now_ns();
    for (int i = 0; i < iters; i++) {
        key[0] = (uint8_t)(8);
        key[1] = 0;
        snprintf((char *)key + 2, 60, "key%05d", i);
        bloom_add(bloom, key);
    }
    uint64_t t1 = now_ns();

    volatile int found = 0;
    for (int i = 0; i < iters; i++) {
        key[0] = (uint8_t)(8);
        key[1] = 0;
        snprintf((char *)key + 2, 60, "key%05d", i);
        found += bloom_check(bloom, key);
    }
    uint64_t t2 = now_ns();
    (void)found;

    double add_ops = (double)iters / ((double)(t1 - t0) / 1e9);
    double chk_ops = (double)iters / ((double)(t2 - t1) / 1e9);
    char abuf[64], cbuf[64];
    fmt_rate(add_ops, abuf, sizeof(abuf));
    fmt_rate(chk_ops, cbuf, sizeof(cbuf));
    printf("  bloom_add (10K keys)      %s\n", abuf);
    printf("  bloom_check (10K keys)    %s\n", cbuf);

    bloom_destroy(bloom);
}

// ============================================================================
// AES Encryption Benchmark
// ============================================================================

static void bench_aes_encrypt(void) {
    void *ctx = crypto_ctx_create();
    uint8_t key[32];
    for (int i = 0; i < 32; i++) key[i] = (uint8_t)(i * 7);
    aes_set_key_impl(ctx, key, 32);

    uint8_t *pt = (uint8_t *)page_alloc(1);
    uint8_t *ct = (uint8_t *)page_alloc(1);
    memset(pt, 0x42, 4096);

    int iters = 100000;
    uint64_t t0 = now_ns();
    for (int i = 0; i < iters; i++) {
        aes_page_encrypt(ctx, pt, ct, (uint64_t)i);
    }
    uint64_t elapsed = now_ns() - t0;

    double ops = (double)iters / ((double)elapsed / 1e9);
    double throughput = (double)iters * 4096.0 / ((double)elapsed / 1e9);
    char rbuf[64], tbuf[64];
    fmt_rate(ops, rbuf, sizeof(rbuf));
    fmt_bytes(throughput, tbuf, sizeof(tbuf));
    printf("  AES-256-CTR encrypt (4KB) %s  (%s)\n", rbuf, tbuf);

    page_free(pt, 1);
    page_free(ct, 1);
    crypto_ctx_destroy(ctx);
}

// ============================================================================
// LZ4 Compression Benchmark
// ============================================================================

static void bench_lz4_compress(void) {
    void *ctx = lz4_ctx_create();
    uint8_t *data = (uint8_t *)page_alloc(1);
    uint8_t *comp = (uint8_t *)page_alloc(2);

    // Compressible data
    for (int i = 0; i < 4096; i++) data[i] = (uint8_t)(i % 64);

    int iters = 100000;
    uint64_t t0 = now_ns();
    volatile int64_t clen = 0;
    for (int i = 0; i < iters; i++) {
        clen = lz4_compress(ctx, data, 4096, comp, 8192);
    }
    uint64_t t1 = now_ns();

    // Decompress benchmark
    int64_t actual_clen = lz4_compress(ctx, data, 4096, comp, 8192);
    uint8_t *decomp = (uint8_t *)page_alloc(1);

    uint64_t t2 = now_ns();
    for (int i = 0; i < iters; i++) {
        lz4_decompress(comp, (size_t)actual_clen, decomp, 4096);
    }
    uint64_t t3 = now_ns();
    (void)clen;

    double comp_ops = (double)iters / ((double)(t1 - t0) / 1e9);
    double decomp_ops = (double)iters / ((double)(t3 - t2) / 1e9);
    double comp_tp = (double)iters * 4096.0 / ((double)(t1 - t0) / 1e9);
    double decomp_tp = (double)iters * 4096.0 / ((double)(t3 - t2) / 1e9);
    char rbuf[64], tbuf[64];
    fmt_rate(comp_ops, rbuf, sizeof(rbuf));
    fmt_bytes(comp_tp, tbuf, sizeof(tbuf));
    printf("  LZ4 compress (4KB)        %s  (%s)  ratio: %.1fx\n",
           rbuf, tbuf, 4096.0 / (double)actual_clen);
    fmt_rate(decomp_ops, rbuf, sizeof(rbuf));
    fmt_bytes(decomp_tp, tbuf, sizeof(tbuf));
    printf("  LZ4 decompress (4KB)      %s  (%s)\n", rbuf, tbuf);

    page_free(data, 1);
    page_free(comp, 2);
    page_free(decomp, 1);
    lz4_ctx_destroy(ctx);
}

// ============================================================================
// LRU Cache Benchmark
// ============================================================================

static void bench_lru_cache(void) {
    void *cache = lru_cache_create(256);

    uint8_t *page = (uint8_t *)page_alloc(1);
    memset(page, 0xAA, 4096);

    // Populate cache
    for (int i = 0; i < 256; i++) {
        lru_cache_insert(cache, (uint32_t)i, page);
        lru_cache_unpin(cache, (uint32_t)i);
    }

    // Benchmark cache hits
    int iters = 1000000;
    uint64_t t0 = now_ns();
    for (int i = 0; i < iters; i++) {
        void *p = lru_cache_fetch(cache, (uint32_t)(i % 256));
        if (p) lru_cache_unpin(cache, (uint32_t)(i % 256));
    }
    uint64_t elapsed = now_ns() - t0;

    double ops = (double)iters / ((double)elapsed / 1e9);
    char rbuf[64];
    fmt_rate(ops, rbuf, sizeof(rbuf));
    printf("  LRU cache fetch (256 pg)  %s\n", rbuf);

    page_free(page, 1);
    lru_cache_destroy(cache);
}

// ============================================================================
// Database API Benchmarks
// ============================================================================

static void bench_api_put(int num_keys) {
    char label[64];
    snprintf(label, sizeof(label), "adb_put (%d keys)", num_keys);

    system("rm -rf /tmp/bench_adb_put");
    adb_t *db = NULL;
    int rc = adb_open("/tmp/bench_adb_put", NULL, &db);
    if (rc != 0 || !db) { printf("  %-28s FAILED (open)\n", label); return; }

    char key[16], val[64];
    uint64_t t0 = now_ns();
    for (int i = 0; i < num_keys; i++) {
        int kl = snprintf(key, sizeof(key), "k%07d", i);
        int vl = snprintf(val, sizeof(val), "value_%07d_padding_data", i);
        adb_put(db, key, (uint16_t)kl, val, (uint16_t)vl);
    }
    uint64_t elapsed = now_ns() - t0;

    double ops = (double)num_keys / ((double)elapsed / 1e9);
    char rbuf[64];
    fmt_rate(ops, rbuf, sizeof(rbuf));
    printf("  %-28s %s\n", label, rbuf);

    adb_close(db);
    system("rm -rf /tmp/bench_adb_put");
}

static void bench_api_get(int num_keys) {
    char label[64];
    snprintf(label, sizeof(label), "adb_get (%d keys)", num_keys);

    system("rm -rf /tmp/bench_adb_get");
    adb_t *db = NULL;
    int rc = adb_open("/tmp/bench_adb_get", NULL, &db);
    if (rc != 0 || !db) { printf("  %-28s FAILED (open)\n", label); return; }

    char key[16], val[64];
    // Insert first
    for (int i = 0; i < num_keys; i++) {
        int kl = snprintf(key, sizeof(key), "k%07d", i);
        int vl = snprintf(val, sizeof(val), "value_%07d_padding_data", i);
        adb_put(db, key, (uint16_t)kl, val, (uint16_t)vl);
    }

    // Benchmark reads
    char vbuf[256];
    uint16_t vlen;
    uint64_t t0 = now_ns();
    for (int i = 0; i < num_keys; i++) {
        int kl = snprintf(key, sizeof(key), "k%07d", i);
        adb_get(db, key, (uint16_t)kl, vbuf, 256, &vlen);
    }
    uint64_t elapsed = now_ns() - t0;

    double ops = (double)num_keys / ((double)elapsed / 1e9);
    char rbuf[64];
    fmt_rate(ops, rbuf, sizeof(rbuf));
    printf("  %-28s %s\n", label, rbuf);

    adb_close(db);
    system("rm -rf /tmp/bench_adb_get");
}

static void bench_api_mixed(int num_keys) {
    char label[64];
    snprintf(label, sizeof(label), "mixed r/w (%d ops)", num_keys * 2);

    system("rm -rf /tmp/bench_adb_mix");
    adb_t *db = NULL;
    int rc = adb_open("/tmp/bench_adb_mix", NULL, &db);
    if (rc != 0 || !db) { printf("  %-28s FAILED (open)\n", label); return; }

    char key[16], val[64], vbuf[256];
    uint16_t vlen;

    uint64_t t0 = now_ns();
    for (int i = 0; i < num_keys; i++) {
        int kl = snprintf(key, sizeof(key), "k%07d", i);
        int vl = snprintf(val, sizeof(val), "value_%07d_padding_data", i);
        adb_put(db, key, (uint16_t)kl, val, (uint16_t)vl);

        // Read a previous key
        if (i > 0) {
            int rkl = snprintf(key, sizeof(key), "k%07d", i / 2);
            adb_get(db, key, (uint16_t)rkl, vbuf, 256, &vlen);
        }
    }
    uint64_t elapsed = now_ns() - t0;

    double ops = (double)(num_keys * 2 - 1) / ((double)elapsed / 1e9);
    char rbuf[64];
    fmt_rate(ops, rbuf, sizeof(rbuf));
    printf("  %-28s %s\n", label, rbuf);

    adb_close(db);
    system("rm -rf /tmp/bench_adb_mix");
}

static void bench_api_batch(void) {
    system("rm -rf /tmp/bench_adb_batch");
    adb_t *db = NULL;
    int rc = adb_open("/tmp/bench_adb_batch", NULL, &db);
    if (rc != 0 || !db) { printf("  %-28s FAILED (open)\n", "adb_batch_put (1000)"); return; }

    int count = 1000;
    adb_batch_entry_t *entries = (adb_batch_entry_t *)malloc(count * sizeof(adb_batch_entry_t));
    char (*keys)[16] = malloc(count * 16);
    char (*vals)[64] = malloc(count * 64);

    for (int i = 0; i < count; i++) {
        int kl = snprintf(keys[i], 16, "bk%06d", i);
        int vl = snprintf(vals[i], 64, "bval_%06d_data", i);
        entries[i].key = keys[i];
        entries[i].key_len = (uint16_t)kl;
        entries[i].val = vals[i];
        entries[i].val_len = (uint16_t)vl;
    }

    uint64_t t0 = now_ns();
    adb_batch_put(db, entries, (uint32_t)count);
    uint64_t elapsed = now_ns() - t0;

    double ops = (double)count / ((double)elapsed / 1e9);
    char rbuf[64];
    fmt_rate(ops, rbuf, sizeof(rbuf));
    printf("  %-28s %s\n", "adb_batch_put (1000)", rbuf);

    free(entries);
    free(keys);
    free(vals);
    adb_close(db);
    system("rm -rf /tmp/bench_adb_batch");
}

static void bench_api_transactions(void) {
    system("rm -rf /tmp/bench_adb_tx");
    adb_t *db = NULL;
    int rc = adb_open("/tmp/bench_adb_tx", NULL, &db);
    if (rc != 0 || !db) { printf("  %-28s FAILED (open)\n", "tx begin+commit (1000)"); return; }

    int count = 1000;
    uint64_t t0 = now_ns();
    for (int i = 0; i < count; i++) {
        uint64_t tx_id;
        adb_tx_begin(db, ADB_ISO_SNAPSHOT, &tx_id);
        adb_tx_commit(db, tx_id);
    }
    uint64_t elapsed = now_ns() - t0;

    double ops = (double)count / ((double)elapsed / 1e9);
    char rbuf[64];
    fmt_rate(ops, rbuf, sizeof(rbuf));
    printf("  %-28s %s\n", "tx begin+commit (1000)", rbuf);

    adb_close(db);
    system("rm -rf /tmp/bench_adb_tx");
}

static void bench_api_delete(void) {
    system("rm -rf /tmp/bench_adb_del");
    adb_t *db = NULL;
    int rc = adb_open("/tmp/bench_adb_del", NULL, &db);
    if (rc != 0 || !db) { printf("  %-28s FAILED (open)\n", "adb_delete (5000)"); return; }

    char key[16], val[32];
    int count = 5000;

    // Insert first
    for (int i = 0; i < count; i++) {
        int kl = snprintf(key, sizeof(key), "dk%06d", i);
        int vl = snprintf(val, sizeof(val), "dv%06d", i);
        adb_put(db, key, (uint16_t)kl, val, (uint16_t)vl);
    }

    // Benchmark deletes
    uint64_t t0 = now_ns();
    for (int i = 0; i < count; i++) {
        int kl = snprintf(key, sizeof(key), "dk%06d", i);
        adb_delete(db, key, (uint16_t)kl);
    }
    uint64_t elapsed = now_ns() - t0;

    double ops = (double)count / ((double)elapsed / 1e9);
    char rbuf[64];
    fmt_rate(ops, rbuf, sizeof(rbuf));
    printf("  %-28s %s\n", "adb_delete (5000)", rbuf);

    adb_close(db);
    system("rm -rf /tmp/bench_adb_del");
}

// ============================================================================
// Memtable Benchmark (raw skip list)
// ============================================================================

static void bench_memtable(void) {
    void *arena = arena_create();
    void *mt = memtable_create2(arena);

    uint8_t key[64], val[256];
    memset(key, 0, 64);
    memset(val, 0, 256);

    int count = 10000;

    // Put benchmark
    uint64_t t0 = now_ns();
    for (int i = 0; i < count; i++) {
        uint16_t kl = (uint16_t)snprintf((char *)key + 2, 60, "mt%06d", i);
        key[0] = (uint8_t)(kl & 0xFF);
        key[1] = (uint8_t)(kl >> 8);
        uint16_t vl = (uint16_t)snprintf((char *)val + 2, 252, "mtval_%06d", i);
        val[0] = (uint8_t)(vl & 0xFF);
        val[1] = (uint8_t)(vl >> 8);
        memtable_put(mt, key, val, 0);
    }
    uint64_t t1 = now_ns();

    // Get benchmark
    uint8_t vbuf[256];
    uint64_t t2 = now_ns();
    for (int i = 0; i < count; i++) {
        uint16_t kl = (uint16_t)snprintf((char *)key + 2, 60, "mt%06d", i);
        key[0] = (uint8_t)(kl & 0xFF);
        key[1] = (uint8_t)(kl >> 8);
        memtable_get(mt, key, vbuf);
    }
    uint64_t t3 = now_ns();

    double put_ops = (double)count / ((double)(t1 - t0) / 1e9);
    double get_ops = (double)count / ((double)(t3 - t2) / 1e9);
    char pbuf[64], gbuf[64];
    fmt_rate(put_ops, pbuf, sizeof(pbuf));
    fmt_rate(get_ops, gbuf, sizeof(gbuf));
    printf("  memtable_put (10K)        %s\n", pbuf);
    printf("  memtable_get (10K)        %s\n", gbuf);

    arena_destroy(arena);
}

// ============================================================================
// Main
// ============================================================================

int main(void) {
    printf("=== AssemblyDB Benchmarks ===\n");
    printf("Platform: AArch64 Linux (Raspberry Pi)\n\n");

    printf("--- Core Primitives ---\n");
    bench_crc32();
    bench_neon_memcpy();
    bench_key_compare();
    bench_bloom_filter();

    printf("\n--- Encryption ---\n");
    bench_aes_encrypt();

    printf("\n--- Compression ---\n");
    bench_lz4_compress();

    printf("\n--- Cache ---\n");
    bench_lru_cache();

    printf("\n--- Skip List (Memtable) ---\n");
    bench_memtable();

    printf("\n--- Database API ---\n");
    bench_api_put(1000);
    bench_api_put(10000);
    bench_api_get(1000);
    bench_api_get(10000);
    bench_api_mixed(5000);
    bench_api_batch();
    bench_api_transactions();
    bench_api_delete();

    printf("\n=== Benchmark Complete ===\n");
    return 0;
}
