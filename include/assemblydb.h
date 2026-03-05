#ifndef ASSEMBLYDB_H
#define ASSEMBLYDB_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct adb_t adb_t;

/* Error codes */
#define ADB_OK              0
#define ADB_ERR_NOT_FOUND   1
#define ADB_ERR_IO          2
#define ADB_ERR_CORRUPT     3
#define ADB_ERR_KEY_TOO_LONG 4
#define ADB_ERR_VAL_TOO_LONG 5
#define ADB_ERR_LOCKED      6
#define ADB_ERR_NOMEM       7
#define ADB_ERR_INVALID     8
#define ADB_ERR_TX_CONFLICT 9
#define ADB_ERR_TX_NOT_FOUND 10
#define ADB_ERR_TX_ABORTED  11
#define ADB_ERR_FULL        12
#define ADB_ERR_EXISTS      13
#define ADB_ERR_DECRYPT     14
#define ADB_ERR_COMPRESS    15

/* WAL sync modes */
#define ADB_WAL_ASYNC       0
#define ADB_WAL_SYNC        1
#define ADB_WAL_FULL        2

/* Isolation levels */
#define ADB_ISO_READ_UNCOMMITTED 0
#define ADB_ISO_READ_COMMITTED   1
#define ADB_ISO_REPEATABLE_READ  2
#define ADB_ISO_SNAPSHOT         3
#define ADB_ISO_SERIALIZABLE     4

/* Backup types */
#define ADB_BACKUP_FULL     0
#define ADB_BACKUP_INCREMENTAL 1

/* Configuration */
typedef struct {
    uint32_t cache_size_pages;
    uint8_t  encryption_enabled;
    uint8_t  compression_enabled;
    uint8_t  wal_sync_mode;
    uint8_t  _pad;
    uint32_t memtable_max_bytes;
} adb_config_t;

/* Metrics */
typedef struct {
    uint64_t puts_total;
    uint64_t gets_total;
    uint64_t deletes_total;
    uint64_t scans_total;
    uint64_t cache_hits;
    uint64_t cache_misses;
    uint64_t bloom_true_positives;
    uint64_t bloom_false_positives;
    uint64_t bytes_written;
    uint64_t bytes_read;
    uint64_t compactions_run;
    uint64_t compaction_bytes;
    uint64_t wal_syncs;
    uint64_t wal_bytes;
    uint64_t tx_commits;
    uint64_t tx_rollbacks;
    uint64_t page_splits;
    uint64_t page_merges;
    uint64_t memtable_flushes;
    uint64_t sstable_count;
    uint64_t _reserved[12];         /* pad to 256 bytes (MET_SIZE) */
} adb_metrics_t;

/* Batch entry */
typedef struct {
    const void *key;
    uint16_t    key_len;
    const void *val;
    uint16_t    val_len;
} adb_batch_entry_t;

/* Scan callback: return 0 to continue, non-zero to stop */
typedef int (*adb_scan_fn)(const void *key, uint16_t key_len,
                           const void *val, uint16_t val_len,
                           void *user_data);

/* Index key extractor: extract secondary key from primary value */
typedef int (*adb_index_fn)(const void *val, uint16_t val_len,
                            void *index_key_buf, uint16_t *index_key_len);

/* ============================================================
 * Lifecycle
 * ============================================================ */
int adb_open(const char *path, const adb_config_t *config, adb_t **db_out);
int adb_close(adb_t *db);
int adb_sync(adb_t *db);
int adb_destroy(const char *path);

/* ============================================================
 * Key-Value Operations (implicit auto-commit transaction)
 * ============================================================ */
int adb_put(adb_t *db,
            const void *key, uint16_t key_len,
            const void *val, uint16_t val_len);

int adb_get(adb_t *db,
            const void *key, uint16_t key_len,
            void *val_buf, uint16_t val_buf_len,
            uint16_t *val_len_out);

int adb_delete(adb_t *db,
               const void *key, uint16_t key_len);

/* ============================================================
 * Range Scan
 * ============================================================ */
int adb_scan(adb_t *db,
             const void *start_key, uint16_t start_key_len,
             const void *end_key, uint16_t end_key_len,
             adb_scan_fn callback, void *user_data);

/* ============================================================
 * Batch Operations
 * ============================================================ */
int adb_batch_put(adb_t *db,
                  const adb_batch_entry_t *entries,
                  uint32_t count);

/* ============================================================
 * Explicit Transactions
 * ============================================================ */
int adb_tx_begin(adb_t *db, uint8_t isolation, uint64_t *tx_id_out);

int adb_tx_put(adb_t *db, uint64_t tx_id,
               const void *key, uint16_t key_len,
               const void *val, uint16_t val_len);

int adb_tx_get(adb_t *db, uint64_t tx_id,
               const void *key, uint16_t key_len,
               void *val_buf, uint16_t val_buf_len,
               uint16_t *val_len_out);

int adb_tx_delete(adb_t *db, uint64_t tx_id,
                  const void *key, uint16_t key_len);

int adb_tx_commit(adb_t *db, uint64_t tx_id);
int adb_tx_rollback(adb_t *db, uint64_t tx_id);

int adb_tx_scan(adb_t *db, uint64_t tx_id,
                const void *start_key, uint16_t start_key_len,
                const void *end_key, uint16_t end_key_len,
                adb_scan_fn callback, void *user_data);

/* ============================================================
 * Secondary Indexes
 * ============================================================ */
int adb_create_index(adb_t *db, const char *name, adb_index_fn extract_key);
int adb_drop_index(adb_t *db, const char *name);
int adb_index_scan(adb_t *db, const char *index_name,
                   const void *key, uint16_t key_len,
                   adb_scan_fn callback, void *user_data);

/* ============================================================
 * Backup & Recovery
 * ============================================================ */
int adb_backup(adb_t *db, const char *dest_path, uint8_t backup_type);
int adb_restore(const char *backup_path, const char *dest_path);

/* ============================================================
 * Metrics & Monitoring
 * ============================================================ */
int adb_get_metrics(adb_t *db, adb_metrics_t *metrics_out);

/* ============================================================
 * Low-Level Core Functions (exposed for testing)
 * ============================================================ */

/* CRC32C hardware accelerated */
uint32_t hw_crc32c(const void *data, size_t len);
uint32_t hw_crc32c_u64(uint32_t crc, uint64_t value);

/* NEON memory operations */
void *neon_memcpy(void *dst, const void *src, size_t len);
void *neon_memset(void *dst, int val, size_t len);
void *neon_memzero(void *dst, size_t len);
int   neon_memcmp(const void *a, const void *b, size_t len);
void  neon_copy_64(void *dst, const void *src);
void  neon_copy_256(void *dst, const void *src);
void  neon_zero_64(void *dst);
void  neon_zero_256(void *dst);
void  neon_copy_page(void *dst, const void *src);

/* Key operations */
int  key_compare(const void *key_a, const void *key_b);
int  key_equal(const void *key_a, const void *key_b);
void build_fixed_key(void *dst, const void *src, uint16_t src_len);
void build_fixed_val(void *dst, const void *src, uint16_t src_len);

/* Bloom filter */
void *bloom_create(size_t expected_keys);
void  bloom_destroy(void *bloom);
void  bloom_add(void *bloom, const void *key);
int   bloom_check(void *bloom, const void *key);

/* Arena allocator */
void *arena_create(void);
void *arena_alloc(void *arena, size_t size);
void  arena_destroy(void *arena);
void  arena_reset(void *arena);

/* Random */
void    prng_seed(uint64_t seed);
uint64_t prng_next(void);
int     random_level(void);

/* String operations */
size_t asm_strlen(const char *s);
char  *asm_strcpy(char *dst, const char *src);
size_t u64_to_dec(uint64_t value, char *buf);
size_t u64_to_padded_dec(uint64_t value, char *buf, size_t width);
size_t build_wal_name(char *buf, uint64_t seq);
size_t build_sst_name(char *buf, int level, uint64_t seq);

/* Error helpers */
int is_error(int64_t code);
int is_syscall_error(int64_t retval);
int syscall_to_adb_error(int64_t retval);

/* Memory allocation */
void *page_alloc(size_t num_pages);
void  page_free(void *ptr, size_t num_pages);
void *alloc_zeroed(size_t size);
void  free_mem(void *ptr, size_t size);

/* Memtable (skip list) */
void    *memtable_create2(void *arena);
int      memtable_put(void *head, void *key, void *val, int is_delete);
int      memtable_get(void *head, void *key, void *val_buf);
int      memtable_delete2(void *head, void *key);
void    *memtable_iter_first(void *head);
void    *memtable_iter_next(void *node);
uint64_t memtable_entry_count(void *head);
uint64_t memtable_data_size(void *head);

/* LSM adapter */
int  lsm_adapter_init(void *db);
int  lsm_put(void *db, void *key, void *val, uint64_t tx_id);
int  lsm_get(void *db, void *key, void *val_buf, uint64_t tx_id);
int  lsm_delete(void *db, void *key, uint64_t tx_id);
int  lsm_adapter_close(void *db);

/* WAL */
int  wal_open(void *db);
int  wal_append(void *db, int op, void *key, void *val, uint64_t tx_id);
int  wal_sync(void *db);
int  wal_close(void *db);
int  wal_recover(void *db, void *callback, void *ctx);

/* SSTable */
int  sstable_flush(void *db, void *memtable_head, int level, int seq);
int  sstable_open(void *db, int level, int seq, void *desc);
int  sstable_get(void *desc, void *key, void *val_buf);
int  sstable_close(void *desc);

/* AES-256 encryption */
void  aes_key_expand_256(const void *key32, void *expanded240);
void  aes_encrypt_block(const void *expanded_key, const void *input16, void *output16);
void  aes_ctr_process(const void *expanded_key, const void *input,
                      void *output, size_t size, uint64_t page_id);

/* Crypto context */
void *crypto_ctx_create(void);
void  crypto_ctx_destroy(void *ctx);
int   aes_page_encrypt(void *ctx, const void *plaintext, void *ciphertext, uint64_t page_id);
int   aes_page_decrypt(void *ctx, const void *ciphertext, void *plaintext, uint64_t page_id);
int   aes_set_key_impl(void *ctx, const void *key, size_t key_len);
void  aes_clear_key_impl(void *ctx);

/* No-op crypto */
int   noop_encrypt_page(void *self, const void *pt, void *ct, uint64_t page_id);
int   noop_decrypt_page(void *self, const void *ct, void *pt, uint64_t page_id);

/* LZ4 compression */
void *lz4_ctx_create(void);
void  lz4_ctx_destroy(void *ctx);
int64_t lz4_compress(void *ctx, const void *input, size_t in_len,
                     void *output, size_t out_cap);
int64_t lz4_decompress(const void *input, size_t in_len,
                       void *output, size_t out_cap);
size_t  lz4_max_compressed_size(size_t in_len);

/* No-op compression */
int64_t noop_compress(void *self, const void *input, size_t in_len,
                      void *output, size_t out_cap);
int64_t noop_decompress(void *self, const void *input, size_t in_len,
                        void *output, size_t out_cap);

/* LRU Cache */
void   *lru_cache_create(size_t capacity);
void    lru_cache_destroy(void *cache);
void   *lru_cache_fetch(void *cache, uint32_t page_id);
void   *lru_cache_insert(void *cache, uint32_t page_id, const void *page_data);
void    lru_cache_unpin(void *cache, uint32_t page_id);
void    lru_cache_mark_dirty(void *cache, uint32_t page_id);
void    lru_cache_stats(void *cache, uint64_t *hits, uint64_t *misses);

/* Secondary Index */
void   *sec_index_create(int dir_fd, const char *name, size_t name_len);
void    sec_index_destroy(void *index);
int     sec_index_insert(void *index, const void *sec_key, const void *pri_key);
int     sec_index_scan(void *index, const void *sec_key,
                       adb_scan_fn callback, void *ctx);
size_t  build_index_filename(char *buf, const char *name, size_t name_len);

/* Backup */
int     backup_full(void *db, const char *dest_path);
int     backup_write_manifest(void *db, int dest_dir_fd);

#ifdef __cplusplus
}
#endif

#endif /* ASSEMBLYDB_H */
