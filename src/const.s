// AssemblyDB - Constants and Structure Offsets
// AArch64 Linux - All .equ directives for the entire project

.ifndef ASSEMBLYDB_CONST_S
.set ASSEMBLYDB_CONST_S, 1

// ============================================================================
// Linux AArch64 Syscall Numbers
// ============================================================================
.equ SYS_ioctl,         29
.equ SYS_flock,         32
.equ SYS_mkdirat,       34
.equ SYS_unlinkat,      35
.equ SYS_renameat2,     276
.equ SYS_ftruncate,     46
.equ SYS_openat,        56
.equ SYS_close,         57
.equ SYS_lseek,         62
.equ SYS_read,          63
.equ SYS_write,         64
.equ SYS_writev,        66
.equ SYS_pread64,       67
.equ SYS_pwrite64,      68
.equ SYS_sendfile,      71
.equ SYS_fstat,         80
.equ SYS_fdatasync,     83
.equ SYS_fsync,         82
.equ SYS_exit,          93
.equ SYS_exit_group,    94
.equ SYS_futex,         98
.equ SYS_clock_gettime, 113
.equ SYS_getpid,        172
.equ SYS_munmap,        215
.equ SYS_clone,         220
.equ SYS_mmap,          222
.equ SYS_madvise,       233
.equ SYS_getdents64,    61

// ============================================================================
// File Open Flags
// ============================================================================
.equ O_RDONLY,      0x0000
.equ O_WRONLY,      0x0001
.equ O_RDWR,        0x0002
.equ O_CREAT,       0x0040
.equ O_EXCL,        0x0080
.equ O_TRUNC,       0x0200
.equ O_APPEND,      0x0400
.equ O_DSYNC,       0x1000
.equ O_DIRECTORY,   0x4000
.equ O_CLOEXEC,     0x80000
.equ AT_FDCWD,      -100

// File permissions
.equ S_IRWXU,       0700
.equ S_IRUSR,       0400
.equ S_IWUSR,       0200
.equ S_IXUSR,       0100
.equ S_IRGRP,       0040
.equ S_IROTH,       0004
.equ MODE_DIR,      0755
.equ MODE_FILE,     0644

// lseek whence
.equ SEEK_SET,      0
.equ SEEK_CUR,      1
.equ SEEK_END,      2

// ============================================================================
// mmap Constants
// ============================================================================
.equ PROT_NONE,     0x0
.equ PROT_READ,     0x1
.equ PROT_WRITE,    0x2
.equ PROT_EXEC,     0x4
.equ PROT_RW,       0x3     // PROT_READ | PROT_WRITE

.equ MAP_SHARED,    0x01
.equ MAP_PRIVATE,   0x02
.equ MAP_ANONYMOUS, 0x20
.equ MAP_FIXED,     0x10
.equ MAP_ANON_PRIV, 0x22    // MAP_PRIVATE | MAP_ANONYMOUS

// madvise
.equ MADV_NORMAL,     0
.equ MADV_SEQUENTIAL, 2
.equ MADV_WILLNEED,   3
.equ MADV_DONTNEED,   4

// msync (via madvise or manual)
.equ MS_ASYNC,       1
.equ MS_SYNC,        4

// ============================================================================
// flock Constants
// ============================================================================
.equ LOCK_SH,       1
.equ LOCK_EX,       2
.equ LOCK_NB,       4
.equ LOCK_UN,       8

// ============================================================================
// clone Flags (for threads)
// ============================================================================
.equ CLONE_VM,              0x00000100
.equ CLONE_FS,              0x00000200
.equ CLONE_FILES,           0x00000400
.equ CLONE_SIGHAND,         0x00000800
.equ CLONE_THREAD,          0x00010000
.equ CLONE_SYSVSEM,         0x00040000
.equ CLONE_SETTLS,          0x00080000
.equ CLONE_PARENT_SETTID,   0x00100000
.equ CLONE_CHILD_CLEARTID,  0x00200000
.equ THREAD_FLAGS,          0x003D0F00

// ============================================================================
// futex Operations
// ============================================================================
.equ FUTEX_WAIT,            0
.equ FUTEX_WAKE,            1
.equ FUTEX_PRIVATE_FLAG,    128
.equ FUTEX_WAIT_PRIVATE,    128
.equ FUTEX_WAKE_PRIVATE,    129

// ============================================================================
// clock_gettime
// ============================================================================
.equ CLOCK_MONOTONIC,       1
.equ CLOCK_REALTIME,        0

// ============================================================================
// Page and Cache Constants
// ============================================================================
.equ PAGE_SIZE,             4096
.equ PAGE_SHIFT,            12
.equ PAGE_MASK,             0xFFFFFFFFFFFFF000
.equ CACHE_LINE,            64
.equ CACHE_LINE_SHIFT,      6

// ============================================================================
// Database Constants
// ============================================================================
.equ KEY_SIZE,              64      // 2B length + 62B data
.equ KEY_DATA_MAX,          62
.equ VAL_SIZE,              256     // 2B length + 254B data
.equ VAL_DATA_MAX,          254
.equ ENTRY_SIZE,            320     // KEY_SIZE + VAL_SIZE (no pointer in leaf)
.equ KEY_SHIFT,             6       // log2(64) for key offset calc
.equ VAL_SHIFT,             8       // log2(256) for val offset calc

// B+ Tree
.equ BTREE_PAGE_SIZE,       4096
.equ BTREE_HEADER_SIZE,     64
.equ BTREE_INT_MAX_KEYS,    55
.equ BTREE_INT_MAX_CHILDREN,56
.equ BTREE_LEAF_MAX_KEYS,   12
.equ BTREE_KEYS_OFFSET,     0x040   // After 64B header
.equ BTREE_INT_CHILDREN_OFF,0xE00   // 0x040 + 55*64 = 0xE00
.equ BTREE_LEAF_VALS_OFF,   0x340   // 0x040 + 12*64
.equ BTREE_LEAF_TXIDS_OFF,  0xF40   // 0x340 + 12*256 = 0xF40
.equ INVALID_PAGE,          0xFFFFFFFF

// Page types
.equ PAGE_TYPE_FREE,        0x00
.equ PAGE_TYPE_INTERNAL,    0x01
.equ PAGE_TYPE_LEAF,        0x02
.equ PAGE_TYPE_OVERFLOW,    0x03

// Page header offsets
.equ PH_PAGE_ID,            0x000
.equ PH_PAGE_TYPE,          0x004
.equ PH_NUM_KEYS,           0x006
.equ PH_PARENT_PAGE,        0x008
.equ PH_CRC32,              0x00C
.equ PH_LSN,                0x010
.equ PH_NEXT_PAGE,          0x018
.equ PH_PREV_PAGE,          0x020

// ============================================================================
// LSM Constants
// ============================================================================
.equ MEMTABLE_MAX,          0x400000    // 4 MB
.equ SSTABLE_BLOCK_SIZE,    4096
.equ SST_ENTRIES_PER_BLOCK, 12
.equ SST_BLOCK_HEADER,      8           // 2B num_entries + 2B pad + 4B crc32
.equ MAX_SKIP_HEIGHT,       20
.equ SKIP_LIST_P_MASK,      3           // p=1/4: (bits & 3) == 0 means go up

// SSTable magic
.equ SST_MAGIC,             0x4153534D44423031  // "ASSMDB01"
.equ MANIFEST_MAGIC,        0x41534D414E494631  // "ASMANIF1"

// Bloom filter
.equ BLOOM_BITS_PER_KEY,    10
.equ BLOOM_NUM_HASHES,      7

// ============================================================================
// WAL Constants
// ============================================================================
.equ WAL_RECORD_SIZE,       338     // Fixed-size WAL record
.equ WAL_SEGMENT_MAX,       0x400000 // 4 MB per segment
.equ WAL_OP_PUT,            0x01
.equ WAL_OP_DELETE,         0x02
.equ WAL_OP_TX_BEGIN,       0x03
.equ WAL_OP_TX_COMMIT,      0x04
.equ WAL_OP_TX_ROLLBACK,    0x05

// WAL record field offsets
.equ WR_RECORD_LEN,         0x000
.equ WR_CRC32,              0x004
.equ WR_SEQUENCE,           0x008
.equ WR_OP_TYPE,            0x010
.equ WR_KEY_LEN,            0x012
.equ WR_KEY_DATA,           0x014
.equ WR_VAL_LEN,            0x052
.equ WR_VAL_DATA,           0x054

// ============================================================================
// Skip List Node Offsets
// ============================================================================
.equ SLN_KEY_LEN,           0x000
.equ SLN_KEY_DATA,          0x002
.equ SLN_VAL_LEN,           0x040
.equ SLN_VAL_DATA,          0x042
.equ SLN_HEIGHT,            0x140
.equ SLN_IS_DELETED,        0x141
.equ SLN_PAD,               0x142
.equ SLN_FORWARD,           0x148   // forward[0] starts here
.equ SLN_BASE_SIZE,         0x148   // Size without forward pointers

// Skip List Head Offsets
.equ SLH_ENTRY_COUNT,       0x000
.equ SLH_MAX_HEIGHT,        0x008
.equ SLH_FORWARD,           0x010   // 20 * 8 = 160 bytes
.equ SLH_ARENA_PTR,         0x0B0
.equ SLH_DATA_SIZE,         0x0B8
.equ SLH_SIZE,              0x100   // 256 bytes total

// ============================================================================
// Arena Allocator Offsets
// ============================================================================
.equ ARENA_BASE_PTR,        0x000
.equ ARENA_CURRENT_OFF,     0x008
.equ ARENA_CHUNK_SIZE,      0x010
.equ ARENA_TOTAL_ALLOC,     0x018
.equ ARENA_CHUNK_LIST,      0x020
.equ ARENA_SIZE,            0x080   // 128 bytes
.equ ARENA_CHUNK_DEFAULT,   0x100000 // 1 MB chunks

// Arena chunk header
.equ ACHUNK_NEXT,           0x000
.equ ACHUNK_SIZE,           0x008
.equ ACHUNK_HEADER_SIZE,    0x010

// ============================================================================
// Database Handle (db_t) Offsets - 1024 bytes
// ============================================================================
.equ DB_DIR_FD,             0x000
.equ DB_LOCK_FD,            0x008

// Port vtable pointers
.equ DB_STORAGE_PORT,       0x010
.equ DB_INDEX_PORT,         0x018
.equ DB_CACHE_PORT,         0x020
.equ DB_WAL_PORT,           0x028
.equ DB_CRYPTO_PORT,        0x030
.equ DB_COMPRESS_PORT,      0x038
.equ DB_TX_PORT,            0x040

// B+ tree state
.equ DB_BTREE_FD,           0x048
.equ DB_BTREE_MMAP,         0x050
.equ DB_BTREE_MMAP_LEN,     0x058
.equ DB_BTREE_ROOT,         0x060
.equ DB_BTREE_NUM_PAGES,    0x068
.equ DB_BTREE_CAPACITY,     0x070

// LSM state
.equ DB_MEMTABLE_PTR,       0x078
.equ DB_MEMTABLE_SIZE,      0x080
.equ DB_IMM_MEMTABLE,       0x088
.equ DB_ARENA_PTR,          0x090

// WAL state
.equ DB_WAL_FD,             0x098
.equ DB_WAL_SEQ,            0x0A0
.equ DB_WAL_OFFSET,         0x0A8

// SSTable state
.equ DB_SST_COUNT_L0,       0x0B0
.equ DB_SST_COUNT_L1,       0x0B4
.equ DB_SST_LIST_L0,        0x0B8   // 16 * 8 = 128 bytes
.equ DB_SST_LIST_L1,        0x138   // 16 * 8 = 128 bytes

// MVCC state
.equ DB_NEXT_TX_ID,         0x1B8
.equ DB_OLDEST_TX,          0x1C0
.equ DB_VERSION_STORE,      0x1C8
.equ DB_ACTIVE_TX_LIST,     0x1D0

// Concurrency
.equ DB_COMPACT_TID,        0x1D8
.equ DB_COMPACT_TRIGGER,    0x1E0
.equ DB_SHUTDOWN_FLAG,      0x1E4
.equ DB_WRITER_MUTEX,       0x1E8
.equ DB_MEMTABLE_RWLOCK,    0x1EC

// Metrics + config
.equ DB_METRICS_PTR,        0x1F0
.equ DB_CONFIG_PTR,         0x1F8

// Crypto
.equ DB_ENCRYPT_KEY,        0x200
.equ DB_ENCRYPT_ENABLED,    0x220
.equ DB_COMPRESS_ENABLED,   0x224

// Path
.equ DB_PATH_PTR,           0x228
.equ DB_PATH_LEN,           0x230

// Manifest sequence
.equ DB_MANIFEST_SEQ,       0x238

.equ DB_SIZE,               0x400   // 1024 bytes total

// ============================================================================
// Port Vtable Offsets (each port = 64 bytes)
// ============================================================================
// storage_port_t
.equ SP_FN_PUT,             0x00
.equ SP_FN_GET,             0x08
.equ SP_FN_DELETE,          0x10
.equ SP_FN_SCAN,            0x18
.equ SP_FN_FLUSH,           0x20
.equ SP_FN_SYNC,            0x28
.equ SP_FN_CLOSE,           0x30
.equ SP_FN_STATS,           0x38
.equ STORAGE_PORT_SIZE,     0x40

// cache_port_t
.equ CP_FN_FETCH,           0x00
.equ CP_FN_UNPIN,           0x08
.equ CP_FN_MARK_DIRTY,      0x10
.equ CP_FN_EVICT,           0x18
.equ CP_FN_FLUSH_ALL,       0x20
.equ CP_FN_STATS,           0x28
.equ CP_FN_RESIZE,          0x30
.equ CACHE_PORT_SIZE,       0x40

// crypto_port_t
.equ XP_FN_ENCRYPT,         0x00
.equ XP_FN_DECRYPT,         0x08
.equ XP_FN_SET_KEY,         0x10
.equ XP_FN_CLEAR_KEY,       0x18
.equ CRYPTO_PORT_SIZE,      0x40

// compress_port_t
.equ ZP_FN_COMPRESS,        0x00
.equ ZP_FN_DECOMPRESS,      0x08
.equ ZP_FN_MAX_SIZE,        0x10
.equ COMPRESS_PORT_SIZE,    0x40

// wal_port_t
.equ WP_FN_APPEND,          0x00
.equ WP_FN_SYNC,            0x08
.equ WP_FN_ROTATE,          0x10
.equ WP_FN_RECOVER,         0x18
.equ WP_FN_TRUNCATE,        0x20
.equ WAL_PORT_SIZE,         0x40

// tx_port_t
.equ TP_FN_BEGIN,            0x00
.equ TP_FN_COMMIT,           0x08
.equ TP_FN_ROLLBACK,         0x10
.equ TP_FN_GET_SNAPSHOT,     0x18
.equ TX_PORT_SIZE,           0x40

// index_port_t
.equ IP_FN_CREATE,           0x00
.equ IP_FN_DROP,             0x08
.equ IP_FN_INSERT,           0x10
.equ IP_FN_DELETE,           0x18
.equ IP_FN_SCAN,             0x20
.equ INDEX_PORT_SIZE,        0x40

// ============================================================================
// Metrics Offsets (256 bytes, all atomic u64)
// ============================================================================
.equ MET_PUTS,              0x00
.equ MET_GETS,              0x08
.equ MET_DELETES,           0x10
.equ MET_SCANS,             0x18
.equ MET_CACHE_HITS,        0x20
.equ MET_CACHE_MISSES,      0x28
.equ MET_BLOOM_TP,          0x30
.equ MET_BLOOM_FP,          0x38
.equ MET_BYTES_WRITTEN,     0x40
.equ MET_BYTES_READ,        0x48
.equ MET_COMPACTIONS,       0x50
.equ MET_COMPACT_BYTES,     0x58
.equ MET_WAL_SYNCS,         0x60
.equ MET_WAL_BYTES,         0x68
.equ MET_TX_COMMITS,        0x70
.equ MET_TX_ROLLBACKS,      0x78
.equ MET_PAGE_SPLITS,       0x80
.equ MET_PAGE_MERGES,       0x88
.equ MET_MT_FLUSHES,        0x90
.equ MET_SST_COUNT,         0x98
.equ METRICS_SIZE,          0x100
.equ MET_SIZE,              METRICS_SIZE

// ============================================================================
// MVCC Version Entry (384 bytes)
// ============================================================================
.equ MVE_TX_ID,             0x000
.equ MVE_END_TX_ID,         0x008
.equ MVE_TIMESTAMP,         0x010
.equ MVE_KEY,               0x018
.equ MVE_VALUE,             0x058
.equ MVE_IS_DELETED,        0x158
.equ MVE_NEXT_VERSION,      0x160
.equ MVE_PREV_VERSION,      0x168
.equ MVE_SIZE,              0x180

// TX_ID_MAX = active/alive version
.equ TX_ID_MAX,             0xFFFFFFFFFFFFFFFF

// ============================================================================
// Transaction Descriptor (128 bytes)
// ============================================================================
.equ TXD_TX_ID,             0x000
.equ TXD_START_TS,          0x008
.equ TXD_STATE,             0x010
.equ TXD_WRITE_SET,         0x018
.equ TXD_WRITE_COUNT,       0x020
.equ TXD_READ_SET,          0x028
.equ TXD_READ_COUNT,        0x030
.equ TXD_ISOLATION,         0x038
.equ TXD_SIZE,              0x080

// Transaction states
.equ TX_ACTIVE,             0
.equ TX_COMMITTED,          1
.equ TX_ABORTED,            2

// Transaction write-set node (linked list of buffered writes)
.equ TXWN_KEY,              0x000   // 64B fixed key
.equ TXWN_VAL,              0x040   // 256B fixed val
.equ TXWN_IS_DELETE,        0x140   // 1B flag
.equ TXWN_NEXT,             0x148   // 8B ptr to next node
.equ TXWN_SIZE,             0x150   // 336B total

// Isolation levels
.equ ISO_READ_UNCOMMITTED,  0
.equ ISO_READ_COMMITTED,    1
.equ ISO_REPEATABLE_READ,   2
.equ ISO_SNAPSHOT,          3
.equ ISO_SERIALIZABLE,      4

// ============================================================================
// SSTable Descriptor (256 bytes, in-memory)
// ============================================================================
.equ SSTD_FD,               0x000
.equ SSTD_MMAP_BASE,        0x008
.equ SSTD_FILE_SIZE,        0x010
.equ SSTD_NUM_ENTRIES,      0x018
.equ SSTD_INDEX_OFFSET,     0x020
.equ SSTD_NUM_DATA_BLOCKS,  0x028
.equ SSTD_NUM_INDEX_BLOCKS, 0x02C
.equ SSTD_BLOOM_OFFSET,     0x030
.equ SSTD_BLOOM_SIZE,       0x038
.equ SSTD_BLOOM_HASHES,     0x03C
.equ SSTD_MIN_KEY,          0x040
.equ SSTD_MAX_KEY,          0x080
.equ SSTD_BLOOM_PTR,        0x0C0
.equ SSTD_SIZE,             0x100

// SSTable footer (on-disk, 256 bytes)
.equ SSTF_MAGIC,            0x000
.equ SSTF_NUM_DATA_BLK,     0x008
.equ SSTF_NUM_IDX_BLK,      0x00C
.equ SSTF_IDX_START,        0x010
.equ SSTF_BLOOM_START,      0x018
.equ SSTF_BLOOM_SIZE,       0x020
.equ SSTF_BLOOM_HASHES,     0x024
.equ SSTF_NUM_ENTRIES,      0x028
.equ SSTF_CRC32,            0x02C
.equ SSTF_MIN_KEY,          0x030
.equ SSTF_MAX_KEY,          0x070
.equ SSTF_SIZE,             0x100

// ============================================================================
// Config (adb_config_t)
// ============================================================================
.equ CFG_CACHE_PAGES,       0x000
.equ CFG_ENCRYPT_ENABLED,   0x004
.equ CFG_COMPRESS_ENABLED,  0x005
.equ CFG_WAL_SYNC_MODE,     0x006
.equ CFG_MEMTABLE_MAX,      0x008
.equ CFG_SIZE,              0x010

// WAL sync modes
.equ WAL_SYNC_ASYNC,        0
.equ WAL_SYNC_SYNC,         1
.equ WAL_SYNC_FULL,         2

// ============================================================================
// Buffer Pool Constants
// ============================================================================
.equ BUFFER_POOL_PAGES,     256
.equ BUFFER_POOL_HASH_SIZE, 512     // 2x frames for low collision

// Buffer frame (64 bytes = 1 cache line)
.equ BF_PAGE_ID,            0x000
.equ BF_PIN_COUNT,          0x004
.equ BF_IS_DIRTY,           0x008
.equ BF_HASH_NEXT,          0x00C
.equ BF_LRU_PREV,           0x010
.equ BF_LRU_NEXT,           0x018
.equ BF_PAGE_PTR,           0x020
.equ BF_SIZE,               0x040

// ============================================================================
// Error Codes
// ============================================================================
.equ ADB_OK,                0
.equ ADB_ERR_NOT_FOUND,     1
.equ ADB_ERR_IO,            2
.equ ADB_ERR_CORRUPT,       3
.equ ADB_ERR_KEY_TOO_LONG,  4
.equ ADB_ERR_VAL_TOO_LONG,  5
.equ ADB_ERR_LOCKED,        6
.equ ADB_ERR_NOMEM,         7
.equ ADB_ERR_INVALID,       8
.equ ADB_ERR_TX_CONFLICT,   9
.equ ADB_ERR_TX_NOT_FOUND,  10
.equ ADB_ERR_TX_ABORTED,    11
.equ ADB_ERR_FULL,          12
.equ ADB_ERR_EXISTS,        13
.equ ADB_ERR_DECRYPT,       14
.equ ADB_ERR_COMPRESS,      15

// ============================================================================
// MANIFEST File Offsets (512 bytes on disk)
// ============================================================================
.equ MF_MAGIC,              0x000
.equ MF_VERSION,            0x008
.equ MF_SEQUENCE,           0x010
.equ MF_BTREE_ROOT,         0x018
.equ MF_BTREE_PAGES,        0x020
.equ MF_WAL_SEQ,            0x028
.equ MF_NUM_L0,             0x030
.equ MF_NUM_L1,             0x034
.equ MF_L0_SEQS,            0x038   // 16 * 8 = 128
.equ MF_L1_SEQS,            0x0B8   // 16 * 8 = 128
.equ MF_CRC32,              0x138
.equ MF_SIZE,               0x200

// ============================================================================
// dirent64 for directory scanning
// ============================================================================
.equ DIRENT_INO,            0x000
.equ DIRENT_OFF,            0x008
.equ DIRENT_RECLEN,         0x010
.equ DIRENT_TYPE,           0x012
.equ DIRENT_NAME,           0x013

.equ DT_REG,                8       // Regular file

// ============================================================================
// Misc
// ============================================================================
.equ STACK_ALIGN,           16
.equ MAX_PATH_LEN,          256
.equ MAX_SST_PER_LEVEL,     16
.equ THREAD_STACK_SIZE,     65536   // 64 KB per thread

.endif // ASSEMBLYDB_CONST_S
