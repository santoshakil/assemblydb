cmake_minimum_required(VERSION 3.10)
project(assemblydb_library VERSION 0.1.0 LANGUAGES ASM)

set(ASSEMBLYDB_SRC_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../../src")

set(ASM_SOURCES
  ${ASSEMBLYDB_SRC_DIR}/core/aes_hw.s
  ${ASSEMBLYDB_SRC_DIR}/core/arena.s
  ${ASSEMBLYDB_SRC_DIR}/core/bloom.s
  ${ASSEMBLYDB_SRC_DIR}/core/crc32.s
  ${ASSEMBLYDB_SRC_DIR}/core/error.s
  ${ASSEMBLYDB_SRC_DIR}/core/key_compare.s
  ${ASSEMBLYDB_SRC_DIR}/core/neon_ops.s
  ${ASSEMBLYDB_SRC_DIR}/core/random.s
  ${ASSEMBLYDB_SRC_DIR}/core/string_ops.s
  ${ASSEMBLYDB_SRC_DIR}/infra/mmap_alloc.s
  ${ASSEMBLYDB_SRC_DIR}/infra/mutex.s
  ${ASSEMBLYDB_SRC_DIR}/infra/rwlock.s
  ${ASSEMBLYDB_SRC_DIR}/infra/syscall.s
  ${ASSEMBLYDB_SRC_DIR}/infra/thread.s
  ${ASSEMBLYDB_SRC_DIR}/init.s
  ${ASSEMBLYDB_SRC_DIR}/ports/cache_port.s
  ${ASSEMBLYDB_SRC_DIR}/ports/compress_port.s
  ${ASSEMBLYDB_SRC_DIR}/ports/crypto_port.s
  ${ASSEMBLYDB_SRC_DIR}/ports/index_port.s
  ${ASSEMBLYDB_SRC_DIR}/ports/storage_port.s
  ${ASSEMBLYDB_SRC_DIR}/ports/tx_port.s
  ${ASSEMBLYDB_SRC_DIR}/ports/wal_port.s
  ${ASSEMBLYDB_SRC_DIR}/domain/api.s
  ${ASSEMBLYDB_SRC_DIR}/domain/backup_manager.s
  ${ASSEMBLYDB_SRC_DIR}/domain/metrics.s
  ${ASSEMBLYDB_SRC_DIR}/domain/mvcc.s
  ${ASSEMBLYDB_SRC_DIR}/domain/router.s
  ${ASSEMBLYDB_SRC_DIR}/domain/tx_manager.s
  ${ASSEMBLYDB_SRC_DIR}/adapters/cache/cache_adapter.s
  ${ASSEMBLYDB_SRC_DIR}/adapters/cache/lru_cache.s
  ${ASSEMBLYDB_SRC_DIR}/adapters/compress/compress_adapter.s
  ${ASSEMBLYDB_SRC_DIR}/adapters/compress/lz4.s
  ${ASSEMBLYDB_SRC_DIR}/adapters/compress/noop_compress.s
  ${ASSEMBLYDB_SRC_DIR}/adapters/crypto/aes256gcm.s
  ${ASSEMBLYDB_SRC_DIR}/adapters/crypto/crypto_adapter.s
  ${ASSEMBLYDB_SRC_DIR}/adapters/crypto/noop_crypto.s
  ${ASSEMBLYDB_SRC_DIR}/adapters/engine/btree/btree_adapter.s
  ${ASSEMBLYDB_SRC_DIR}/adapters/engine/btree/btree_delete.s
  ${ASSEMBLYDB_SRC_DIR}/adapters/engine/btree/btree_insert.s
  ${ASSEMBLYDB_SRC_DIR}/adapters/engine/btree/btree_page.s
  ${ASSEMBLYDB_SRC_DIR}/adapters/engine/btree/btree_scan.s
  ${ASSEMBLYDB_SRC_DIR}/adapters/engine/btree/btree_search.s
  ${ASSEMBLYDB_SRC_DIR}/adapters/engine/lsm/compaction.s
  ${ASSEMBLYDB_SRC_DIR}/adapters/engine/lsm/lsm_adapter.s
  ${ASSEMBLYDB_SRC_DIR}/adapters/engine/lsm/memtable.s
  ${ASSEMBLYDB_SRC_DIR}/adapters/engine/lsm/sstable_read.s
  ${ASSEMBLYDB_SRC_DIR}/adapters/engine/lsm/sstable_write.s
  ${ASSEMBLYDB_SRC_DIR}/adapters/index/btree_index.s
  ${ASSEMBLYDB_SRC_DIR}/adapters/index/index_adapter.s
  ${ASSEMBLYDB_SRC_DIR}/adapters/wal/wal_adapter.s
  ${ASSEMBLYDB_SRC_DIR}/adapters/wal/wal_recover.s
  ${ASSEMBLYDB_SRC_DIR}/adapters/wal/wal_writer.s
)

add_library(assemblydb SHARED ${ASM_SOURCES})

set(CMAKE_ASM_FLAGS "${CMAKE_ASM_FLAGS} -march=armv8-a+crc+crypto -I${ASSEMBLYDB_SRC_DIR}/..")

set_target_properties(assemblydb PROPERTIES
  OUTPUT_NAME "assemblydb"
  PUBLIC_HEADER "${CMAKE_CURRENT_SOURCE_DIR}/../../include/assemblydb.h"
)

target_include_directories(assemblydb PUBLIC
  "${CMAKE_CURRENT_SOURCE_DIR}/../../include"
)

set(ASSEMBLYDB_BUNDLED_LIBRARIES
  "$<TARGET_FILE:assemblydb>"
  PARENT_SCOPE
)
