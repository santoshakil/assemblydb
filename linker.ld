/* AssemblyDB - Linker Script
 * Optimized for minimal binary size and cache-friendly layout
 * Target: AArch64 Linux shared library
 */

PHDRS
{
    text PT_LOAD FLAGS(5);    /* PF_R | PF_X */
    data PT_LOAD FLAGS(6);    /* PF_R | PF_W */
}

SECTIONS
{
    /* Code section - read+execute */
    .text : ALIGN(4096)
    {
        /* Hot path: API entry points first for iTLB locality */
        *api.o(.text .text.*)
        *router.o(.text .text.*)

        /* Core operations - frequently called */
        *key_compare.o(.text .text.*)
        *neon_ops.o(.text .text.*)
        *crc32.o(.text .text.*)

        /* B+ tree engine - hot read path */
        *btree_search.o(.text .text.*)
        *btree_page.o(.text .text.*)
        *btree_insert.o(.text .text.*)
        *btree_scan.o(.text .text.*)
        *btree_delete.o(.text .text.*)
        *btree_adapter.o(.text .text.*)

        /* LSM engine */
        *memtable.o(.text .text.*)
        *lsm_adapter.o(.text .text.*)
        *sstable_read.o(.text .text.*)
        *sstable_write.o(.text .text.*)
        *compaction.o(.text .text.*)

        /* WAL */
        *wal_writer.o(.text .text.*)
        *wal_recover.o(.text .text.*)
        *wal_adapter.o(.text .text.*)

        /* Transaction + MVCC */
        *tx_manager.o(.text .text.*)
        *mvcc.o(.text .text.*)

        /* Cache */
        *lru_cache.o(.text .text.*)
        *cache_adapter.o(.text .text.*)

        /* Crypto */
        *aes_hw.o(.text .text.*)
        *aes256gcm.o(.text .text.*)
        *crypto_adapter.o(.text .text.*)
        *noop_crypto.o(.text .text.*)

        /* Compression */
        *lz4.o(.text .text.*)
        *compress_adapter.o(.text .text.*)
        *noop_compress.o(.text .text.*)

        /* Index */
        *btree_index.o(.text .text.*)
        *index_adapter.o(.text .text.*)

        /* Domain */
        *backup_manager.o(.text .text.*)
        *merge_iter.o(.text .text.*)
        *metrics.o(.text .text.*)

        /* Core utilities */
        *bloom.o(.text .text.*)
        *arena.o(.text .text.*)
        *random.o(.text .text.*)
        *string_ops.o(.text .text.*)
        *error.o(.text .text.*)

        /* Infrastructure */
        *syscall.o(.text .text.*)
        *mmap_alloc.o(.text .text.*)
        *mutex.o(.text .text.*)
        *rwlock.o(.text .text.*)
        *thread.o(.text .text.*)

        /* Init + ports + remaining */
        *init.o(.text .text.*)
        *(.text .text.*)
    } :text

    /* Read-only data in text segment */
    .rodata : ALIGN(64)
    {
        *(.rodata .rodata.*)
    } :text

    /* Data section - read+write, new segment */
    .data : ALIGN(4096)
    {
        *(.data .data.*)
    } :data

    /* BSS - zero-initialized, in data segment */
    .bss : ALIGN(64)
    {
        *(.bss .bss.*)
        *(COMMON)
    } :data

    /* Discard unnecessary sections */
    /DISCARD/ :
    {
        *(.comment)
        *(.note.*)
        *(.ARM.attributes)
    }
}
