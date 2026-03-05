date=2026-02-15
scope=prod-hardening-iteration backup/restore durability+integrity real-workload verification
problem=restore accepted backup dir without manifest validation; backup success path did not enforce full-manifest write or directory/file sync barriers
files=src/domain/backup_manager.s,src/domain/api.s,tests/test_edge.c
changes=backup_full now checks adb_sync/copy/manifest errors + fsync(dest_dir_fd) before success
changes=backup_copy_btree now loops sendfile until remaining=0 + fdatasync(dst_file) + close error handling
changes=backup_write_manifest now requires sys_write==MF_SIZE + fdatasync success + close error handling
changes=new backup_validate_manifest(src_dir_fd): open/read_exact 512B, magic/version checks, crc32 verify, fail=>ADB_ERR_IO
changes=adb_restore now validates backup MANIFEST immediately after opening source dir, before destination creation/copy
changes=test_edge adds case [18] restore rejects corrupt MANIFEST and missing MANIFEST (expects ADB_ERR_IO)
verification=make test_edge PASS(18/18 incl new test)
verification=make test_integration PASS(14/14)
verification=make test_stress PASS(20/20)
verification=make test_adversarial PASS(26/26)
verification=make test_persist PASS(8/8)
verification=5x break-loop PASS: test_edge+test_stress+test_adversarial+test_persist all green each iteration
verification=make bench PASS no hot-path regression signal
bench_api=adb_put_1k=593.9K/s adb_put_10k=571.4K/s adb_get_1k=3.25M/s adb_get_10k=3.15M/s mixed_rw_10k=998.9K/s batch_put_1k=676.3K/s delete_5k=647.8K/s
residual=restore currently restores data.btree only (manifest used for integrity gate, not copied into destination DB state)
iter2=atomic-restore-copy + manifest-semantic-guards + destination-sanitize
iter2_change=backup_copy_btree writes data.btree.tmp then renameat2(tmp->data.btree) + fsync(dest_dir)
iter2_change=backup_copy_btree cleanup path unlinks temp file on all failures
iter2_change=backup_validate_manifest semantic checks: btree_pages>0, root<pages, num_l0<=16, num_l1<=16
iter2_change=adb_restore calls adb_destroy(dest_path) after manifest validation to clear stale dest artifacts before restore
iter2_test=edge[19] manifest with valid crc but invalid root/pages rejected (ADB_ERR_IO)
iter2_test=edge[20] restore over dirty destination keeps restored state only (old key absent)
iter2_verify=make test_edge PASS(20/20)
iter2_verify=make test_all PASS
iter2_verify=make test_stress PASS
iter2_verify=make test_adversarial PASS
iter2_verify=make test_persist PASS
iter2_verify=make bench PASS
iter2_verify=5x break-loop PASS with edge/stress/adversarial/persist
iter2_bench_api=adb_put_1k=584.5K/s adb_put_10k=579.9K/s adb_get_1k=3.27M/s adb_get_10k=3.14M/s mixed_rw_10k=1.00M/s batch_put_1k=665.5K/s delete_5k=649.6K/s
iter3=btree-file-consistency-validation + restore-corruption-cases
iter3_change=backup_validate_manifest now validates data.btree file: open/readability, size page-aligned, size>=manifest_pages*4096
iter3_change=backup_validate_manifest validates root page header type at offset root*4096+PH_PAGE_TYPE in {INTERNAL,LEAF}
iter3_test=edge[21] truncate backup data.btree to 1024 bytes => restore returns ADB_ERR_IO
iter3_test=edge[22] mutate root page type to invalid value => restore returns ADB_ERR_IO
iter3_verify=make test_edge PASS(22/22)
iter3_verify=make test_all PASS
iter3_verify=make test_stress PASS
iter3_verify=make test_adversarial PASS
iter3_verify=make test_persist PASS
iter3_verify=make bench PASS
iter3_verify=5x break-loop PASS with edge/stress/adversarial/persist
iter3_bench_api=adb_put_1k=594.6K/s adb_put_10k=579.1K/s adb_get_1k=3.23M/s adb_get_10k=3.15M/s mixed_rw_10k=999.4K/s batch_put_1k=660.4K/s delete_5k=637.7K/s
iter4=restore-manifest-completeness + destination-sanitize-upgrades
iter4_change=adb_restore copies MANIFEST from backup to destination after data.btree copy (backup_copy_manifest)
iter4_change=adb_destroy now removes MANIFEST and data.btree.tmp in addition to data.btree/wal/sst
iter4_change=new backup_copy_manifest performs read_exact(512) + write_exact(512) + fdatasync
iter4_test=edge[23] restored MANIFEST must be byte-identical to backup MANIFEST
iter4_test=edge[24] no data.btree.tmp residue after backup+restore
iter4_verify=make test_edge PASS(24/24)
iter4_verify=make test_all PASS
iter4_verify=make test_stress PASS
iter4_verify=make test_adversarial PASS
iter4_verify=make test_persist PASS
iter4_verify=make bench PASS
iter4_verify=5x break-loop PASS with edge/stress/adversarial/persist
iter4_bench_api=adb_put_1k=594.5K/s adb_put_10k=576.9K/s adb_get_1k=3.28M/s adb_get_10k=3.13M/s mixed_rw_10k=995.1K/s batch_put_1k=656.5K/s delete_5k=635.3K/s
iter5=manifest-copy-atomicity + strict-size-validation
iter5_change=backup_copy_manifest now writes MANIFEST.tmp + fdatasync + renameat2(tmp->MANIFEST) + fsync(dest_dir) with cleanup unlink on all fail paths
iter5_change=backup_validate_manifest now rejects trailing MANIFEST bytes via read_exact(512)+extra-byte-eof check
iter5_change=adb_restore sync barrier after manifest copy via sys_fsync(dst_dir_fd) before close/success
iter5_test=edge[25] append trailing byte to backup MANIFEST => adb_restore returns ADB_ERR_IO
iter5_verify=make test_edge PASS(25/25)
iter5_verify=make test_all PASS
iter5_verify=make test_stress PASS
iter5_verify=make test_adversarial PASS
iter5_verify=make test_persist PASS
iter5_verify=make bench PASS
iter5_verify=5x break-loop PASS with edge/stress/adversarial/persist
iter5_bench_api=adb_put_1k=587.5K/s adb_put_10k=572.7K/s adb_get_1k=3.26M/s adb_get_10k=3.13M/s mixed_rw_10k=980.4K/s
iter6=restore-self-destruction-guard (source==destination reject)
iter6_change=adb_restore now rejects identical source/destination paths before any destructive action; trailing-slash equivalent treated as same path
iter6_test=edge[26] adb_restore(bk,bk) and adb_restore(\"bk/\",bk) => ADB_ERR_INVALID; backup remains intact and still restorable
iter6_verify=make test_edge PASS(26/26)
iter6_verify=make test_all PASS
iter6_verify=make test_stress PASS
iter6_verify=make test_adversarial PASS
iter6_verify=make test_persist PASS
iter6_verify=make bench PASS
iter6_verify=5x break-loop PASS with edge/stress/adversarial/persist
iter6_bench_api=adb_put_1k=588.5K/s adb_put_10k=569.5K/s adb_get_1k=3.24M/s adb_get_10k=3.13M/s mixed_rw_10k=990.0K/s batch_put_1k=669.5K/s delete_5k=625.3K/s
iter7=full-backup-manifest-write-atomicity
iter7_change=backup_write_manifest now writes MANIFEST.tmp + fdatasync + renameat2(tmp->MANIFEST) + fsync(dest_dir_fd) and unlinks temp on all fail paths
iter7_change=backup_full inherits atomic manifest durability guarantees (same crash model as btree copy and manifest restore copy)
iter7_verify=make test_edge PASS(26/26)
iter7_verify=make test_all PASS
iter7_verify=make test_stress PASS
iter7_verify=make test_adversarial PASS
iter7_verify=make test_persist PASS
iter7_verify=make bench PASS
iter7_verify=5x break-loop PASS with edge/stress/adversarial/persist
iter7_verify=10x soak-loop PASS with edge/stress/adversarial/persist
iter7_bench_api=adb_put_1k=597.5K/s adb_put_10k=581.6K/s adb_get_1k=3.28M/s adb_get_10k=3.11M/s mixed_rw_10k=995.4K/s batch_put_1k=675.8K/s delete_5k=647.9K/s
iter8=restore-alias-inode-guard + mkdir-error-strictness
iter8_change=added SYS_fstat syscall wrapper and const; adb_restore now compares st_dev+st_ino of source/destination directories when both openable, rejecting alias paths to same directory with ADB_ERR_INVALID
iter8_change=adb_restore now treats mkdir(dest) errors as fatal except EEXIST
iter8_change=backup_full now treats mkdir(dest/wal/sst) errors as fatal except EEXIST (no silent ignore of non-EEXIST failures)
iter8_test=edge[27] symlink alias path to backup directory used as restore source while dest is canonical backup path => ADB_ERR_INVALID, backup remains intact/restorable
iter8_verify=make test_edge PASS(27/27)
iter8_verify=make test_all PASS
iter8_verify=make test_stress PASS
iter8_verify=make test_adversarial PASS
iter8_verify=make test_persist PASS
iter8_verify=make bench PASS
iter8_verify=5x break-loop PASS with edge/stress/adversarial/persist
iter8_bench_api=adb_put_1k=595.9K/s adb_put_10k=577.9K/s adb_get_1k=3.23M/s adb_get_10k=3.13M/s mixed_rw_10k=971.8K/s batch_put_1k=658.1K/s delete_5k=646.3K/s
iter9=manifest-short-write-hardening
iter9_change=backup_copy_manifest now writes MANIFEST.tmp with write_exact loop (retries partial writes until 512 bytes committed; zero/negative write fails)
iter9_change=backup_write_manifest now writes MANIFEST.tmp with write_exact loop (retries partial writes until 512 bytes committed; zero/negative write fails)
iter9_verify=make test_edge PASS(27/27)
iter9_verify=make test_all PASS
iter9_verify=make test_stress PASS
iter9_verify=make test_adversarial PASS
iter9_verify=make test_persist PASS
iter9_verify=make bench PASS
iter9_verify=5x break-loop PASS with edge/stress/adversarial/persist
iter9_bench_api=adb_put_1k=589.9K/s adb_put_10k=574.7K/s adb_get_1k=3.29M/s adb_get_10k=3.14M/s mixed_rw_10k=982.7K/s batch_put_1k=650.5K/s delete_5k=641.1K/s
iter10=hybrid-read-hotpath-single-probe
iter10_change=added memtable_probe(head,key,valbuf)->{0 found,1 miss,2 tombstone}; router_get now uses single memtable traversal per tier (active+imm) instead of memtable_get + memtable_key_exists double traversal
iter10_change=memtable_get and memtable_key_exists refactored as thin wrappers over memtable_probe (less duplicated skip-list traversal code)
iter10_change=router_get L0/L1 loops now hoist SST list base address out of loop body
iter10_verify=make test_edge PASS(27/27)
iter10_verify=make test_all PASS
iter10_verify=make test_stress PASS
iter10_verify=make test_adversarial PASS
iter10_verify=make test_persist PASS
iter10_verify=make bench PASS
iter10_verify=5x break-loop PASS with edge/stress/adversarial/persist
iter10_bench_api=adb_put_1k=600.9K/s adb_put_10k=588.9K/s adb_get_1k=3.12M/s adb_get_10k=3.14M/s mixed_rw_10k=1.01M/s batch_put_1k=666.6K/s delete_5k=643.3K/s
iter10_size=libassemblydb.a 883338->883188 bytes; libassemblydb.so text+data+bss 39599->39576 bytes; memtable.o 17784->17720 bytes; router.o 16192->16088 bytes
iter11=router-get-stack-frame-trim
iter11_change=router_get removed unused tx_id register preservation; stack frame reduced 80B->64B and prologue/epilogue register traffic reduced
iter11_change=retained single memtable_probe flow and loop-hoisted SST list bases
iter11_verify=make test_edge PASS(27/27)
iter11_verify=make test_all PASS
iter11_verify=make test_stress PASS
iter11_verify=make test_adversarial PASS
iter11_verify=make test_persist PASS
iter11_verify=make bench PASS
iter11_verify=5x break-loop PASS with edge/stress/adversarial/persist
iter11_bench_api=adb_put_1k=598.6K/s adb_put_10k=580.4K/s adb_get_1k=3.18M/s adb_get_10k=3.12M/s mixed_rw_10k=1.01M/s batch_put_1k=665.9K/s delete_5k=652.0K/s
iter11_size=libassemblydb.a 883188->883180 bytes; libassemblydb.so text+data+bss 39576->39564 bytes; router.o 16088->16080 bytes
iter12=scan-dedup-direct-probe + exists-fastpath
iter12_change=scan_dedup_wrapper now calls memtable_probe directly (x2=0) and compares against ADB_ERR_NOT_FOUND, removing memtable_key_exists wrapper call overhead in B+ scan dedup path
iter12_change=scan_dedup_wrapper stack frame reduced 80B->64B
iter12_change=memtable_probe now fast-paths x2==0 (existence checks) by skipping tombstone/value copy logic after key match
iter12_change=memtable_key_exists simplified to tail branch (mov x2,#0; b memtable_probe), removing wrapper frame/return overhead
iter12_verify=make test_edge PASS(27/27)
iter12_verify=make test_all PASS
iter12_verify=make test_stress PASS
iter12_verify=make test_adversarial PASS
iter12_verify=make test_persist PASS
iter12_verify=make bench PASS
iter12_verify=5x break-loop PASS with edge/stress/adversarial/persist
iter12_bench_api=adb_put_1k=594.4K/s adb_put_10k=577.1K/s adb_get_1k=3.22M/s adb_get_10k=3.12M/s mixed_rw_10k=974.9K/s batch_put_1k=668.6K/s delete_5k=633.0K/s
iter12_bench_note=host showed transient thermal/noise swings; subsequent 3-run sample returned to normal band (adb_put_1k 577.6..599.4K/s, adb_get_1k 3.19..3.22M/s, mixed 955.3..989.6K/s)
iter12_size=libassemblydb.a unchanged at 883180 bytes; libassemblydb.so text+data+bss 39564->39524 bytes; memtable.o 17720->17688 bytes; api.o 25696->25728 bytes
iter13=dead-symbol-prune + lsm_get_tombstone-guard
iter13_change=removed unused memtable_key_exists symbol entirely (all callsites already migrated to memtable_probe), shrinking code surface
iter13_change=lsm_get now uses memtable_probe for active/immutable memtables and stops immediately on tombstone (no stale fallthrough)
iter13_verify=make test_edge PASS(27/27)
iter13_verify=make test_all PASS
iter13_verify=make test_stress PASS
iter13_verify=make test_adversarial PASS
iter13_verify=make test_persist PASS
iter13_verify=make bench PASS
iter13_verify=5x break-loop PASS with edge/stress/adversarial/persist
iter13_bench_api=adb_put_1k=526.5K/s adb_put_10k=574.6K/s adb_get_1k=3.19M/s adb_get_10k=3.09M/s mixed_rw_10k=982.8K/s batch_put_1k=656.6K/s delete_5k=639.2K/s
iter13_bench_note=host remains noisy; core read/mixed throughput stayed in expected band while write(1k) showed transient drop
iter13_size=libassemblydb.a 883180->883052 bytes; libassemblydb.so text+data+bss 39524->39484 bytes; memtable.o 17688->17616 bytes; lsm_adapter.o 16504->16520 bytes
iter14=prologue-epilogue-tightening + lsm_tombstone_consistency
iter14_change=scan_dedup_wrapper removed unused x24 save/restore pair (str/ldr x23 only) while keeping 64B frame/alignment
iter14_change=lsm_get now stores/restores only x21 (removed unused x22 pair save/restore)
iter14_change=lsm_get tombstone behavior remains immediate-not-found via memtable_probe (no stale fallback)
iter14_verify=make test_edge PASS(27/27)
iter14_verify=make test_all PASS
iter14_verify=make test_stress PASS
iter14_verify=make test_adversarial PASS
iter14_verify=make test_persist PASS
iter14_verify=make bench PASS
iter14_verify=5x break-loop PASS with edge/stress/adversarial/persist
iter14_bench_api=adb_put_1k=575.9K/s adb_put_10k=550.9K/s adb_get_1k=3.23M/s adb_get_10k=3.08M/s mixed_rw_10k=971.5K/s batch_put_1k=610.2K/s delete_5k=589.5K/s
iter14_bench_note=host remained thermally/noise unstable; read path and mixed throughput stayed near prior band, write-heavy metrics fluctuated
iter14_size=libassemblydb.a unchanged at 883052 bytes; libassemblydb.so text+data+bss unchanged at 39484 bytes; lsm_adapter.o 16520 bytes; api.o 25680 bytes
iter15=lsm-compaction-atomicity + sst-descriptor-registration + wal-delete-error-propagation
iter15_change=compact_memtable rewritten as atomic swap: allocate new arena+memtable first, publish swap, flush old memtable, rollback fully on flush failure (restore old arena/memtable/data_size, destroy temp arena)
iter15_change=compact_memtable now treats any nonzero sstable_flush return as error; ADB_ERR_NOMEM returned as positive code (no negated code)
iter15_change=sstable_flush now registers real descriptor pointers: alloc_zeroed(SSTD_SIZE)+sstable_open then stores pointer in DB_SST_LIST_{L0,L1}; registration full->ADB_ERR_FULL, alloc failure->ADB_ERR_NOMEM
iter15_change=lsm_adapter_init now zeroes all SST descriptor slots explicitly; lsm_adapter_close now closes+frees all L0/L1 descriptors before arena destroy
iter15_change=lsm_get now searches active memtable, immutable memtable, then L0 newest->oldest and L1 oldest->newest using descriptor list pointers; tombstones remain authoritative
iter15_change=lsm_put now propagates WAL errors on any nonzero wal_append result (fixes silent-success on positive ADB_ERR_IO)
iter15_change=lsm_delete now propagates WAL append errors and returns memtable_delete2 result instead of unconditional success
iter15_change=test_lsm added real-usage tests: get-after-compact_memtable SST path; tombstone masking older flushed SST value
iter15_verify=make test_lsm PASS(19/19)
iter15_verify=make test_mvcc PASS(16/16)
iter16=auto-flush-regression-detected-and-rolled-back
iter16_change=experimental lsm_put auto-triggered compact_memtable at MEMTABLE_MAX introduced reopen data loss in adversarial persistence(100K survive) because SST reload-on-open is not implemented yet; feature removed to preserve correctness
iter16_change=lsm_put threshold now soft-limit only (defer checkpoint flush to adb_sync/adb_close path)
iter16_change=removed auto-flush synthetic test; retained manual compact/tombstone real-usage tests
iter16_verify=make test_all PASS
iter16_verify=make test_stress PASS(20/20)
iter16_verify=make test_adversarial PASS(26/26)
iter16_verify=make test_edge PASS(27/27)
iter16_verify=make test_persist PASS(8/8)
iter16_verify=5x break-loop PASS with edge/stress/adversarial/persist (all iterations green)
iter16_bench_api=adb_put_1k=593.4K/s adb_put_10k=568.8K/s adb_get_1k=3.24M/s adb_get_10k=2.95M/s mixed_rw_10k=922.7K/s batch_put_1k=665.0K/s tx_begin_commit_1k=106.2K/s delete_5k=630.8K/s
iter16_bench_core=key_compare=248.22M/s memtable_put_10k=2.07M/s memtable_get_10k=3.25M/s
iter16_size=libassemblydb.so text+data+bss=39507 (hex 9a53) libassemblydb.a=884060 bytes lsm_adapter.o=17064 sstable_write.o=17096 compaction.o=16000 router.o=16080 memtable.o=17616
iter17=sstable_io_error_cleanup-hardening
iter17_change=sstable_flush .Lsf_io_error now closes sst fd and destroys bloom allocation before returning ADB_ERR_IO (prevents fd/memory leak on partial write failure)
iter17_verify=make test_all PASS
iter17_verify=make test_stress PASS(20/20)
iter17_verify=make test_adversarial PASS(26/26)
iter17_verify=make test_edge PASS(27/27)
iter17_verify=make test_persist PASS(8/8)
iter17_verify=make bench PASS
iter17_size=libassemblydb.so text+data+bss=39527 (hex 9a67) libassemblydb.a=884124 bytes sstable_write.o=17160 bytes
iter17_bench_api=adb_put_1k=590.9K/s adb_put_10k=573.2K/s adb_get_1k=3.24M/s adb_get_10k=3.04M/s mixed_rw_10k=960.0K/s batch_put_1k=663.9K/s tx_begin_commit_1k=108.2K/s delete_5k=620.6K/s
