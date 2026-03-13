import 'dart:convert';
import 'dart:ffi';
import 'dart:typed_data';

import 'package:ffi/ffi.dart';

import 'bindings/assemblydb_bindings.g.dart';
import 'bindings/native_lib.dart';
import 'config.dart';
import 'errors.dart';
import 'metrics.dart';
import 'scan_entry.dart';
import 'transaction.dart';

class AssemblyDB {
  AssemblyDB._(this._bindings, this._db);

  final AssemblyDBBindings _bindings;
  final Pointer<adb_t> _db;
  bool _closed = false;

  bool get isOpen => !_closed;

  static AssemblyDB open(String path, {
    AssemblyDBConfig? config,
    String? libraryPath,
  }) {
    final bindings = loadBindings(libraryPath: libraryPath);
    return using((arena) {
      final pathPtr = path.toNativeUtf8(allocator: arena);
      final dbOut = arena<Pointer<adb_t>>();

      Pointer<adb_config_t> cfgPtr = nullptr;
      if (config != null) {
        cfgPtr = arena<adb_config_t>();
        cfgPtr.ref
          ..cache_size_pages = config.cacheSizePages
          ..encryption_enabled = config.encryptionEnabled ? 1 : 0
          ..compression_enabled = config.compressionEnabled ? 1 : 0
          ..wal_sync_mode = config.walSyncMode.value
          ..memtable_max_bytes = config.memtableMaxBytes;
      }

      final err = bindings.adb_open(pathPtr.cast(), cfgPtr, dbOut);
      if (err != ADB_OK) throw AssemblyDBException.fromCode(err);
      return AssemblyDB._(bindings, dbOut.value);
    });
  }

  void close() {
    if (_closed) return;
    _closed = true;
    _bindings.adb_close(_db);
  }

  void sync() {
    _checkOpen();
    final err = _bindings.adb_sync(_db);
    if (err != ADB_OK) throw AssemblyDBException.fromCode(err);
  }

  static void destroy(String path, {String? libraryPath}) {
    final bindings = loadBindings(libraryPath: libraryPath);
    using((arena) {
      final p = path.toNativeUtf8(allocator: arena);
      final err = bindings.adb_destroy(p.cast());
      if (err != ADB_OK) throw AssemblyDBException.fromCode(err);
    });
  }

  void put(Uint8List key, Uint8List value) {
    _checkOpen();
    using((arena) {
      final kp = allocNative(arena, key);
      final vp = allocNative(arena, value);
      final err =
          _bindings.adb_put(_db, kp.cast(), key.length, vp.cast(), value.length);
      if (err != ADB_OK) throw AssemblyDBException.fromCode(err);
    });
  }

  Uint8List? get(Uint8List key) {
    _checkOpen();
    return using((arena) {
      final kp = allocNative(arena, key);
      final vbuf = arena<Uint8>(256);
      final vlen = arena<Uint16>();
      final err = _bindings.adb_get(
          _db, kp.cast(), key.length, vbuf.cast(), 256, vlen);
      if (err == ADB_ERR_NOT_FOUND) return null;
      if (err != ADB_OK) throw AssemblyDBException.fromCode(err);
      return Uint8List.fromList(vbuf.asTypedList(vlen.value));
    });
  }

  void delete(Uint8List key) {
    _checkOpen();
    using((arena) {
      final kp = allocNative(arena, key);
      final err = _bindings.adb_delete(_db, kp.cast(), key.length);
      if (err != ADB_OK) throw AssemblyDBException.fromCode(err);
    });
  }

  bool exists(Uint8List key) {
    _checkOpen();
    return using((arena) {
      final kp = allocNative(arena, key);
      final vbuf = arena<Uint8>(1);
      final vlen = arena<Uint16>();
      final err = _bindings.adb_get(
          _db, kp.cast(), key.length, vbuf.cast(), 1, vlen);
      if (err == ADB_ERR_NOT_FOUND) return false;
      if (err != ADB_OK) throw AssemblyDBException.fromCode(err);
      return true;
    });
  }

  void putString(String key, String value) => put(encodeUtf8(key), encodeUtf8(value));

  String? getString(String key) {
    final v = get(encodeUtf8(key));
    return v == null ? null : utf8.decode(v);
  }

  void deleteString(String key) => delete(encodeUtf8(key));
  bool existsString(String key) => exists(encodeUtf8(key));

  Uint8List? operator [](Uint8List key) => get(key);
  void operator []=(Uint8List key, Uint8List value) => put(key, value);

  int scan({
    Uint8List? start,
    Uint8List? end,
    required bool Function(Uint8List key, Uint8List value) onEntry,
  }) {
    _checkOpen();
    return using((arena) => doScan(
      (cb) {
        final sp = allocOpt(arena, start);
        final ep = allocOpt(arena, end);
        return _bindings.adb_scan(
            _db, sp, start?.length ?? 0, ep, end?.length ?? 0, cb, nullptr);
      },
      onEntry,
    ));
  }

  List<ScanEntry> scanAll({Uint8List? start, Uint8List? end}) {
    final results = <ScanEntry>[];
    scan(start: start, end: end, onEntry: (k, v) {
      results.add(ScanEntry(k, v));
      return true;
    });
    return results;
  }

  int scanStrings({
    String? start,
    String? end,
    required bool Function(String key, String value) onEntry,
  }) => scan(
    start: start != null ? encodeUtf8(start) : null,
    end: end != null ? encodeUtf8(end) : null,
    onEntry: (k, v) => onEntry(utf8.decode(k), utf8.decode(v)),
  );

  int count({Uint8List? start, Uint8List? end}) =>
      scan(start: start, end: end, onEntry: (_, __) => true);

  void batchPut(Map<Uint8List, Uint8List> entries) {
    _checkOpen();
    if (entries.isEmpty) return;
    using((arena) {
      final n = entries.length;
      final arr = arena<adb_batch_entry_t>(n);
      var i = 0;
      for (final e in entries.entries) {
        final kp = allocNative(arena, e.key);
        final vp = allocNative(arena, e.value);
        arr[i]
          ..key = kp.cast()
          ..key_len = e.key.length
          ..val = vp.cast()
          ..val_len = e.value.length;
        i++;
      }
      final err = _bindings.adb_batch_put(_db, arr, n);
      if (err != ADB_OK) throw AssemblyDBException.fromCode(err);
    });
  }

  void batchPutStrings(Map<String, String> entries) {
    _checkOpen();
    if (entries.isEmpty) return;
    using((arena) {
      final n = entries.length;
      final arr = arena<adb_batch_entry_t>(n);
      var i = 0;
      for (final e in entries.entries) {
        final kb = encodeUtf8(e.key);
        final vb = encodeUtf8(e.value);
        final kp = allocNative(arena, kb);
        final vp = allocNative(arena, vb);
        arr[i]
          ..key = kp.cast()
          ..key_len = kb.length
          ..val = vp.cast()
          ..val_len = vb.length;
        i++;
      }
      final err = _bindings.adb_batch_put(_db, arr, n);
      if (err != ADB_OK) throw AssemblyDBException.fromCode(err);
    });
  }

  Transaction begin({IsolationLevel isolation = IsolationLevel.snapshot}) {
    _checkOpen();
    return using((arena) {
      final txOut = arena<Uint64>();
      final err = _bindings.adb_tx_begin(_db, isolation.value, txOut);
      if (err != ADB_OK) throw AssemblyDBException.fromCode(err);
      return Transaction.fromNative(_bindings, _db, txOut.value);
    });
  }

  T transaction<T>(
    T Function(Transaction tx) action, {
    IsolationLevel isolation = IsolationLevel.snapshot,
  }) {
    final tx = begin(isolation: isolation);
    try {
      final result = action(tx);
      tx.commit();
      return result;
    } catch (_) {
      if (tx.isActive) tx.rollback();
      rethrow;
    }
  }

  void createIndex(String name) {
    _checkOpen();
    using((arena) {
      final np = name.toNativeUtf8(allocator: arena);
      final err = _bindings.adb_create_index(_db, np.cast(), nullptr);
      if (err != ADB_OK) throw AssemblyDBException.fromCode(err);
    });
  }

  void dropIndex(String name) {
    _checkOpen();
    using((arena) {
      final np = name.toNativeUtf8(allocator: arena);
      final err = _bindings.adb_drop_index(_db, np.cast());
      if (err != ADB_OK) throw AssemblyDBException.fromCode(err);
    });
  }

  int indexScan(
    String indexName,
    Uint8List key, {
    required bool Function(Uint8List key, Uint8List value) onEntry,
  }) {
    _checkOpen();
    return using((arena) => doScan(
      (cb) {
        final np = indexName.toNativeUtf8(allocator: arena);
        final kp = allocNative(arena, key);
        return _bindings.adb_index_scan(
            _db, np.cast(), kp.cast(), key.length, cb, nullptr);
      },
      onEntry,
    ));
  }

  void backup(String destPath) {
    _checkOpen();
    using((arena) {
      final dp = destPath.toNativeUtf8(allocator: arena);
      final err = _bindings.adb_backup(_db, dp.cast(), ADB_BACKUP_FULL);
      if (err != ADB_OK) throw AssemblyDBException.fromCode(err);
    });
  }

  static void restore(String backupPath, String destPath,
      {String? libraryPath}) {
    final bindings = loadBindings(libraryPath: libraryPath);
    using((arena) {
      final bp = backupPath.toNativeUtf8(allocator: arena);
      final dp = destPath.toNativeUtf8(allocator: arena);
      final err = bindings.adb_restore(bp.cast(), dp.cast());
      if (err != ADB_OK) throw AssemblyDBException.fromCode(err);
    });
  }

  AssemblyDBMetrics get metrics {
    _checkOpen();
    return using((arena) {
      final m = arena<adb_metrics_t>();
      final err = _bindings.adb_get_metrics(_db, m);
      if (err != ADB_OK) throw AssemblyDBException.fromCode(err);
      return AssemblyDBMetrics(
        putsTotal: m.ref.puts_total,
        getsTotal: m.ref.gets_total,
        deletesTotal: m.ref.deletes_total,
        scansTotal: m.ref.scans_total,
        cacheHits: m.ref.cache_hits,
        cacheMisses: m.ref.cache_misses,
        bloomTruePositives: m.ref.bloom_true_positives,
        bloomFalsePositives: m.ref.bloom_false_positives,
        bytesWritten: m.ref.bytes_written,
        bytesRead: m.ref.bytes_read,
        compactionsRun: m.ref.compactions_run,
        compactionBytes: m.ref.compaction_bytes,
        walSyncs: m.ref.wal_syncs,
        walBytes: m.ref.wal_bytes,
        txCommits: m.ref.tx_commits,
        txRollbacks: m.ref.tx_rollbacks,
        pageSplits: m.ref.page_splits,
        pageMerges: m.ref.page_merges,
        memtableFlushes: m.ref.memtable_flushes,
        sstableCount: m.ref.sstable_count,
      );
    });
  }

  void _checkOpen() {
    if (_closed) throw StateError('Database is closed');
  }
}

Pointer<Uint8> allocNative(Allocator alloc, Uint8List data) {
  if (data.isEmpty) {
    final p = alloc<Uint8>(1);
    p.value = 0;
    return p;
  }
  final p = alloc<Uint8>(data.length);
  p.asTypedList(data.length).setAll(0, data);
  return p;
}

Uint8List encodeUtf8(String s) => utf8.encoder.convert(s);

int doScan(
  int Function(adb_scan_fn cb) invoke,
  bool Function(Uint8List key, Uint8List value) onEntry,
) {
  var count = 0;
  Object? scanErr;
  final cb = NativeCallable<
      Int Function(Pointer<Void>, Uint16, Pointer<Void>, Uint16,
          Pointer<Void>)>.isolateLocal(
    (Pointer<Void> k, int kl, Pointer<Void> v, int vl, Pointer<Void> ud) {
      try {
        final key = Uint8List.fromList(k.cast<Uint8>().asTypedList(kl));
        final val = Uint8List.fromList(v.cast<Uint8>().asTypedList(vl));
        count++;
        return onEntry(key, val) ? 0 : 1;
      } catch (e) {
        scanErr = e;
        return 1;
      }
    },
    exceptionalReturn: 1,
  );
  try {
    final err = invoke(cb.nativeFunction);
    if (scanErr != null) {
      // ignore: only_throw_errors
      throw scanErr!;
    }
    if (err != ADB_OK) throw AssemblyDBException.fromCode(err);
  } finally {
    cb.close();
  }
  return count;
}

Pointer<Void> allocOpt(Allocator alloc, Uint8List? data) {
  if (data == null || data.isEmpty) return nullptr;
  final p = alloc<Uint8>(data.length);
  p.asTypedList(data.length).setAll(0, data);
  return p.cast();
}
