import 'dart:convert';
import 'dart:ffi';
import 'dart:typed_data';

import 'package:ffi/ffi.dart';

import 'assemblydb_base.dart';
import 'bindings/assemblydb_bindings.g.dart';
import 'errors.dart';
import 'scan_entry.dart';

class Transaction {
  Transaction.fromNative(this._bindings, this._db, this._txId, this._isDbOpen);

  final AssemblyDBBindings _bindings;
  final Pointer<adb_t> _db;
  final int _txId;
  final bool Function() _isDbOpen;
  bool _active = true;

  bool get isActive => _active;

  void put(Uint8List key, Uint8List value) {
    _checkActive();
    if (key.length > 62) throw const KeyTooLongError();
    if (value.length > 254) throw const ValTooLongError();
    using((arena) {
      final kp = allocNative(arena, key);
      final vp = allocNative(arena, value);
      final err = _bindings.adb_tx_put(
          _db, _txId, kp.cast(), key.length, vp.cast(), value.length);
      if (err != ADB_OK) throw AssemblyDBException.fromCode(err);
    });
  }

  void putString(String key, String value) => put(encodeUtf8(key), encodeUtf8(value));

  Uint8List? get(Uint8List key) {
    _checkActive();
    if (key.length > 62) throw const KeyTooLongError();
    return using((arena) {
      final kp = allocNative(arena, key);
      final vbuf = arena<Uint8>(256);
      final vlen = arena<Uint16>();
      final err = _bindings.adb_tx_get(
          _db, _txId, kp.cast(), key.length, vbuf.cast(), 256, vlen);
      if (err == ADB_ERR_NOT_FOUND) return null;
      if (err != ADB_OK) throw AssemblyDBException.fromCode(err);
      return Uint8List.fromList(vbuf.asTypedList(vlen.value));
    });
  }

  String? getString(String key) {
    final v = get(encodeUtf8(key));
    return v == null ? null : utf8.decode(v);
  }

  void delete(Uint8List key) {
    _checkActive();
    if (key.length > 62) throw const KeyTooLongError();
    using((arena) {
      final kp = allocNative(arena, key);
      final err = _bindings.adb_tx_delete(_db, _txId, kp.cast(), key.length);
      if (err != ADB_OK) throw AssemblyDBException.fromCode(err);
    });
  }

  void deleteString(String key) => delete(encodeUtf8(key));

  int scan({
    Uint8List? start,
    Uint8List? end,
    required bool Function(Uint8List key, Uint8List value) onEntry,
  }) {
    _checkActive();
    return using((arena) => doScan(
      (cb) {
        final sp = allocOpt(arena, start);
        final ep = allocOpt(arena, end);
        return _bindings.adb_tx_scan(
            _db, _txId, sp, start?.length ?? 0,
            ep, end?.length ?? 0, cb, nullptr);
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

  void commit() {
    _checkActive();
    _active = false;
    final err = _bindings.adb_tx_commit(_db, _txId);
    if (err != ADB_OK) throw AssemblyDBException.fromCode(err);
  }

  void rollback() {
    _checkActive();
    _active = false;
    final err = _bindings.adb_tx_rollback(_db, _txId);
    if (err != ADB_OK) throw AssemblyDBException.fromCode(err);
  }

  void _checkActive() {
    if (!_isDbOpen()) throw StateError('Database is closed');
    if (!_active) throw StateError('Transaction is no longer active');
  }
}
