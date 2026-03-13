import 'dart:convert';
import 'dart:io';
import 'dart:math';
import 'dart:typed_data';

import 'package:assemblydb/assemblydb.dart';
import 'package:test/test.dart';

Uint8List _b(String s) => utf8.encoder.convert(s);
String _s(Uint8List b) => utf8.decode(b);

late String _dbPath;
late Directory _tmpDir;

void main() {
  setUp(() {
    _tmpDir = Directory.systemTemp.createTempSync('adb_test_');
    _dbPath = '${_tmpDir.path}/testdb';
  });

  tearDown(() {
    try { AssemblyDB.destroy(_dbPath); } catch (_) {}
    try { _tmpDir.deleteSync(recursive: true); } catch (_) {}
  });

  // ============================================================
  // LIFECYCLE
  // ============================================================
  group('lifecycle', () {
    test('open and close', () {
      final db = AssemblyDB.open(_dbPath);
      expect(db.isOpen, isTrue);
      db.close();
      expect(db.isOpen, isFalse);
    });

    test('open with all config options', () {
      final db = AssemblyDB.open(_dbPath, config: const AssemblyDBConfig(
        cacheSizePages: 128,
        walSyncMode: WalSyncMode.full,
        encryptionEnabled: false,
        compressionEnabled: false,
        memtableMaxBytes: 65536,
      ));
      db.close();
    });

    test('close is idempotent', () {
      final db = AssemblyDB.open(_dbPath);
      db.close();
      db.close();
      db.close();
    });

    test('operations after close throw StateError', () {
      final db = AssemblyDB.open(_dbPath);
      db.close();
      expect(() => db.putString('k', 'v'), throwsStateError);
      expect(() => db.getString('k'), throwsStateError);
      expect(() => db.deleteString('k'), throwsStateError);
      expect(() => db.sync(), throwsStateError);
      expect(() => db.existsString('k'), throwsStateError);
      expect(() => db.begin(), throwsStateError);
      expect(() => db.count(), throwsStateError);
      expect(() => db.metrics, throwsStateError);
      expect(() => db.scan(onEntry: (_, __) => true), throwsStateError);
      expect(() => db.createIndex('i'), throwsStateError);
    });

    test('destroy removes all database files', () {
      final db = AssemblyDB.open(_dbPath);
      db.putString('k', 'v');
      db.sync();
      db.close();
      AssemblyDB.destroy(_dbPath);
      final db2 = AssemblyDB.open(_dbPath);
      expect(db2.getString('k'), isNull);
      db2.close();
    });

    test('reopen preserves data', () {
      var db = AssemblyDB.open(_dbPath);
      db.putString('persist', 'yes');
      db.sync();
      db.close();

      db = AssemblyDB.open(_dbPath);
      expect(db.getString('persist'), 'yes');
      db.close();
    });

    test('reopen after many writes', () {
      var db = AssemblyDB.open(_dbPath);
      for (var i = 0; i < 100; i++) {
        db.putString('k$i', 'v$i');
      }
      db.sync();
      db.close();

      db = AssemblyDB.open(_dbPath);
      for (var i = 0; i < 100; i++) {
        expect(db.getString('k$i'), 'v$i');
      }
      db.close();
    });
  });

  // ============================================================
  // CRUD (bytes)
  // ============================================================
  group('crud bytes', () {
    late AssemblyDB db;
    setUp(() => db = AssemblyDB.open(_dbPath));
    tearDown(() => db.close());

    test('put and get', () {
      db.put(_b('hello'), _b('world'));
      expect(_s(db.get(_b('hello'))!), 'world');
    });

    test('get missing returns null', () {
      expect(db.get(_b('missing')), isNull);
    });

    test('overwrite value', () {
      db.put(_b('k'), _b('v1'));
      db.put(_b('k'), _b('v2'));
      expect(_s(db.get(_b('k'))!), 'v2');
    });

    test('delete key', () {
      db.put(_b('k'), _b('v'));
      db.delete(_b('k'));
      expect(db.get(_b('k')), isNull);
    });

    test('delete missing key succeeds', () {
      db.delete(_b('nonexistent'));
    });

    test('exists returns correct state', () {
      expect(db.exists(_b('k')), isFalse);
      db.put(_b('k'), _b('v'));
      expect(db.exists(_b('k')), isTrue);
      db.delete(_b('k'));
      expect(db.exists(_b('k')), isFalse);
    });

    test('operator[] and operator[]=', () {
      db[_b('op')] = _b('test');
      expect(_s(db[_b('op')]!), 'test');
      expect(db[_b('nope')], isNull);
    });

    test('binary key with null bytes', () {
      final key = Uint8List.fromList([0, 1, 2, 0, 3]);
      final val = Uint8List.fromList([255, 0, 128]);
      db.put(key, val);
      final got = db.get(key);
      expect(got, isNotNull);
      expect(got, equals(val));
    });

    test('max key size (62 bytes)', () {
      final key = Uint8List(62);
      for (var i = 0; i < 62; i++) key[i] = i + 1;
      db.put(key, _b('maxkey'));
      expect(_s(db.get(key)!), 'maxkey');
    });

    test('max value size (254 bytes)', () {
      final val = Uint8List(254);
      for (var i = 0; i < 254; i++) val[i] = i & 0xFF;
      db.put(_b('k'), val);
      expect(db.get(_b('k')), equals(val));
    });

    test('single byte key and value', () {
      db.put(Uint8List.fromList([42]), Uint8List.fromList([99]));
      expect(db.get(Uint8List.fromList([42])), equals(Uint8List.fromList([99])));
    });

    test('put overwrite multiple times', () {
      for (var i = 0; i < 50; i++) {
        db.put(_b('key'), _b('val_$i'));
      }
      expect(_s(db.get(_b('key'))!), 'val_49');
    });
  });

  // ============================================================
  // CRUD (strings)
  // ============================================================
  group('crud strings', () {
    late AssemblyDB db;
    setUp(() => db = AssemblyDB.open(_dbPath));
    tearDown(() => db.close());

    test('putString and getString', () {
      db.putString('greeting', 'hello world');
      expect(db.getString('greeting'), 'hello world');
    });

    test('getString missing returns null', () {
      expect(db.getString('nope'), isNull);
    });

    test('deleteString', () {
      db.putString('k', 'v');
      db.deleteString('k');
      expect(db.getString('k'), isNull);
    });

    test('existsString', () {
      expect(db.existsString('k'), isFalse);
      db.putString('k', 'v');
      expect(db.existsString('k'), isTrue);
    });

    test('unicode keys and values', () {
      db.putString('emoji_key', 'value');
      expect(db.getString('emoji_key'), 'value');
    });

    test('empty string value', () {
      db.putString('empty', '');
      expect(db.getString('empty'), '');
    });

    test('special characters', () {
      db.putString('special', 'tab\tnewline\nquote"slash/');
      expect(db.getString('special'), 'tab\tnewline\nquote"slash/');
    });
  });

  // ============================================================
  // SCAN
  // ============================================================
  group('scan', () {
    late AssemblyDB db;
    setUp(() {
      db = AssemblyDB.open(_dbPath);
      for (var i = 0; i < 10; i++) {
        db.putString('key${i.toString().padLeft(2, '0')}', 'val$i');
      }
      db.sync();
    });
    tearDown(() => db.close());

    test('scan all entries', () {
      final entries = <String>[];
      db.scanStrings(onEntry: (k, v) {
        entries.add(k);
        return true;
      });
      expect(entries.length, 10);
    });

    test('scanAll returns list', () {
      final results = db.scanAll();
      expect(results.length, 10);
      expect(results.first, isA<ScanEntry>());
    });

    test('scan with early stop', () {
      var count = 0;
      db.scan(onEntry: (k, v) {
        count++;
        return count < 3;
      });
      expect(count, 3);
    });

    test('scan with range', () {
      final keys = <String>[];
      db.scanStrings(
        start: 'key03',
        end: 'key07',
        onEntry: (k, v) { keys.add(k); return true; },
      );
      expect(keys, containsAll(['key03', 'key04', 'key05', 'key06']));
      expect(keys, isNot(contains('key08')));
    });

    test('count returns total', () {
      expect(db.count(), 10);
    });

    test('count with range', () {
      final c = db.count(
        start: _b('key03'),
        end: _b('key07'),
      );
      expect(c, greaterThanOrEqualTo(3));
    });

    test('scan empty database', () {
      final db2 = AssemblyDB.open('${_tmpDir.path}/emptydb');
      expect(db2.count(), 0);
      expect(db2.scanAll(), isEmpty);
      db2.close();
      AssemblyDB.destroy('${_tmpDir.path}/emptydb');
    });

    test('scan returns sorted order', () {
      final keys = db.scanAll().map((e) => e.keyString).toList();
      final sorted = List<String>.from(keys)..sort();
      expect(keys, equals(sorted));
    });

    test('scanEntry equality', () {
      final a = ScanEntry(_b('k'), _b('v'));
      final b = ScanEntry(_b('k'), _b('v'));
      final c = ScanEntry(_b('k'), _b('x'));
      expect(a, equals(b));
      expect(a.hashCode, equals(b.hashCode));
      expect(a, isNot(equals(c)));
    });

    test('scanEntry string accessors', () {
      final e = ScanEntry(_b('hello'), _b('world'));
      expect(e.keyString, 'hello');
      expect(e.valueString, 'world');
    });

    test('scan callback exception propagates', () {
      expect(
        () => db.scan(onEntry: (_, __) => throw FormatException('boom')),
        throwsFormatException,
      );
    });
  });

  // ============================================================
  // BATCH
  // ============================================================
  group('batch', () {
    late AssemblyDB db;
    setUp(() => db = AssemblyDB.open(_dbPath));
    tearDown(() => db.close());

    test('batchPut writes multiple entries', () {
      db.batchPut({
        _b('a'): _b('1'),
        _b('b'): _b('2'),
        _b('c'): _b('3'),
      });
      expect(_s(db.get(_b('a'))!), '1');
      expect(_s(db.get(_b('b'))!), '2');
      expect(_s(db.get(_b('c'))!), '3');
    });

    test('batchPutStrings', () {
      db.batchPutStrings({'x': 'y', 'a': 'b'});
      expect(db.getString('x'), 'y');
      expect(db.getString('a'), 'b');
    });

    test('empty batch is no-op', () {
      db.batchPut({});
      db.batchPutStrings({});
    });

    test('large batch', () {
      final entries = <String, String>{};
      for (var i = 0; i < 50; i++) {
        entries['batch_$i'] = 'val_$i';
      }
      db.batchPutStrings(entries);
      for (var i = 0; i < 50; i++) {
        expect(db.getString('batch_$i'), 'val_$i');
      }
    });

    test('batch overwrites existing', () {
      db.putString('existing', 'old');
      db.batchPutStrings({'existing': 'new', 'fresh': 'val'});
      expect(db.getString('existing'), 'new');
      expect(db.getString('fresh'), 'val');
    });
  });

  // ============================================================
  // TRANSACTIONS
  // ============================================================
  group('transactions', () {
    late AssemblyDB db;
    setUp(() => db = AssemblyDB.open(_dbPath));
    tearDown(() => db.close());

    test('commit makes writes visible', () {
      final tx = db.begin();
      tx.putString('tk', 'tv');
      tx.commit();
      expect(db.getString('tk'), 'tv');
    });

    test('rollback discards writes', () {
      db.putString('existing', 'yes');
      final tx = db.begin();
      tx.putString('existing', 'changed');
      tx.rollback();
      expect(db.getString('existing'), 'yes');
    });

    test('transaction helper auto-commits', () {
      db.transaction((tx) {
        tx.putString('auto', 'commit');
      });
      expect(db.getString('auto'), 'commit');
    });

    test('transaction helper returns value', () {
      final val = db.transaction((tx) {
        tx.putString('ret', 'v');
        return 42;
      });
      expect(val, 42);
    });

    test('transaction auto-rollbacks on exception', () {
      db.putString('safe', 'original');
      expect(
        () => db.transaction((tx) {
          tx.putString('safe', 'changed');
          throw Exception('oops');
        }),
        throwsException,
      );
      expect(db.getString('safe'), 'original');
    });

    test('tx read own writes', () {
      final tx = db.begin();
      tx.putString('rw', 'val');
      expect(tx.getString('rw'), 'val');
      tx.commit();
    });

    test('tx delete own write', () {
      final tx = db.begin();
      tx.putString('td', 'val');
      tx.deleteString('td');
      expect(tx.getString('td'), isNull);
      tx.commit();
    });

    test('tx delete existing key', () {
      db.putString('del', 'v');
      final tx = db.begin();
      tx.deleteString('del');
      expect(tx.getString('del'), isNull);
      tx.commit();
      expect(db.getString('del'), isNull);
    });

    test('tx multiple puts and deletes', () {
      final tx = db.begin();
      tx.putString('a', '1');
      tx.putString('b', '2');
      tx.putString('c', '3');
      tx.deleteString('b');
      tx.commit();
      expect(db.getString('a'), '1');
      expect(db.getString('b'), isNull);
      expect(db.getString('c'), '3');
    });

    test('operations after commit throw StateError', () {
      final tx = db.begin();
      tx.commit();
      expect(() => tx.putString('k', 'v'), throwsStateError);
      expect(() => tx.getString('k'), throwsStateError);
      expect(() => tx.deleteString('k'), throwsStateError);
      expect(() => tx.commit(), throwsStateError);
      expect(() => tx.rollback(), throwsStateError);
    });

    test('operations after rollback throw StateError', () {
      final tx = db.begin();
      tx.rollback();
      expect(() => tx.putString('k', 'v'), throwsStateError);
      expect(() => tx.commit(), throwsStateError);
    });

    test('isActive reflects state', () {
      final tx = db.begin();
      expect(tx.isActive, isTrue);
      tx.commit();
      expect(tx.isActive, isFalse);
    });
  });

  // ============================================================
  // SECONDARY INDEXES
  // ============================================================
  group('indexes', () {
    late AssemblyDB db;
    setUp(() => db = AssemblyDB.open(_dbPath));
    tearDown(() => db.close());

    test('create and drop index', () {
      db.createIndex('idx1');
      db.dropIndex('idx1');
    });
  });

  // ============================================================
  // SYNC
  // ============================================================
  group('sync', () {
    late AssemblyDB db;
    setUp(() => db = AssemblyDB.open(_dbPath));
    tearDown(() => db.close());

    test('sync flushes data', () {
      db.putString('s', 'v');
      db.sync();
      expect(db.getString('s'), 'v');
    });

    test('multiple syncs', () {
      for (var i = 0; i < 5; i++) {
        db.putString('sync$i', 'v$i');
        db.sync();
      }
      for (var i = 0; i < 5; i++) {
        expect(db.getString('sync$i'), 'v$i');
      }
    });
  });

  // ============================================================
  // BACKUP & RESTORE
  // ============================================================
  group('backup/restore', () {
    late AssemblyDB db;
    late String backupPath;
    late String restorePath;

    setUp(() {
      db = AssemblyDB.open(_dbPath);
      backupPath = '${_tmpDir.path}/backup';
      restorePath = '${_tmpDir.path}/restored';
    });
    tearDown(() {
      db.close();
      try { AssemblyDB.destroy(backupPath); } catch (_) {}
      try { AssemblyDB.destroy(restorePath); } catch (_) {}
    });

    test('backup and restore roundtrip', () {
      for (var i = 0; i < 20; i++) {
        db.putString('bk$i', 'data$i');
      }
      db.sync();
      db.backup(backupPath);

      AssemblyDB.restore(backupPath, restorePath);
      final db2 = AssemblyDB.open(restorePath);
      for (var i = 0; i < 20; i++) {
        expect(db2.getString('bk$i'), 'data$i');
      }
      db2.close();
    });

    test('backup preserves after modifications', () {
      db.putString('before', 'yes');
      db.sync();
      db.backup(backupPath);

      db.putString('after', 'also');
      db.sync();

      AssemblyDB.restore(backupPath, restorePath);
      final db2 = AssemblyDB.open(restorePath);
      expect(db2.getString('before'), 'yes');
      expect(db2.getString('after'), isNull);
      db2.close();
    });
  });

  // ============================================================
  // METRICS
  // ============================================================
  group('metrics', () {
    late AssemblyDB db;
    setUp(() => db = AssemblyDB.open(_dbPath));
    tearDown(() => db.close());

    test('metrics reflect operations', () {
      db.putString('m1', 'v1');
      db.putString('m2', 'v2');
      db.getString('m1');
      db.getString('missing');
      db.deleteString('m2');
      final m = db.metrics;
      expect(m.putsTotal, greaterThanOrEqualTo(2));
      expect(m.getsTotal, greaterThanOrEqualTo(2));
      expect(m.deletesTotal, greaterThanOrEqualTo(1));
      expect(m.toString(), contains('puts='));
      expect(m.toString(), contains('cacheHitRate='));
    });

    test('metrics equality', () {
      db.putString('x', 'y');
      final m1 = db.metrics;
      final m2 = db.metrics;
      expect(m1, equals(m2));
    });
  });

  // ============================================================
  // ERROR HANDLING
  // ============================================================
  group('errors', () {
    test('fromCode maps all known codes', () {
      expect(AssemblyDBException.fromCode(1), isA<NotFoundError>());
      expect(AssemblyDBException.fromCode(2), isA<IOError>());
      expect(AssemblyDBException.fromCode(3), isA<CorruptError>());
      expect(AssemblyDBException.fromCode(4), isA<KeyTooLongError>());
      expect(AssemblyDBException.fromCode(5), isA<ValTooLongError>());
      expect(AssemblyDBException.fromCode(6), isA<LockedError>());
      expect(AssemblyDBException.fromCode(7), isA<OutOfMemoryError>());
      expect(AssemblyDBException.fromCode(8), isA<InvalidError>());
      expect(AssemblyDBException.fromCode(9), isA<TxConflictError>());
      expect(AssemblyDBException.fromCode(10), isA<TxNotFoundError>());
      expect(AssemblyDBException.fromCode(11), isA<TxAbortedError>());
      expect(AssemblyDBException.fromCode(12), isA<FullError>());
      expect(AssemblyDBException.fromCode(13), isA<ExistsError>());
      expect(AssemblyDBException.fromCode(14), isA<DecryptError>());
      expect(AssemblyDBException.fromCode(15), isA<CompressError>());
      expect(AssemblyDBException.fromCode(999), isA<AssemblyDBUnknownError>());
    });

    test('error toString includes code and message', () {
      final e = AssemblyDBException.fromCode(1);
      expect(e.toString(), contains('1'));
      expect(e.toString(), contains('not found'));
    });

    test('key too long throws KeyTooLongError', () {
      final db = AssemblyDB.open(_dbPath);
      expect(() => db.put(Uint8List(100), _b('v')), throwsA(isA<KeyTooLongError>()));
      db.close();
    });

    test('value too long throws ValTooLongError', () {
      final db = AssemblyDB.open(_dbPath);
      expect(() => db.put(_b('k'), Uint8List(300)), throwsA(isA<ValTooLongError>()));
      db.close();
    });

    test('key exactly 63 bytes throws', () {
      final db = AssemblyDB.open(_dbPath);
      expect(() => db.put(Uint8List(63), _b('v')), throwsA(isA<KeyTooLongError>()));
      db.close();
    });

    test('value exactly 255 bytes throws', () {
      final db = AssemblyDB.open(_dbPath);
      expect(() => db.put(_b('k'), Uint8List(255)), throwsA(isA<ValTooLongError>()));
      db.close();
    });
  });

  // ============================================================
  // CONFIG
  // ============================================================
  group('config', () {
    test('default values', () {
      const cfg = AssemblyDBConfig();
      expect(cfg.cacheSizePages, 64);
      expect(cfg.encryptionEnabled, isFalse);
      expect(cfg.compressionEnabled, isFalse);
      expect(cfg.walSyncMode, WalSyncMode.sync_);
      expect(cfg.memtableMaxBytes, 0);
    });

    test('WalSyncMode values', () {
      expect(WalSyncMode.async_.value, 0);
      expect(WalSyncMode.sync_.value, 1);
      expect(WalSyncMode.full.value, 2);
    });

    test('IsolationLevel values', () {
      expect(IsolationLevel.readUncommitted.value, 0);
      expect(IsolationLevel.readCommitted.value, 1);
      expect(IsolationLevel.repeatableRead.value, 2);
      expect(IsolationLevel.snapshot.value, 3);
      expect(IsolationLevel.serializable.value, 4);
    });
  });

  // ============================================================
  // REAL-WORLD: Key-Value Store (settings/config)
  // ============================================================
  group('real-world: app settings', () {
    late AssemblyDB db;
    setUp(() => db = AssemblyDB.open(_dbPath));
    tearDown(() => db.close());

    test('store and retrieve app settings', () {
      db.putString('settings:theme', 'dark');
      db.putString('settings:lang', 'en');
      db.putString('settings:notifications', 'true');
      db.putString('settings:font_size', '14');

      expect(db.getString('settings:theme'), 'dark');
      expect(db.getString('settings:lang'), 'en');

      db.putString('settings:theme', 'light');
      expect(db.getString('settings:theme'), 'light');
    });

    test('enumerate all settings via scan', () {
      db.putString('settings:a', '1');
      db.putString('settings:b', '2');
      db.putString('settings:c', '3');
      db.putString('other:x', 'y');
      db.sync();

      final settings = <String, String>{};
      db.scanStrings(
        start: 'settings:',
        end: 'settings:\xff',
        onEntry: (k, v) { settings[k] = v; return true; },
      );
      expect(settings.length, 3);
      expect(settings['settings:a'], '1');
      expect(settings.containsKey('other:x'), isFalse);
    });
  });

  // ============================================================
  // REAL-WORLD: Restaurant order system
  // ============================================================
  group('real-world: restaurant orders', () {
    late AssemblyDB db;
    setUp(() => db = AssemblyDB.open(_dbPath));
    tearDown(() => db.close());

    test('create orders in transaction', () {
      db.transaction((tx) {
        tx.putString('order:001:status', 'pending');
        tx.putString('order:001:table', '5');
        tx.putString('order:001:item:0', 'Burger');
        tx.putString('order:001:item:1', 'Fries');
        tx.putString('order:001:total', '15.99');
      });

      expect(db.getString('order:001:status'), 'pending');
      expect(db.getString('order:001:item:0'), 'Burger');
    });

    test('update order status atomically', () {
      db.putString('order:002:status', 'pending');
      db.putString('order:002:item:0', 'Pizza');

      db.transaction((tx) {
        final status = tx.getString('order:002:status');
        expect(status, 'pending');
        tx.putString('order:002:status', 'preparing');
        tx.putString('order:002:cook', 'Chef Bob');
      });

      expect(db.getString('order:002:status'), 'preparing');
      expect(db.getString('order:002:cook'), 'Chef Bob');
    });

    test('cancel order rolls back', () {
      db.putString('order:003:status', 'pending');

      expect(() {
        db.transaction((tx) {
          tx.putString('order:003:status', 'cancelled');
          throw Exception('Payment refund failed');
        });
      }, throwsException);

      expect(db.getString('order:003:status'), 'pending');
    });

    test('batch insert menu items', () {
      final menu = <String, String>{};
      for (var i = 0; i < 30; i++) {
        menu['menu:item:$i'] = '{"name":"Item $i","price":${(i + 1) * 1.5}}';
      }
      db.batchPutStrings(menu);

      for (var i = 0; i < 30; i++) {
        expect(db.existsString('menu:item:$i'), isTrue);
      }
    });
  });

  // ============================================================
  // REAL-WORLD: Session/cache store
  // ============================================================
  group('real-world: session cache', () {
    late AssemblyDB db;
    setUp(() => db = AssemblyDB.open(_dbPath));
    tearDown(() => db.close());

    test('store and expire sessions', () {
      for (var i = 0; i < 20; i++) {
        db.putString('session:user$i', '{"token":"abc$i","expires":${1000 + i}}');
      }

      db.deleteString('session:user5');
      db.deleteString('session:user10');

      expect(db.existsString('session:user5'), isFalse);
      expect(db.existsString('session:user0'), isTrue);
    });

    test('rapid put/get cycles (cache hot path)', () {
      for (var round = 0; round < 5; round++) {
        for (var i = 0; i < 50; i++) {
          db.putString('cache:$i', 'round$round');
        }
        for (var i = 0; i < 50; i++) {
          expect(db.getString('cache:$i'), 'round$round');
        }
      }
    });
  });

  // ============================================================
  // STRESS: Volume
  // ============================================================
  group('stress', () {
    late AssemblyDB db;
    setUp(() => db = AssemblyDB.open(_dbPath));
    tearDown(() => db.close());

    test('1000 sequential puts then verify', () {
      for (var i = 0; i < 1000; i++) {
        db.putString('stress:$i', 'v$i');
      }
      for (var i = 0; i < 1000; i++) {
        expect(db.getString('stress:$i'), 'v$i');
      }
    });

    test('interleaved put/get/delete', () {
      final deleted = <int>{};
      for (var i = 0; i < 200; i++) {
        db.putString('inter:$i', 'v$i');
        if (i > 0 && i % 3 == 0) {
          db.deleteString('inter:${i - 1}');
          deleted.add(i - 1);
        }
        if (i > 5) {
          final idx = i - 5;
          final v = db.getString('inter:$idx');
          if (deleted.contains(idx)) {
            expect(v, isNull);
          } else {
            expect(v, 'v$idx');
          }
        }
      }
    });

    test('sync after every N writes', () {
      for (var i = 0; i < 100; i++) {
        db.putString('synced:$i', 'v$i');
        if (i % 10 == 9) db.sync();
      }
      for (var i = 0; i < 100; i++) {
        expect(db.getString('synced:$i'), 'v$i');
      }
    });

    test('many small transactions', () {
      for (var i = 0; i < 50; i++) {
        db.transaction((tx) {
          tx.putString('tx:$i:a', 'v1');
          tx.putString('tx:$i:b', 'v2');
        });
      }
      for (var i = 0; i < 50; i++) {
        expect(db.getString('tx:$i:a'), 'v1');
        expect(db.getString('tx:$i:b'), 'v2');
      }
    });

    test('random key patterns', () {
      final rng = Random(42);
      final keys = <String>[];
      for (var i = 0; i < 200; i++) {
        final key = 'rnd:${rng.nextInt(100)}';
        final val = 'v${rng.nextInt(10000)}';
        db.putString(key, val);
        keys.add(key);
      }
      for (final key in keys) {
        expect(db.existsString(key), isTrue);
      }
    });

    test('large batch stress', () {
      for (var batch = 0; batch < 10; batch++) {
        final entries = <String, String>{};
        for (var i = 0; i < 30; i++) {
          entries['lb:$batch:$i'] = 'val';
        }
        db.batchPutStrings(entries);
      }
      expect(db.count(), greaterThanOrEqualTo(200));
    });
  });

  // ============================================================
  // PERSISTENCE: Close/reopen cycles
  // ============================================================
  group('persistence', () {
    test('write-close-reopen-verify cycle', () {
      for (var cycle = 0; cycle < 5; cycle++) {
        final db = AssemblyDB.open(_dbPath);
        db.putString('cycle', '$cycle');
        db.putString('persist:$cycle', 'yes');
        db.sync();
        db.close();

        final db2 = AssemblyDB.open(_dbPath);
        expect(db2.getString('cycle'), '$cycle');
        for (var j = 0; j <= cycle; j++) {
          expect(db2.getString('persist:$j'), 'yes');
        }
        db2.close();
      }
    });

    test('delete persists across reopen', () {
      var db = AssemblyDB.open(_dbPath);
      db.putString('delme', 'v');
      db.sync();
      db.close();

      db = AssemblyDB.open(_dbPath);
      db.deleteString('delme');
      db.sync();
      db.close();

      db = AssemblyDB.open(_dbPath);
      expect(db.getString('delme'), isNull);
      db.close();
    });

    test('transaction commit persists', () {
      var db = AssemblyDB.open(_dbPath);
      db.transaction((tx) {
        tx.putString('txp:a', '1');
        tx.putString('txp:b', '2');
      });
      db.sync();
      db.close();

      db = AssemblyDB.open(_dbPath);
      expect(db.getString('txp:a'), '1');
      expect(db.getString('txp:b'), '2');
      db.close();
    });
  });

  // ============================================================
  // EDGE CASES
  // ============================================================
  group('edge cases', () {
    test('open with default config (null)', () {
      final db = AssemblyDB.open(_dbPath);
      db.putString('test', 'val');
      expect(db.getString('test'), 'val');
      db.close();
    });

    test('get after delete after put', () {
      final db = AssemblyDB.open(_dbPath);
      db.putString('k', 'v');
      db.deleteString('k');
      db.putString('k', 'v2');
      expect(db.getString('k'), 'v2');
      db.close();
    });

    test('scan on database with deletes', () {
      final db = AssemblyDB.open(_dbPath);
      for (var i = 0; i < 10; i++) {
        db.putString('sd:$i', 'v$i');
      }
      db.sync();

      db.deleteString('sd:3');
      db.deleteString('sd:7');
      db.sync();

      final found = <String>[];
      db.scanStrings(
        start: 'sd:',
        end: 'sd:\xff',
        onEntry: (k, v) { found.add(k); return true; },
      );
      expect(found, isNot(contains('sd:3')));
      expect(found, isNot(contains('sd:7')));
      expect(found.length, 8);
      db.close();
    });

    test('batch with max-size keys and values', () {
      final db = AssemblyDB.open(_dbPath);
      final maxKey = Uint8List(62)..fillRange(0, 62, 0x41);
      final maxVal = Uint8List(254)..fillRange(0, 254, 0x42);
      db.batchPut({maxKey: maxVal});
      expect(db.get(maxKey), equals(maxVal));
      db.close();
    });
  });

  // ============================================================
  // ADVERSARIAL: Concurrency & Locking
  // ============================================================
  group('adversarial: locking', () {
    late AssemblyDB db;
    setUp(() => db = AssemblyDB.open(_dbPath));
    tearDown(() => db.close());

    test('second tx begin while first active throws LockedError', () {
      final tx = db.begin();
      expect(() => db.begin(), throwsA(isA<LockedError>()));
      tx.rollback();
    });

    test('begin succeeds after commit', () {
      final tx1 = db.begin();
      tx1.putString('k', 'v');
      tx1.commit();
      final tx2 = db.begin();
      tx2.putString('k2', 'v2');
      tx2.commit();
      expect(db.getString('k'), 'v');
      expect(db.getString('k2'), 'v2');
    });

    test('begin succeeds after rollback', () {
      final tx1 = db.begin();
      tx1.putString('k', 'v');
      tx1.rollback();
      final tx2 = db.begin();
      tx2.putString('k', 'v2');
      tx2.commit();
      expect(db.getString('k'), 'v2');
    });

    test('open same path twice throws LockedError', () {
      expect(() => AssemblyDB.open(_dbPath), throwsA(isA<LockedError>()));
    });
  });

  // ============================================================
  // ADVERSARIAL: Transaction Scan
  // ============================================================
  group('adversarial: tx scan', () {
    late AssemblyDB db;
    setUp(() {
      db = AssemblyDB.open(_dbPath);
      for (var i = 0; i < 10; i++) {
        db.putString('ts:${i.toString().padLeft(2, '0')}', 'v$i');
      }
      db.sync();
    });
    tearDown(() => db.close());

    test('tx scan all entries', () {
      final tx = db.begin();
      final entries = <String>[];
      tx.scan(onEntry: (k, v) {
        entries.add(utf8.decode(k));
        return true;
      });
      expect(entries.length, greaterThanOrEqualTo(10));
      tx.rollback();
    });

    test('tx scanAll returns list', () {
      final tx = db.begin();
      final results = tx.scanAll();
      expect(results.length, greaterThanOrEqualTo(10));
      for (final e in results) {
        expect(e, isA<ScanEntry>());
      }
      tx.rollback();
    });

    test('tx scan with boundaries', () {
      final tx = db.begin();
      final keys = <String>[];
      tx.scan(
        start: _b('ts:03'),
        end: _b('ts:07'),
        onEntry: (k, v) { keys.add(utf8.decode(k)); return true; },
      );
      expect(keys, contains('ts:03'));
      expect(keys, isNot(contains('ts:09')));
      tx.rollback();
    });

    test('tx scan with early stop', () {
      final tx = db.begin();
      var count = 0;
      tx.scan(onEntry: (_, __) {
        count++;
        return count < 3;
      });
      expect(count, 3);
      tx.rollback();
    });

    test('tx scan callback exception propagates', () {
      final tx = db.begin();
      expect(
        () => tx.scan(onEntry: (_, __) => throw ArgumentError('boom')),
        throwsArgumentError,
      );
      tx.rollback();
    });
  });

  // ============================================================
  // ADVERSARIAL: Transaction Edge Cases
  // ============================================================
  group('adversarial: tx edge cases', () {
    late AssemblyDB db;
    setUp(() => db = AssemblyDB.open(_dbPath));
    tearDown(() => db.close());

    test('empty transaction commit', () {
      final tx = db.begin();
      tx.commit();
    });

    test('empty transaction rollback', () {
      final tx = db.begin();
      tx.rollback();
    });

    test('tx overwrite same key multiple times', () {
      final tx = db.begin();
      tx.putString('ow', 'v1');
      tx.putString('ow', 'v2');
      tx.putString('ow', 'v3');
      expect(tx.getString('ow'), 'v3');
      tx.commit();
      expect(db.getString('ow'), 'v3');
    });

    test('tx read-only commit', () {
      db.putString('ro', 'val');
      final tx = db.begin();
      expect(tx.getString('ro'), 'val');
      tx.commit();
      expect(db.getString('ro'), 'val');
    });

    test('tx delete then put same key', () {
      db.putString('dp', 'original');
      final tx = db.begin();
      tx.deleteString('dp');
      expect(tx.getString('dp'), isNull);
      tx.putString('dp', 'reborn');
      expect(tx.getString('dp'), 'reborn');
      tx.commit();
      expect(db.getString('dp'), 'reborn');
    });

    test('tx put then delete then put again', () {
      final tx = db.begin();
      tx.putString('pdp', 'v1');
      tx.deleteString('pdp');
      tx.putString('pdp', 'v2');
      tx.commit();
      expect(db.getString('pdp'), 'v2');
    });

    test('transaction helper with early return value', () {
      final result = db.transaction((tx) {
        tx.putString('early', 'val');
        return 'done';
      });
      expect(result, 'done');
      expect(db.getString('early'), 'val');
    });

    test('many tx operations before commit', () {
      final tx = db.begin();
      for (var i = 0; i < 50; i++) {
        tx.putString('mtx:$i', 'v$i');
      }
      for (var i = 0; i < 50; i += 3) {
        tx.deleteString('mtx:$i');
      }
      tx.commit();
      for (var i = 0; i < 50; i++) {
        if (i % 3 == 0) {
          expect(db.getString('mtx:$i'), isNull);
        } else {
          expect(db.getString('mtx:$i'), 'v$i');
        }
      }
    });
  });

  // ============================================================
  // ADVERSARIAL: Scan Edge Cases
  // ============================================================
  group('adversarial: scan edge', () {
    late AssemblyDB db;
    setUp(() => db = AssemblyDB.open(_dbPath));
    tearDown(() => db.close());

    test('scan single entry database', () {
      db.putString('only', 'one');
      db.sync();
      final results = db.scanAll();
      expect(results.length, 1);
      expect(results.first.keyString, 'only');
    });

    test('scanStrings with empty string boundaries', () {
      db.putString('a', '1');
      db.putString('b', '2');
      db.putString('c', '3');
      db.sync();
      final keys = <String>[];
      db.scanStrings(onEntry: (k, v) { keys.add(k); return true; });
      expect(keys.length, 3);
    });

    test('scan with callback returning false on first entry', () {
      for (var i = 0; i < 5; i++) {
        db.putString('f:$i', 'v');
      }
      db.sync();
      var count = 0;
      db.scan(onEntry: (_, __) { count++; return false; });
      expect(count, 1);
    });

    test('scanAll on heavy database', () {
      for (var i = 0; i < 200; i++) {
        db.putString('heavy:${i.toString().padLeft(4, '0')}', 'data$i');
      }
      db.sync();
      final all = db.scanAll();
      expect(all.length, 200);
      final keys = all.map((e) => e.keyString).toList();
      final sorted = List<String>.from(keys)..sort();
      expect(keys, equals(sorted));
    });

    test('count matches scanAll length', () {
      for (var i = 0; i < 50; i++) {
        db.putString('cnt:$i', 'v');
      }
      db.sync();
      expect(db.count(), db.scanAll().length);
    });
  });

  // ============================================================
  // ADVERSARIAL: Error Recovery
  // ============================================================
  group('adversarial: error recovery', () {
    late AssemblyDB db;
    setUp(() => db = AssemblyDB.open(_dbPath));
    tearDown(() => db.close());

    test('database usable after key too long error', () {
      expect(() => db.put(Uint8List(100), _b('v')), throwsA(isA<KeyTooLongError>()));
      db.putString('ok', 'fine');
      expect(db.getString('ok'), 'fine');
    });

    test('database usable after value too long error', () {
      expect(() => db.put(_b('k'), Uint8List(300)), throwsA(isA<ValTooLongError>()));
      db.putString('ok2', 'fine2');
      expect(db.getString('ok2'), 'fine2');
    });

    test('database usable after scan callback exception', () {
      db.putString('pre', 'val');
      db.sync();
      expect(
        () => db.scan(onEntry: (_, __) => throw StateError('kaboom')),
        throwsStateError,
      );
      db.putString('post', 'val');
      expect(db.getString('post'), 'val');
    });

    test('database usable after tx rollback', () {
      final tx = db.begin();
      tx.putString('k', 'v');
      tx.rollback();
      db.putString('after', 'rollback');
      expect(db.getString('after'), 'rollback');
      expect(db.getString('k'), isNull);
    });

    test('catch base AssemblyDBException type', () {
      try {
        db.put(Uint8List(100), _b('v'));
        fail('should throw');
      } on AssemblyDBException catch (e) {
        expect(e.code, 4);
        expect(e, isA<KeyTooLongError>());
      }
    });
  });

  // ============================================================
  // ADVERSARIAL: Data Integrity
  // ============================================================
  group('adversarial: data integrity', () {
    test('reopen without explicit sync (WAL recovery)', () {
      var db = AssemblyDB.open(_dbPath);
      for (var i = 0; i < 20; i++) {
        db.putString('wal:$i', 'data$i');
      }
      db.close();

      db = AssemblyDB.open(_dbPath);
      for (var i = 0; i < 20; i++) {
        expect(db.getString('wal:$i'), 'data$i');
      }
      db.close();
    });

    test('tx commit without sync persists', () {
      var db = AssemblyDB.open(_dbPath);
      db.transaction((tx) {
        tx.putString('txwal:a', '1');
        tx.putString('txwal:b', '2');
      });
      db.close();

      db = AssemblyDB.open(_dbPath);
      expect(db.getString('txwal:a'), '1');
      expect(db.getString('txwal:b'), '2');
      db.close();
    });

    test('overwrite then reopen preserves latest', () {
      var db = AssemblyDB.open(_dbPath);
      db.putString('ow', 'v1');
      db.putString('ow', 'v2');
      db.putString('ow', 'v3');
      db.sync();
      db.close();

      db = AssemblyDB.open(_dbPath);
      expect(db.getString('ow'), 'v3');
      db.close();
    });

    test('delete then reopen confirms deletion', () {
      var db = AssemblyDB.open(_dbPath);
      db.putString('gone', 'bye');
      db.sync();
      db.deleteString('gone');
      db.sync();
      db.close();

      db = AssemblyDB.open(_dbPath);
      expect(db.getString('gone'), isNull);
      db.close();
    });

    test('mixed operations across reopen cycles', () {
      for (var cycle = 0; cycle < 3; cycle++) {
        final db = AssemblyDB.open(_dbPath);
        for (var i = 0; i < 20; i++) {
          db.putString('mix:${cycle * 20 + i}', 'c$cycle');
        }
        if (cycle > 0) {
          for (var i = 0; i < 5; i++) {
            db.deleteString('mix:${(cycle - 1) * 20 + i}');
          }
        }
        db.sync();
        db.close();
      }

      final db = AssemblyDB.open(_dbPath);
      expect(db.getString('mix:0'), isNull);
      expect(db.getString('mix:5'), 'c0');
      expect(db.getString('mix:40'), 'c2');
      db.close();
    });
  });

  // ============================================================
  // ADVERSARIAL: Multiple Databases
  // ============================================================
  group('adversarial: multi-db', () {
    test('two databases at different paths', () {
      final path2 = '${_tmpDir.path}/testdb2';
      final db1 = AssemblyDB.open(_dbPath);
      final db2 = AssemblyDB.open(path2);

      db1.putString('db', '1');
      db2.putString('db', '2');

      expect(db1.getString('db'), '1');
      expect(db2.getString('db'), '2');

      expect(db1.getString('db'), isNot(equals(db2.getString('db'))));

      db1.close();
      db2.close();
      AssemblyDB.destroy(path2);
    });
  });

  // ============================================================
  // ADVERSARIAL: Backup Edge Cases
  // ============================================================
  group('adversarial: backup', () {
    test('backup with data then restore to new path', () {
      final db = AssemblyDB.open(_dbPath);
      db.putString('bkdata', 'yes');
      db.sync();
      final bp = '${_tmpDir.path}/data_backup';
      db.backup(bp);

      AssemblyDB.restore(bp, '${_tmpDir.path}/data_restored');
      final db2 = AssemblyDB.open('${_tmpDir.path}/data_restored');
      expect(db2.getString('bkdata'), 'yes');
      db2.close();
      db.close();
      AssemblyDB.destroy(bp);
      AssemblyDB.destroy('${_tmpDir.path}/data_restored');
    });

    test('backup after deletes preserves state', () {
      final db = AssemblyDB.open(_dbPath);
      for (var i = 0; i < 10; i++) {
        db.putString('bd:$i', 'v$i');
      }
      db.deleteString('bd:3');
      db.deleteString('bd:7');
      db.sync();
      final bp = '${_tmpDir.path}/del_backup';
      db.backup(bp);

      AssemblyDB.restore(bp, '${_tmpDir.path}/del_restored');
      final db2 = AssemblyDB.open('${_tmpDir.path}/del_restored');
      expect(db2.getString('bd:3'), isNull);
      expect(db2.getString('bd:7'), isNull);
      expect(db2.getString('bd:0'), 'v0');
      expect(db2.getString('bd:9'), 'v9');
      db2.close();
      db.close();
      AssemblyDB.destroy(bp);
      AssemblyDB.destroy('${_tmpDir.path}/del_restored');
    });
  });

  // ============================================================
  // ADVERSARIAL: Metrics Deep
  // ============================================================
  group('adversarial: metrics', () {
    late AssemblyDB db;
    setUp(() => db = AssemblyDB.open(_dbPath));
    tearDown(() => db.close());

    test('all metric fields non-negative', () {
      db.putString('m', 'v');
      db.getString('m');
      db.sync();
      final m = db.metrics;
      expect(m.putsTotal, greaterThanOrEqualTo(0));
      expect(m.getsTotal, greaterThanOrEqualTo(0));
      expect(m.deletesTotal, greaterThanOrEqualTo(0));
      expect(m.scansTotal, greaterThanOrEqualTo(0));
      expect(m.cacheHits, greaterThanOrEqualTo(0));
      expect(m.cacheMisses, greaterThanOrEqualTo(0));
      expect(m.bytesWritten, greaterThanOrEqualTo(0));
      expect(m.bytesRead, greaterThanOrEqualTo(0));
      expect(m.walSyncs, greaterThanOrEqualTo(0));
      expect(m.txCommits, greaterThanOrEqualTo(0));
      expect(m.txRollbacks, greaterThanOrEqualTo(0));
      expect(m.cacheHitRate, greaterThanOrEqualTo(0.0));
      expect(m.cacheHitRate, lessThanOrEqualTo(1.0));
    });

    test('metrics after heavy mixed operations', () {
      for (var i = 0; i < 100; i++) {
        db.putString('hm:$i', 'v$i');
      }
      for (var i = 0; i < 50; i++) {
        db.getString('hm:$i');
      }
      for (var i = 0; i < 20; i++) {
        db.deleteString('hm:$i');
      }
      db.sync();
      db.scan(onEntry: (_, __) => true);

      final tx = db.begin();
      tx.putString('htx', 'v');
      tx.commit();

      final m = db.metrics;
      expect(m.putsTotal, greaterThanOrEqualTo(100));
      expect(m.getsTotal, greaterThanOrEqualTo(50));
      expect(m.deletesTotal, greaterThanOrEqualTo(20));
      expect(m.scansTotal, greaterThanOrEqualTo(1));
      expect(m.txCommits, greaterThanOrEqualTo(1));
      expect(m.walSyncs, greaterThanOrEqualTo(0));
    });

    test('metrics toString contains all key counters', () {
      db.putString('x', 'y');
      final s = db.metrics.toString();
      expect(s, contains('puts='));
      expect(s, contains('gets='));
      expect(s, contains('deletes='));
      expect(s, contains('scans='));
      expect(s, contains('txCommits='));
      expect(s, contains('cacheHitRate='));
    });

    test('scanEntry toString', () {
      final e = ScanEntry(_b('key'), _b('value'));
      expect(e.toString(), contains('keyLen=3'));
      expect(e.toString(), contains('valLen=5'));
    });
  });

  // ============================================================
  // ADVERSARIAL: Config Edge Cases
  // ============================================================
  group('adversarial: config', () {
    test('config with minimal cache', () {
      final db = AssemblyDB.open(_dbPath, config: const AssemblyDBConfig(
        cacheSizePages: 1,
      ));
      db.putString('k', 'v');
      expect(db.getString('k'), 'v');
      db.close();
    });

    test('config with large cache', () {
      final db = AssemblyDB.open(_dbPath, config: const AssemblyDBConfig(
        cacheSizePages: 1024,
      ));
      db.putString('k', 'v');
      expect(db.getString('k'), 'v');
      db.close();
    });

    test('config with custom memtable size', () {
      final db = AssemblyDB.open(_dbPath, config: const AssemblyDBConfig(
        memtableMaxBytes: 4096,
      ));
      for (var i = 0; i < 50; i++) {
        db.putString('mt:$i', 'v$i');
      }
      for (var i = 0; i < 50; i++) {
        expect(db.getString('mt:$i'), 'v$i');
      }
      db.close();
    });

    test('all wal sync modes work', () {
      for (final mode in WalSyncMode.values) {
        final path = '${_tmpDir.path}/walmode_${mode.name}';
        final db = AssemblyDB.open(path, config: AssemblyDBConfig(
          walSyncMode: mode,
        ));
        db.putString('k', 'v');
        expect(db.getString('k'), 'v');
        db.sync();
        db.close();
        AssemblyDB.destroy(path);
      }
    });
  });

  // ============================================================
  // REAL-WORLD: IoT Sensor Data (time-series pattern)
  // ============================================================
  group('real-world: IoT sensor', () {
    late AssemblyDB db;
    setUp(() => db = AssemblyDB.open(_dbPath));
    tearDown(() => db.close());

    test('rapid sensor ingestion and range query', () {
      for (var t = 1000; t < 1100; t++) {
        db.putString('sensor:temp:$t', '${20.0 + (t % 10) * 0.5}');
        db.putString('sensor:hum:$t', '${40 + t % 20}');
      }
      db.sync();

      final temps = <String, String>{};
      db.scanStrings(
        start: 'sensor:temp:1050',
        end: 'sensor:temp:1060',
        onEntry: (k, v) { temps[k] = v; return true; },
      );
      expect(temps.length, greaterThanOrEqualTo(5));
      expect(temps['sensor:temp:1050'], isNotNull);
    });

    test('sensor data overwrites (latest value wins)', () {
      for (var round = 0; round < 5; round++) {
        db.putString('sensor:current', '${round * 10 + 5}');
      }
      expect(db.getString('sensor:current'), '45');
    });
  });

  // ============================================================
  // REAL-WORLD: Chat Message Store
  // ============================================================
  group('real-world: chat messages', () {
    late AssemblyDB db;
    setUp(() => db = AssemblyDB.open(_dbPath));
    tearDown(() => db.close());

    test('store and retrieve messages by channel', () {
      db.batchPutStrings({
        'chat:general:001': 'Hello everyone',
        'chat:general:002': 'Hi there!',
        'chat:general:003': 'How are you?',
        'chat:dev:001': 'Build failed',
        'chat:dev:002': 'Fixed it!',
      });
      db.sync();

      final general = <String>[];
      db.scanStrings(
        start: 'chat:general:',
        end: 'chat:general:\xff',
        onEntry: (k, v) { general.add(v); return true; },
      );
      expect(general.length, 3);
      expect(general.first, 'Hello everyone');

      final dev = <String>[];
      db.scanStrings(
        start: 'chat:dev:',
        end: 'chat:dev:\xff',
        onEntry: (k, v) { dev.add(v); return true; },
      );
      expect(dev.length, 2);
    });

    test('delete message and verify scan', () {
      for (var i = 0; i < 5; i++) {
        db.putString('msg:$i', 'text$i');
      }
      db.sync();
      db.deleteString('msg:2');
      db.sync();

      final msgs = db.scanAll();
      final keys = msgs.map((e) => e.keyString).toList();
      expect(keys, isNot(contains('msg:2')));
      expect(keys.length, 4);
    });
  });

  // ============================================================
  // REAL-WORLD: User Profile CRUD
  // ============================================================
  group('real-world: user profiles', () {
    late AssemblyDB db;
    setUp(() => db = AssemblyDB.open(_dbPath));
    tearDown(() => db.close());

    test('create, update, delete user with cascading keys', () {
      db.transaction((tx) {
        tx.putString('user:42:name', 'Santo');
        tx.putString('user:42:email', 'santo@example.com');
        tx.putString('user:42:role', 'admin');
      });

      expect(db.getString('user:42:name'), 'Santo');

      db.transaction((tx) {
        tx.putString('user:42:email', 'newemail@example.com');
      });
      expect(db.getString('user:42:email'), 'newemail@example.com');

      db.transaction((tx) {
        tx.deleteString('user:42:name');
        tx.deleteString('user:42:email');
        tx.deleteString('user:42:role');
      });
      expect(db.getString('user:42:name'), isNull);
      expect(db.getString('user:42:email'), isNull);
      expect(db.getString('user:42:role'), isNull);
    });

    test('list all users via prefix scan', () {
      db.batchPutStrings({
        'user:1:name': 'Alice',
        'user:2:name': 'Bob',
        'user:3:name': 'Charlie',
      });
      db.sync();

      final names = <String>[];
      db.scanStrings(
        start: 'user:',
        end: 'user:\xff',
        onEntry: (k, v) {
          if (k.endsWith(':name')) names.add(v);
          return true;
        },
      );
      expect(names, containsAll(['Alice', 'Bob', 'Charlie']));
    });
  });

  // ============================================================
  // PERFORMANCE BENCHMARKS
  // ============================================================
  group('performance', () {
    late AssemblyDB db;
    setUp(() => db = AssemblyDB.open(_dbPath));
    tearDown(() => db.close());

    test('put throughput > 10K ops/sec', () {
      final sw = Stopwatch()..start();
      const n = 2000;
      for (var i = 0; i < n; i++) {
        db.putString('perf:$i', 'v$i');
      }
      sw.stop();
      final opsPerSec = n * 1000000 / sw.elapsedMicroseconds;
      expect(opsPerSec, greaterThan(10000),
          reason: 'put: ${opsPerSec.toStringAsFixed(0)} ops/s');
    });

    test('get throughput > 50K ops/sec', () {
      for (var i = 0; i < 500; i++) {
        db.putString('gp:$i', 'v$i');
      }

      final sw = Stopwatch()..start();
      const n = 2000;
      for (var i = 0; i < n; i++) {
        db.getString('gp:${i % 500}');
      }
      sw.stop();
      final opsPerSec = n * 1000000 / sw.elapsedMicroseconds;
      expect(opsPerSec, greaterThan(50000),
          reason: 'get: ${opsPerSec.toStringAsFixed(0)} ops/s');
    });

    test('batch throughput > 50K entries/sec', () {
      final sw = Stopwatch()..start();
      const batches = 20;
      const perBatch = 30;
      for (var b = 0; b < batches; b++) {
        final entries = <String, String>{};
        for (var i = 0; i < perBatch; i++) {
          entries['bp:${b * perBatch + i}'] = 'val';
        }
        db.batchPutStrings(entries);
      }
      sw.stop();
      final total = batches * perBatch;
      final opsPerSec = total * 1000000 / sw.elapsedMicroseconds;
      expect(opsPerSec, greaterThan(50000),
          reason: 'batch: ${opsPerSec.toStringAsFixed(0)} entries/s');
    });

    test('scan throughput > 100K entries/sec', () {
      for (var i = 0; i < 500; i++) {
        db.putString('sp:${i.toString().padLeft(4, '0')}', 'v$i');
      }
      db.sync();

      final sw = Stopwatch()..start();
      var scanned = 0;
      for (var round = 0; round < 10; round++) {
        db.scan(onEntry: (_, __) { scanned++; return true; });
      }
      sw.stop();
      final opsPerSec = scanned * 1000000 / sw.elapsedMicroseconds;
      expect(opsPerSec, greaterThan(100000),
          reason: 'scan: ${opsPerSec.toStringAsFixed(0)} entries/s ($scanned total)');
    });

    test('tx commit throughput > 5K tx/sec', () {
      final sw = Stopwatch()..start();
      const n = 200;
      for (var i = 0; i < n; i++) {
        db.transaction((tx) {
          tx.putString('txp:$i', 'v');
        });
      }
      sw.stop();
      final opsPerSec = n * 1000000 / sw.elapsedMicroseconds;
      expect(opsPerSec, greaterThan(5000),
          reason: 'tx: ${opsPerSec.toStringAsFixed(0)} tx/s');
    });

    test('mixed workload throughput', () {
      final sw = Stopwatch()..start();
      const n = 500;
      for (var i = 0; i < n; i++) {
        db.putString('mixed:$i', 'v$i');
        if (i > 0 && i % 5 == 0) {
          db.getString('mixed:${i - 3}');
        }
        if (i > 10 && i % 10 == 0) {
          db.deleteString('mixed:${i - 10}');
        }
      }
      sw.stop();
      final opsPerSec = n * 1000000 / sw.elapsedMicroseconds;
      expect(opsPerSec, greaterThan(10000),
          reason: 'mixed: ${opsPerSec.toStringAsFixed(0)} ops/s');
    });
  });

  // ============================================================
  // STRESS: Heavy Reopen
  // ============================================================
  group('stress: reopen', () {
    test('rapid open-close cycles', () {
      for (var i = 0; i < 20; i++) {
        final db = AssemblyDB.open(_dbPath);
        db.putString('cycle_$i', 'v$i');
        db.close();
      }
      final db = AssemblyDB.open(_dbPath);
      expect(db.getString('cycle_19'), 'v19');
      db.close();
    });

    test('open-write-sync-close-verify x10', () {
      for (var i = 0; i < 10; i++) {
        final db = AssemblyDB.open(_dbPath);
        for (var j = 0; j < 20; j++) {
          db.putString('rw:${i * 20 + j}', 'v');
        }
        db.sync();
        db.close();
      }
      final db = AssemblyDB.open(_dbPath);
      expect(db.count(), greaterThanOrEqualTo(200));
      db.close();
    });
  });

  // ============================================================
  // ADVERSARIAL: Uint8List views & sublist safety
  // ============================================================
  group('adversarial: byte views', () {
    late AssemblyDB db;
    setUp(() => db = AssemblyDB.open(_dbPath));
    tearDown(() => db.close());

    test('put with Uint8List.sublistView', () {
      final full = Uint8List.fromList([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
      final key = Uint8List.sublistView(full, 2, 5);
      final val = Uint8List.sublistView(full, 5, 9);
      db.put(key, val);
      final got = db.get(Uint8List.fromList([2, 3, 4]));
      expect(got, isNotNull);
      expect(got, equals(Uint8List.fromList([5, 6, 7, 8])));
    });

    test('batch with Uint8List views', () {
      final buf = Uint8List.fromList(List.generate(20, (i) => i));
      db.batchPut({
        Uint8List.sublistView(buf, 0, 3): Uint8List.sublistView(buf, 3, 6),
        Uint8List.sublistView(buf, 6, 9): Uint8List.sublistView(buf, 9, 12),
      });
      expect(db.get(Uint8List.fromList([0, 1, 2])),
          equals(Uint8List.fromList([3, 4, 5])));
      expect(db.get(Uint8List.fromList([6, 7, 8])),
          equals(Uint8List.fromList([9, 10, 11])));
    });

    test('get returns independent copy (not a view into native memory)', () {
      db.put(_b('copy'), _b('data'));
      final v1 = db.get(_b('copy'))!;
      final v2 = db.get(_b('copy'))!;
      expect(v1, equals(v2));
      v1[0] = 0xFF;
      expect(v2[0], isNot(0xFF));
    });

    test('scan returns independent copies', () {
      db.putString('sc:1', 'aaa');
      db.putString('sc:2', 'bbb');
      db.sync();
      final entries = db.scanAll();
      expect(entries.length, 2);
      entries.first.key[0] = 0xFF;
      final entries2 = db.scanAll();
      expect(entries2.first.key[0], isNot(0xFF));
    });

    test('empty value round-trip', () {
      db.put(_b('empty_val'), Uint8List(0));
      final got = db.get(_b('empty_val'));
      expect(got, isNotNull);
      expect(got!.length, 0);
    });

    test('all-zero key and value', () {
      final key = Uint8List(5);
      final val = Uint8List(10);
      db.put(key, val);
      final got = db.get(key);
      expect(got, isNotNull);
      expect(got, equals(val));
    });

    test('all-0xFF key and value', () {
      final key = Uint8List(8)..fillRange(0, 8, 0xFF);
      final val = Uint8List(16)..fillRange(0, 16, 0xFF);
      db.put(key, val);
      expect(db.get(key), equals(val));
    });
  });

  // ============================================================
  // ADVERSARIAL: Explicit library path loading
  // ============================================================
  group('adversarial: library loading', () {
    test('open with explicit libraryPath', () {
      final path = '${Directory.current.path}/libassemblydb.so';
      if (!File(path).existsSync()) return;

      final db = AssemblyDB.open(
        _dbPath,
        libraryPath: path,
      );
      db.putString('explicit', 'lib');
      expect(db.getString('explicit'), 'lib');
      db.close();
    });

    test('destroy with explicit libraryPath', () {
      final db = AssemblyDB.open(_dbPath);
      db.putString('k', 'v');
      db.sync();
      db.close();

      final path = '${Directory.current.path}/libassemblydb.so';
      if (!File(path).existsSync()) return;

      AssemblyDB.destroy(_dbPath, libraryPath: path);
      final db2 = AssemblyDB.open(_dbPath);
      expect(db2.getString('k'), isNull);
      db2.close();
    });
  });

  // ============================================================
  // ADVERSARIAL: Rapid sequential tx commit/rollback
  // ============================================================
  group('adversarial: tx rapid cycling', () {
    late AssemblyDB db;
    setUp(() => db = AssemblyDB.open(_dbPath));
    tearDown(() => db.close());

    test('100 rapid commit cycles', () {
      for (var i = 0; i < 100; i++) {
        db.transaction((tx) {
          tx.putString('rc:$i', 'v$i');
        });
      }
      for (var i = 0; i < 100; i++) {
        expect(db.getString('rc:$i'), 'v$i');
      }
    });

    test('alternating commit/rollback', () {
      for (var i = 0; i < 50; i++) {
        final tx = db.begin();
        tx.putString('alt:$i', 'v$i');
        if (i.isEven) {
          tx.commit();
        } else {
          tx.rollback();
        }
      }
      for (var i = 0; i < 50; i++) {
        if (i.isEven) {
          expect(db.getString('alt:$i'), 'v$i');
        } else {
          expect(db.getString('alt:$i'), isNull);
        }
      }
    });

    test('tx exception does not corrupt db state', () {
      db.putString('safe', 'initial');
      for (var i = 0; i < 20; i++) {
        try {
          db.transaction((tx) {
            tx.putString('safe', 'attempt_$i');
            if (i.isOdd) throw FormatException('fail_$i');
          });
        } on FormatException catch (_) {}
      }
      final val = db.getString('safe')!;
      expect(val, startsWith('attempt_'));
      expect(int.parse(val.split('_')[1]).isEven, isTrue);
    });
  });

  // ============================================================
  // REAL-WORLD: Inventory Management
  // ============================================================
  group('real-world: inventory', () {
    late AssemblyDB db;
    setUp(() => db = AssemblyDB.open(_dbPath));
    tearDown(() => db.close());

    test('stock level tracking with transactions', () {
      db.putString('inv:SKU001:qty', '100');
      db.putString('inv:SKU001:name', 'Widget');

      db.transaction((tx) {
        final qty = int.parse(tx.getString('inv:SKU001:qty')!);
        tx.putString('inv:SKU001:qty', '${qty - 15}');
      });

      expect(db.getString('inv:SKU001:qty'), '85');
    });

    test('batch add new products', () {
      final products = <String, String>{};
      for (var i = 0; i < 20; i++) {
        products['inv:P$i:name'] = 'Product $i';
        products['inv:P$i:qty'] = '${(i + 1) * 10}';
        products['inv:P$i:price'] = '${(i + 1) * 9.99}';
      }
      db.batchPutStrings(products);

      db.sync();
      final items = <String>[];
      db.scanStrings(
        start: 'inv:P',
        end: 'inv:P\xff',
        onEntry: (k, v) {
          if (k.endsWith(':name')) items.add(v);
          return true;
        },
      );
      expect(items.length, 20);
    });

    test('audit trail with ordered keys', () {
      for (var i = 0; i < 10; i++) {
        final ts = (1000000 + i).toString();
        db.putString('audit:$ts', '{"action":"update","user":"admin"}');
      }
      db.sync();

      final events = db.scanAll();
      final keys = events.map((e) => e.keyString).toList();
      final sorted = List<String>.from(keys)..sort();
      expect(keys, equals(sorted));
    });
  });

  // ============================================================
  // STRESS: Mixed High-Volume
  // ============================================================
  group('stress: high volume', () {
    late AssemblyDB db;
    setUp(() => db = AssemblyDB.open(_dbPath));
    tearDown(() => db.close());

    test('2000 puts + scan + delete half + verify', () {
      for (var i = 0; i < 2000; i++) {
        db.putString('hv:${i.toString().padLeft(5, '0')}', 'data$i');
      }
      db.sync();

      expect(db.count(), 2000);

      for (var i = 0; i < 1000; i++) {
        db.deleteString('hv:${(i * 2).toString().padLeft(5, '0')}');
      }
      db.sync();

      var remaining = 0;
      db.scan(onEntry: (_, __) { remaining++; return true; });
      expect(remaining, 1000);

      for (var i = 0; i < 2000; i++) {
        final key = 'hv:${i.toString().padLeft(5, '0')}';
        if (i.isEven) {
          expect(db.getString(key), isNull);
        } else {
          expect(db.getString(key), 'data$i');
        }
      }
    });

    test('batch + tx + scan combined workflow', () {
      db.batchPutStrings({
        for (var i = 0; i < 100; i++)
          'combo:${i.toString().padLeft(3, '0')}': 'init$i',
      });
      db.sync();

      for (var round = 0; round < 5; round++) {
        db.transaction((tx) {
          for (var j = 0; j < 10; j++) {
            final idx = round * 10 + j;
            tx.putString('combo:${idx.toString().padLeft(3, '0')}', 'updated_r$round');
          }
        });
      }

      final updated = <String>[];
      db.scanStrings(
        start: 'combo:',
        end: 'combo:\xff',
        onEntry: (k, v) {
          if (v.startsWith('updated_')) updated.add(k);
          return true;
        },
      );
      expect(updated.length, 50);
    });
  });

  // ============================================================
  // BOUNDARY VALUES: Key/Value size limits
  // ============================================================
  group('boundary: key sizes', () {
    late AssemblyDB db;
    setUp(() => db = AssemblyDB.open(_dbPath));
    tearDown(() => db.close());

    test('key 1 byte works', () {
      db.put(Uint8List.fromList([0x41]), _b('v'));
      expect(db.get(Uint8List.fromList([0x41])), isNotNull);
    });

    test('key 61 bytes works', () {
      final key = Uint8List(61)..fillRange(0, 61, 0x42);
      db.put(key, _b('v'));
      expect(db.get(key), isNotNull);
    });

    test('key 62 bytes works (max)', () {
      final key = Uint8List(62)..fillRange(0, 62, 0x43);
      db.put(key, _b('v'));
      expect(db.get(key), isNotNull);
    });

    test('key 63 bytes throws', () {
      final key = Uint8List(63);
      expect(() => db.put(key, _b('v')), throwsA(isA<KeyTooLongError>()));
    });

    test('key 64 bytes throws', () {
      final key = Uint8List(64);
      expect(() => db.put(key, _b('v')), throwsA(isA<KeyTooLongError>()));
    });

    test('key 100 bytes throws', () {
      final key = Uint8List(100);
      expect(() => db.put(key, _b('v')), throwsA(isA<KeyTooLongError>()));
    });

    test('utf8 string key at byte boundary', () {
      final key20 = 'a' * 20;
      expect(utf8.encode(key20).length, 20);
      db.putString(key20, 'v');
      expect(db.getString(key20), 'v');
    });

    test('utf8 multibyte string exceeding 62 bytes throws', () {
      final key = '\u00e9' * 32;
      expect(utf8.encode(key).length, 64);
      expect(() => db.putString(key, 'v'), throwsA(isA<KeyTooLongError>()));
    });
  });

  group('boundary: value sizes', () {
    late AssemblyDB db;
    setUp(() => db = AssemblyDB.open(_dbPath));
    tearDown(() => db.close());

    test('value 0 bytes works', () {
      db.put(_b('k'), Uint8List(0));
      expect(db.get(_b('k'))!.length, 0);
    });

    test('value 1 byte works', () {
      db.put(_b('k'), Uint8List.fromList([0xFF]));
      expect(db.get(_b('k')), equals(Uint8List.fromList([0xFF])));
    });

    test('value 253 bytes works', () {
      final val = Uint8List(253)..fillRange(0, 253, 0x44);
      db.put(_b('k'), val);
      expect(db.get(_b('k')), equals(val));
    });

    test('value 254 bytes works (max)', () {
      final val = Uint8List(254)..fillRange(0, 254, 0x45);
      db.put(_b('k'), val);
      expect(db.get(_b('k')), equals(val));
    });

    test('value 255 bytes throws', () {
      final val = Uint8List(255);
      expect(() => db.put(_b('k'), val), throwsA(isA<ValTooLongError>()));
    });

    test('value 256 bytes throws', () {
      final val = Uint8List(256);
      expect(() => db.put(_b('k'), val), throwsA(isA<ValTooLongError>()));
    });

    test('value 1000 bytes throws', () {
      final val = Uint8List(1000);
      expect(() => db.put(_b('k'), val), throwsA(isA<ValTooLongError>()));
    });

    test('max key + max value together', () {
      final key = Uint8List(62)..fillRange(0, 62, 0x46);
      final val = Uint8List(254)..fillRange(0, 254, 0x47);
      db.put(key, val);
      expect(db.get(key), equals(val));
    });
  });

  // ============================================================
  // ADVERSARIAL: Batch error handling
  // ============================================================
  group('adversarial: batch errors', () {
    late AssemblyDB db;
    setUp(() => db = AssemblyDB.open(_dbPath));
    tearDown(() => db.close());

    test('batch with oversized key rejects', () {
      expect(
        () => db.batchPut({Uint8List(100): _b('v')}),
        throwsA(isA<KeyTooLongError>()),
      );
    });

    test('batch with oversized value rejects', () {
      expect(
        () => db.batchPut({_b('k'): Uint8List(300)}),
        throwsA(isA<ValTooLongError>()),
      );
    });

    test('batch single entry works', () {
      db.batchPut({_b('single'): _b('entry')});
      expect(_s(db.get(_b('single'))!), 'entry');
    });

    test('batchPutStrings single entry', () {
      db.batchPutStrings({'one': 'two'});
      expect(db.getString('one'), 'two');
    });
  });

  // ============================================================
  // ADVERSARIAL: Destroy edge cases
  // ============================================================
  group('adversarial: destroy', () {
    test('destroy then reuse same path', () {
      var db = AssemblyDB.open(_dbPath);
      db.putString('before', 'destroy');
      db.sync();
      db.close();

      AssemblyDB.destroy(_dbPath);

      db = AssemblyDB.open(_dbPath);
      expect(db.getString('before'), isNull);
      db.putString('after', 'destroy');
      expect(db.getString('after'), 'destroy');
      db.close();
    });

    test('destroy twice on same path', () {
      final db = AssemblyDB.open(_dbPath);
      db.putString('k', 'v');
      db.sync();
      db.close();

      AssemblyDB.destroy(_dbPath);
      AssemblyDB.destroy(_dbPath);
    });
  });

  // ============================================================
  // ADVERSARIAL: Metrics edge cases
  // ============================================================
  group('adversarial: metrics edge', () {
    test('cacheHitRate zero when no gets', () {
      final db = AssemblyDB.open(_dbPath);
      db.putString('k', 'v');
      final m = db.metrics;
      expect(m.cacheHitRate, 0.0);
      db.close();
    });

    test('metrics unchanged after close+reopen', () {
      var db = AssemblyDB.open(_dbPath);
      for (var i = 0; i < 10; i++) {
        db.putString('mk:$i', 'v$i');
      }
      db.sync();
      db.close();

      db = AssemblyDB.open(_dbPath);
      final m = db.metrics;
      expect(m.putsTotal, 0);
      db.close();
    });
  });

  // ============================================================
  // ADVERSARIAL: ScanEntry edge cases
  // ============================================================
  group('adversarial: scan entry', () {
    test('scanEntry with empty key', () {
      final e = ScanEntry(Uint8List(0), _b('val'));
      expect(e.key.length, 0);
      expect(e.keyString, '');
    });

    test('scanEntry with empty value', () {
      final e = ScanEntry(_b('key'), Uint8List(0));
      expect(e.value.length, 0);
      expect(e.valueString, '');
    });

    test('scanEntry hashCode consistency', () {
      final e1 = ScanEntry(_b('a'), _b('b'));
      final e2 = ScanEntry(_b('a'), _b('b'));
      expect(e1.hashCode, e2.hashCode);

      final s = <ScanEntry>{e1, e2};
      expect(s.length, 1);
    });

    test('scanEntry inequality', () {
      final e1 = ScanEntry(_b('a'), _b('b'));
      final e2 = ScanEntry(_b('a'), _b('c'));
      final e3 = ScanEntry(_b('x'), _b('b'));
      expect(e1, isNot(equals(e2)));
      expect(e1, isNot(equals(e3)));
      expect(e1, isNot(equals('not a ScanEntry')));
    });
  });

  // ============================================================
  // ADVERSARIAL: Rapid put/get on same key
  // ============================================================
  group('adversarial: rapid same-key', () {
    late AssemblyDB db;
    setUp(() => db = AssemblyDB.open(_dbPath));
    tearDown(() => db.close());

    test('500 overwrites same key', () {
      for (var i = 0; i < 500; i++) {
        db.putString('rapid', 'v$i');
      }
      expect(db.getString('rapid'), 'v499');
    });

    test('put-delete-put-delete cycle', () {
      for (var i = 0; i < 100; i++) {
        db.putString('toggle', 'on$i');
        expect(db.getString('toggle'), 'on$i');
        db.deleteString('toggle');
        expect(db.getString('toggle'), isNull);
      }
    });

    test('exists during rapid mutations', () {
      for (var i = 0; i < 100; i++) {
        db.putString('ex', 'v');
        expect(db.existsString('ex'), isTrue);
        db.deleteString('ex');
        expect(db.existsString('ex'), isFalse);
      }
    });
  });

  // ============================================================
  // ADVERSARIAL: Transaction with isolation levels
  // ============================================================
  group('adversarial: isolation levels', () {
    late AssemblyDB db;
    setUp(() => db = AssemblyDB.open(_dbPath));
    tearDown(() => db.close());

    test('snapshot isolation (default)', () {
      db.transaction((tx) {
        tx.putString('iso', 'snapshot');
      }, isolation: IsolationLevel.snapshot);
      expect(db.getString('iso'), 'snapshot');
    });

    test('serializable isolation', () {
      db.transaction((tx) {
        tx.putString('iso', 'serial');
      }, isolation: IsolationLevel.serializable);
      expect(db.getString('iso'), 'serial');
    });

    test('read committed isolation', () {
      db.transaction((tx) {
        tx.putString('iso', 'rc');
      }, isolation: IsolationLevel.readCommitted);
      expect(db.getString('iso'), 'rc');
    });
  });

  // ============================================================
  // STRESS: 50 reopen cycles
  // ============================================================
  group('stress: heavy reopen', () {
    test('50 open-write-close cycles', () {
      for (var i = 0; i < 50; i++) {
        final db = AssemblyDB.open(_dbPath);
        db.putString('hr:$i', 'v$i');
        db.close();
      }
      final db = AssemblyDB.open(_dbPath);
      expect(db.getString('hr:49'), 'v49');
      expect(db.getString('hr:0'), 'v0');
      db.close();
    });
  });

  // ============================================================
  // STRESS: Backup large dataset
  // ============================================================
  group('stress: backup large', () {
    test('backup 500 entries and restore', () {
      final db = AssemblyDB.open(_dbPath);
      for (var i = 0; i < 500; i++) {
        db.putString('bk:${i.toString().padLeft(4, '0')}', 'data_$i');
      }
      db.sync();

      final bp = '${_tmpDir.path}/large_backup';
      final rp = '${_tmpDir.path}/large_restored';
      db.backup(bp);
      db.close();

      AssemblyDB.restore(bp, rp);
      final db2 = AssemblyDB.open(rp);
      expect(db2.getString('bk:0000'), 'data_0');
      expect(db2.getString('bk:0499'), 'data_499');
      expect(db2.count(), 500);
      db2.close();

      AssemblyDB.destroy(bp);
      AssemblyDB.destroy(rp);
    });
  });

  // ============================================================
  // ADVERSARIAL: Error type hierarchy
  // ============================================================
  group('adversarial: error hierarchy', () {
    test('all error types are AssemblyDBException', () {
      final errors = [
        const NotFoundError(),
        const IOError(),
        const CorruptError(),
        const KeyTooLongError(),
        const ValTooLongError(),
        const LockedError(),
        const OutOfMemoryError(),
        const InvalidError(),
        const TxConflictError(),
        const TxNotFoundError(),
        const TxAbortedError(),
        const FullError(),
        const ExistsError(),
        const DecryptError(),
        const CompressError(),
        const AssemblyDBUnknownError(999),
      ];
      for (final e in errors) {
        expect(e, isA<AssemblyDBException>());
        expect(e, isA<Exception>());
        expect(e.code, isNonNegative);
        expect(e.message, isNotEmpty);
        expect(e.toString(), contains(e.code.toString()));
      }
    });

    test('unknown error preserves code', () {
      final e = AssemblyDBException.fromCode(42);
      expect(e, isA<AssemblyDBUnknownError>());
      expect(e.code, 42);
      expect(e.message, 'Unknown error');
    });

    test('error codes are unique', () {
      final codes = <int>{};
      for (var i = 1; i <= 15; i++) {
        final e = AssemblyDBException.fromCode(i);
        expect(codes.add(e.code), isTrue);
      }
    });
  });

  // ============================================================
  // ADVERSARIAL: Scan ordering after mixed ops
  // ============================================================
  group('adversarial: scan ordering', () {
    late AssemblyDB db;
    setUp(() => db = AssemblyDB.open(_dbPath));
    tearDown(() => db.close());

    test('scan order correct after deletes in middle', () {
      for (var i = 0; i < 20; i++) {
        db.putString('ord:${i.toString().padLeft(2, '0')}', 'v$i');
      }
      db.sync();

      for (var i = 5; i < 15; i++) {
        db.deleteString('ord:${i.toString().padLeft(2, '0')}');
      }
      db.sync();

      final keys = <String>[];
      db.scanStrings(
        start: 'ord:',
        end: 'ord:\xff',
        onEntry: (k, v) { keys.add(k); return true; },
      );
      expect(keys.length, 10);
      final sorted = List<String>.from(keys)..sort();
      expect(keys, equals(sorted));
    });

    test('scan order correct after overwrites', () {
      for (var i = 0; i < 10; i++) {
        db.putString('ow:$i', 'first');
      }
      for (var i = 9; i >= 0; i--) {
        db.putString('ow:$i', 'second');
      }
      db.sync();

      final entries = <String, String>{};
      db.scanStrings(
        start: 'ow:',
        end: 'ow:\xff',
        onEntry: (k, v) { entries[k] = v; return true; },
      );
      expect(entries.length, 10);
      for (final v in entries.values) {
        expect(v, 'second');
      }
    });
  });

  // ============================================================
  // HARDENING: tx-after-close, validation, double-failure
  // ============================================================
  group('hardening: tx after db close', () {
    test('tx put after db.close throws StateError', () {
      final db = AssemblyDB.open(_dbPath);
      final tx = db.begin();
      db.close();
      expect(() => tx.put(_b('k'), _b('v')), throwsStateError);
    });

    test('tx get after db.close throws StateError', () {
      final db = AssemblyDB.open(_dbPath);
      final tx = db.begin();
      db.close();
      expect(() => tx.get(_b('k')), throwsStateError);
    });

    test('tx delete after db.close throws StateError', () {
      final db = AssemblyDB.open(_dbPath);
      final tx = db.begin();
      db.close();
      expect(() => tx.delete(_b('k')), throwsStateError);
    });

    test('tx commit after db.close throws StateError', () {
      final db = AssemblyDB.open(_dbPath);
      final tx = db.begin();
      db.close();
      expect(() => tx.commit(), throwsStateError);
    });

    test('tx rollback after db.close throws StateError', () {
      final db = AssemblyDB.open(_dbPath);
      final tx = db.begin();
      db.close();
      expect(() => tx.rollback(), throwsStateError);
    });

    test('tx scan after db.close throws StateError', () {
      final db = AssemblyDB.open(_dbPath);
      final tx = db.begin();
      db.close();
      expect(() => tx.scan(onEntry: (_, __) => true), throwsStateError);
    });

    test('tx scanAll after db.close throws StateError', () {
      final db = AssemblyDB.open(_dbPath);
      final tx = db.begin();
      db.close();
      expect(() => tx.scanAll(), throwsStateError);
    });
  });

  group('hardening: tx validation', () {
    late AssemblyDB db;
    setUp(() => db = AssemblyDB.open(_dbPath));
    tearDown(() => db.close());

    test('tx put key too long throws KeyTooLongError', () {
      final tx = db.begin();
      expect(
        () => tx.put(Uint8List(63), _b('v')),
        throwsA(isA<KeyTooLongError>()),
      );
      tx.rollback();
    });

    test('tx put value too long throws ValTooLongError', () {
      final tx = db.begin();
      expect(
        () => tx.put(_b('k'), Uint8List(255)),
        throwsA(isA<ValTooLongError>()),
      );
      tx.rollback();
    });

    test('tx get key too long throws KeyTooLongError', () {
      final tx = db.begin();
      expect(
        () => tx.get(Uint8List(63)),
        throwsA(isA<KeyTooLongError>()),
      );
      tx.rollback();
    });

    test('tx delete key too long throws KeyTooLongError', () {
      final tx = db.begin();
      expect(
        () => tx.delete(Uint8List(63)),
        throwsA(isA<KeyTooLongError>()),
      );
      tx.rollback();
    });

    test('tx put max key + max value succeeds', () {
      final tx = db.begin();
      tx.put(Uint8List(62), Uint8List(254));
      tx.commit();
    });

    test('tx remains active after validation error', () {
      final tx = db.begin();
      try { tx.put(Uint8List(63), _b('v')); } on KeyTooLongError catch (_) {}
      expect(tx.isActive, true);
      tx.putString('valid', 'ok');
      tx.commit();
      expect(db.getString('valid'), 'ok');
    });

    test('tx putString validates utf8 byte length', () {
      final tx = db.begin();
      final longKey = String.fromCharCodes(List.filled(63, 0x41));
      expect(
        () => tx.putString(longKey, 'v'),
        throwsA(isA<KeyTooLongError>()),
      );
      tx.rollback();
    });
  });

  group('hardening: transaction double-failure', () {
    test('transaction() handles rollback failure gracefully', () {
      final db = AssemblyDB.open(_dbPath);
      expect(
        () => db.transaction((tx) {
          tx.putString('k', 'v');
          throw FormatException('forced');
        }),
        throwsFormatException,
      );
      expect(db.getString('k'), isNull);
      db.close();
    });

    test('transaction() propagates original exception not rollback error', () {
      final db = AssemblyDB.open(_dbPath);
      Object? caught;
      try {
        db.transaction((tx) {
          tx.putString('before_err', 'val');
          throw ArgumentError('original');
        });
      } catch (e) {
        caught = e;
      }
      expect(caught, isA<ArgumentError>());
      expect((caught as ArgumentError).message, 'original');
      db.close();
    });
  });

  group('hardening: db close during operations', () {
    test('db usable up until close, then not', () {
      final db = AssemblyDB.open(_dbPath);
      db.putString('a', '1');
      expect(db.getString('a'), '1');
      db.close();
      expect(() => db.putString('b', '2'), throwsStateError);
      expect(() => db.getString('a'), throwsStateError);
      expect(() => db.sync(), throwsStateError);
    });

    test('isOpen reflects state', () {
      final db = AssemblyDB.open(_dbPath);
      expect(db.isOpen, true);
      db.close();
      expect(db.isOpen, false);
    });
  });

  // ============================================================
  // PRODUCTION: realistic multi-step workflows
  // ============================================================
  group('production: migration workflow', () {
    test('read all keys, transform, write back', () {
      final db = AssemblyDB.open(_dbPath);
      for (var i = 0; i < 50; i++) {
        db.putString('old:$i', 'data_$i');
      }
      db.sync();

      final entries = <String, String>{};
      db.scanStrings(
        start: 'old:',
        end: 'old:\xff',
        onEntry: (k, v) { entries[k] = v; return true; },
      );
      expect(entries.length, 50);

      for (final e in entries.entries) {
        final newKey = e.key.replaceFirst('old:', 'new:');
        db.putString(newKey, '${e.value}_migrated');
        db.deleteString(e.key);
      }
      db.sync();

      expect(db.count(start: _b('old:'), end: _b('old:\xff')), 0);
      final migrated = <String>[];
      db.scanStrings(
        start: 'new:',
        end: 'new:\xff',
        onEntry: (k, v) {
          expect(v, endsWith('_migrated'));
          migrated.add(k);
          return true;
        },
      );
      expect(migrated.length, 50);
      db.close();
    });
  });

  group('production: prefix namespacing', () {
    late AssemblyDB db;
    setUp(() => db = AssemblyDB.open(_dbPath));
    tearDown(() => db.close());

    test('multiple namespaces coexist', () {
      for (var i = 0; i < 20; i++) {
        db.putString('users:$i', 'user_$i');
        db.putString('orders:$i', 'order_$i');
        db.putString('config:$i', 'cfg_$i');
      }
      db.sync();

      var userCount = 0;
      db.scanStrings(
        start: 'users:',
        end: 'users:\xff',
        onEntry: (_, __) { userCount++; return true; },
      );
      expect(userCount, 20);

      var orderCount = 0;
      db.scanStrings(
        start: 'orders:',
        end: 'orders:\xff',
        onEntry: (_, __) { orderCount++; return true; },
      );
      expect(orderCount, 20);

      db.deleteString('users:5');
      expect(db.existsString('users:5'), false);
      expect(db.existsString('orders:5'), true);
    });
  });

  group('production: write-heavy then read-heavy', () {
    test('sequential phases', () {
      final db = AssemblyDB.open(_dbPath);
      for (var i = 0; i < 500; i++) {
        db.putString('wr:${i.toString().padLeft(4, '0')}', 'payload_$i');
      }
      db.sync();

      for (var i = 0; i < 500; i++) {
        final v = db.getString('wr:${i.toString().padLeft(4, '0')}');
        expect(v, 'payload_$i');
      }

      final m = db.metrics;
      expect(m.putsTotal, greaterThanOrEqualTo(500));
      expect(m.getsTotal, greaterThanOrEqualTo(500));
      db.close();
    });
  });

  group('production: transactional counter', () {
    late AssemblyDB db;
    setUp(() {
      db = AssemblyDB.open(_dbPath);
      db.putString('counter', '0');
      db.sync();
    });
    tearDown(() => db.close());

    test('increment counter in transactions', () {
      for (var i = 0; i < 50; i++) {
        db.transaction((tx) {
          final cur = int.parse(tx.getString('counter')!);
          tx.putString('counter', '${cur + 1}');
        });
      }
      expect(db.getString('counter'), '50');
    });

    test('failed increment does not change counter', () {
      final before = db.getString('counter');
      try {
        db.transaction((tx) {
          final cur = int.parse(tx.getString('counter')!);
          tx.putString('counter', '${cur + 1}');
          throw StateError('abort');
        });
      } on StateError catch (_) {}
      expect(db.getString('counter'), before);
    });
  });

  // ============================================================
  // HARDENING: countStrings, indexScan validation, close-safety
  // ============================================================
  group('hardening: countStrings', () {
    late AssemblyDB db;
    setUp(() {
      db = AssemblyDB.open(_dbPath);
      for (var i = 0; i < 30; i++) {
        db.putString('ns:${i.toString().padLeft(3, '0')}', 'v$i');
      }
      db.sync();
    });
    tearDown(() => db.close());

    test('countStrings all', () {
      expect(db.countStrings(start: 'ns:', end: 'ns:\xff'), 30);
    });

    test('countStrings subset', () {
      final c = db.countStrings(start: 'ns:010', end: 'ns:020');
      expect(c, greaterThan(0));
      expect(c, lessThanOrEqualTo(11));
    });

    test('countStrings no match', () {
      expect(db.countStrings(start: 'zzz:', end: 'zzz:\xff'), 0);
    });
  });

  group('hardening: close safety all methods', () {
    test('backup after close throws', () {
      final db = AssemblyDB.open(_dbPath);
      db.close();
      expect(
        () => db.backup('${_tmpDir.path}/bak'),
        throwsStateError,
      );
    });

    test('batch after close throws', () {
      final db = AssemblyDB.open(_dbPath);
      db.close();
      expect(
        () => db.batchPut({_b('k'): _b('v')}),
        throwsStateError,
      );
    });

    test('batchPutStrings after close throws', () {
      final db = AssemblyDB.open(_dbPath);
      db.close();
      expect(
        () => db.batchPutStrings({'k': 'v'}),
        throwsStateError,
      );
    });

    test('scan after close throws', () {
      final db = AssemblyDB.open(_dbPath);
      db.close();
      expect(
        () => db.scan(onEntry: (_, __) => true),
        throwsStateError,
      );
    });

    test('scanAll after close throws', () {
      final db = AssemblyDB.open(_dbPath);
      db.close();
      expect(() => db.scanAll(), throwsStateError);
    });

    test('scanStrings after close throws', () {
      final db = AssemblyDB.open(_dbPath);
      db.close();
      expect(
        () => db.scanStrings(onEntry: (_, __) => true),
        throwsStateError,
      );
    });

    test('count after close throws', () {
      final db = AssemblyDB.open(_dbPath);
      db.close();
      expect(() => db.count(), throwsStateError);
    });

    test('countStrings after close throws', () {
      final db = AssemblyDB.open(_dbPath);
      db.close();
      expect(() => db.countStrings(), throwsStateError);
    });

    test('begin after close throws', () {
      final db = AssemblyDB.open(_dbPath);
      db.close();
      expect(() => db.begin(), throwsStateError);
    });

    test('createIndex after close throws', () {
      final db = AssemblyDB.open(_dbPath);
      db.close();
      expect(() => db.createIndex('idx'), throwsStateError);
    });

    test('dropIndex after close throws', () {
      final db = AssemblyDB.open(_dbPath);
      db.close();
      expect(() => db.dropIndex('idx'), throwsStateError);
    });

    test('metrics after close throws', () {
      final db = AssemblyDB.open(_dbPath);
      db.close();
      expect(() => db.metrics, throwsStateError);
    });

    test('delete after close throws', () {
      final db = AssemblyDB.open(_dbPath);
      db.close();
      expect(() => db.delete(_b('k')), throwsStateError);
    });

    test('exists after close throws', () {
      final db = AssemblyDB.open(_dbPath);
      db.close();
      expect(() => db.exists(_b('k')), throwsStateError);
    });

    test('operator[] after close throws', () {
      final db = AssemblyDB.open(_dbPath);
      db.close();
      expect(() => db[_b('k')], throwsStateError);
    });

    test('operator[]= after close throws', () {
      final db = AssemblyDB.open(_dbPath);
      db.close();
      expect(() => db[_b('k')] = _b('v'), throwsStateError);
    });
  });

  // ============================================================
  // STRESS: heavy mixed workloads
  // ============================================================
  group('stress: tx-heavy workflow', () {
    test('200 transactions alternating read-modify-write', () {
      final db = AssemblyDB.open(_dbPath);
      db.putString('acc:balance', '1000');
      db.sync();

      for (var i = 0; i < 200; i++) {
        db.transaction((tx) {
          final bal = int.parse(tx.getString('acc:balance')!);
          tx.putString('acc:balance', '${bal + 1}');
          tx.putString('acc:log:${i.toString().padLeft(4, '0')}', 'deposit:1');
        });
      }

      expect(db.getString('acc:balance'), '1200');
      final logCount = db.countStrings(start: 'acc:log:', end: 'acc:log:\xff');
      expect(logCount, 200);
      db.close();
    });
  });

  group('stress: rapid sync cycles', () {
    test('put-sync 100 times', () {
      final db = AssemblyDB.open(_dbPath);
      for (var i = 0; i < 100; i++) {
        db.putString('s:$i', 'v$i');
        db.sync();
      }
      for (var i = 0; i < 100; i++) {
        expect(db.getString('s:$i'), 'v$i');
      }
      db.close();
    });
  });

  group('stress: scan during mutations', () {
    test('scan then mutate then scan again', () {
      final db = AssemblyDB.open(_dbPath);
      for (var i = 0; i < 100; i++) {
        db.putString('m:${i.toString().padLeft(3, '0')}', 'orig_$i');
      }
      db.sync();

      final first = db.scanAll(start: _b('m:'), end: _b('m:\xff'));
      expect(first.length, 100);

      for (var i = 50; i < 100; i++) {
        db.deleteString('m:${i.toString().padLeft(3, '0')}');
      }
      for (var i = 100; i < 150; i++) {
        db.putString('m:${i.toString().padLeft(3, '0')}', 'new_$i');
      }
      db.sync();

      final second = db.scanAll(start: _b('m:'), end: _b('m:\xff'));
      expect(second.length, 100);
      db.close();
    });
  });

  group('production: backup-restore-verify cycle', () {
    test('backup, modify original, restore, verify independence', () {
      final db = AssemblyDB.open(_dbPath);
      for (var i = 0; i < 100; i++) {
        db.putString('bak:$i', 'original_$i');
      }
      db.sync();

      final bakPath = '${_tmpDir.path}/backup1';
      db.backup(bakPath);

      for (var i = 0; i < 50; i++) {
        db.putString('bak:$i', 'modified_$i');
      }
      db.sync();
      db.close();

      final restorePath = '${_tmpDir.path}/restored';
      AssemblyDB.restore(bakPath, restorePath);
      final restored = AssemblyDB.open(restorePath);

      for (var i = 0; i < 100; i++) {
        expect(restored.getString('bak:$i'), 'original_$i');
      }
      restored.close();
      AssemblyDB.destroy(restorePath);
    });
  });

  group('production: batch + tx interleave', () {
    late AssemblyDB db;
    setUp(() => db = AssemblyDB.open(_dbPath));
    tearDown(() => db.close());

    test('batch then tx then batch', () {
      db.batchPutStrings({
        for (var i = 0; i < 50; i++) 'b1:$i': 'v$i',
      });
      db.sync();

      db.transaction((tx) {
        for (var i = 0; i < 50; i++) {
          tx.putString('tx:$i', 'tv$i');
        }
      });

      db.batchPutStrings({
        for (var i = 0; i < 50; i++) 'b2:$i': 'v$i',
      });
      db.sync();

      expect(db.countStrings(start: 'b1:', end: 'b1:\xff'), 50);
      expect(db.countStrings(start: 'tx:', end: 'tx:\xff'), 50);
      expect(db.countStrings(start: 'b2:', end: 'b2:\xff'), 50);
    });
  });

  group('adversarial: empty key', () {
    late AssemblyDB db;
    setUp(() => db = AssemblyDB.open(_dbPath));
    tearDown(() => db.close());

    test('empty Uint8List key put and get', () {
      db.put(Uint8List(0), _b('empty_key_val'));
      final v = db.get(Uint8List(0));
      expect(v, isNotNull);
      expect(_s(v!), 'empty_key_val');
    });

    test('empty string key', () {
      db.putString('', 'empty_str_val');
      expect(db.getString(''), 'empty_str_val');
    });

    test('empty key exists and delete', () {
      db.putString('', 'val');
      expect(db.existsString(''), true);
      db.deleteString('');
      expect(db.existsString(''), false);
    });
  });

  group('adversarial: empty value', () {
    late AssemblyDB db;
    setUp(() => db = AssemblyDB.open(_dbPath));
    tearDown(() => db.close());

    test('empty Uint8List value round-trip', () {
      db.put(_b('ek'), Uint8List(0));
      final v = db.get(_b('ek'));
      expect(v, isNotNull);
      expect(v!.length, 0);
    });

    test('empty string value round-trip', () {
      db.putString('ek2', '');
      expect(db.getString('ek2'), '');
    });
  });

  group('adversarial: tx empty key/value', () {
    late AssemblyDB db;
    setUp(() => db = AssemblyDB.open(_dbPath));
    tearDown(() => db.close());

    test('tx put empty key', () {
      final tx = db.begin();
      tx.put(Uint8List(0), _b('val'));
      tx.commit();
      expect(db.getString(''), isNotNull);
    });

    test('tx put empty value', () {
      final tx = db.begin();
      tx.put(_b('k'), Uint8List(0));
      tx.commit();
      final v = db.get(_b('k'));
      expect(v, isNotNull);
      expect(v!.length, 0);
    });
  });

  group('production: kiosk order queue', () {
    late AssemblyDB db;
    setUp(() => db = AssemblyDB.open(_dbPath));
    tearDown(() => db.close());

    test('enqueue orders, process, mark done', () {
      for (var i = 0; i < 20; i++) {
        final ts = (1000000 + i).toString();
        db.transaction((tx) {
          tx.putString('q:pending:$ts', '{"items":[$i]}');
          tx.putString('q:meta:$ts', 'pending');
        });
      }

      final pending = <String, String>{};
      db.scanStrings(
        start: 'q:pending:',
        end: 'q:pending:\xff',
        onEntry: (k, v) { pending[k] = v; return true; },
      );
      expect(pending.length, 20);

      var processed = 0;
      for (final e in pending.entries) {
        final ts = e.key.split(':').last;
        db.transaction((tx) {
          tx.deleteString(e.key);
          tx.putString('q:done:$ts', e.value);
          tx.putString('q:meta:$ts', 'done');
        });
        processed++;
        if (processed >= 10) break;
      }

      expect(db.countStrings(start: 'q:pending:', end: 'q:pending:\xff'), 10);
      expect(db.countStrings(start: 'q:done:', end: 'q:done:\xff'), 10);
    });
  });

  group('production: config store with defaults', () {
    late AssemblyDB db;
    setUp(() => db = AssemblyDB.open(_dbPath));
    tearDown(() => db.close());

    test('get with fallback default', () {
      String getConfig(String key, String def) => db.getString('cfg:$key') ?? def;

      expect(getConfig('theme', 'dark'), 'dark');
      db.putString('cfg:theme', 'light');
      expect(getConfig('theme', 'dark'), 'light');

      db.putString('cfg:lang', 'bn');
      expect(getConfig('lang', 'en'), 'bn');
      expect(getConfig('missing', 'fallback'), 'fallback');
    });
  });

  group('stress: boundary hammering', () {
    test('put max-size key+value 100 times', () {
      final db = AssemblyDB.open(_dbPath);
      for (var i = 0; i < 100; i++) {
        final k = Uint8List(62);
        k[0] = i & 0xFF;
        k[1] = (i >> 8) & 0xFF;
        db.put(k, Uint8List(254));
      }
      db.sync();

      for (var i = 0; i < 100; i++) {
        final k = Uint8List(62);
        k[0] = i & 0xFF;
        k[1] = (i >> 8) & 0xFF;
        expect(db.get(k), isNotNull);
      }
      db.close();
    });
  });

  // ============================================================
  // NEW METHODS: tx exists, tx scanStrings, tx count, scanAllStrings
  // ============================================================
  group('hardening: tx exists', () {
    late AssemblyDB db;
    setUp(() {
      db = AssemblyDB.open(_dbPath);
      db.putString('pre', 'val');
      db.sync();
    });
    tearDown(() => db.close());

    test('tx exists for pre-existing key', () {
      final tx = db.begin();
      expect(tx.exists(_b('pre')), true);
      expect(tx.exists(_b('nope')), false);
      tx.rollback();
    });

    test('tx exists for own writes', () {
      final tx = db.begin();
      tx.put(_b('new'), _b('v'));
      expect(tx.exists(_b('new')), true);
      tx.rollback();
    });

    test('tx existsString', () {
      final tx = db.begin();
      expect(tx.existsString('pre'), true);
      expect(tx.existsString('missing'), false);
      tx.rollback();
    });

    test('tx exists validates key length', () {
      final tx = db.begin();
      expect(() => tx.exists(Uint8List(63)), throwsA(isA<KeyTooLongError>()));
      tx.rollback();
    });

    test('tx exists after db close throws', () {
      final tx = db.begin();
      db.close();
      expect(() => tx.exists(_b('pre')), throwsStateError);
    });
  });

  group('hardening: tx scanStrings', () {
    late AssemblyDB db;
    setUp(() {
      db = AssemblyDB.open(_dbPath);
      for (var i = 0; i < 10; i++) {
        db.putString('txs:${i.toString().padLeft(2, '0')}', 'v$i');
      }
      db.sync();
    });
    tearDown(() => db.close());

    test('tx scanStrings returns all entries', () {
      final tx = db.begin();
      final keys = <String>[];
      tx.scanStrings(
        start: 'txs:',
        end: 'txs:\xff',
        onEntry: (k, v) { keys.add(k); return true; },
      );
      expect(keys.length, greaterThanOrEqualTo(10));
      tx.rollback();
    });

    test('tx count', () {
      final tx = db.begin();
      final c = tx.count(start: _b('txs:'), end: _b('txs:\xff'));
      expect(c, greaterThanOrEqualTo(10));
      tx.rollback();
    });

    test('tx scanStrings with early stop', () {
      final tx = db.begin();
      var seen = 0;
      tx.scanStrings(
        start: 'txs:',
        end: 'txs:\xff',
        onEntry: (_, __) { seen++; return seen < 3; },
      );
      expect(seen, 3);
      tx.rollback();
    });
  });

  group('hardening: scanAllStrings', () {
    late AssemblyDB db;
    setUp(() {
      db = AssemblyDB.open(_dbPath);
      for (var i = 0; i < 15; i++) {
        db.putString('sas:${i.toString().padLeft(2, '0')}', 'val_$i');
      }
      db.sync();
    });
    tearDown(() => db.close());

    test('scanAllStrings returns all entries', () {
      final entries = db.scanAllStrings(start: 'sas:', end: 'sas:\xff');
      expect(entries.length, 15);
      for (final e in entries) {
        expect(e.keyString, startsWith('sas:'));
        expect(e.valueString, startsWith('val_'));
      }
    });

    test('scanAllStrings empty range', () {
      final entries = db.scanAllStrings(start: 'zzz:', end: 'zzz:\xff');
      expect(entries, isEmpty);
    });
  });

  // ============================================================
  // PRODUCTION: real-world POS workflow
  // ============================================================
  group('production: POS table management', () {
    late AssemblyDB db;
    setUp(() => db = AssemblyDB.open(_dbPath));
    tearDown(() => db.close());

    test('manage tables and assign orders', () {
      for (var t = 1; t <= 10; t++) {
        db.putString('table:$t:status', 'available');
      }

      db.transaction((tx) {
        tx.putString('table:3:status', 'occupied');
        tx.putString('table:3:order', '{"items":["burger","fries"]}');
        tx.putString('table:3:time', '2026-03-13T14:30:00');
      });

      expect(db.getString('table:3:status'), 'occupied');
      expect(db.getString('table:3:order'), contains('burger'));

      var available = 0;
      db.scanStrings(
        start: 'table:',
        end: 'table:\xff',
        onEntry: (k, v) {
          if (k.endsWith(':status') && v == 'available') available++;
          return true;
        },
      );
      expect(available, 9);

      db.transaction((tx) {
        tx.putString('table:3:status', 'available');
        tx.deleteString('table:3:order');
        tx.deleteString('table:3:time');
      });
      expect(db.getString('table:3:status'), 'available');
      expect(db.getString('table:3:order'), isNull);
    });
  });

  group('production: leaderboard', () {
    late AssemblyDB db;
    setUp(() => db = AssemblyDB.open(_dbPath));
    tearDown(() => db.close());

    test('store and query scores', () {
      final scores = {
        'player:alice': '00950',
        'player:bob': '01200',
        'player:charlie': '00800',
        'player:dave': '01500',
        'player:eve': '01100',
      };
      db.batchPutStrings(scores);
      db.sync();

      final all = <String, int>{};
      db.scanStrings(
        start: 'player:',
        end: 'player:\xff',
        onEntry: (k, v) {
          all[k.replaceFirst('player:', '')] = int.parse(v);
          return true;
        },
      );
      expect(all.length, 5);

      final sorted = all.entries.toList()
        ..sort((a, b) => b.value.compareTo(a.value));
      expect(sorted.first.key, 'dave');
      expect(sorted.last.key, 'charlie');
    });
  });

  // ============================================================
  // ADVERSARIAL: deeply nested exception types in scan
  // ============================================================
  group('adversarial: scan exception types', () {
    late AssemblyDB db;
    setUp(() {
      db = AssemblyDB.open(_dbPath);
      db.putString('se:1', 'v1');
      db.sync();
    });
    tearDown(() => db.close());

    test('FormatException in scan propagates', () {
      expect(
        () => db.scan(onEntry: (_, __) => throw const FormatException('bad')),
        throwsFormatException,
      );
    });

    test('RangeError in scan propagates', () {
      expect(
        () => db.scan(onEntry: (_, __) => throw RangeError('out')),
        throwsRangeError,
      );
    });

    test('String thrown in scan propagates', () {
      Object? caught;
      try {
        db.scan(onEntry: (_, __) => throw 'string_error');
      } catch (e) {
        caught = e;
      }
      expect(caught, 'string_error');
    });

    test('db usable after any scan exception type', () {
      try { db.scan(onEntry: (_, __) => throw StateError('x')); } catch (_) {}
      db.putString('se:2', 'v2');
      expect(db.getString('se:2'), 'v2');
    });
  });

  // ============================================================
  // ADVERSARIAL: maxKeyLength and maxValueLength constants
  // ============================================================
  group('hardening: exported constants', () {
    test('maxKeyLength is 62', () {
      expect(maxKeyLength, 62);
    });

    test('maxValueLength is 254', () {
      expect(maxValueLength, 254);
    });

    test('key at maxKeyLength works, maxKeyLength+1 throws', () {
      final db = AssemblyDB.open(_dbPath);
      db.put(Uint8List(maxKeyLength), _b('v'));
      expect(db.get(Uint8List(maxKeyLength)), isNotNull);
      expect(
        () => db.put(Uint8List(maxKeyLength + 1), _b('v')),
        throwsA(isA<KeyTooLongError>()),
      );
      db.close();
    });

    test('value at maxValueLength works, maxValueLength+1 throws', () {
      final db = AssemblyDB.open(_dbPath);
      db.put(_b('k'), Uint8List(maxValueLength));
      expect(db.get(_b('k')), isNotNull);
      expect(
        () => db.put(_b('k'), Uint8List(maxValueLength + 1)),
        throwsA(isA<ValTooLongError>()),
      );
      db.close();
    });
  });

  // ============================================================
  // STRESS: combined heavy workload
  // ============================================================
  group('stress: full lifecycle simulation', () {
    test('3000 mixed operations with periodic sync', () {
      final db = AssemblyDB.open(_dbPath);
      final rng = Random(42);

      for (var i = 0; i < 1000; i++) {
        db.putString('fl:${i.toString().padLeft(4, '0')}', 'val_$i');
      }
      db.sync();

      for (var i = 0; i < 500; i++) {
        final key = 'fl:${rng.nextInt(1000).toString().padLeft(4, '0')}';
        db.getString(key);
      }

      for (var i = 0; i < 500; i++) {
        final key = 'fl:${rng.nextInt(1000).toString().padLeft(4, '0')}';
        db.putString(key, 'updated_$i');
        if (i % 100 == 0) db.sync();
      }

      for (var i = 0; i < 200; i++) {
        final key = 'fl:${rng.nextInt(1000).toString().padLeft(4, '0')}';
        db.deleteString(key);
      }
      db.sync();

      for (var i = 0; i < 50; i++) {
        db.transaction((tx) {
          final key = 'fl:${(1000 + i).toString().padLeft(4, '0')}';
          tx.putString(key, 'tx_$i');
        });
      }

      final total = db.countStrings(start: 'fl:', end: 'fl:\xff');
      expect(total, greaterThan(0));

      final m = db.metrics;
      expect(m.putsTotal, greaterThanOrEqualTo(1000));
      expect(m.getsTotal, greaterThanOrEqualTo(500));

      db.close();

      final db2 = AssemblyDB.open(_dbPath);
      final total2 = db2.countStrings(start: 'fl:', end: 'fl:\xff');
      expect(total2, total);
      db2.close();
    });
  });

  // ============================================================
  // ADVERSARIAL: reopen after destroy then recreate
  // ============================================================
  group('adversarial: destroy-recreate cycle', () {
    test('destroy then reopen fresh', () {
      final db = AssemblyDB.open(_dbPath);
      db.putString('old', 'data');
      db.sync();
      db.close();

      AssemblyDB.destroy(_dbPath);

      final db2 = AssemblyDB.open(_dbPath);
      expect(db2.getString('old'), isNull);
      db2.putString('new', 'fresh');
      expect(db2.getString('new'), 'fresh');
      db2.close();
    });
  });

  // ============================================================
  // PRODUCTION: event log with timestamp keys
  // ============================================================
  group('production: event log', () {
    late AssemblyDB db;
    setUp(() => db = AssemblyDB.open(_dbPath));
    tearDown(() => db.close());

    test('append events and query by time range', () {
      for (var i = 0; i < 100; i++) {
        final ts = (1710000000 + i * 60).toString();
        db.putString('evt:$ts', '{"type":"click","id":$i}');
      }
      db.sync();

      final startTs = (1710000000 + 30 * 60).toString();
      final endTs = (1710000000 + 60 * 60).toString();
      final range = <String>[];
      db.scanStrings(
        start: 'evt:$startTs',
        end: 'evt:$endTs',
        onEntry: (k, v) { range.add(v); return true; },
      );
      expect(range.length, greaterThanOrEqualTo(29));
      expect(range.length, lessThanOrEqualTo(31));
    });
  });

  // ============================================================
  // ADVERSARIAL: transaction generic return type
  // ============================================================
  group('hardening: transaction return types', () {
    late AssemblyDB db;
    setUp(() => db = AssemblyDB.open(_dbPath));
    tearDown(() => db.close());

    test('transaction returns int', () {
      db.putString('tr', '42');
      final val = db.transaction((tx) => int.parse(tx.getString('tr')!));
      expect(val, 42);
    });

    test('transaction returns bool', () {
      db.putString('tr', 'yes');
      final val = db.transaction((tx) => tx.getString('tr') == 'yes');
      expect(val, true);
    });

    test('transaction returns list', () {
      db.putString('a', '1');
      db.putString('b', '2');
      db.sync();
      final vals = db.transaction((tx) {
        return [tx.getString('a'), tx.getString('b')];
      });
      expect(vals, ['1', '2']);
    });

    test('transaction returns null', () {
      final val = db.transaction((tx) => tx.getString('nonexistent'));
      expect(val, isNull);
    });
  });
}
