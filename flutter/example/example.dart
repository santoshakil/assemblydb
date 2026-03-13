// ignore_for_file: avoid_print
import 'dart:io';
import 'dart:typed_data';

import 'package:assemblydb/assemblydb.dart';

void main() {
  final dir = Directory.systemTemp.createTempSync('adb_example_');
  final dbPath = '${dir.path}/mydb';

  try {
    final db = AssemblyDB.open(dbPath, config: const AssemblyDBConfig(
      walSyncMode: WalSyncMode.sync_,
      cacheSizePages: 128,
    ));

    // Basic CRUD
    db.putString('name', 'AssemblyDB');
    db.putString('lang', 'AArch64 Assembly');
    print('name = ${db.getString("name")}');
    print('exists: ${db.existsString("name")}');

    // Map-like operators (bytes)
    db[Uint8List.fromList([1, 2, 3])] = Uint8List.fromList([4, 5, 6]);

    // Batch insert
    db.batchPutStrings({
      'fruit:apple': 'red',
      'fruit:banana': 'yellow',
      'fruit:grape': 'purple',
    });

    // Range scan with prefix
    print('\nFruits:');
    db.scanStrings(
      start: 'fruit:',
      end: 'fruit:\xff',
      onEntry: (key, value) {
        print('  $key => $value');
        return true;
      },
    );

    // Count entries
    print('\nTotal entries: ${db.count()}');

    // Atomic transaction
    db.transaction((tx) {
      tx.putString('tx_key1', 'value1');
      tx.putString('tx_key2', 'value2');
      print('\nIn tx: tx_key1 = ${tx.getString("tx_key1")}');
    });
    print('Committed: tx_key1 = ${db.getString("tx_key1")}');

    // Backup and restore
    final backupPath = '${dir.path}/backup';
    final restorePath = '${dir.path}/restored';
    db.sync();
    db.backup(backupPath);

    AssemblyDB.restore(backupPath, restorePath);
    final db2 = AssemblyDB.open(restorePath);
    print('\nRestored: name = ${db2.getString("name")}');
    db2.close();

    // Metrics
    print('\n${db.metrics}');

    db.close();

    AssemblyDB.destroy(dbPath);
    AssemblyDB.destroy(backupPath);
    AssemblyDB.destroy(restorePath);
  } finally {
    dir.deleteSync(recursive: true);
  }
}
