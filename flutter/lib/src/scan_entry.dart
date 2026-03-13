import 'dart:convert';
import 'dart:typed_data';

class ScanEntry {
  const ScanEntry(this.key, this.value);

  final Uint8List key;
  final Uint8List value;

  String get keyString => utf8.decode(key);
  String get valueString => utf8.decode(value);

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is ScanEntry &&
          _bytesEqual(key, other.key) &&
          _bytesEqual(value, other.value);

  @override
  int get hashCode => Object.hash(Object.hashAll(key), Object.hashAll(value));

  @override
  String toString() => 'ScanEntry(keyLen=${key.length}, valLen=${value.length})';
}

bool _bytesEqual(Uint8List a, Uint8List b) {
  if (a.length != b.length) return false;
  for (var i = 0; i < a.length; i++) {
    if (a[i] != b[i]) return false;
  }
  return true;
}
