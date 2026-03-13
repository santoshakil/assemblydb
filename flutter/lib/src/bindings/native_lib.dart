import 'dart:ffi';
import 'dart:io';

import 'assemblydb_bindings.g.dart';

AssemblyDBBindings? _defaultBindings;

AssemblyDBBindings loadBindings({String? libraryPath}) {
  if (libraryPath != null) {
    return AssemblyDBBindings(DynamicLibrary.open(libraryPath));
  }
  return _defaultBindings ??= AssemblyDBBindings(_openDefaultLibrary());
}

DynamicLibrary _openDefaultLibrary() {
  if (Platform.isAndroid || Platform.isLinux) {
    return DynamicLibrary.open('libassemblydb.so');
  }
  throw UnsupportedError(
    'AssemblyDB only supports AArch64 Linux and Android ARM64. '
    'Current platform: ${Platform.operatingSystem}',
  );
}
