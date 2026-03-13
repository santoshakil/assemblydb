sealed class AssemblyDBException implements Exception {
  const AssemblyDBException(this.code, this.message);

  final int code;
  final String message;

  @override
  String toString() => 'AssemblyDBException($code): $message';

  static AssemblyDBException fromCode(int code) => switch (code) {
        1 => const NotFoundError(),
        2 => const IOError(),
        3 => const CorruptError(),
        4 => const KeyTooLongError(),
        5 => const ValTooLongError(),
        6 => const LockedError(),
        7 => const OutOfMemoryError(),
        8 => const InvalidError(),
        9 => const TxConflictError(),
        10 => const TxNotFoundError(),
        11 => const TxAbortedError(),
        12 => const FullError(),
        13 => const ExistsError(),
        14 => const DecryptError(),
        15 => const CompressError(),
        _ => AssemblyDBUnknownError(code),
      };
}

final class NotFoundError extends AssemblyDBException {
  const NotFoundError() : super(1, 'Key not found');
}

final class IOError extends AssemblyDBException {
  const IOError() : super(2, 'I/O error');
}

final class CorruptError extends AssemblyDBException {
  const CorruptError() : super(3, 'Data corruption detected');
}

final class KeyTooLongError extends AssemblyDBException {
  const KeyTooLongError() : super(4, 'Key exceeds 62 bytes');
}

final class ValTooLongError extends AssemblyDBException {
  const ValTooLongError() : super(5, 'Value exceeds 254 bytes');
}

final class LockedError extends AssemblyDBException {
  const LockedError() : super(6, 'Database or transaction locked');
}

final class OutOfMemoryError extends AssemblyDBException {
  const OutOfMemoryError() : super(7, 'Memory allocation failed');
}

final class InvalidError extends AssemblyDBException {
  const InvalidError() : super(8, 'Invalid argument');
}

final class TxConflictError extends AssemblyDBException {
  const TxConflictError() : super(9, 'Transaction conflict');
}

final class TxNotFoundError extends AssemblyDBException {
  const TxNotFoundError() : super(10, 'Invalid transaction ID');
}

final class TxAbortedError extends AssemblyDBException {
  const TxAbortedError() : super(11, 'Transaction aborted');
}

final class FullError extends AssemblyDBException {
  const FullError() : super(12, 'Storage full');
}

final class ExistsError extends AssemblyDBException {
  const ExistsError() : super(13, 'Already exists');
}

final class DecryptError extends AssemblyDBException {
  const DecryptError() : super(14, 'Decryption failed');
}

final class CompressError extends AssemblyDBException {
  const CompressError() : super(15, 'Compression failed');
}

final class AssemblyDBUnknownError extends AssemblyDBException {
  const AssemblyDBUnknownError(int code) : super(code, 'Unknown error');
}
