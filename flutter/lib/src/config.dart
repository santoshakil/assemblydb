enum WalSyncMode {
  async_(0),
  sync_(1),
  full(2);

  const WalSyncMode(this.value);
  final int value;
}

class AssemblyDBConfig {
  const AssemblyDBConfig({
    this.cacheSizePages = 64,
    this.encryptionEnabled = false,
    this.compressionEnabled = false,
    this.walSyncMode = WalSyncMode.sync_,
    this.memtableMaxBytes = 0,
  });

  final int cacheSizePages;
  final bool encryptionEnabled;
  final bool compressionEnabled;
  final WalSyncMode walSyncMode;
  final int memtableMaxBytes;
}

enum IsolationLevel {
  readUncommitted(0),
  readCommitted(1),
  repeatableRead(2),
  snapshot(3),
  serializable(4);

  const IsolationLevel(this.value);
  final int value;
}
