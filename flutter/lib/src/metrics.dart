class AssemblyDBMetrics {
  const AssemblyDBMetrics({
    required this.putsTotal,
    required this.getsTotal,
    required this.deletesTotal,
    required this.scansTotal,
    required this.cacheHits,
    required this.cacheMisses,
    required this.bloomTruePositives,
    required this.bloomFalsePositives,
    required this.bytesWritten,
    required this.bytesRead,
    required this.compactionsRun,
    required this.compactionBytes,
    required this.walSyncs,
    required this.walBytes,
    required this.txCommits,
    required this.txRollbacks,
    required this.pageSplits,
    required this.pageMerges,
    required this.memtableFlushes,
    required this.sstableCount,
  });

  final int putsTotal;
  final int getsTotal;
  final int deletesTotal;
  final int scansTotal;
  final int cacheHits;
  final int cacheMisses;
  final int bloomTruePositives;
  final int bloomFalsePositives;
  final int bytesWritten;
  final int bytesRead;
  final int compactionsRun;
  final int compactionBytes;
  final int walSyncs;
  final int walBytes;
  final int txCommits;
  final int txRollbacks;
  final int pageSplits;
  final int pageMerges;
  final int memtableFlushes;
  final int sstableCount;

  double get cacheHitRate =>
      (cacheHits + cacheMisses) == 0 ? 0.0 : cacheHits / (cacheHits + cacheMisses);

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is AssemblyDBMetrics &&
          putsTotal == other.putsTotal &&
          getsTotal == other.getsTotal &&
          deletesTotal == other.deletesTotal &&
          scansTotal == other.scansTotal &&
          cacheHits == other.cacheHits &&
          cacheMisses == other.cacheMisses &&
          bloomTruePositives == other.bloomTruePositives &&
          bloomFalsePositives == other.bloomFalsePositives &&
          bytesWritten == other.bytesWritten &&
          bytesRead == other.bytesRead &&
          compactionsRun == other.compactionsRun &&
          compactionBytes == other.compactionBytes &&
          walSyncs == other.walSyncs &&
          walBytes == other.walBytes &&
          txCommits == other.txCommits &&
          txRollbacks == other.txRollbacks &&
          pageSplits == other.pageSplits &&
          pageMerges == other.pageMerges &&
          memtableFlushes == other.memtableFlushes &&
          sstableCount == other.sstableCount;

  @override
  int get hashCode => Object.hash(
      putsTotal, getsTotal, deletesTotal, scansTotal,
      cacheHits, cacheMisses, txCommits, txRollbacks,
      bytesWritten, bytesRead);

  @override
  String toString() =>
      'Metrics(puts=$putsTotal gets=$getsTotal deletes=$deletesTotal '
      'scans=$scansTotal txCommits=$txCommits txRollbacks=$txRollbacks '
      'cacheHitRate=${(cacheHitRate * 100).toStringAsFixed(1)}%)';
}
