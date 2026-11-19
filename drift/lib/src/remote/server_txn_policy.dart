/// Controls how [ServerImplementation] handles idle transactions that block
/// other requests in the executor backlog.
enum ServerTransactionMode {
  /// Disabled: no idle monitoring, logging, or rollback.
  off,

  /// Record-only: emit `drift server: idle transaction:` and
  /// `drift server: idle transaction end:` diagnostics via
  /// `driftRuntimeOptions.debugPrint`, without rolling back any transaction.
  record,

  /// Enforce: in addition to record diagnostics, roll back idle top-level
  /// transaction trees (excluding `before_open` and `exclusive` executors)
  /// once [ServerTransactionPolicy.idleLimit] is reached while external waiters
  /// are queued.
  enforce,
}

/// Per-isolate policy for server-side idle transaction detection and
/// enforcement.
final class ServerTransactionPolicy {
  /// Operating mode (`off`, `record`, or `enforce`).
  final ServerTransactionMode mode;

  /// Accumulated awake idle duration (with external waiters) after which a
  /// stuck transaction tree is revoked in [ServerTransactionMode.enforce].
  final Duration idleLimit;

  /// Accumulated awake idle duration (with external waiters) after which the
  /// first `drift server: idle transaction:` line is emitted.
  final Duration recordAfter;

  /// Minimum awake idle interval between repeated `idle transaction` records
  /// for the same stuck tree.
  final Duration recordEvery;

  /// Optional monotonic elapsed clock source used by tests instead of a live
  /// [Stopwatch].
  final Duration Function()? elapsed;

  /// Execution duration at or above which [ServerImplementation] emits
  /// `drift server: slow statement:` via `driftRuntimeOptions.debugPrint`.
  /// Set to `null` to disable slow-statement logging.
  final Duration? slowStatementThreshold;

  /// Privacy-safe schema allowlist used to populate `tables=` on slow-statement
  /// lines without leaking dynamic identifiers or query content.
  final Set<String> knownTables;

  /// Creates a server transaction policy.
  const ServerTransactionPolicy({
    this.mode = ServerTransactionMode.off,
    this.idleLimit = const Duration(seconds: 30),
    this.recordAfter = const Duration(seconds: 10),
    this.recordEvery = const Duration(seconds: 60),
    this.elapsed,
    this.slowStatementThreshold = const Duration(seconds: 2),
    this.knownTables = const <String>{},
  });
}

ServerTransactionPolicy _serverTransactionPolicy =
    const ServerTransactionPolicy();

/// The active [ServerTransactionPolicy] for the current isolate.
ServerTransactionPolicy get serverTransactionPolicy => _serverTransactionPolicy;

/// Sets the active [ServerTransactionPolicy] for the current isolate.
set serverTransactionPolicy(ServerTransactionPolicy value) {
  _serverTransactionPolicy = value;
}
