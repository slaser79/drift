import 'dart:async';

import 'package:drift/drift.dart';
import 'package:drift/remote.dart';
import 'package:meta/meta.dart';
import 'package:stream_channel/stream_channel.dart';

import '../runtime/cancellation_zone.dart';
import 'communication.dart';
import 'protocol.dart';
import 'server_txn_policy.dart';

/// The implementation of a drift server, managing remote channels to send
/// database requests.
@internal
class ServerImplementation implements DriftServer {
  /// The Underlying database connection that will be used.
  final QueryExecutor connection;

  /// Whether clients are allowed to shutdown this server for all.
  final bool allowRemoteShutdown;

  /// Whether this server should close the executor after shutting down.
  final bool closeExecutorWhenShutdown;

  final Map<int, QueryExecutor> _managedExecutors = {};
  int _currentExecutorId = 0;
  int _knownSchemaVersion = 0;

  final Map<int, CancellationToken> _cancellableOperations = {};

  /// when a transaction is active, all queries that don't operate on another
  /// query executor have to wait!
  ///
  /// When this list is empty, the top-level executor is active. When not, the
  /// first transaction id in the backlog is active at the moment. Whenever a
  /// transaction completes, we emit an item on [_backlogUpdated]. This can be
  /// used to implement a lock.
  final List<int> _executorBacklog = [];
  final StreamController<void> _backlogUpdated =
      StreamController.broadcast(sync: true);

  bool _isShuttingDown = false;
  final Set<DriftCommunication> _activeChannels = {};

  /// The connection that opened each transaction or exclusive executor in
  /// [_managedExecutors]. When a connection closes (its isolate exited, or
  /// the client closed without ending them), its executors are rolled back
  /// and released: nothing else would ever end them, and every other
  /// connection would wait behind them in [_executorBacklog] forever.
  final Map<int, DriftCommunication> _executorOwners = {};

  /// Executors of a closed connection that [_releaseExecutorsOf] is about to
  /// end. They are all marked before the first one is rolled back: a request
  /// for an outer transaction that waits behind an inner one wakes up as soon
  /// as the inner one is released, and must not get the outer executor while
  /// it is still waiting for its own rollback.
  final Set<int> _releasing = {};
  final Map<int, DriftCommunication> _revoked = {};
  final Completer<void> _done = Completer();

  int _nextConnectionId = 0;
  final Map<DriftCommunication, int> _connectionIds = {};
  final Map<int, int> _parentOf = {};
  final Map<int, _TrackedExecutorTree> _trees = {};
  final List<_WaiterEntry> _waiters = [];
  final Stopwatch _monotonic = Stopwatch()..start();
  Timer? _monitorTimer;

  final StreamController<NotifyTablesUpdated> _tableUpdateNotifications =
      StreamController();

  /// Creates a server from the underlying connection and further options.
  ServerImplementation(this.connection, this.allowRemoteShutdown,
      this.closeExecutorWhenShutdown) {
    done.then((_) {
      _closeRemainingConnections();
      _tableUpdateNotifications.close();
    });
  }

  @override
  Future<void> get done => _done.future;

  @override
  Stream<NotifyTablesUpdated> get tableUpdateNotifications {
    return _tableUpdateNotifications.stream;
  }

  @override
  Future<void> serve(StreamChannel<Object?> channel, {bool serialize = true}) {
    if (_isShuttingDown) {
      throw StateError('Cannot add new channels after shutdown() was called');
    }

    final comm = DriftCommunication(channel, serialize: serialize);
    _connectionIds[comm] = _nextConnectionId++;
    comm.setRequestHandler((request) => _handleRequest(comm, request));
    comm.notify(ServerInfo(connection.dialect));

    _activeChannels.add(comm);
    return comm.closed.then((_) {
      _activeChannels.remove(comm);
      return _releaseExecutorsOf(comm);
    }).then((_) {}, onError: (Object error) {
      // Callers such as RunningDriftServer drop this future; an error here
      // must never become an uncaught error of the server's isolate.
      _logClosedConnection(null, null, 'release_error', error);
    }).whenComplete(() {
      _revoked.removeWhere(
          (id, owner) => owner == comm && !_releasing.contains(id));
      _connectionIds.remove(comm);
    });
  }

  @override
  Future<void> shutdown() {
    if (!_isShuttingDown) {
      _isShuttingDown = true;
      _monitorTimer?.cancel();
      _monitorTimer = null;
      _done.complete(closeExecutorWhenShutdown ? connection.close() : null);
    }

    return done;
  }

  void _closeRemainingConnections() {
    for (final channel in _activeChannels) {
      channel.close();
    }
  }

  FutureOr<ResponsePayload?> _handleRequest(
      DriftCommunication comms, Request request) {
    final payload = request.payload;

    if (payload is NoArgsRequest) {
      switch (payload) {
        case NoArgsRequest.terminateAll:
          if (allowRemoteShutdown) {
            _backlogUpdated.close();
            shutdown();
          } else {
            throw StateError('Remote shutdowns not allowed');
          }

          break;
      }
    } else if (payload is EnsureOpen) {
      return _handleEnsureOpen(comms, payload);
    } else if (payload is ExecuteQuery) {
      final token = runCancellable(() => _runQuery(
          payload.method, payload.sql, payload.args, payload.executorId));
      _cancellableOperations[request.id] = token;
      return token.result
          .whenComplete(() => _cancellableOperations.remove(request.id));
    } else if (payload is ExecuteBatchedStatement) {
      return _runBatched(payload.stmts, payload.executorId);
    } else if (payload is NotifyTablesUpdated) {
      _tableUpdateNotifications.add(payload);
      dispatchTableUpdateNotification(payload, comms);
    } else if (payload is RunNestedExecutorControl) {
      return _transactionControl(comms, payload.control, payload.executorId);
    } else if (payload is RequestCancellation) {
      _cancellableOperations[payload.originalRequestId]?.cancel();
      return null;
    }

    return null;
  }

  Future<ResponsePayload> _handleEnsureOpen(
      DriftCommunication comms, EnsureOpen open) async {
    final executor = await _loadExecutor(open.executorId);
    return _runWithTreeActivity(
      open.executorId,
      () async {
        _knownSchemaVersion = open.schemaVersion;

        return PrimitiveResponsePayload.bool(await executor
            .ensureOpen(_ServerDbUser(this, comms, open.schemaVersion)));
      },
      countStmt: false,
    );
  }

  Future<ResponsePayload?> _runQuery(StatementMethod method, String sql,
      List<Object?> args, int? transactionId) async {
    final executor = await _loadExecutor(transactionId);

    // Give cancellations more time to come in
    await Future<void>.delayed(Duration.zero);
    checkIfCancelled();
    if (transactionId != null && !_isUsable(transactionId)) {
      // Released (or being released) while this statement was waiting: its
      // connection closed or its transaction was revoked. Running it now would
      // run it outside of the transaction.
      if (_revoked.containsKey(transactionId)) {
        throw StateError('[drift:txn_revoked] executor $transactionId');
      }
      throw StateError('Executor $transactionId is no longer active');
    }

    return _runWithTreeActivity(
      transactionId,
      () async {
        switch (method) {
          case StatementMethod.custom:
            await executor.runCustom(sql, args);
            return null;
          case StatementMethod.deleteOrUpdate:
            return PrimitiveResponsePayload.int(
                await executor.runDelete(sql, args));
          case StatementMethod.insert:
            return PrimitiveResponsePayload.int(
                await executor.runInsert(sql, args));
          case StatementMethod.select:
            return SelectResult(await executor.runSelect(sql, args));
        }
      },
      countStmt: true,
    );
  }

  Future<ResponsePayload?> _runBatched(
      BatchedStatements stmts, int? transactionId) async {
    final executor = await _loadExecutor(transactionId);
    return _runWithTreeActivity(
      transactionId,
      () async {
        await executor.runBatched(stmts);
        return null;
      },
      countStmt: true,
    );
  }

  Future<QueryExecutor> _loadExecutor(int? transactionId) async {
    await _waitForTurn(transactionId);
    if (transactionId == null) return connection;

    if (_revoked.containsKey(transactionId)) {
      throw StateError('[drift:txn_revoked] executor $transactionId');
    }
    final executor = _managedExecutors[transactionId];
    if (executor == null || _releasing.contains(transactionId)) {
      // Released, or being released, because its connection closed (see
      // [_releaseExecutorsOf]). Every path that runs something on an executor
      // (statements, batches, commit, rollback, nested executors) loads it
      // here.
      throw StateError('Executor $transactionId is no longer active');
    }
    return executor;
  }

  bool _isUsable(int id) =>
      _managedExecutors.containsKey(id) &&
      !_releasing.contains(id) &&
      !_revoked.containsKey(id);

  Future<int> _spawnTransaction(DriftCommunication comm, int? executor) async {
    final parent = await _loadExecutor(executor);
    return _runWithTreeActivity(
      executor,
      () async {
        _checkStillConnected(comm, 'transaction');
        final transaction = parent.beginTransaction();
        await transaction
            .ensureOpen(_ServerDbUser(this, comm, _knownSchemaVersion));
        return _putOwnedExecutor(
          comm,
          transaction,
          transaction.rollback,
          'transaction',
          parentId: executor,
        );
      },
      countStmt: executor != null,
    );
  }

  Future<int> _spawnExclusive(DriftCommunication comm, int? executor) async {
    final parent = await _loadExecutor(executor);
    return _runWithTreeActivity(
      executor,
      () async {
        _checkStillConnected(comm, 'exclusive');
        final exclusive = parent.beginExclusive();
        await exclusive
            .ensureOpen(_ServerDbUser(this, comm, _knownSchemaVersion));
        return _putOwnedExecutor(
          comm,
          exclusive,
          exclusive.close,
          'exclusive',
          parentId: executor,
        );
      },
      countStmt: executor != null,
    );
  }

  /// A request to open a transaction can wait in the backlog for a long time.
  /// If its connection closed in the meantime, opening it now would create a
  /// transaction nobody can end.
  void _checkStillConnected(DriftCommunication comm, String kind) {
    if (comm.isClosed) {
      _logClosedConnection(null, kind, 'not_opened');
      throw StateError('Connection closed before the executor was opened');
    }
  }

  Future<int> _putOwnedExecutor(
      DriftCommunication comm,
      QueryExecutor executor,
      Future<void> Function() end,
      String kind,
      {int? parentId}) async {
    if (comm.isClosed) {
      // Closed while the executor was being opened: [_releaseExecutorsOf] has
      // already run for this connection, so end it here.
      Object? failure;
      try {
        await end();
      } catch (e) {
        failure = e;
      }
      _logClosedConnection(null, kind,
          failure == null ? 'ended_unregistered' : 'end_failed', failure);
      throw StateError('Connection closed while the executor was being opened');
    }

    final id = _putExecutor(executor, beforeCurrent: true);
    _executorOwners[id] = comm;
    _registerExecutorInTree(id, _connectionIds[comm] ?? 0, kind,
        parentId: parentId);
    return id;
  }

  /// Rolls back and releases the executors [comm] left open, innermost (most
  /// recently opened) first.
  ///
  /// An executor whose commit or rollback was already running when the
  /// connection closed is not expected here: requests for it are only taken
  /// once it is at the head of the backlog, and with a synchronous delegate
  /// (NativeDatabase) the whole commit completes within the event that
  /// received it, before the close is processed.
  Future<void> _releaseExecutorsOf(DriftCommunication comm) async {
    final ids = [
      for (final entry in _executorOwners.entries)
        if (entry.value == comm && !_releasing.contains(entry.key)) entry.key,
    ]..sort((a, b) => b.compareTo(a));
    _releasing.addAll(ids);

    for (final id in ids) {
      final executor = _managedExecutors[id];
      if (executor == null) {
        _releasing.remove(id);
        continue;
      }
      final isTransaction = executor is TransactionExecutor;
      Object? failure;
      try {
        if (executor is TransactionExecutor) {
          await executor.rollback();
        } else {
          await executor.close();
        }
      } catch (e) {
        // The executor is released either way, like a failed rollback.
        failure = e;
      } finally {
        _releaseExecutor(id);
      }
      _logClosedConnection(
        id,
        isTransaction ? 'transaction' : 'exclusive',
        failure != null
            ? (isTransaction ? 'rollback_failed' : 'close_failed')
            : (isTransaction ? 'rolled_back' : 'closed'),
        failure,
      );
    }
  }

  /// One line per executor ended because its connection closed. Never
  /// contains SQL or arguments, and never throws.
  void _logClosedConnection(int? id, String? kind, String outcome,
      [Object? error]) {
    try {
      final buffer = StringBuffer('drift server: closed connection: ')
        ..write('executor=${id ?? '-'} kind=${kind ?? '-'} outcome=$outcome');
      if (error != null) buffer.write(' error=${error.runtimeType}');
      driftRuntimeOptions.debugPrint(buffer.toString());
    } catch (_) {
      // Logging must not change what the server does.
    }
  }

  int _putExecutor(QueryExecutor executor, {bool beforeCurrent = false}) {
    final id = _currentExecutorId++;
    _managedExecutors[id] = executor;

    if (beforeCurrent && _executorBacklog.isNotEmpty) {
      _executorBacklog.insert(0, id);
    } else {
      _executorBacklog.add(id);
    }

    return id;
  }

  Future<ResponsePayload?> _transactionControl(DriftCommunication comm,
      NestedExecutorControl action, int? executorId) async {
    if (action == NestedExecutorControl.beginTransaction) {
      return PrimitiveResponsePayload.int(
          await _spawnTransaction(comm, executorId));
    } else if (action == NestedExecutorControl.startExclusive) {
      return PrimitiveResponsePayload.int(
          await _spawnExclusive(comm, executorId));
    }

    if (action == NestedExecutorControl.rollback &&
        executorId != null &&
        _revoked.containsKey(executorId)) {
      while (_releasing.contains(executorId) &&
          !_isShuttingDown &&
          !_backlogUpdated.isClosed) {
        try {
          await _backlogUpdated.stream.first;
        } catch (_) {
          break;
        }
      }
      return PrimitiveResponsePayload.bool(true);
    }

    final executor = await _loadExecutor(executorId);
    return _runWithTreeActivity(
      executorId,
      () async {
        if (action == NestedExecutorControl.endExclusive) {
          await executor.close();
          _releaseExecutor(executorId!, outcome: 'committed');
          return null;
        }

        if (executor is! TransactionExecutor) {
          throw ArgumentError.value(
            executorId,
            'transactionId',
            "Does not reference a transaction. This might happen if you don't "
                'await all operations made inside a transaction, in which case the '
                'transaction might complete with pending operations.',
          );
        }

        switch (action) {
          case NestedExecutorControl.commit:
            await executor.send();
            // The transaction should only be released if the commit doesn't throw.
            _releaseExecutor(executorId!, outcome: 'committed');
            break;
          case NestedExecutorControl.rollback:
            // Rollbacks shouldn't fail. Other parts of drift assume the transaction
            // to be over after a rollback either way, so we always release the
            // executor in this case.
            try {
              await executor.rollback();
            } finally {
              _releaseExecutor(executorId!, outcome: 'rolled_back');
            }
            break;
          default:
            assert(false, 'Unknown TransactionControl');
        }

        return null;
      },
      countStmt: true,
    );
  }

  void _releaseExecutor(
    int id, {
    String outcome = 'released',
    bool notifyIdleEnd = true,
  }) {
    final tree = _treeForExecutor(id);
    _managedExecutors.remove(id);
    _executorOwners.remove(id);
    _releasing.remove(id);
    _executorBacklog.remove(id);
    _parentOf.remove(id);
    final revokedOwner = _revoked[id];
    if (revokedOwner != null && revokedOwner.isClosed) {
      _revoked.remove(id);
    }
    if (tree != null) {
      tree.ids.remove(id);
      if (id == tree.rootId || tree.ids.isEmpty) {
        _trees.remove(tree.rootId);
        if (notifyIdleEnd && tree.everRecordedIdle) {
          _logIdleTransactionEnd(tree, _now(), outcome);
        }
        _stopMonitorTimerIfEmpty();
      }
    }
    _notifyActiveExecutorUpdated();
  }

  Future<void> _waitForTurn(int? transactionId) {
    bool idIsActive() {
      if (transactionId == null) {
        return _executorBacklog.isEmpty;
      } else {
        // A released executor never becomes active: stop waiting for it, so
        // that [_loadExecutor] can report it.
        return !_isUsable(transactionId) ||
            (_executorBacklog.isNotEmpty &&
                _executorBacklog.first == transactionId);
      }
    }

    // Don't wait for a backlog update if the current transaction id is active
    if (idIsActive()) return Future.value(null);

    final waiter = _WaiterEntry(transactionId, _now());
    _waiters.add(waiter);
    return _backlogUpdated.stream
        .firstWhere((_) => idIsActive())
        .whenComplete(() => _waiters.remove(waiter));
  }

  Duration _now() => serverTransactionPolicy.elapsed?.call() ?? _monotonic.elapsed;

  int _rootIdOf(int id) {
    var cur = id;
    while (_parentOf.containsKey(cur)) {
      cur = _parentOf[cur]!;
    }
    return cur;
  }

  _TrackedExecutorTree? _treeForExecutor(int? id) {
    if (id == null) return null;
    return _trees[_rootIdOf(id)];
  }

  void _registerExecutorInTree(int id, int connId, String kind,
      {int? parentId}) {
    final now = _now();
    if (parentId != null) {
      final rootId = _rootIdOf(parentId);
      final rootTree = _trees[rootId];
      if (rootTree != null) {
        _parentOf[id] = parentId;
        rootTree.ids.add(id);
        return;
      }
    }
    _trees[id] = _TrackedExecutorTree(
      rootId: id,
      connId: connId,
      kind: kind,
      openedAt: now,
      lastIdleMark: now,
    );
    _ensureMonitorTimer();
  }

  Future<T> _runWithTreeActivity<T>(
    int? executorId,
    Future<T> Function() body, {
    required bool countStmt,
  }) async {
    final tree = _treeForExecutor(executorId);
    if (tree == null) return body();
    final now = _now();
    tree.inProgress++;
    tree.awakeIdle = Duration.zero;
    tree.lastIdleMark = now;
    if (countStmt) tree.stmts++;
    try {
      return await body();
    } finally {
      tree.inProgress--;
      if (tree.inProgress == 0) {
        final finishedAt = _now();
        tree.awakeIdle = Duration.zero;
        tree.lastIdleMark = finishedAt;
      }
    }
  }

  void _ensureMonitorTimer() {
    if (_monitorTimer != null || _trees.isEmpty || _isShuttingDown) return;
    _monitorTimer = Timer.periodic(
      const Duration(seconds: 1),
      (_) => _onMonitorTick(),
    );
  }

  void _stopMonitorTimerIfEmpty() {
    if (_trees.isEmpty || _isShuttingDown) {
      _monitorTimer?.cancel();
      _monitorTimer = null;
    }
  }

  void _onMonitorTick() {
    if (_isShuttingDown || _trees.isEmpty) {
      _stopMonitorTimerIfEmpty();
      return;
    }
    final policy = serverTransactionPolicy;
    final now = _now();
    if (policy.mode == ServerTransactionMode.off) {
      for (final tree in _trees.values) {
        tree.lastIdleMark = now;
      }
      return;
    }

    for (final tree in List.of(_trees.values)) {
      if (!_trees.containsKey(tree.rootId)) continue;
      if (_executorBacklog.isEmpty ||
          !tree.ids.contains(_executorBacklog.first)) {
        tree.lastIdleMark = now;
        continue;
      }
      if (tree.inProgress > 0) {
        tree.lastIdleMark = now;
        continue;
      }
      final tickDelta = now - tree.lastIdleMark;
      if (tickDelta > Duration.zero) {
        const maxCredit = Duration(seconds: 2);
        final credit = tickDelta > maxCredit ? maxCredit : tickDelta;
        tree.awakeIdle += credit;
        if (tree.awakeIdle > tree.maxAwakeIdle) {
          tree.maxAwakeIdle = tree.awakeIdle;
        }
      }
      tree.lastIdleMark = now;

      var waitersOutside = 0;
      var oldestWait = Duration.zero;
      for (final waiter in _waiters) {
        if (waiter.targetExecutorId == null ||
            !tree.ids.contains(waiter.targetExecutorId)) {
          waitersOutside++;
          final wait = now - waiter.startedAt;
          if (wait > oldestWait) {
            oldestWait = wait;
          }
        }
      }
      if (waitersOutside == 0) continue;

      if (policy.mode == ServerTransactionMode.enforce &&
          tree.kind == 'transaction' &&
          tree.awakeIdle >= policy.idleLimit &&
          !tree.ids.any(
              (id) => _releasing.contains(id) || _revoked.containsKey(id))) {
        unawaited(_revokeTree(
          tree,
          now: now,
          waiters: waitersOutside,
          oldestWait: oldestWait,
        ));
        continue;
      }

      if (tree.awakeIdle >= policy.recordAfter) {
        final shouldEmit = tree.lastRecordedAwakeIdle == null ||
            (tree.awakeIdle - tree.lastRecordedAwakeIdle!) >=
                policy.recordEvery;
        if (shouldEmit) {
          tree.everRecordedIdle = true;
          tree.lastRecordedAwakeIdle = tree.awakeIdle;
          _logIdleTransaction(
            tree,
            now: now,
            waiters: waitersOutside,
            oldestWait: oldestWait,
          );
        }
      }
    }
  }

  Future<void> _revokeTree(
    _TrackedExecutorTree tree, {
    required Duration now,
    required int waiters,
    required Duration oldestWait,
  }) async {
    final ids = tree.ids.toList()..sort((a, b) => b.compareTo(a));
    final nested = tree.nestedCount;
    _trees.remove(tree.rootId);
    _stopMonitorTimerIfEmpty();
    _releasing.addAll(ids);
    for (final id in ids) {
      final owner = _executorOwners[id];
      if (owner != null) {
        _revoked[id] = owner;
      }
    }

    Object? failure;
    for (final id in ids) {
      final executor = _managedExecutors[id];
      if (executor == null) {
        _releasing.remove(id);
        continue;
      }
      try {
        if (executor is TransactionExecutor) {
          await executor.rollback();
        } else {
          await executor.close();
        }
      } catch (e) {
        failure ??= e;
      } finally {
        _releaseExecutor(id, notifyIdleEnd: false);
      }
    }

    if (tree.everRecordedIdle) {
      _logIdleTransactionEnd(tree, _now(), 'released');
    }
    _logRevokedTransaction(
      tree,
      now: now,
      waiters: waiters,
      oldestWait: oldestWait,
      nested: nested,
      outcome: failure == null ? 'rolled_back' : 'rollback_failed',
    );
  }

  void _logRevokedTransaction(
    _TrackedExecutorTree tree, {
    required Duration now,
    required int waiters,
    required Duration oldestWait,
    required int nested,
    required String outcome,
  }) {
    try {
      final ageMs = (now - tree.openedAt).inMilliseconds;
      final idleMs = tree.awakeIdle.inMilliseconds;
      final oldestWaitMs = oldestWait.inMilliseconds;
      driftRuntimeOptions.debugPrint(
        'drift server: revoked transaction: executor=${tree.rootId} '
        'conn=${tree.connId} age_ms=${ageMs < 0 ? 0 : ageMs} '
        'idle_ms=$idleMs waiters=$waiters '
        'oldest_wait_ms=${oldestWaitMs < 0 ? 0 : oldestWaitMs} '
        'stmts=${tree.stmts} nested=$nested outcome=$outcome',
      );
    } catch (_) {
      // Logging must not change what the server does.
    }
  }

  void _logIdleTransaction(
    _TrackedExecutorTree tree, {
    required Duration now,
    required int waiters,
    required Duration oldestWait,
  }) {
    try {
      final ageMs = (now - tree.openedAt).inMilliseconds;
      final idleMs = tree.awakeIdle.inMilliseconds;
      final oldestWaitMs = oldestWait.inMilliseconds;
      final action = tree.kind == 'transaction'
          ? 'action=would_revoke'
          : 'kind=${tree.kind} action=record_only';
      driftRuntimeOptions.debugPrint(
        'drift server: idle transaction: executor=${tree.rootId} '
        'conn=${tree.connId} age_ms=${ageMs < 0 ? 0 : ageMs} '
        'idle_ms=$idleMs waiters=$waiters '
        'oldest_wait_ms=${oldestWaitMs < 0 ? 0 : oldestWaitMs} '
        'stmts=${tree.stmts} nested=${tree.nestedCount} $action',
      );
    } catch (_) {
      // Logging must not change what the server does.
    }
  }

  void _logIdleTransactionEnd(
    _TrackedExecutorTree tree,
    Duration now,
    String outcome,
  ) {
    try {
      driftRuntimeOptions.debugPrint(
        'drift server: idle transaction end: executor=${tree.rootId} '
        'max_idle_ms=${tree.maxAwakeIdle.inMilliseconds} outcome=$outcome',
      );
    } catch (_) {
      // Logging must not change what the server does.
    }
  }

  void _notifyActiveExecutorUpdated() {
    if (!_backlogUpdated.isClosed) {
      _backlogUpdated.add(null);
    }
  }

  @override
  void dispatchTableUpdateNotification(NotifyTablesUpdated notification,
      [DriftCommunication? source]) {
    for (final connected in _activeChannels) {
      if (connected != source) {
        connected.notify(notification);
      }
    }
  }
}

class _ServerDbUser implements QueryExecutorUser {
  final ServerImplementation _server;
  final DriftCommunication connection;
  @override
  final int schemaVersion;

  _ServerDbUser(this._server, this.connection, this.schemaVersion);

  @override
  Future<void> beforeOpen(
      QueryExecutor executor, OpeningDetails details) async {
    final id = _server._putExecutor(executor, beforeCurrent: true);
    _server._registerExecutorInTree(
      id,
      _server._connectionIds[connection] ?? 0,
      'before_open',
    );
    var outcome = 'committed';
    try {
      await connection.request<void>(RunBeforeOpen(details, id));
    } catch (_) {
      outcome = 'released';
      rethrow;
    } finally {
      _server._releaseExecutor(id, outcome: outcome);
    }
  }
}

class _TrackedExecutorTree {
  _TrackedExecutorTree({
    required this.rootId,
    required this.connId,
    required this.kind,
    required this.openedAt,
    required this.lastIdleMark,
  }) : ids = {rootId};

  final int rootId;
  final int connId;
  final String kind;
  final Duration openedAt;
  final Set<int> ids;

  Duration lastIdleMark;
  Duration awakeIdle = Duration.zero;
  Duration maxAwakeIdle = Duration.zero;
  Duration? lastRecordedAwakeIdle;
  bool everRecordedIdle = false;
  int inProgress = 0;
  int stmts = 0;

  int get nestedCount => ids.length > 1 ? ids.length - 1 : 0;
}

class _WaiterEntry {
  _WaiterEntry(this.targetExecutorId, this.startedAt);

  final int? targetExecutorId;
  final Duration startedAt;
}
