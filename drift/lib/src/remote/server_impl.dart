import 'dart:async';

import 'package:drift/drift.dart';
import 'package:drift/remote.dart';
import 'package:meta/meta.dart';
import 'package:stream_channel/stream_channel.dart';

import '../runtime/cancellation_zone.dart';
import 'communication.dart';
import 'protocol.dart';

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
  final Completer<void> _done = Completer();

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
    });
  }

  @override
  Future<void> shutdown() {
    if (!_isShuttingDown) {
      _isShuttingDown = true;
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
    _knownSchemaVersion = open.schemaVersion;

    return PrimitiveResponsePayload.bool(await executor
        .ensureOpen(_ServerDbUser(this, comms, open.schemaVersion)));
  }

  Future<ResponsePayload?> _runQuery(StatementMethod method, String sql,
      List<Object?> args, int? transactionId) async {
    final executor = await _loadExecutor(transactionId);

    // Give cancellations more time to come in
    await Future<void>.delayed(Duration.zero);
    checkIfCancelled();
    if (transactionId != null && !_isUsable(transactionId)) {
      // Released (or being released) while this statement was waiting: its
      // connection closed. Running it now would run it outside of the
      // transaction.
      throw StateError('Executor $transactionId is no longer active');
    }

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
  }

  Future<ResponsePayload?> _runBatched(
      BatchedStatements stmts, int? transactionId) async {
    final executor = await _loadExecutor(transactionId);
    await executor.runBatched(stmts);
    return null;
  }

  Future<QueryExecutor> _loadExecutor(int? transactionId) async {
    await _waitForTurn(transactionId);
    if (transactionId == null) return connection;

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
      _managedExecutors.containsKey(id) && !_releasing.contains(id);

  Future<int> _spawnTransaction(DriftCommunication comm, int? executor) async {
    final parent = await _loadExecutor(executor);
    _checkStillConnected(comm, 'transaction');
    final transaction = parent.beginTransaction();
    await transaction
        .ensureOpen(_ServerDbUser(this, comm, _knownSchemaVersion));
    return _putOwnedExecutor(
        comm, transaction, transaction.rollback, 'transaction');
  }

  Future<int> _spawnExclusive(DriftCommunication comm, int? executor) async {
    final parent = await _loadExecutor(executor);
    _checkStillConnected(comm, 'exclusive');
    final exclusive = parent.beginExclusive();
    await exclusive.ensureOpen(_ServerDbUser(this, comm, _knownSchemaVersion));
    return _putOwnedExecutor(comm, exclusive, exclusive.close, 'exclusive');
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

  Future<int> _putOwnedExecutor(DriftCommunication comm, QueryExecutor executor,
      Future<void> Function() end, String kind) async {
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
        if (entry.value == comm) entry.key,
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

    final executor = await _loadExecutor(executorId);
    if (action == NestedExecutorControl.endExclusive) {
      await executor.close();
      _releaseExecutor(executorId!);
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
        _releaseExecutor(executorId!);
        break;
      case NestedExecutorControl.rollback:
        // Rollbacks shouldn't fail. Other parts of drift assume the transaction
        // to be over after a rollback either way, so we always release the
        // executor in this case.
        try {
          await executor.rollback();
        } finally {
          _releaseExecutor(executorId!);
        }
        break;
      default:
        assert(false, 'Unknown TransactionControl');
    }

    return null;
  }

  void _releaseExecutor(int id) {
    _managedExecutors.remove(id);
    _executorOwners.remove(id);
    _releasing.remove(id);
    _executorBacklog.remove(id);
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

    return _backlogUpdated.stream.firstWhere((_) => idIsActive());
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
    try {
      await connection.request<void>(RunBeforeOpen(details, id));
    } finally {
      _server._releaseExecutor(id);
    }
  }
}
