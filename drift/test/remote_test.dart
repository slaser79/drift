import 'dart:async';

import 'package:async/async.dart';
import 'package:drift/drift.dart' hide isNotNull, isNull;
import 'package:drift/remote.dart';
import 'package:drift/src/remote/communication.dart';
import 'package:drift/src/remote/protocol.dart';
import 'package:drift/src/utils/synchronized.dart';
import 'package:fake_async/fake_async.dart';
import 'package:mockito/mockito.dart';
import 'package:stream_channel/stream_channel.dart';
import 'package:test/test.dart';

import 'generated/todos.dart';
import 'test_utils/test_utils.dart';

void main() {
  test('closes channel in shutdown', () async {
    final controller = StreamChannelController<Object?>();
    final server =
        DriftServer(testInMemoryDatabase(), allowRemoteShutdown: true);
    server.serve(controller.foreign);

    await shutdown(controller.local.expectedToClose);
  });

  test('can shutdown server on close', () async {
    final controller = StreamChannelController<Object?>();
    final server =
        DriftServer(testInMemoryDatabase(), allowRemoteShutdown: true);
    server.serve(controller.foreign);

    final client = await connectToRemoteAndInitialize(
        controller.local.expectedToClose,
        singleClientMode: true);
    final db = TodoDb(client);

    await db.todosTable.select().get();
    await db.close();

    expect(server.done, completes);
  });

  test(
    'does not send table update notifications in single client mode',
    () async {
      final server =
          DriftServer(testInMemoryDatabase(), allowRemoteShutdown: true);
      final controller = StreamChannelController<Object?>();
      server.serve(controller.foreign, serialize: false);

      final client = await connectToRemoteAndInitialize(
        controller.local.transformSink(StreamSinkTransformer.fromHandlers(
          handleData: (data, out) {
            expect(data, isNot(isA<NotifyTablesUpdated>()));
            out.add(data);
          },
        )),
        serialize: false,
        singleClientMode: true,
      );

      final db = TodoDb(client);
      await db.todosTable.select().get();
      await db.close();
    },
  );

  test('Uint8Lists are mapped from and to Uint8Lists', () async {
    const protocol = DriftProtocol();

    final request = Request(
      1,
      ExecuteQuery(StatementMethod.select, 'SELECT ?', [
        Uint8List.fromList([1, 2, 3])
      ]),
    );

    final mapped = protocol.deserialize(protocol.serialize(request)!);
    expect(
      mapped,
      isA<Request>().having((e) => e.id, 'id', 1).having(
            (e) => e.payload,
            'payload',
            isA<ExecuteQuery>()
                .having((e) => e.method, 'method', StatementMethod.select)
                .having((e) => e.args, 'args', [isA<Uint8List>()]),
          ),
    );
  });

  test('BigInts are serialied', () {
    const protocol = DriftProtocol();

    final request = Request(
      1,
      ExecuteQuery(StatementMethod.select, 'SELECT ?', [BigInt.one]),
    );

    final mapped = _checkSimpleRoundtrip(protocol, request);
    expect(
      mapped,
      isA<Request>().having((e) => e.id, 'id', 1).having(
            (e) => e.payload,
            'payload',
            isA<ExecuteQuery>()
                .having((e) => e.method, 'method', StatementMethod.select)
                .having((e) => e.args, 'args', [isA<BigInt>()]),
          ),
    );

    final response = SuccessResponse(
        1,
        SelectResult([
          {'col': BigInt.one}
        ]));
    final mappedResponse = _checkSimpleRoundtrip(protocol, response);
    expect(
      mappedResponse,
      isA<SuccessResponse>().having((e) => e.requestId, 'requestId', 1).having(
            (e) => e.response,
            'response',
            isA<SelectResult>().having(
              (e) => e.rows,
              'rows',
              ([
                {'col': BigInt.one}
              ]),
            ),
          ),
    );

    final batchRequest = _checkSimpleRoundtrip(
      protocol,
      Request(
        1,
        ExecuteBatchedStatement(BatchedStatements(
          ['SELECT ?'],
          [
            ArgumentsForBatchedStatement(0, [BigInt.zero]),
            ArgumentsForBatchedStatement(0, [BigInt.one]),
            ArgumentsForBatchedStatement(0, [BigInt.two]),
          ],
        )),
      ),
    );
    expect(
      batchRequest,
      isA<Request>().having((e) => e.id, 'id', 1).having(
            (e) => e.payload,
            'payload',
            isA<ExecuteBatchedStatement>().having(
              (e) => e.stmts,
              'stmts',
              BatchedStatements(
                ['SELECT ?'],
                [
                  ArgumentsForBatchedStatement(0, [BigInt.zero]),
                  ArgumentsForBatchedStatement(0, [BigInt.one]),
                  ArgumentsForBatchedStatement(0, [BigInt.two]),
                ],
              ),
            ),
          ),
    );
  });

  test('can run protocol without using complex types', () async {
    final executor = MockExecutor();
    final server = DriftServer(DatabaseConnection(executor));
    addTearDown(server.shutdown);

    final channelController = StreamChannelController<Object?>();
    server.serve(channelController.foreign.changeStream(_checkStreamOfSimple),
        serialize: true);

    final connection = await connectToRemoteAndInitialize(
        channelController.local
            .changeStream(_checkStreamOfSimple)
            .expectedToClose,
        serialize: true);
    final db = TodoDb(connection);

    await db.customSelect('SELECT ?, ?, ?, ?', variables: [
      Variable.withBigInt(BigInt.one),
      Variable.withBool(true),
      Variable.withReal(1.2),
      Variable.withBlob(Uint8List(12)),
    ]).get();
    verify(executor.runSelect('SELECT ?, ?, ?, ?', [
      BigInt.one,
      1,
      1.2,
      Uint8List(12),
    ]));

    when(executor.runInsert(any, any)).thenAnswer(
        (realInvocation) => Future.error(UnimplementedError('error!')));
    await expectLater(
      db.categories
          .insertOne(CategoriesCompanion.insert(description: 'description')),
      throwsA(isA<DriftRemoteException>().having(
          (e) => e.remoteCause, 'remoteCause', 'UnimplementedError: error!')),
    );

    final statements =
        BatchedStatements(['SELECT 1'], [ArgumentsForBatchedStatement(0, [])]);
    when(executor.runBatched(any)).thenAnswer((i) => Future.value());
    // Not using db.batch because that starts a transaction, we want to test
    // this working with the default executor.
    // Regression test for: https://github.com/simolus3/drift/pull/2707
    await db.executor.runBatched(statements);
    verify(executor.runBatched(statements));

    // Regression test for https://github.com/simolus3/drift/issues/3194
    await db.transaction(() async {
      await db.customUpdate('DELETE FROM "users" WHERE 0');
    });

    await db.close();
  });

  test('nested transactions', () async {
    final controller = StreamChannelController<Object?>();
    final executor = MockExecutor();
    final outerTransaction = executor.transactions;
    // avoid this object being created implicitly in the beginTransaction() when
    // stub because that breaks mockito.
    outerTransaction.transactions; // ignore: unnecessary_statements
    final innerTransactions = <MockTransactionExecutor>[];

    TransactionExecutor newTransaction(Invocation _) {
      final transaction = MockTransactionExecutor()..transactions;
      innerTransactions.add(transaction);
      when(transaction.beginTransaction()).thenAnswer(newTransaction);
      return transaction;
    }

    when(outerTransaction.beginTransaction()).thenAnswer(newTransaction);

    final server = DriftServer(DatabaseConnection(executor));
    server.serve(controller.foreign);
    addTearDown(server.shutdown);

    final db = TodoDb(await connectToRemoteAndInitialize(controller.local));
    addTearDown(db.close);

    await db.transaction(() async {
      final abortException = Exception('abort');

      await expectLater(db.transaction(() async {
        await db.select(db.todosTable).get();
        throw abortException;
      }), throwsA(abortException));

      await db.transaction(() async {
        await db.select(db.todosTable).get();

        await db.transaction(() => db.select(db.todosTable).get());
      });
    });

    verify(outerTransaction.beginTransaction());
    verify(innerTransactions[0].ensureOpen(any));
    verify(innerTransactions[0].rollback());
    verify(innerTransactions[1].ensureOpen(any));
    verify(innerTransactions[1].beginTransaction());
    verify(innerTransactions[2].ensureOpen(any));
    verify(innerTransactions[2].send());
    verify(innerTransactions[1].send());
    verify(outerTransaction.send());
  });

  test('handles exclusive executors', () async {
    final controller = StreamChannelController<Object?>();
    final executor = MockExecutor();
    final multi = MultiChannel<Object?>(controller.local);

    final testEvents = StreamController<String>();
    final testEventQueue = StreamQueue(testEvents.stream);
    final lock = Lock();

    final server = DriftServer(DatabaseConnection(executor));
    controller.foreign.serveMulti(server);
    addTearDown(server.shutdown);

    final a = TodoDb(await multi.newRemoteConnection());
    final b = TodoDb(await multi.newRemoteConnection());

    final exclusiveA = MockExecutor();
    final exclusiveB = MockExecutor();

    var exclusiveCount = 0;
    when(executor.beginExclusive()).thenAnswer(expectAsync1((_) {
      if (exclusiveCount == 0) {
        exclusiveCount++;
        testEvents.add('try-a');
        return exclusiveA;
      } else {
        testEvents.add('try-b');
        return exclusiveB;
      }
    }, count: 2, id: 'beginExclusive'));

    for (final (name, executor) in [('a', exclusiveA), ('b', exclusiveB)]) {
      final closeCompleter = Completer<void>();

      when(executor.ensureOpen(any)).thenAnswer((i) {
        if (!executor.opened) {
          return expectAsync0(() async {
            await Future<void>.delayed(Duration.zero);

            final ready = Completer<bool>();
            lock.synchronized(() {
              testEvents.add('grant-$name');
              ready.complete(true);
              executor.opened = true;
              return closeCompleter.future;
            });

            return ready.future;
          }, id: 'ensureOpen-$name')();
        } else {
          return Future.value(true);
        }
      });

      when(executor.close()).thenAnswer(expectAsync1((_) async {
        testEvents.add('close-$name');
        closeCompleter.complete();
      }, id: 'close-$name'));
    }

    final wait = Completer<void>();
    a.exclusively(() async {
      await a.customSelect('SELECT 1').get();
      await wait.future;
    });

    b.exclusively(() async {
      await b.customSelect('SELECT 1').get();
    });

    await expectLater(
      testEventQueue,
      emitsInOrder(['try-a', 'try-b']),
    );

    wait.complete();
    await expectLater(
      testEventQueue,
      emitsInOrder(['grant-a', 'close-a', 'grant-b', 'close-b']),
    );
  });

  test('reports correct dialect of remote', () async {
    final executor = MockExecutor();
    when(executor.dialect).thenReturn(SqlDialect.postgres);

    final controller = StreamChannelController<Object?>();
    final server = DriftServer(DatabaseConnection(executor))
      ..serve(controller.foreign);

    final client = await connectToRemoteAndInitialize(controller.local);
    await server.shutdown();
    expect(client.executor.dialect, SqlDialect.postgres);
  });

  test('respond failure becomes an error response', () async {
    final controller = StreamChannelController<Object?>();
    var threwOnSuccess = false;
    final serverSide = controller.foreign.transformSink(
      StreamSinkTransformer<Object?, Object?>.fromHandlers(
        handleData: (Object? data, EventSink<Object?> out) {
          if (!threwOnSuccess && data is SuccessResponse) {
            threwOnSuccess = true;
            throw ArgumentError('simulated unsendable success payload');
          }
          out.add(data);
        },
      ),
    );

    final logged = <String>[];
    final previousPrint = driftRuntimeOptions.debugPrint;
    driftRuntimeOptions.debugPrint = logged.add;
    addTearDown(() => driftRuntimeOptions.debugPrint = previousPrint);

    final serverComm = DriftCommunication(serverSide, serialize: false);
    serverComm.setRequestHandler(
      (request) async => const SelectResult([
        {'ok': 1}
      ]),
    );
    final clientComm = DriftCommunication(controller.local, serialize: false);
    addTearDown(() async {
      await clientComm.close();
      await serverComm.close();
    });

    await expectLater(
      clientComm
          .request<SelectResult>(NoArgsRequest.terminateAll)
          .timeout(const Duration(seconds: 1)),
      throwsA(
        isA<DriftRemoteException>().having(
          (DriftRemoteException e) => e.remoteCause,
          'remoteCause',
          isA<StateError>().having(
            (StateError s) => s.message,
            'message',
            contains('[drift:respond_failed] ArgumentError'),
          ),
        ),
      ),
    );
    expect(
      logged,
      contains('drift server: respond failed: type=ArgumentError'),
    );

    // Control: subsequent normal response succeeds over the same channel.
    final ok = await clientComm
        .request<SelectResult>(NoArgsRequest.terminateAll)
        .timeout(const Duration(seconds: 1));
    expect(ok.rows, [
      {'ok': 1}
    ]);
  });

  test('respond failure when fallback also fails does not throw unhandled',
      () async {
    final controller = StreamChannelController<Object?>();
    final serverSide = controller.foreign.transformSink(
      StreamSinkTransformer<Object?, Object?>.fromHandlers(
        handleData: (Object? data, EventSink<Object?> out) {
          if (data is SuccessResponse || data is ErrorResponse) {
            throw StateError('sink broken');
          }
          out.add(data);
        },
      ),
    );

    final logged = <String>[];
    final previousPrint = driftRuntimeOptions.debugPrint;
    driftRuntimeOptions.debugPrint = logged.add;
    addTearDown(() => driftRuntimeOptions.debugPrint = previousPrint);

    final uncaught = <Object>[];
    await runZonedGuarded(() async {
      final serverComm = DriftCommunication(serverSide, serialize: false);
      serverComm.setRequestHandler(
        (request) async => const SelectResult([
          {'ok': 1}
        ]),
      );
      final clientComm = DriftCommunication(controller.local, serialize: false);

      await expectLater(
        clientComm
            .request<SelectResult>(NoArgsRequest.terminateAll)
            .timeout(const Duration(milliseconds: 200)),
        throwsA(isA<TimeoutException>()),
      );
      await clientComm.close();
      await serverComm.close();
    }, (error, _) {
      uncaught.add(error);
    });

    expect(uncaught, isEmpty);
    expect(logged, ['drift server: respond failed: type=StateError']);
  });

  test(
      'idle transaction detector: freeze credit caps tick delta at 2 s and tracks idle_end',
      () {
    fakeAsync((async) {
      var fakeNow = Duration.zero;
      final previousPolicy = serverTransactionPolicy;
      serverTransactionPolicy = ServerTransactionPolicy(
        mode: ServerTransactionMode.record,
        recordAfter: const Duration(seconds: 3),
        recordEvery: const Duration(seconds: 60),
        idleLimit: const Duration(seconds: 30),
        elapsed: () => fakeNow,
      );
      final logged = <String>[];
      final previousPrint = driftRuntimeOptions.debugPrint;
      driftRuntimeOptions.debugPrint = logged.add;
      try {
        final server = DriftServer(testInMemoryDatabase());
        final c1 = StreamChannelController<Object?>();
        final c2 = StreamChannelController<Object?>();
        server.serve(c1.foreign, serialize: false);
        server.serve(c2.foreign, serialize: false);

        late DatabaseConnection conn1;
        late DatabaseConnection conn2;
        connectToRemoteAndInitialize(c1.local, serialize: false)
            .then((c) => conn1 = c);
        connectToRemoteAndInitialize(c2.local, serialize: false)
            .then((c) => conn2 = c);
        async.flushMicrotasks();

        final user = CustomQueryExecutorUser(
          schemaVersion: 1,
          beforeOpenCallback: (_, __, ___) async {},
        );
        conn1.executor.ensureOpen(user);
        conn2.executor.ensureOpen(user);
        async.flushMicrotasks();

        final txn = conn1.executor.beginTransaction();
        expect(driftRemoteExecutorId(txn), isNull);
        txn.ensureOpen(user);
        async.flushMicrotasks();
        final xid = driftRemoteExecutorId(txn);
        expect(xid, isNotNull);
        expect(driftWasRemoteTransactionRevoked(txn), isFalse);

        // External waiter starts waiting.
        var waiterDone = false;
        conn2.executor.runSelect('SELECT 1', const []).then((_) {
          waiterDone = true;
        });
        async.flushMicrotasks();
        expect(waiterDone, isFalse);

        // Tick 1 (1s) -> idle = 1s (below 3s).
        fakeNow += const Duration(seconds: 1);
        async.elapse(const Duration(seconds: 1));
        expect(logged.where((l) => l.startsWith('drift server: idle')), isEmpty);

        // Simulated OS freeze: stopwatch jumps 300s between two ticks.
        fakeNow += const Duration(seconds: 300);
        async.elapse(const Duration(seconds: 1));
        // Awake idle grew by at most 2s (1s + 2s = 3s == recordAfter).
        final idleLines = logged
            .where((l) => l.startsWith('drift server: idle transaction:'))
            .toList();
        expect(idleLines, hasLength(1));
        expect(idleLines.single, contains('executor=$xid'));
        expect(idleLines.single, contains('idle_ms=3000'));
        expect(idleLines.single, contains('waiters=1'));
        expect(idleLines.single, contains('action=would_revoke'));
        expect(waiterDone, isFalse);

        // Commit ends the tree and logs idle_end with max_idle_ms=3000.
        txn.send();
        async.elapse(Duration.zero);
        expect(waiterDone, isTrue);

        final endLines = logged
            .where((l) => l.startsWith('drift server: idle transaction end:'))
            .toList();
        expect(endLines, hasLength(1));
        expect(endLines.single, contains('executor=$xid'));
        expect(endLines.single, contains('max_idle_ms=3000'));
        expect(endLines.single, contains('outcome=committed'));

        conn1.executor.close();
        conn2.executor.close();
        server.shutdown();
        async.elapse(Duration.zero);
      } finally {
        driftRuntimeOptions.debugPrint = previousPrint;
        serverTransactionPolicy = previousPolicy;
      }
    });
  });

  test('idle transaction detector: busy transaction and no-waiter control emit nothing',
      () {
    fakeAsync((async) {
      var fakeNow = Duration.zero;
      final previousPolicy = serverTransactionPolicy;
      serverTransactionPolicy = ServerTransactionPolicy(
        mode: ServerTransactionMode.record,
        recordAfter: const Duration(seconds: 3),
        recordEvery: const Duration(seconds: 60),
        idleLimit: const Duration(seconds: 30),
        elapsed: () => fakeNow,
      );
      final logged = <String>[];
      final previousPrint = driftRuntimeOptions.debugPrint;
      driftRuntimeOptions.debugPrint = logged.add;
      try {
        final server = DriftServer(testInMemoryDatabase());
        final c1 = StreamChannelController<Object?>();
        final c2 = StreamChannelController<Object?>();
        server.serve(c1.foreign, serialize: false);
        server.serve(c2.foreign, serialize: false);

        late DatabaseConnection conn1;
        late DatabaseConnection conn2;
        connectToRemoteAndInitialize(c1.local, serialize: false)
            .then((c) => conn1 = c);
        connectToRemoteAndInitialize(c2.local, serialize: false)
            .then((c) => conn2 = c);
        async.flushMicrotasks();

        final user = CustomQueryExecutorUser(
          schemaVersion: 1,
          beforeOpenCallback: (_, __, ___) async {},
        );
        conn1.executor.ensureOpen(user);
        conn2.executor.ensureOpen(user);
        async.flushMicrotasks();

        // Part 1: no waiter for 5s -> silent.
        final noWaiterTxn = conn1.executor.beginTransaction();
        noWaiterTxn.ensureOpen(user);
        async.flushMicrotasks();
        for (var i = 0; i < 5; i++) {
          fakeNow += const Duration(seconds: 1);
          async.elapse(const Duration(seconds: 1));
        }
        noWaiterTxn.send();
        async.flushMicrotasks();
        expect(logged.where((l) => l.startsWith('drift server: idle')), isEmpty);

        // Part 2: busy transaction (statement every 300ms for 6s) with waiter -> silent.
        final busyTxn = conn1.executor.beginTransaction();
        busyTxn.ensureOpen(user);
        async.elapse(Duration.zero);
        var busyWaiterDone = false;
        conn2.executor.runSelect('SELECT 1', const []).then((_) {
          busyWaiterDone = true;
        });
        async.elapse(Duration.zero);
        for (var i = 0; i < 20; i++) {
          fakeNow += const Duration(milliseconds: 300);
          async.elapse(const Duration(milliseconds: 300));
          busyTxn.runSelect('SELECT 1', const []);
          async.elapse(Duration.zero);
        }
        busyTxn.send();
        async.elapse(Duration.zero);
        expect(busyWaiterDone, isTrue);
        expect(logged.where((l) => l.startsWith('drift server: idle')), isEmpty);

        conn1.executor.close();
        conn2.executor.close();
        server.shutdown();
        async.elapse(Duration.zero);
      } finally {
        driftRuntimeOptions.debugPrint = previousPrint;
        serverTransactionPolicy = previousPolicy;
      }
    });
  });

  test(
      'idle transaction detector: nested transaction inside beforeOpen records kind=before_open action=record_only',
      () {
    fakeAsync((async) {
      var fakeNow = Duration.zero;
      final previousPolicy = serverTransactionPolicy;
      serverTransactionPolicy = ServerTransactionPolicy(
        mode: ServerTransactionMode.record,
        recordAfter: const Duration(seconds: 2),
        recordEvery: const Duration(seconds: 60),
        idleLimit: const Duration(seconds: 30),
        elapsed: () => fakeNow,
      );
      final logged = <String>[];
      final previousPrint = driftRuntimeOptions.debugPrint;
      driftRuntimeOptions.debugPrint = logged.add;
      try {
        final server = DriftServer(testInMemoryDatabase());
        final c1 = StreamChannelController<Object?>();
        final c2 = StreamChannelController<Object?>();
        server.serve(c1.foreign, serialize: false);
        server.serve(c2.foreign, serialize: false);

        late DatabaseConnection conn1;
        late DatabaseConnection conn2;
        connectToRemoteAndInitialize(c1.local, serialize: false)
            .then((c) => conn1 = c);
        connectToRemoteAndInitialize(c2.local, serialize: false)
            .then((c) => conn2 = c);
        async.flushMicrotasks();

        final holdMigration = Completer<void>();
        final migrationDb = TodoDb(conn1)
          ..migration = MigrationStrategy(
            onCreate: (m) async {
              await m.database.transaction(() async {
                await holdMigration.future;
              });
            },
          );
        final waiterDb = TodoDb(conn2);

        migrationDb.customSelect('SELECT 1').get();
        async.flushMicrotasks();
        var waiterDone = false;
        waiterDb.customSelect('SELECT 1').get().then((_) => waiterDone = true);
        async.flushMicrotasks();

        for (var i = 0; i < 3; i++) {
          fakeNow += const Duration(seconds: 1);
          async.elapse(const Duration(seconds: 1));
        }

        final idleLines = logged
            .where((l) => l.startsWith('drift server: idle transaction:'))
            .toList();
        expect(idleLines, hasLength(1));
        expect(idleLines.single, contains('kind=before_open'));
        expect(idleLines.single, contains('action=record_only'));
        expect(waiterDone, isFalse);

        holdMigration.complete();
        async.elapse(Duration.zero);
        expect(waiterDone, isTrue);

        migrationDb.close();
        waiterDb.close();
        server.shutdown();
        async.elapse(Duration.zero);
      } finally {
        driftRuntimeOptions.debugPrint = previousPrint;
        serverTransactionPolicy = previousPolicy;
      }
    });
  });

  test(
      'enforce: revokes stuck holder, fails subsequent ops with marker, rollback reports revoked',
      () {
    fakeAsync((async) {
      var fakeNow = Duration.zero;
      final previousPolicy = serverTransactionPolicy;
      serverTransactionPolicy = ServerTransactionPolicy(
        mode: ServerTransactionMode.enforce,
        recordAfter: const Duration(seconds: 10),
        recordEvery: const Duration(seconds: 60),
        idleLimit: const Duration(seconds: 2),
        elapsed: () => fakeNow,
      );
      final logged = <String>[];
      final previousPrint = driftRuntimeOptions.debugPrint;
      driftRuntimeOptions.debugPrint = logged.add;
      try {
        final server = DriftServer(testInMemoryDatabase());
        final c1 = StreamChannelController<Object?>();
        final c2 = StreamChannelController<Object?>();
        server.serve(c1.foreign, serialize: true);
        server.serve(c2.foreign, serialize: true);

        late DatabaseConnection conn1;
        late DatabaseConnection conn2;
        connectToRemoteAndInitialize(c1.local, serialize: true)
            .then((c) => conn1 = c);
        connectToRemoteAndInitialize(c2.local, serialize: true)
            .then((c) => conn2 = c);
        async.elapse(Duration.zero);

        final db1 = TodoDb(conn1);
        final db2 = TodoDb(conn2);
        db1.customStatement('CREATE TABLE IF NOT EXISTS items (v INT)');
        async.elapse(Duration.zero);

        final holdTxn = Completer<void>();
        Object? txnError;
        final txn = conn1.executor.beginTransaction();
        txn.ensureOpen(db1);
        async.elapse(Duration.zero);
        txn.runCustom('INSERT INTO items VALUES (1)', const []);
        async.elapse(Duration.zero);

        // Waiter on conn2.
        var waiterDone = false;
        db2.customSelect('SELECT count(*) AS n FROM items').getSingle().then(
          (row) {
            expect(row.read<int>('n'), 0);
            waiterDone = true;
          },
        );
        async.elapse(Duration.zero);
        expect(waiterDone, isFalse);

        for (var i = 0; i < 3; i++) {
          fakeNow += const Duration(seconds: 1);
          async.elapse(const Duration(seconds: 1));
        }

        expect(waiterDone, isTrue);
        final revokedLines = logged
            .where((l) => l.startsWith('drift server: revoked transaction:'))
            .toList();
        expect(revokedLines, hasLength(1));
        expect(revokedLines.single, contains('outcome=rolled_back'));

        // Subsequent statement and commit on revoked txn fail with marker.
        Object? stmtErr;
        txn.runCustom('INSERT INTO items VALUES (2)', const []).then<void>(
          (_) {},
          onError: (Object e) {
            stmtErr = e;
          },
        );
        async.elapse(Duration.zero);
        expect(stmtErr, isA<DriftRemoteException>());
        expect(stmtErr.toString(), contains('[drift:txn_revoked]'));
        expect(driftWasRemoteTransactionRevoked(txn), isTrue);

        Object? commitErr;
        txn.send().then<void>((_) {}, onError: (Object e) {
          commitErr = e;
        });
        async.elapse(Duration.zero);
        expect(commitErr.toString(), contains('[drift:txn_revoked]'));

        // Rollback on revoked txn succeeds without CouldNotRollBackException.
        Object? rbErr;
        txn.rollback().then<void>((_) {}, onError: (Object e) {
          rbErr = e;
        });
        async.elapse(Duration.zero);
        expect(rbErr, isNull);
        expect(driftWasRemoteTransactionRevoked(txn), isTrue);

        holdTxn.complete();
        expect(txnError, isNull);
        db1.close();
        db2.close();
        server.shutdown();
        async.elapse(Duration.zero);
      } finally {
        driftRuntimeOptions.debugPrint = previousPrint;
        serverTransactionPolicy = previousPolicy;
      }
    });
  });

  test(
      'enforce: nested savepoint releases innermost first, ignores own outer pipelined waiter, self-wait and E5 fire cleanly',
      () {
    fakeAsync((async) {
      var fakeNow = Duration.zero;
      final previousPolicy = serverTransactionPolicy;
      serverTransactionPolicy = ServerTransactionPolicy(
        mode: ServerTransactionMode.enforce,
        recordAfter: const Duration(seconds: 10),
        recordEvery: const Duration(seconds: 60),
        idleLimit: const Duration(seconds: 2),
        elapsed: () => fakeNow,
      );
      final logged = <String>[];
      final previousPrint = driftRuntimeOptions.debugPrint;
      driftRuntimeOptions.debugPrint = logged.add;
      try {
        final rollbackOrder = <String>[];
        final innerMock = MockExecutor();
        final outerTxn = innerMock.transactions;
        final nestedTxn = outerTxn.transactions;
        when(outerTxn.rollback()).thenAnswer((_) async {
          rollbackOrder.add('outer');
        });
        when(nestedTxn.rollback()).thenAnswer((_) async {
          rollbackOrder.add('nested');
        });

        final server = DriftServer(DatabaseConnection(innerMock));
        final c1 = StreamChannelController<Object?>();
        server.serve(c1.foreign, serialize: false);
        late DatabaseConnection conn1;
        connectToRemoteAndInitialize(c1.local, serialize: false)
            .then((c) => conn1 = c);
        async.elapse(Duration.zero);

        final user = CustomQueryExecutorUser(
          schemaVersion: 1,
          beforeOpenCallback: (_, __, ___) async {},
        );
        conn1.executor.ensureOpen(user);
        async.elapse(Duration.zero);

        final outer = conn1.executor.beginTransaction();
        outer.ensureOpen(user);
        async.elapse(Duration.zero);
        final nested = outer.beginTransaction();
        nested.ensureOpen(user);
        async.elapse(Duration.zero);

        // Pipelined outer statement waits behind nested savepoint (inside tree).
        Object? outerStmtErr;
        outer.runCustom('INSERT INTO t VALUES (1)', const []).then<void>(
          (_) {},
          onError: (Object e) {
            outerStmtErr = e;
          },
        );
        async.elapse(Duration.zero);

        // Wait 5 * L with ONLY the inside-tree waiter: must NOT revoke!
        for (var i = 0; i < 10; i++) {
          fakeNow += const Duration(seconds: 1);
          async.elapse(const Duration(seconds: 1));
        }
        expect(rollbackOrder, isEmpty);
        expect(outerStmtErr, isNull);

        // E4 + E5: self-wait plain statement from conn1 arrives outside tree.
        var plainDone = false;
        conn1.executor.runSelect('SELECT 1', const []).then((_) {
          plainDone = true;
        });
        async.elapse(Duration.zero);
        expect(plainDone, isFalse);

        // Next tick fires within 1 tick!
        fakeNow += const Duration(seconds: 1);
        async.elapse(const Duration(seconds: 1));
        expect(rollbackOrder, ['nested', 'outer']);
        expect(outerStmtErr.toString(), contains('[drift:txn_revoked]'));
        expect(plainDone, isTrue);

        conn1.executor.close();
        server.shutdown();
        async.elapse(Duration.zero);
      } finally {
        driftRuntimeOptions.debugPrint = previousPrint;
        serverTransactionPolicy = previousPolicy;
      }
    });
  });

  test(
      'enforce: exempts in-progress statement (E7) and races safely with #13421 (E8) and #13413 (E9)',
      () {
    fakeAsync((async) {
      var fakeNow = Duration.zero;
      final previousPolicy = serverTransactionPolicy;
      serverTransactionPolicy = ServerTransactionPolicy(
        mode: ServerTransactionMode.enforce,
        recordAfter: const Duration(seconds: 10),
        recordEvery: const Duration(seconds: 60),
        idleLimit: const Duration(seconds: 2),
        elapsed: () => fakeNow,
      );
      final logged = <String>[];
      final previousPrint = driftRuntimeOptions.debugPrint;
      driftRuntimeOptions.debugPrint = logged.add;
      try {
        var rollbackCount = 0;
        Completer<void>? slowRollback;
        final mockExec = MockExecutor();
        final txnExec = mockExec.transactions;
        final longQueryGate = Completer<void>();
        var holdLongQuery = true;

        when(txnExec.runSelect(any, any)).thenAnswer((_) async {
          if (holdLongQuery) {
            await longQueryGate.future;
          }
          return const [];
        });
        when(txnExec.rollback()).thenAnswer((_) async {
          rollbackCount++;
          final gate = slowRollback;
          if (gate != null) await gate.future;
        });

        final server = DriftServer(DatabaseConnection(mockExec));
        final c1 = StreamChannelController<Object?>();
        final c2 = StreamChannelController<Object?>();
        server.serve(c1.foreign, serialize: false);
        server.serve(c2.foreign, serialize: false);

        late DatabaseConnection conn1;
        late DatabaseConnection conn2;
        connectToRemoteAndInitialize(c1.local, serialize: false)
            .then((c) => conn1 = c);
        connectToRemoteAndInitialize(c2.local, serialize: false)
            .then((c) => conn2 = c);
        async.elapse(Duration.zero);

        final user = CustomQueryExecutorUser(
          schemaVersion: 1,
          beforeOpenCallback: (_, __, ___) async {},
        );
        conn1.executor.ensureOpen(user);
        conn2.executor.ensureOpen(user);
        async.elapse(Duration.zero);

        // Part A (E7): statement in progress for 3 * L is NOT revoked.
        final busyTxn = conn1.executor.beginTransaction();
        busyTxn.ensureOpen(user);
        async.elapse(Duration.zero);
        var selectDone = false;
        busyTxn.runSelect('SELECT 1', const []).then((_) => selectDone = true);
        conn2.executor.runSelect('SELECT 1', const []);
        async.elapse(Duration.zero);
        for (var i = 0; i < 6; i++) {
          fakeNow += const Duration(seconds: 1);
          async.elapse(const Duration(seconds: 1));
        }
        expect(rollbackCount, 0);
        expect(selectDone, isFalse);
        longQueryGate.complete();
        holdLongQuery = false;
        async.elapse(Duration.zero);
        expect(selectDone, isTrue);
        busyTxn.send();
        async.elapse(Duration.zero);

        // Part B (E8 + E9): revoke in progress while client calls rollback (#13413)
        // AND then closes (#13421).
        slowRollback = Completer<void>();
        final stuckTxn = conn1.executor.beginTransaction();
        stuckTxn.ensureOpen(user);
        async.elapse(Duration.zero);
        var waiterAfterRevoke = false;
        conn2.executor.runSelect('SELECT 1', const []).then((_) {
          waiterAfterRevoke = true;
        });
        async.elapse(Duration.zero);
        for (var i = 0; i < 3; i++) {
          fakeNow += const Duration(seconds: 1);
          async.elapse(const Duration(seconds: 1));
        }
        expect(rollbackCount, 1);

        // Client rollback arrives while server rollback is still in flight (E9).
        var clientRollbackDone = false;
        Object? clientRollbackErr;
        stuckTxn.rollback().then<void>(
          (_) => clientRollbackDone = true,
          onError: (Object e) {
            clientRollbackErr = e;
          },
        );
        async.elapse(Duration.zero);
        expect(clientRollbackDone, isFalse);

        // Finish server rollback while client is still waiting for rollback reply.
        slowRollback!.complete();
        async.elapse(Duration.zero);
        expect(rollbackCount, 1);
        expect(clientRollbackErr, isNull);
        expect(clientRollbackDone, isTrue);
        expect(driftWasRemoteTransactionRevoked(stuckTxn), isTrue);
        expect(waiterAfterRevoke, isTrue);
        conn1.executor.close();
        async.elapse(Duration.zero);
        expect(rollbackCount, 1);
        expect(
          logged.where((l) => l.contains('release_error')),
          isEmpty,
        );

        // Part C (E8 forward): channel closes (#13421) while _revokeTree is
        // still awaiting slowRollback -> each executor ended once, 0 release_error.
        final c2b = StreamChannelController<Object?>();
        server.serve(c2b.foreign, serialize: false);
        late DatabaseConnection conn1b;
        connectToRemoteAndInitialize(c2b.local, serialize: false)
            .then((c) => conn1b = c);
        async.elapse(Duration.zero);
        conn1b.executor.ensureOpen(user);
        async.elapse(Duration.zero);

        slowRollback = Completer<void>();
        final stuck1b = conn1b.executor.beginTransaction();
        stuck1b.ensureOpen(user);
        async.elapse(Duration.zero);
        var waiter1bServed = false;
        conn2.executor.runSelect('SELECT 1', const []).then((_) {
          waiter1bServed = true;
        });
        async.elapse(Duration.zero);
        for (var i = 0; i < 3; i++) {
          fakeNow += const Duration(seconds: 1);
          async.elapse(const Duration(seconds: 1));
        }
        expect(rollbackCount, 2);
        conn1b.executor.close();
        async.elapse(Duration.zero);
        expect(rollbackCount, 2);
        slowRollback!.complete();
        async.elapse(Duration.zero);
        expect(rollbackCount, 2);
        expect(waiter1bServed, isTrue);
        expect(logged.where((l) => l.contains('release_error')), isEmpty);

        // Part D (E8 reverse): close (#13421) starts rolling back while monitor
        // tick fires mid-rollback -> monitor skips without double rollback.
        final c3 = StreamChannelController<Object?>();
        server.serve(c3.foreign, serialize: false);
        late DatabaseConnection conn3;
        connectToRemoteAndInitialize(c3.local, serialize: false)
            .then((c) => conn3 = c);
        async.elapse(Duration.zero);
        conn3.executor.ensureOpen(user);
        async.elapse(Duration.zero);

        slowRollback = Completer<void>();
        final revTxn = conn3.executor.beginTransaction();
        revTxn.ensureOpen(user);
        async.elapse(Duration.zero);
        var waiter2Served = false;
        conn2.executor.runSelect('SELECT 1', const []).then((_) {
          waiter2Served = true;
        });
        async.elapse(Duration.zero);
        conn3.executor.close();
        async.elapse(Duration.zero);
        expect(rollbackCount, 3);
        for (var i = 0; i < 4; i++) {
          fakeNow += const Duration(seconds: 1);
          async.elapse(const Duration(seconds: 1));
        }
        expect(rollbackCount, 3);
        slowRollback!.complete();
        async.elapse(Duration.zero);
        expect(waiter2Served, isTrue);

        conn2.executor.close();
        server.shutdown();
        async.elapse(Duration.zero);
      } finally {
        driftRuntimeOptions.debugPrint = previousPrint;
        serverTransactionPolicy = previousPolicy;
      }
    });
  });

  test(
      'D0b: slow statement logging records fixed metadata with zero SQL or arg text',
      () async {
      final previousPrint = driftRuntimeOptions.debugPrint;
      final previousPolicy = serverTransactionPolicy;
      final printed = <String>[];
      var now = Duration.zero;
      var advancePerSelect = Duration.zero;
      var advancePerBatch = Duration.zero;
      var selectRowCount = 2;

      final fakeInner = _SlowExecFake(
        onSelect: () {
          now += advancePerSelect;
          if (selectRowCount > 2) {
            return List.generate(
              selectRowCount,
              (_) => const {'secret_col': '1234567890123456'},
            );
          }
          return [
            {'secret_col': 'a' * 2048},
            {'secret_col': 'b' * 2048},
          ];
        },
        onBatch: () {
          now += advancePerBatch;
        },
        onDeleteOrUpdate: () {
          now += advancePerSelect;
          return 1;
        },
      );

      driftRuntimeOptions.debugPrint = printed.add;
      serverTransactionPolicy = ServerTransactionPolicy(
        elapsed: () => now,
        slowStatementThreshold: const Duration(seconds: 2),
        knownTables: const {'agent_run_state_records', 'chat_message_records'},
      );

      try {
        final server = DriftServer(fakeInner, allowRemoteShutdown: true);
        final controller = StreamChannelController<Object?>();
        unawaited(server.serve(controller.foreign, serialize: false));

        final conn = await connectToRemoteAndInitialize(
          controller.local,
          serialize: false,
        );

        // Fast query below 2 s emits nothing.
        advancePerSelect = const Duration(milliseconds: 500);
        await conn.executor.runSelect(
          "SELECT secret_col FROM agent_run_state_records WHERE secret_col = 'SENSITIVE_USER_TEXT'",
          const ['SECRET_ARG_VALUE'],
        );
        expect(
          printed.where((l) => l.startsWith('drift server: slow statement:')),
          isEmpty,
        );

        // Slow select >= 2 s emits metadata only (no SQL/args/unknown tables).
        advancePerSelect = const Duration(milliseconds: 2500);
        await conn.executor.runSelect(
          "/* comment with secret_table */ SELECT secret_col FROM agent_run_state_records JOIN unknown_secret_tbl ON 1=1 WHERE body = 'USER_EMAIL_AT_EXAMPLE'",
          const ['SECRET_ARG_2'],
        );
        final slowSelectLines = printed
            .where((l) => l.startsWith('drift server: slow statement:'))
            .toList();
        expect(slowSelectLines, hasLength(1));
        final line = slowSelectLines.single;
        expect(line, contains('ms=2500'));
        expect(line, contains('kind=select'));
        expect(line, contains('tables=agent_run_state_records'));
        expect(line, contains('rows=2'));
        expect(line, contains('kb=4'));
        expect(line, isNot(contains('SENSITIVE')));
        expect(line, isNot(contains('SECRET')));
        expect(line, isNot(contains('USER_EMAIL')));
        expect(line, isNot(contains('unknown_secret_tbl')));
        expect(line, isNot(contains('secret_col')));

        // Slow batch >= 2 s emits kind=batch and statement count.
        printed.clear();
        advancePerBatch = const Duration(milliseconds: 2100);
        await conn.executor.runBatched(
          BatchedStatements(
            const ['INSERT INTO chat_message_records(id) VALUES (?)'],
            [
              ArgumentsForBatchedStatement(0, const ['msg-secret-id']),
              ArgumentsForBatchedStatement(0, const ['msg-secret-id-2']),
            ],
          ),
        );
        final slowBatchLines = printed
            .where((l) => l.startsWith('drift server: slow statement:'))
            .toList();
        expect(slowBatchLines, hasLength(1));
        final batchLine = slowBatchLines.single;
        expect(batchLine, contains('ms=2100'));
        expect(batchLine, contains('kind=batch'));
        expect(batchLine, contains('tables=chat_message_records'));
        expect(batchLine, contains('rows=1'));
        expect(batchLine, contains('kb=0'));
        expect(batchLine, isNot(contains('msg-secret-id')));

        // Large select (>2000 rows) still estimates kb cleanly and stays bounded.
        printed.clear();
        selectRowCount = 2500;
        await conn.executor.runSelect(
          'SELECT secret_col FROM agent_run_state_records',
          const [],
        );
        final bigLine = printed
            .where((l) => l.startsWith('drift server: slow statement:'))
            .single;
        expect(bigLine, contains('rows=2500'));
        expect(bigLine, contains('kb=40'));

        // Update kind and null threshold disable check.
        printed.clear();
        await conn.executor.runUpdate(
          'UPDATE agent_run_state_records SET status = ? WHERE run_id = ?',
          const ['completed', 'run-secret-1'],
        );
        final updateLine = printed
            .where((l) => l.startsWith('drift server: slow statement:'))
            .single;
        expect(updateLine, contains('kind=update'));
        expect(updateLine, contains('tables=agent_run_state_records'));
        expect(updateLine, isNot(contains('run-secret-1')));

        printed.clear();
        serverTransactionPolicy = ServerTransactionPolicy(
          elapsed: () => now,
          slowStatementThreshold: null,
          knownTables: const {'agent_run_state_records'},
        );
        selectRowCount = 2;
        await conn.executor.runSelect(
          'SELECT secret_col FROM agent_run_state_records',
          const [],
        );
        expect(
          printed.where((l) => l.startsWith('drift server: slow statement:')),
          isEmpty,
        );

        await conn.executor.close();
        await server.shutdown();
      } finally {
        driftRuntimeOptions.debugPrint = previousPrint;
        serverTransactionPolicy = previousPolicy;
      }
    });
}

class _SlowExecFake extends QueryExecutor {
  _SlowExecFake({
    required this.onSelect,
    required this.onBatch,
    required this.onDeleteOrUpdate,
  });

  final List<Map<String, Object?>> Function() onSelect;
  final void Function() onBatch;
  final int Function() onDeleteOrUpdate;

  @override
  SqlDialect get dialect => SqlDialect.sqlite;

  @override
  Future<bool> ensureOpen(QueryExecutorUser user) async => true;

  @override
  Future<List<Map<String, Object?>>> runSelect(
    String statement,
    List<Object?> args,
  ) async =>
      onSelect();

  @override
  Future<void> runBatched(BatchedStatements statements) async => onBatch();

  @override
  Future<int> runDelete(String statement, List<Object?> args) async =>
      onDeleteOrUpdate();

  @override
  Future<int> runInsert(String statement, List<Object?> args) async => 1;

  @override
  Future<int> runUpdate(String statement, List<Object?> args) async => 0;

  @override
  Future<void> runCustom(String statement, [List<Object?>? args]) async {}

  @override
  TransactionExecutor beginTransaction() => throw UnimplementedError();

  @override
  QueryExecutor beginExclusive() => throw UnimplementedError();
}

Stream<Object?> _checkStreamOfSimple(Stream<Object?> source) {
  return source.map((event) {
    _checkSimple(event);
    return transportRoundtrip(event);
  });
}

void _checkSimple(Object? object) {
  if (object is String || object is num || object is bool || object == null) {
    // fine, these objects are allowed
  } else if (object is List) {
    // lists of simple objects are allowed too
    object.forEach(_checkSimple);
  } else {
    fail('Invalid message over wire: $object');
  }
}

Message _checkSimpleRoundtrip(DriftProtocol protocol, Message source) {
  final serialized = protocol.serialize(source);
  _checkSimple(serialized);
  return protocol.deserialize(serialized!);
}

extension<T> on StreamChannel<T> {
  StreamChannel<T> get expectedToClose {
    return transformStream(StreamTransformer.fromHandlers(
      handleDone: expectAsync1((out) => out.close()),
    ));
  }

  void serveMulti(DriftServer server) {
    final multi = MultiChannel<T>(this);
    multi.stream.listen((message) {
      server.serve(multi.virtualChannel(message as int));
    });
  }
}

extension on MultiChannel<Object?> {
  Future<DatabaseConnection> newRemoteConnection() async {
    final channel = virtualChannel();
    sink.add(channel.id);

    return await connectToRemoteAndInitialize(channel);
  }
}
