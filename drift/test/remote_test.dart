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
