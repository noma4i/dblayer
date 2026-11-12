import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

private final class BridgeCredential: CredentialStore, @unchecked Sendable {
  private let lock = NSLock()
  private var value: String?

  func get() throws -> String? { lock.withLock { value } }
  func set(credential: String) throws { lock.withLock { value = credential } }
  func clear() throws { lock.withLock { value = nil } }
}

private final class BridgeAccount: @unchecked Sendable {
  private let lock = NSLock()
  private var name: String?
  private var created = 0

  var current: String? { lock.withLock { name } }
  func signIn(_ account: String) { lock.withLock { name = account } }
  func signOut() { lock.withLock { name = nil } }
  func nextId() -> Int { lock.withLock { created += 1; return created } }
}

private struct BridgeLogin: Encodable, Sendable { let account: String }

/// The value bridge through its public API: streams of snapshots and deltas, a command, and the epoch of the session. The tests share `DB.database` with `BridgeProbeTests`, so they run in its serialized suite.
extension BridgeProbeTests {
  private static let bridgeAccount = BridgeAccount()

  @MainActor
  static func openBridgeSession() async throws -> SealedServer {
    await DB.closeForCleanup()
    let account = Self.bridgeAccount
    account.signOut()
    let server = try SealedServer { path, body in
      if path == "/api/auth/login_link" {
        let name = body["account"] as? String ?? ""
        account.signIn(name)
        return ["sessionId": name]
      }
      if path == "/api/logout" {
        account.signOut()
        return ["revoked": true]
      }
      guard let name = account.current else { return ["error": "UNAUTHENTICATED"] }
      switch body["operationName"] as? String {
      case "SessionProfile": return ["data": ["sessionProfile": ["id": name]]]
      case "UserProfile": return ["data": ["me": ["__typename": "User", "id": "1", "name": name]]]
      case "CreateMoment":
        let number = account.nextId()
        let text = (body["variables"] as? [String: Any])?["text"] as? String ?? ""
        return ["data": ["createMoment": ["__typename": "Moment", "id": "m\(number)", "uuid": "u\(number)", "text": text, "likedByMe": false, "media": [String: Any](), "createdAt": "2026-10-08T00:00:00.000Z"]]]
      default: return ["errors": [["message": "unexpected operation"]]]
      }
    }
    let encrypted = EncryptedConfig(origin: SealedServer.origin, serverKey: SealedServer.serverKey, pin: SealedServer.pin, endpoints: EncryptedEndpoints(graphql: "/api/graphql", cable: "/api/cable", logout: "/api/logout", bootstrap: "/api/bootstrap", sessionOperations: ["login_link": EncryptedSessionOperation(path: "/api/auth/login_link", class: .authenticated)]), protocolLabel: "example-ws-v1", cablePurpose: "ws.hello", cableChannel: "EncryptedGraphqlChannel")
    let transport = MockTransport()
    transport.cable = EncryptedCablePeer(server: server)
    try await DB.openTesting(transport: transport, modelsJSON: withRawCalls(modelsJSON), encrypted: encrypted, credentials: BridgeCredential(), http: server)
    let fixtureOperations = #"[{"name":"UserProfile","document":"query UserProfile { me { id name } }"},{"name":"SessionProfile","document":"query SessionProfile { sessionProfile { id } }"},{"name":"CreateMoment","document":"mutation CreateMoment($text: String) { createMoment(text: $text) { id uuid text likedByMe media createdAt } }"}]"#
    try server.registerDocuments(fixtureOperations)
    try await registerStandOperations(DB.database, defsJson: fixtureOperations)
    return server
  }

  @MainActor
  static func signIn(_ account: String) async throws {
    try await DB.sessionCall("login_link", body: BridgeLogin(account: account))
    try await DB.call("SessionProfile", variables: [:])
    try await User.profile.refresh()
  }

  /// B06: the reader that stops ends the stream in the core.
  @MainActor
  @Test
  func theReaderThatStopsEndsTheStreamInTheCore() async throws {
    _ = try await Self.openBridgeSession()
    try await Self.signIn("A")
    let scope = try DB.account()
    var projected: Projected<User>? = Projected<User>(try scope.stream(User.profile))
    try await projected?.loaded()
    #expect(try await DB.subscriptionCensus().observedQueries == 1)

    let stream = projected?.stream
    projected = nil
    await stream?.cancel()
    #expect(try await DB.subscriptionCensus().observedQueries == 0)
  }

  /// boundary_reader_orders, boundary_reader_early_exit (D-175): a stream has one reader, the second iterator is invalid, and a reader that leaves early lets the stream end while the `ValueStream` is held.
  @MainActor
  @Test
  func aSecondReaderIsInvalidAndAReaderThatLeavesEarlyFreesTheStream() async throws {
    _ = try await Self.openBridgeSession()
    try await Self.signIn("A")
    let stream = try DB.account().stream(User.profile)
    let first = Cursor(stream)
    #expect(await first.next("the first value") != nil)
    let second = Cursor(stream)
    guard case .ended(.invalid)? = await second.next("the refusal of the second reader") else { Issue.record("a second reader must end with .invalid"); return }
    #expect(await second.next("the end of the second reader") == nil)
    #expect(try await DB.subscriptionCensus().observedQueries == 1, "the refusal of the second reader leaves the stream to the first")

    do {
      let early = Cursor(try DB.account().stream(User.profile))
      #expect(await early.next("the first value of the early reader") != nil)
    }
    await stream.cancel()
    #expect(try await DB.subscriptionCensus().observedQueries == 0)
  }

  @MainActor
  @Test
  func bridge_reader_release_parity_c9() async throws {
    _ = try await Self.openBridgeSession()
    try await Self.signIn("A")
    let stream = try DB.account().stream(User.profile)
    var first: Cursor<ValueStream>? = Cursor(stream)
    guard case .snapshot? = await first?.next("the first list value") else { Issue.record("the first reader did not receive a snapshot"); return }
    first = nil
    let next = Cursor(stream)
    guard case .ended(.closed)? = await next.next("the released stream ends closed") else { Issue.record("the released stream did not end closed"); return }
    let another = Cursor(stream)
    guard case .ended(.closed)? = await another.next("another reader sees the closed stream") else { Issue.record("the next reader did not see closed"); return }
  }

  @MainActor
  @Test
  func bridge_reader_cancel_pending_pull_parity_c9() async throws {
    _ = try await Self.openBridgeSession()
    try await Self.signIn("A")
    let scope = try DB.account()
    let stream = try scope.stream(Moment.recent)
    let (ready, notify) = AsyncStream<Void>.makeStream()
    let waiting = Task { () throws -> ReadValue? in
      let reader = Cursor(stream)
      guard case .snapshot? = await reader.next("the first snapshot") else { throw WaitFailure.unexpectedOutcome }
      notify.yield(())
      return await reader.next("the pending pull ends after cancellation")
    }
    let started = Cursor(ready)
    guard await started.next("the first reader has a snapshot") != nil else { waiting.cancel(); return }
    waiting.cancel()
    let ended = try await valueBeforeDeadline(waiting) { notify.finish() }
    if let ended {
      guard case .ended(.closed) = ended else { Issue.record("the cancelled reader must end closed: \(ended)"); return }
    }
    let closed = Cursor(stream)
    guard case .ended(.closed)? = await closed.next("the cancelled stream ends closed") else { Issue.record("the cancelled stream did not end closed"); return }
    let next = Cursor(try scope.stream(Moment.recent))
    guard let initial = await next.next("the new stream receives the current value"), case .snapshot = initial else { Issue.record("the new stream did not receive a snapshot"); return }
    var projection = ListProjection<Moment>()
    _ = try projection.apply(initial)
    try DB.database.setOnline(online: false)
    let sent = try await scope.send(Moment.self, values: ["text": .string("after cancellation")])
    let id = try #require(sent.optimisticValue?.id)
    guard let changed = await next.next("the new reader receives the write") else { Issue.record("the new reader did not receive the write"); return }
    _ = try projection.apply(changed)
    #expect(projection.rows.contains { $0.id == id }, "the new reader receives the written row")
  }

  /// K08: a row the platform cannot decode is an error of the projection, and the next good snapshot clears it. A count stream and an events stream report their source error.
  @MainActor
  @Test
  func aRowThatDoesNotDecodeThrowsAndAStreamReportsItsSourceEnd() async throws {
    _ = try await Self.openBridgeSession()
    try await Self.signIn("A")
    let scope = try DB.account()
    let profile = Projected<User>(try scope.stream(User.profile))
    try await profile.loaded()
    await eventually("the stream shows A") { profile.value?.name == "A" }
    let good = try #require(try await User.profile.read().first)
    _ = good
    var projection = ListProjection<User>()
    let state = try #require(profile.state)
    var bad = try #require(try await DB.database.readQuery(epoch: scope.epoch, query: try User.profile.spec()).first)
    bad.layout = "another layout"
    expectDbErrorNow("Invalid") { try projection.apply(.snapshot(revision: 1, rows: [bad], edits: [], state: state)) }
    #expect(projection.rows.isEmpty && projection.revision == nil, "a row that does not decode changed the projection")
    let whole = try #require(try await DB.database.readQuery(epoch: scope.epoch, query: try User.profile.spec()).first)
    try projection.apply(.snapshot(revision: 2, rows: [whole], edits: [], state: state))
    #expect(projection.rows.count == 1 && projection.error == nil)

    let database = try DB.database
    let unknown = ValueStream(epoch: scope.epoch, reader: database.subscribeValues(epoch: scope.epoch, request: .events(model: "Nope")))
    guard case .ended(.invalid)? = await Cursor(EventStream<User>(stream: unknown)).next("the source error of the events stream") else {
      Issue.record("the events stream did not end with the invalid cause")
      return
    }
  }
}

extension BridgeProbeTests {
  /// K21 (C02): a scope keeps the database it was made for. After that database closed and another one opened with the same epoch number, a command and a read of the old scope are refused and the new database does not change.
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func aScopeKeepsItsOwnDatabaseAfterTheDatabaseIsReplaced() async throws {
    _ = try await Self.openBridgeSession()
    try await Self.signIn("A")
    let oldScope = try DB.account()
    let row = try #require(try await User.profile.read(in: oldScope).first)
    await DB.closeForCleanup()
    _ = try await Self.openBridgeSession()
    try await Self.signIn("A")

    await expectDbError("Closed") { try await row.update(values: ["name": .string("hijacked")], in: oldScope) }
    await expectDbError("Closed") { try await User.profile.read(in: oldScope) }
    let current = try await User.profile.read().first
    #expect(current?.name == "A", "a command of the old scope changed the new database")
  }

  /// K16 (B8): a reader that falls behind reads at most 256 values of a burst from the journal of the core and ends with the whole list from one new snapshot.
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func aReaderThatFallsBehindKeepsAtMost256DeliveriesAndEndsWithTheWholeList() async throws {
    _ = try await Self.openBridgeSession()
    try await Self.signIn("A")
    let scope = try DB.account()
    let stream = try scope.stream(Moment.recent)
    let cursor = Cursor(stream)
    var projection = ListProjection<Moment>()
    var received = 0
    await consuming(cursor, "the first settled snapshot", until: { projection.state?.settled == true }) { _ = try? projection.apply($0) }
    try DB.database.setOnline(online: false)
    for number in 1...300 { _ = try await scope.send(Moment.self, values: ["text": .string("m\(number)")]) }
    for _ in 0..<400 {
      if projection.rows.count == 300 { break }
      guard let value = await cursor.next("a value of the burst") else { break }
      received += 1
      _ = try projection.apply(value)
    }
    #expect(projection.rows.count == 300, "the reader did not end with the whole list")
    #expect(received <= 260, "the reader received \(received) values of a burst of 300: the journal bounded nothing")
    await stream.cancel()
  }
}
