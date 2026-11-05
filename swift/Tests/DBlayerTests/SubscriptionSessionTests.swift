import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

private final class SessionSubscriptions: Transport, @unchecked Sendable {
  let mock = MockTransport()
  private let lock = NSLock()
  private var sink: SubscriptionSink?
  var subscribed: Bool { lock.withLock { sink != nil } }
  func fail() { lock.withLock { sink }?.failed(error: .Session(code: .unauthenticated, detail: "session_unknown")) }
  func openCable(lease: CableLease, url: String) async throws { try await mock.openCable(lease: lease, url: url) }
  func cableSend(token: String, text: String) async throws { try await mock.cableSend(token: token, text: text) }
  func cableNext(token: String) async -> EncryptedSocketEvent? { await mock.cableNext(token: token) }
  func cableClose(token: String) async { await mock.cableClose(token: token) }
  func query(request: TransportRequest) async throws -> String { try await mock.query(request: request) }
  func mutate(request: TransportRequest) async throws -> String { try await mock.mutate(request: request) }
  func subscribe(request: TransportRequest, sink: SubscriptionSink) async throws {
    lock.withLock { self.sink = sink }
    try await mock.subscribe(request: request, sink: sink)
  }
  func unsubscribe(token: String) { mock.unsubscribe(token: token) }
  func put(url: String, headers: [String: String], file: PutFile, identity: TransferIdentity, progress: ByteProgress) async throws -> PutReply {
    try await mock.put(url: url, headers: headers, file: file, identity: identity, progress: progress)
  }
}

private final class SessionChanges: @unchecked Sendable {
  private let lock = NSLock()
  private var errors: [DbError] = []
  var failures: [DbError] { lock.withLock { errors } }
  func take(_ delivery: StreamDelivery) {
    if let error = stateOf(delivery)?.error { lock.withLock { errors.append(error) } }
  }
}

struct SubscriptionSessionTests {
  @Test func queryListenerReceivesOneTypedSessionFailureWithoutReads() async throws {
    let transport = SessionSubscriptions()
    let database = try await openDatabase(config: Config(path: nil, dataVersion: nil, cacheRows: 8), modelsJson: modelsJSON, transport: transport)
    try database.registerOperations(defsJson: #"[{"name":"BulletinAdded","document":"subscription BulletinAdded { bulletinAdded { body occurredAt } }","rootPath":"bulletinAdded"}]"#)
    let listener = SessionChanges()
    let pulled = PulledStream { listener.take($0) }
    let live = database.subscribe(epoch: database.sessionEpoch(), request: .query(queryJson: #"{"model":"Notice","relation":"recent","params":{}}"#), sink: pulled)
    pulled.bind(live, database)
    defer { database.cancelStream(stream: live) }
    await eventually("notice subscription", timeout: .seconds(2)) { transport.subscribed }
    transport.fail()
    await eventually("typed session failure", timeout: .seconds(2)) { !listener.failures.isEmpty }
    #expect(listener.failures.count == 1)
    guard case let .Session(code, detail) = listener.failures.first else {
      Issue.record("query listener lost the session error")
      return
    }
    #expect(code == .unauthenticated)
    #expect(detail == "session_unknown")
    #expect(transport.mock.calls.map(\.operationName) == ["BulletinAdded"])
  }
}
