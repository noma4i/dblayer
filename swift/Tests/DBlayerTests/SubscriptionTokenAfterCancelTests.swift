import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

private final class DatabaseBox: @unchecked Sendable {
  private let lock = NSLock()
  private var database: Database?
  var current: Database? { lock.withLock { database } }
  func hold(_ database: Database) { lock.withLock { self.database = database } }
  func release() { lock.withLock { database = nil } }
}

private final class CancellationEvent: @unchecked Sendable {
  private let lock = NSLock()
  private var fired = false
  private var waiter: CheckedContinuation<Void, Never>?
  func wait(_ continuation: CheckedContinuation<Void, Never>) {
    let ready = lock.withLock { () -> Bool in
      if fired { return true }
      waiter = continuation
      return false
    }
    if ready { continuation.resume() }
  }
  func fire() {
    let continuation = lock.withLock { () -> CheckedContinuation<Void, Never>? in
      fired = true
      defer { waiter = nil }
      return waiter
    }
    continuation?.resume()
  }
}

private final class ReturnsAfterDrop: Transport, @unchecked Sendable {
  let mock = MockTransport()
  let box = DatabaseBox()
  private let lock = NSLock()
  private var issuedTokens: [String] = []
  private var revoked: [String] = []
  var issued: [String] { lock.withLock { issuedTokens } }
  var revokedTokens: [String] { lock.withLock { revoked } }
  func openCable() async throws -> String { try await mock.openCable() }
  func cableSend(token: String, text: String) async throws { try await mock.cableSend(token: token, text: text) }
  func cableNext(token: String) async -> EncryptedSocketEvent? { await mock.cableNext(token: token) }
  func cableClose(token: String) async { await mock.cableClose(token: token) }
  func query(request: TransportRequest) async throws -> String { try await mock.query(request: request) }
  func mutate(request: TransportRequest) async throws -> String { try await mock.mutate(request: request) }
  func subscribe(request: TransportRequest, sink: SubscriptionSink) async throws -> String {
    let token = try await mock.subscribe(request: request, sink: sink)
    lock.withLock { issuedTokens.append(token) }
    box.release()
    let dropped = CancellationEvent()
    await withTaskCancellationHandler {
      await withCheckedContinuation { dropped.wait($0) }
    } onCancel: {
      dropped.fire()
    }
    return token
  }
  func unsubscribe(token: String) {
    lock.withLock { revoked.append(token) }
    mock.unsubscribe(token: token)
  }
  func put(url: String, headers: [String: String], bytes: Data, progress: ByteProgress) async throws -> PutReply {
    try await mock.put(url: url, headers: headers, bytes: bytes, progress: progress)
  }
}

private final class IgnoredChanges: QueryListener, @unchecked Sendable {
  func changed(change: LiveChange) {}
}

struct SubscriptionTokenAfterCancelTests {
  @Test func aTokenReturnedAfterTheCoreDroppedTheCallIsHandedBack() async throws {
    let transport = ReturnsAfterDrop()
    transport.mock.enqueue("documentBySlug", reply: #"{"data":{"documentBySlug":{"id":"d1","slug":"a"}}}"#)
    transport.box.hold(try await openDatabase(config: Config(path: nil, dataVersion: nil, cacheRows: 8), modelsJson: modelsJSON, transport: transport))
    try transport.box.current?.registerOperations(defsJson: #"[{"name":"documentBySlug","document":"query documentBySlug($slug: String!, $source: String!) { documentBySlug(slug: $slug, source: $source) { id slug } }","rootPath":"documentBySlug"},{"name":"documentUpdated","document":"subscription documentUpdated($slug: String!, $source: String!) { documentUpdated(slug: $slug, source: $source) { id slug } }","rootPath":"documentUpdated"}]"#)
    _ = transport.box.current?.liveQuery(queryJson: #"{"model":"Document","relation":"bySlug","params":{"slug":"a","source":"web"}}"#, listener: IgnoredChanges())
    await eventually("the call returns its token") { transport.issued.count == 1 }
    await eventually("the token is handed back") { transport.revokedTokens.count == 1 }
    #expect(transport.revokedTokens == transport.issued)
  }
}
