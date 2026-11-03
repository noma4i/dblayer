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

private enum Order {
  case openedBeforeTheDrop
  case openedAfterTheDrop
}

private final class OrderedSubscribe: Transport, @unchecked Sendable {
  let mock = MockTransport()
  let box = DatabaseBox()
  let order: Order
  private let lock = NSLock()
  private var issuedTokens: [String] = []
  private var revoked: [String] = []
  var issued: [String] { lock.withLock { issuedTokens } }
  var revokedTokens: [String] { lock.withLock { revoked } }
  init(_ order: Order) { self.order = order }
  func openCable(lease: CableLease) async throws { try await mock.openCable(lease: lease) }
  func cableSend(token: String, text: String) async throws { try await mock.cableSend(token: token, text: text) }
  func cableNext(token: String) async -> EncryptedSocketEvent? { await mock.cableNext(token: token) }
  func cableClose(token: String) async { await mock.cableClose(token: token) }
  func query(request: TransportRequest) async throws -> String { try await mock.query(request: request) }
  func mutate(request: TransportRequest) async throws -> String { try await mock.mutate(request: request) }
  func subscribe(request: TransportRequest, sink: SubscriptionSink) async throws {
    let token = "token-\(request.operationName)"
    lock.withLock { issuedTokens.append(token) }
    if order == .openedBeforeTheDrop { try sink.opened(token: token) }
    let closing = box.current
    box.release()
    Task { try? await closing?.closeDatabase() }
    let dropped = CancellationEvent()
    await withTaskCancellationHandler {
      await withCheckedContinuation { dropped.wait($0) }
    } onCancel: {
      dropped.fire()
    }
    if order == .openedAfterTheDrop { try sink.opened(token: token) }
  }
  func unsubscribe(token: String) {
    lock.withLock { revoked.append(token) }
    mock.unsubscribe(token: token)
  }
  func put(url: String, headers: [String: String], file: PutFile, tag: String, progress: ByteProgress) async throws -> PutReply {
    try await mock.put(url: url, headers: headers, file: file, tag: tag, progress: progress)
  }
}

private final class IgnoredChanges: ValueSink, @unchecked Sendable {
  func delivered(delivery: StreamDelivery) {}
}

struct SubscriptionTokenAfterCancelTests {
  private func handedBack(_ order: Order) async throws {
    let transport = OrderedSubscribe(order)
    transport.mock.enqueue("documentBySlug", reply: #"{"data":{"documentBySlug":{"id":"d1","slug":"a"}}}"#)
    transport.box.hold(try await openDatabase(config: Config(path: nil, dataVersion: nil, cacheRows: 8), modelsJson: modelsJSON, transport: transport))
    try transport.box.current?.registerOperations(defsJson: #"[{"name":"documentBySlug","document":"query documentBySlug($slug: String!, $source: String!) { documentBySlug(slug: $slug, source: $source) { id slug } }","rootPath":"documentBySlug"},{"name":"documentUpdated","document":"subscription documentUpdated($slug: String!, $source: String!) { documentUpdated(slug: $slug, source: $source) { id slug } }","rootPath":"documentUpdated"}]"#)
    if let current = transport.box.current { _ = current.subscribe(epoch: current.sessionEpoch(), request: .query(queryJson: #"{"model":"Document","relation":"bySlug","params":{"slug":"a","source":"web"}}"#), sink: IgnoredChanges()) }
    await eventually("the call issues its token") { transport.issued.count == 1 }
    await eventually("the token is handed back") { transport.revokedTokens.count == 1 }
    #expect(transport.revokedTokens == transport.issued)
  }

  @Test func aTokenOpenedBeforeTheCoreDroppedTheCallIsHandedBack() async throws { try await handedBack(.openedBeforeTheDrop) }
  @Test func aTokenOpenedAfterTheCoreDroppedTheCallIsHandedBack() async throws { try await handedBack(.openedAfterTheDrop) }
}
