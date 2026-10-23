import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

private final class HeldSubscribe: Transport, @unchecked Sendable {
  let mock = MockTransport()
  private let lock = NSLock()
  private var entered = false
  private var cancelled = false
  private var revoked: [String] = []
  var hasEntered: Bool { lock.withLock { entered } }
  var wasCancelled: Bool { lock.withLock { cancelled } }
  var revokedTokens: [String] { lock.withLock { revoked } }
  func openCable(lease: CableLease) async throws { try await mock.openCable(lease: lease) }
  func cableSend(token: String, text: String) async throws { try await mock.cableSend(token: token, text: text) }
  func cableNext(token: String) async -> EncryptedSocketEvent? { await mock.cableNext(token: token) }
  func cableClose(token: String) async { await mock.cableClose(token: token) }
  func query(request: TransportRequest) async throws -> String { try await mock.query(request: request) }
  func mutate(request: TransportRequest) async throws -> String { try await mock.mutate(request: request) }
  func subscribe(request: TransportRequest, sink: SubscriptionSink) async throws {
    lock.withLock { entered = true }
    do {
      try await parkUntilCancelled()
    } catch {
      lock.withLock { cancelled = true }
      throw error
    }
    try await mock.subscribe(request: request, sink: sink)
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

struct SubscriptionTokenTests {
  @Test func cancellingWhileSubscribeIsInFlightCancelsTheForeignTaskAndIssuesNoToken() async throws {
    let transport = HeldSubscribe()
    transport.mock.enqueue("documentBySlug", reply: #"{"data":{"documentBySlug":{"id":"d1","slug":"a"}}}"#)
    var database: Database? = try await openDatabase(config: Config(path: nil, dataVersion: nil, cacheRows: 8), modelsJson: modelsJSON, transport: transport)
    try database?.registerOperations(defsJson: #"[{"name":"documentBySlug","document":"query documentBySlug($slug: String!, $source: String!) { documentBySlug(slug: $slug, source: $source) { id slug } }","rootPath":"documentBySlug"},{"name":"documentUpdated","document":"subscription documentUpdated($slug: String!, $source: String!) { documentUpdated(slug: $slug, source: $source) { id slug } }","rootPath":"documentUpdated"}]"#)
    let live = try #require(database).liveQuery(queryJson: #"{"model":"Document","relation":"bySlug","params":{"slug":"a","source":"web"}}"#, listener: IgnoredChanges())
    await eventually("subscribe in flight", timeout: .seconds(5)) { transport.hasEntered }
    live.cancel()
    database = nil
    await eventually("foreign task cancelled", timeout: .seconds(5)) { transport.wasCancelled }
    #expect(transport.revokedTokens.isEmpty)
    #expect(transport.mock.unknownTokens.isEmpty)
  }
}
