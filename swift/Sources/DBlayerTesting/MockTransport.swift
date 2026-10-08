import DBlayer
import Foundation

/// Answers by operation name, can delay or fail answers, and lets a test push or break a subscription.
public final class MockTransport: Transport, @unchecked Sendable {
  private struct Reply {
    let body: String?
    let failure: DbError?
    let delay: TimeInterval
  }

  private let lock = NSLock()
  private var replies: [String: [Reply]] = [:]
  private var sinks: [String: SubscriptionSink] = [:]
  private var recorded: [TransportRequest] = []

  public init() {}

  public var calls: [TransportRequest] { lock.withLock { recorded } }

  public func enqueue(_ operation: String, reply: String, delay: TimeInterval = 0) {
    lock.withLock { replies[operation, default: []].append(Reply(body: reply, failure: nil, delay: delay)) }
  }

  public func enqueue(_ operation: String, failure: DbError, delay: TimeInterval = 0) {
    lock.withLock { replies[operation, default: []].append(Reply(body: nil, failure: failure, delay: delay)) }
  }

  public func emit(_ operation: String, json: String) throws {
    try lock.withLock { sinks[operation] }?.event(json: json)
  }

  public func breakSubscription(_ operation: String) {
    lock.withLock { sinks.removeValue(forKey: operation) }?.failed(kind: "transport", detail: "connection lost")
  }

  public func query(request: TransportRequest) async throws -> String { try await answer(request) }
  public func mutate(request: TransportRequest) async throws -> String { try await answer(request) }

  public func subscribe(request: TransportRequest, sink: SubscriptionSink) async throws -> String {
    lock.withLock { recorded.append(request); sinks[request.operationName] = sink }
    sink.ready()
    return request.operationName
  }

  public func unsubscribe(token: String) { lock.withLock { _ = sinks.removeValue(forKey: token) } }

  public func put(url: String, headers: [String: String], bytes: Data, progress: ByteProgress) async throws -> PutReply {
    progress.sent(bytes: UInt64(bytes.count))
    return PutReply(status: 200, etag: nil)
  }

  private func answer(_ request: TransportRequest) async throws -> String {
    let reply = lock.withLock { () -> Reply? in
      recorded.append(request)
      guard var queue = replies[request.operationName], !queue.isEmpty else { return nil }
      let next = queue.removeFirst()
      replies[request.operationName] = queue
      return next
    }
    guard let reply else { throw DbError.Network(detail: "no reply queued for \(request.operationName)") }
    if reply.delay > 0 { try await Task.sleep(for: .seconds(reply.delay)) }
    if let failure = reply.failure { throw failure }
    return reply.body ?? "{}"
  }
}
