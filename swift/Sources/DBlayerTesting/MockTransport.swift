import DBlayer
import Foundation

/// What a call without a queued reply gets: `.strict` fails it with `Invalid` and records it in `unscripted`, `.networkError` throws the retried network error.
public enum Unanswered: Sendable { case strict, networkError }

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
  private var issuedTokens: Set<String> = []
  private var unknown: [String] = []

  private let unanswered: Unanswered
  private var missed: [TransportRequest] = []
  public var unscripted: [TransportRequest] { lock.withLock { missed } }

  public init(unanswered: Unanswered = .strict) { self.unanswered = unanswered }

  public func openCable() async throws -> String { throw DbError.Invalid(detail: "MockTransport does not support WebSocket connections") }
  public func cableSend(token: String, text: String) async throws { throw DbError.Interrupted(detail: "mock cable socket is closed") }
  public func cableNext(token: String) async -> EncryptedSocketEvent? { nil }
  public func cableClose(token: String) async {}

  public var calls: [TransportRequest] { lock.withLock { recorded } }
  public var unknownTokens: [String] { lock.withLock { unknown } }

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
    lock.withLock { sinks.removeValue(forKey: operation) }?.failed(error: .Network(detail: "connection lost"))
  }

  public func query(request: TransportRequest) async throws -> String { try await answer(request) }
  public func mutate(request: TransportRequest) async throws -> String { try await answer(request) }

  public func subscribe(request: TransportRequest, sink: SubscriptionSink) async throws -> String {
    lock.withLock { recorded.append(request); sinks[request.operationName] = sink; issuedTokens.insert(request.operationName) }
    sink.ready()
    return request.operationName
  }

  public func unsubscribe(token: String) {
    lock.withLock {
      if !issuedTokens.contains(token) { unknown.append(token) }
      _ = sinks.removeValue(forKey: token)
    }
  }

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
    guard let reply else { throw refuse(request) }
    if reply.delay > 0 { try await Task.sleep(for: .seconds(reply.delay)) }
    if let failure = reply.failure { throw failure }
    return reply.body ?? "{}"
  }

  private func refuse(_ request: TransportRequest) -> DbError {
    switch unanswered {
    case .networkError:
      return .Network(detail: "no reply queued for \(request.operationName)")
    case .strict:
      lock.withLock { missed.append(request) }
      return .Invalid(detail: "MockTransport has no reply queued for \(request.operationName) variables=\(request.variablesJson)")
    }
  }
}
