import DBlayer
import Foundation
import Testing

/// What a call without a queued reply gets: `.strict` fails it with `Invalid`, records it in `unscripted` and records a Swift Testing issue on the test that created the mock, `.networkError` throws the retried network error.
public enum Unanswered: Sendable { case strict, networkError }

/// The server end of the encrypted session cable. `MockTransport` hands the cable calls of the session to it and pushes model events through it.
public protocol CablePeer: AnyObject, Sendable {
  /// Called with the request of every subscription the session opens on the cable.
  var onSubscribe: (@Sendable (TransportRequest) -> Void)? { get set }
  /// A data event of the open subscription of that operation. False while it is not open.
  func event(operation: String, result: String) throws -> Bool
  func openCable(url: String, events: CableEvents) async -> CableOutcome
}

/// Answers by operation name, can delay or fail answers, and lets a test push a cable event.
public final class MockTransport: ScriptedTransport, @unchecked Sendable {
  private struct Reply {
    let body: String?
    let failure: DbError?
    let delay: TimeInterval
    var hold: Hold? = nil
  }

  private let lock = NSLock()
  private var replies: [String: [Reply]] = [:]
  private var recorded: [TransportRequest] = []

  private let unanswered: Unanswered
  private var missed: [TransportRequest] = []
  public var unscripted: [TransportRequest] { lock.withLock { missed } }

  private struct Report: Sendable {
    let message: String
    let done: CheckedContinuation<Void, Never>
  }

  private let reports: AsyncStream<Report>.Continuation

  public init(unanswered: Unanswered = .strict) {
    self.unanswered = unanswered
    let (stream, continuation) = AsyncStream.makeStream(of: Report.self)
    reports = continuation
    Task {
      for await report in stream {
        Issue.record(Comment(rawValue: report.message))
        report.done.resume()
      }
    }
  }

  deinit { reports.finish() }

  private var peer: (any CablePeer)?

  /// The cable of the session: events a test pushes with `emit` for an operation without a plain subscription go through it.
  public var cable: (any CablePeer)? {
    get { lock.withLock { peer } }
    set {
      newValue?.onSubscribe = { [unowned self] request in lock.withLock { recorded.append(request) } }
      lock.withLock { peer = newValue }
    }
  }

  public func scriptedCable(url: String, events: CableEvents) async -> CableOutcome {
    guard let peer = cable else { return .failed(failure: .scripted(.io, "MockTransport has no cable peer"), handshakeStatus: nil) }
    return await peer.openCable(url: url, events: events)
  }

  public var calls: [TransportRequest] { lock.withLock { recorded } }

  public func enqueue(_ operation: String, reply: String, delay: TimeInterval = 0) {
    lock.withLock { replies[operation, default: []].append(Reply(body: reply, failure: nil, delay: delay)) }
  }

  public func enqueue(_ operation: String, failure: DbError, delay: TimeInterval = 0) {
    lock.withLock { replies[operation, default: []].append(Reply(body: nil, failure: failure, delay: delay)) }
  }

  /// Queues an answer that waits at the server: `entered()` returns when the request arrived, and the answer goes out when the test calls `release()`.
  public func hold(_ operation: String, reply: String = "{}", failure: DbError? = nil) -> Hold {
    let hold = Hold()
    lock.withLock { replies[operation, default: []].append(Reply(body: failure == nil ? reply : nil, failure: failure, delay: 0, hold: hold)) }
    return hold
  }

  public func emit(_ operation: String, json: String) throws {
    _ = try cable?.event(operation: operation, result: json)
  }

  public func query(request: TransportRequest) async throws -> String { try await answer(request) }
  public func mutate(request: TransportRequest) async throws -> String { try await answer(request) }

  public func scriptedPut(url: String, headers: [String: String], file: PutFile, identity: TransferIdentity, progress: ByteProgress) async throws -> PutReply {
    progress.sent(bytes: file.length)
    return PutReply(status: 200, etag: nil)
  }

  /// The reply the server gave a request whose answer the library cancelled before it arrived. The request carries no request id on this side of the bridge, so the identity of a repeat is `(operationName, variablesJson)`: the repeat gets the same reply, any other request takes the next queued one.
  private var settled: [String: String] = [:]

  private func answer(_ request: TransportRequest) async throws -> String {
    let identity = request.operationName + "\u{0}" + request.variablesJson
    let reply = lock.withLock { () -> Reply? in
      recorded.append(request)
      if let known = settled[identity] { return Reply(body: known, failure: nil, delay: 0) }
      guard var queue = replies[request.operationName], !queue.isEmpty else { return nil }
      let next = queue.removeFirst()
      replies[request.operationName] = queue
      return next
    }
    guard let reply else { throw await refuse(request) }
    do {
      if reply.delay > 0 { try await Task.sleep(for: .seconds(reply.delay)) }
      if let hold = reply.hold { await hold.pass() }
      try Task.checkCancellation()
    } catch {
      if let body = reply.body { lock.withLock { settled[identity] = body } }
      throw error
    }
    if let failure = reply.failure { throw failure }
    return reply.body ?? "{}"
  }

  private func refuse(_ request: TransportRequest) async -> DbError {
    switch unanswered {
    case .networkError:
      return .Network(detail: "no reply queued for \(request.operationName)")
    case .strict:
      lock.withLock { missed.append(request) }
      let detail = "MockTransport has no reply queued for \(request.operationName) variables=\(request.variablesJson)"
      await withCheckedContinuation { done in
        if case .terminated = reports.yield(Report(message: detail, done: done)) { done.resume() }
      }
      return .Invalid(detail: detail)
    }
  }
}

/// A request held at the server by `MockTransport.hold`.
public final class Hold: @unchecked Sendable {
  private let arrived = AsyncStream<Void>.makeStream()
  private let freed = AsyncStream<Void>.makeStream()
  private let gone = AsyncStream<Void>.makeStream()

  /// Returns when the request reached the transport.
  public func entered() async {
    var arrivals = arrived.stream.makeAsyncIterator()
    _ = await arrivals.next()
  }

  /// Returns when the library dropped the held request before its release.
  public func dropped() async {
    var drops = gone.stream.makeAsyncIterator()
    _ = await drops.next()
  }

  public func release() { freed.continuation.yield(()) }

  fileprivate func pass() async {
    arrived.continuation.yield(())
    var releases = freed.stream.makeAsyncIterator()
    if await releases.next() == nil { gone.continuation.yield(()) }
  }
}
