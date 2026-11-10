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

/// Answers by operation name, can hold or fail answers, and lets a test push a cable event.
public final class MockTransport: ScriptedTransport, @unchecked Sendable {
  private struct Reply {
    let body: String?
    let failure: DbError?
    var hold: Hold? = nil
  }

  private let lock = NSLock()
  private let contract = ServerContract()
  private var replies: [String: Reply] = [:]
  private var tokens = 0
  private var recorded: [TransportRequest] = []
  private var responders: [String: @Sendable (TransportRequest) -> String?] = [:]
  private var puts: [String: [PutScript]] = [:]
  private var recordedPuts: [String] = []

  private struct PutScript {
    let etag: String?
    let failure: DbError?
  }

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
      newValue?.onSubscribe = { [unowned self] request in lock.withLock { recorded.append(request) }; TestPulse.shared.pulse() }
      lock.withLock { peer = newValue }
    }
  }

  public func scriptedCable(url: String, events: CableEvents) async -> CableOutcome {
    guard let peer = cable else { return .failed(failure: .scripted(.io, "MockTransport has no cable peer"), handshakeStatus: nil) }
    return await peer.openCable(url: url, events: events)
  }

  public var calls: [TransportRequest] { lock.withLock { recorded } }

  /// The server contract holds the order of the scripted replies: the transport keeps the reply behind each token.
  private func script(_ operation: String, _ reply: Reply) {
    contract.script(operation: operation, token: store(reply))
  }

  private func store(_ reply: Reply) -> String {
    lock.withLock {
      tokens += 1
      replies["r\(tokens)"] = reply
      return "r\(tokens)"
    }
  }

  public func enqueue(_ operation: String, reply: String) {
    script(operation, Reply(body: reply, failure: nil))
  }

  public func enqueue(_ operation: String, failure: DbError) {
    script(operation, Reply(body: nil, failure: failure))
  }

  /// Queues the answer to the next PUT of `url`: each PUT takes one, a PUT without one is refused like a call without a reply.
  public func enqueuePut(_ url: String, etag: String? = nil) {
    lock.withLock { puts[url, default: []].append(PutScript(etag: etag, failure: nil)) }
  }

  public func enqueuePut(_ url: String, failure: DbError) {
    lock.withLock { puts[url, default: []].append(PutScript(etag: nil, failure: failure)) }
  }

  /// The URLs of the PUTs the library sent, in order.
  public var putCalls: [String] { lock.withLock { recordedPuts } }

  /// Queues an answer that waits at the server: `entered()` returns when the request arrived, and the answer goes out when the test calls `release()`.
  public func hold(_ operation: String, reply: String = "{}", failure: DbError? = nil) -> Hold {
    let hold = Hold()
    script(operation, Reply(body: failure == nil ? reply : nil, failure: failure, hold: hold))
    return hold
  }

  public func respondWith(_ operation: String, _ responder: @escaping @Sendable (TransportRequest) -> String?) {
    lock.withLock { responders[operation] = responder }
  }

  public func emit(_ operation: String, json: String) throws {
    _ = try cable?.event(operation: operation, result: json)
    TestPulse.shared.pulse()
  }

  public func query(request: TransportRequest) async throws -> String { try await answer(request) }
  public func mutate(request: TransportRequest) async throws -> String { try await answer(request) }

  public func scriptedPut(url: String, headers: [String: String], file: PutFile, identity: TransferIdentity, progress: ByteProgress) async throws -> PutReply {
    let script = lock.withLock { () -> PutScript? in
      recordedPuts.append(url)
      guard var queue = puts[url], !queue.isEmpty else { return nil }
      let next = queue.removeFirst()
      puts[url] = queue
      return next
    }
    TestPulse.shared.pulse()
    guard let script else { throw await refusePut(url) }
    progress.sent(bytes: file.length)
    if let failure = script.failure { throw failure }
    return PutReply(status: 200, etag: script.etag)
  }

  /// The reply the server gave a request: a body or a refusal of the server, not a failure of the transport. The server contract decides: a repeat of a pair gets the reply of the first send, a second body under one pair and a request nobody scripted are refused.
  private static func wire(_ request: TransportRequest) -> String {
    let variables = try! JSONSerialization.jsonObject(with: Data(request.variablesJson.utf8), options: [.fragmentsAllowed])
    let object: [String: Any] = ["requestId": "\(request.requestId)", "sequence": NSNull(), "body": ["operationName": request.operationName, "variables": variables, "query": request.document]]
    return String(decoding: try! JSONSerialization.data(withJSONObject: object, options: [.sortedKeys]), as: UTF8.self)
  }

  private func settle(_ scope: String, _ wire: String, _ reply: Reply) throws {
    try contract.settle(scope: scope, requestJson: wire, token: store(Reply(body: reply.body, failure: reply.failure)))
  }

  private func answer(_ request: TransportRequest) async throws -> String {
    let scope = "\(request.author)"
    let wire = Self.wire(request)
    let responder = lock.withLock { responders[request.operationName] }
    lock.withLock { recorded.append(request) }
    var reply: Reply?
    var twoBodies = false
    switch try contract.admit(scope: scope, requestJson: wire) {
    case .stored(let token): reply = lock.withLock { replies[token] }
    case .twoBodies: twoBodies = true
    case .refused: break
    case .fresh:
      if let body = responder?(request) {
        reply = Reply(body: body, failure: nil)
        try settle(scope, wire, reply!)
      } else if let token = contract.take(operation: request.operationName), let next = lock.withLock({ replies[token] }) {
        reply = next
        if !Self.lostAnswer(next.failure) { try settle(scope, wire, next) }
      }
    }
    TestPulse.shared.pulse()
    if twoBodies { throw await refuse(request, detail: "MockTransport got the pair \(scope)#\(request.requestId) with a second body: \(request.operationName) variables=\(request.variablesJson)") }
    guard let reply else { throw await refuse(request) }
    if let hold = reply.hold { await hold.pass() }
    try Task.checkCancellation()
    defer { TestPulse.shared.pulse() }
    if let failure = reply.failure { throw failure }
    return reply.body ?? "{}"
  }

  private static func lostAnswer(_ failure: DbError?) -> Bool {
    switch failure {
    case .Network?, .Offline?, .Interrupted?, .Dropped?: return true
    default: return false
    }
  }

  private func refusePut(_ url: String) async -> DbError {
    switch unanswered {
    case .networkError:
      return .Network(detail: "no PUT queued for \(url)")
    case .strict:
      let detail = "MockTransport has no PUT queued for \(url)"
      await withCheckedContinuation { done in
        if case .terminated = reports.yield(Report(message: detail, done: done)) { done.resume() }
      }
      return .Invalid(detail: detail)
    }
  }

  private func refuse(_ request: TransportRequest, detail given: String? = nil) async -> DbError {
    switch unanswered {
    case .networkError where given == nil:
      return .Network(detail: "no reply queued for \(request.operationName)")
    default:
      lock.withLock { missed.append(request) }
      TestPulse.shared.pulse()
      let detail = given ?? "MockTransport has no reply queued for \(request.operationName) variables=\(request.variablesJson)"
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

  public func release() {
    freed.continuation.yield(())
    TestPulse.shared.pulse()
  }

  func pass() async {
    arrived.continuation.yield(())
    TestPulse.shared.pulse()
    var releases = freed.stream.makeAsyncIterator()
    if await releases.next() == nil { gone.continuation.yield(()) }
    TestPulse.shared.pulse()
  }
}
