import DBlayer
import Foundation

/// The transport mock of the shared vocabulary (`specs/machines/corpus.md`). It answers no request by itself: a request waits until an input of the trace answers, fails or drops it.
final class CorpusMock: Transport, EncryptedHttp, @unchecked Sendable {
  private struct Seen { let id: String; let kind: String; let purpose: String; var announced = false }

  private let lock = NSLock()
  private var next = 0
  private var seen: [Seen] = []
  private var waiting: [String: CheckedContinuation<String, Error>] = [:]
  private var waitingPuts: [String: CheckedContinuation<PutReply, Error>] = [:]
  private var waitingHttp: [String: (request: [String: Any], continuation: CheckedContinuation<EncryptedReply, Error>)] = [:]
  private var sinks: [String: SubscriptionSink] = [:]
  private let sealed: SealedServer = { do { return try SealedServer { _, _ in [:] } } catch { fatalError("\(error)") } }()

  var count: Int { lock.withLock { seen.count } }

  /// The requests that arrived since the last call, in arrival order, as `request` observations.
  func announce() -> [String] {
    lock.withLock {
      var out: [String] = []
      for index in seen.indices where !seen[index].announced {
        seen[index].announced = true
        out.append(observation("request", ["id": seen[index].id, "kind": seen[index].kind, "purpose": seen[index].purpose]))
      }
      return out
    }
  }

  private func record(_ kind: String, _ purpose: NetworkPurpose) -> String {
    lock.withLock {
      next += 1
      let id = "r\(next)"
      seen.append(Seen(id: id, kind: kind, purpose: purpose == .standard ? "standard" : "transfer"))
      return id
    }
  }

  func answer(_ id: String, _ body: String) -> Bool {
    if let continuation = lock.withLock({ waiting.removeValue(forKey: id) }) { continuation.resume(returning: body); return true }
    if let held = lock.withLock({ waitingHttp.removeValue(forKey: id) }) {
      guard let reply = (try? JSONSerialization.jsonObject(with: Data(body.utf8))) as? [String: Any], let sealedBody = try? sealed.seal(held.request, reply) else { return false }
      held.continuation.resume(returning: EncryptedReply(status: 200, body: sealedBody))
      return true
    }
    if let continuation = lock.withLock({ waitingPuts.removeValue(forKey: id) }) {
      let status = ((try? JSONSerialization.jsonObject(with: Data(body.utf8))) as? [String: Any])?["status"] as? Int ?? 200
      continuation.resume(returning: PutReply(status: UInt16(status), etag: nil))
      return true
    }
    return false
  }

  private static func failure(_ kind: String) -> DbError? {
    switch kind {
    case "temporary": return .Network(detail: "temporary")
    case "permanent": return .Graphql(messages: ["permanent"], extensions: nil)
    case "auth": return .Http(status: 401, detail: "auth")
    default: return nil
    }
  }

  func fail(_ id: String, _ kind: String) -> Bool {
    guard let error = Self.failure(kind) else { return false }
    if kind == "auth", let held = lock.withLock({ waitingHttp.removeValue(forKey: id) }) {
      held.continuation.resume(returning: EncryptedReply(status: 401, body: ""))
      return true
    }
    return finish(id, error)
  }

  func dropConnection(_ id: String) -> Bool { finish(id, .Network(detail: "dropped")) }

  private func finish(_ id: String, _ error: DbError) -> Bool {
    if let continuation = lock.withLock({ waiting.removeValue(forKey: id) }) { continuation.resume(throwing: error); return true }
    if let continuation = lock.withLock({ waitingPuts.removeValue(forKey: id) }) { continuation.resume(throwing: error); return true }
    if let held = lock.withLock({ waitingHttp.removeValue(forKey: id) }) { held.continuation.resume(throwing: error); return true }
    if let sink = lock.withLock({ sinks.removeValue(forKey: id) }) { sink.failed(error: error); return true }
    return false
  }

  /// Fails every waiting request with `Interrupted`. The runner calls it before a close: the app's transport cancels its calls when the database closes.
  func releaseAll() {
    let (calls, puts) = lock.withLock { () -> ([CheckedContinuation<String, Error>], [CheckedContinuation<PutReply, Error>]) in
      defer { waiting.removeAll(); waitingPuts.removeAll() }
      return (Array(waiting.values), Array(waitingPuts.values))
    }
    for continuation in calls { continuation.resume(throwing: DbError.Interrupted(detail: "closed")) }
    for continuation in puts { continuation.resume(throwing: DbError.Interrupted(detail: "closed")) }
    let https = lock.withLock { () -> [CheckedContinuation<EncryptedReply, Error>] in
      defer { waitingHttp.removeAll() }
      return waitingHttp.values.map(\.continuation)
    }
    for continuation in https { continuation.resume(throwing: DbError.Interrupted(detail: "closed")) }
  }

  func ready(_ id: String) -> Bool {
    guard let sink = lock.withLock({ sinks[id] }) else { return false }
    sink.ready()
    return true
  }

  func frame(_ id: String, _ event: String) throws -> Bool {
    guard let sink = lock.withLock({ sinks[id] }) else { return false }
    try sink.event(json: event)
    return true
  }

  private func wait(_ kind: String, _ request: TransportRequest) async throws -> String {
    let id = record(kind, request.purpose)
    return try await withCheckedThrowingContinuation { continuation in lock.withLock { waiting[id] = continuation } }
  }

  func openCable(lease: CableLease) async throws {
    _ = record("subscription", .standard)
    throw DbError.Network(detail: "no cable")
  }

  func post(path: String, body: String, purpose: NetworkPurpose) async throws -> EncryptedReply {
    let request = try sealed.open(body)
    let id = record("http", purpose)
    return try await withCheckedThrowingContinuation { continuation in lock.withLock { waitingHttp[id] = (request, continuation) } }
  }
  func cableSend(token: String, text: String) async throws { throw DbError.Network(detail: "no cable") }
  func cableNext(token: String) async -> EncryptedSocketEvent? { nil }
  func cableClose(token: String) async {}
  func query(request: TransportRequest) async throws -> String { try await wait("query", request) }
  func mutate(request: TransportRequest) async throws -> String { try await wait("mutation", request) }

  func subscribe(request: TransportRequest, sink: SubscriptionSink) async throws {
    let id = record("subscription", request.purpose)
    try sink.opened(token: id)
    lock.withLock { sinks[id] = sink }
  }

  func unsubscribe(token: String) { lock.withLock { _ = sinks.removeValue(forKey: token) } }

  func put(url: String, headers: [String: String], file: PutFile, identity: TransferIdentity, progress: ByteProgress) async throws -> PutReply {
    let id = record("put", .transfer)
    return try await withCheckedThrowingContinuation { continuation in lock.withLock { waitingPuts[id] = continuation } }
  }
}

func canonical(_ value: Any) -> String {
  guard JSONSerialization.isValidJSONObject(value), let data = try? JSONSerialization.data(withJSONObject: value, options: [.sortedKeys]) else { return "\(value)" }
  return String(decoding: data, as: UTF8.self)
}

/// One observation as canonical JSON text: `{"o":kind,"v":value}`, `v` left out when there is none.
func observation(_ kind: String, _ value: Any? = nil) -> String {
  guard let value else { return canonical(["o": kind]) }
  return canonical(["o": kind, "v": value])
}
