import DBlayer
import DBlayerTesting
import Foundation

/// The transport mock of the shared vocabulary. It answers no request by itself: a request waits until an input of the trace answers, fails or drops it.
final class CorpusMock: ScriptedTransport, ScriptedHttp, @unchecked Sendable {
  private struct Seen { let id: String; let kind: String; let purpose: String; var announced = false }

  func appHeaders() -> [String: String] { [:] }

  private let lock = NSLock()
  private var next = 0
  private var seen: [Seen] = []
  private var waiting: [String: CheckedContinuation<String, Error>] = [:]
  private var waitingPuts: [String: CheckedContinuation<PutReply, Error>] = [:]
  private var waitingHttp: [String: (request: [String: Any], continuation: CheckedContinuation<EncryptedReply, Error>)] = [:]
  private var cables: [String: String] = [:]
  private let sealed: SealedServer = { do { return try SealedServer { _, _ in [:] } } catch { fatalError("\(error)") } }()
  private lazy var peer = EncryptedCablePeer(server: sealed)

  var count: Int { lock.withLock { seen.count } }

  func knows(_ id: String) -> Bool { lock.withLock { waiting[id] != nil || waitingPuts[id] != nil || waitingHttp[id] != nil || cables[id] != nil } }

  var unannounced: Int { lock.withLock { seen.filter { !$0.announced }.count } }

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
    if kind == "permanent", lock.withLock({ waitingHttp[id] != nil }) { return answer(id, #"{"errors":[{"message":"permanent","extensions":{"code":"permanent"}}]}"#) }
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
    if let token = lock.withLock({ cables.removeValue(forKey: id) }) { return (try? peer.drop(token: token)) != nil }
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
    lock.withLock { cables[id] != nil }
  }

  func frame(_ id: String, _ event: String) throws -> Bool {
    guard let token = lock.withLock({ cables[id] }), let result = try JSONSerialization.jsonObject(with: Data(event.utf8)) as? [String: Any] else { return false }
    return try peer.event(token: token, result: result)
  }

  private func wait(_ kind: String, _ request: TransportRequest) async throws -> String {
    let id = record(kind, request.purpose)
    return try await withCheckedThrowingContinuation { continuation in lock.withLock { waiting[id] = continuation } }
  }

  func scriptedCable(url: String, events: CableEvents) async -> CableOutcome {
    let id = record("subscription", .standard)
    let outcome = await peer.openCable(url: url, events: events)
    lock.withLock { cables[id] = peer.lastOpened }
    return outcome
  }

  func send(request: HttpRequest) async -> RawOutcome {
    request.headers["Content-Type"] == "application/jose" ? await sendEncrypted(request: request) : await sendGraphql(request: request)
  }

  func post(path: String, body: String, purpose: NetworkPurpose) async throws -> EncryptedReply {
    let request = try sealed.open(body)
    let id = record("http", purpose)
    return try await withCheckedThrowingContinuation { continuation in lock.withLock { waitingHttp[id] = (request, continuation) } }
  }
  func query(request: TransportRequest) async throws -> String { try await wait("query", request) }
  func mutate(request: TransportRequest) async throws -> String { try await wait("mutation", request) }

  func scriptedPut(url: String, headers: [String: String], file: PutFile, identity: TransferIdentity, progress: ByteProgress) async throws -> PutReply {
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
