import CryptoKit
import Foundation
import DBlayerTesting
import Testing
@testable import DBlayer

final class EncryptedCablePeer: CablePeer, @unchecked Sendable {
  final class Connection: @unchecked Sendable {
    let events = AsyncStream<EncryptedSocketEvent>.makeStream()
    let secret = Curve25519.KeyAgreement.PrivateKey()
    let salt = Data(repeating: 5, count: 32)
    var sendKey: SymmetricKey?
    var receiveKey: SymmetricKey?
    var sequence = 0
    var subscriptions: [String: String] = [:]
    let order = NSLock()
  }

  let server: SealedServer
  var onSubscribe: (@Sendable (TransportRequest) -> Void)?
  let opened = AsyncStream<String>.makeStream()
  let subscribed = AsyncStream<String>.makeStream()
  let closed = AsyncStream<String>.makeStream()
  private let lock = NSLock()
  private var connections: [String: Connection] = [:]
  private var latest: String?
  var lastOpened: String? { lock.withLock { latest } }

  init(server: SealedServer) { self.server = server }

  private func encode(_ data: Data) -> String {
    data.base64EncodedString().replacingOccurrences(of: "+", with: "-").replacingOccurrences(of: "/", with: "_").replacingOccurrences(of: "=", with: "")
  }

  private func decode(_ text: String) -> Data {
    let base = text.replacingOccurrences(of: "-", with: "+").replacingOccurrences(of: "_", with: "/")
    return Data(base64Encoded: base + String(repeating: "=", count: (4 - base.count % 4) % 4))!
  }

  private func text(_ value: [String: Any]) throws -> String {
    String(decoding: try JSONSerialization.data(withJSONObject: value), as: UTF8.self)
  }

  private func connection(_ token: String) throws -> Connection {
    guard let value = lock.withLock({ connections[token] }) else { throw DbError.Network(detail: "closed peer") }
    return value
  }

  func openCable(lease: CableLease, url: String) async throws {
    let token = UUID().uuidString
    let connection = Connection()
    lock.withLock { connections[token] = connection; latest = token }
    try lease.opened(token: token)
    let challenge: [String: Any] = ["type": "challenge", "v": 1, "kid": SealedServer.pin, "challenge": encode(connection.salt), "x25519": encode(connection.secret.publicKey.rawRepresentation)]
    connection.events.continuation.yield(.text(text: try text(challenge)))
    opened.continuation.yield(token)
  }

  func cableSend(token: String, text: String) async throws {
    let connection = try connection(token)
    let command = try JSONSerialization.jsonObject(with: Data(text.utf8)) as! [String: Any]
    if command["command"] as? String == "subscribe" {
      connection.events.continuation.yield(.text(text: #"{"type":"confirm_subscription"}"#))
      return
    }
    let data = try JSONSerialization.jsonObject(with: Data((command["data"] as! String).utf8)) as! [String: Any]
    if let hello = data["hello"] as? String {
      let plain = try server.open(hello)
      let publicKey = try Curve25519.KeyAgreement.PublicKey(rawRepresentation: decode(plain["clientKey"] as! String))
      let shared = try connection.secret.sharedSecretFromKeyAgreement(with: publicKey)
      let material = shared.withUnsafeBytes { Data($0) } + decode(plain["responseKey"] as! String)
      func key(_ direction: String) -> SymmetricKey {
        let info = ["example-ws-v1", direction, SealedServer.pin, SealedServer.origin, "/api/cable"].joined(separator: "\0")
        return HKDF<SHA256>.deriveKey(inputKeyMaterial: SymmetricKey(data: material), salt: connection.salt, info: Data(info.utf8), outputByteCount: 32)
      }
      connection.sendKey = key("s2c")
      connection.receiveKey = key("c2s")
      let digest = SHA256.hash(data: connection.salt).map { String(format: "%02x", $0) }.joined()
      try emit(token: token, body: ["t": "ready", "connectionId": "00000000-0000-4000-8000-000000000001", "challengeDigest": digest])
      return
    }
    let frame = data["frame"] as! String
    let body = try openFrame(frame, key: connection.receiveKey!)
    if body["t"] as? String == "subscribe" {
      let id = body["id"] as! String
      connection.subscriptions[body["operationName"] as? String ?? ""] = id
      try emit(token: token, body: ["t": "subscribed", "id": id])
      subscribed.continuation.yield(token)
      let variables = (body["variables"] as? [String: Any]).flatMap { try? JSONSerialization.data(withJSONObject: $0, options: [.sortedKeys]) }.flatMap { String(data: $0, encoding: .utf8) } ?? "{}"
      onSubscribe?(TransportRequest(document: body["query"] as? String ?? "", operationName: body["operationName"] as? String ?? "", variablesJson: variables, purpose: .standard))
    }
  }

  private func openFrame(_ token: String, key: SymmetricKey) throws -> [String: Any] {
    let parts = token.split(separator: ".", omittingEmptySubsequences: false).map(String.init)
    let box = try AES.GCM.SealedBox(nonce: AES.GCM.Nonce(data: decode(parts[2])), ciphertext: decode(parts[3]), tag: decode(parts[4]))
    let plain = try AES.GCM.open(box, using: key, authenticating: Data(parts[0].utf8))
    return try JSONSerialization.jsonObject(with: plain) as! [String: Any]
  }

  func emit(token: String, body: [String: Any]) throws {
    let connection = try connection(token)
    connection.order.lock()
    defer { connection.order.unlock() }
    var body = body
    body["n"] = connection.sequence
    connection.sequence += 1
    let header = encode(Data(#"{"alg":"dir","enc":"A256GCM"}"#.utf8))
    let nonce = AES.GCM.Nonce()
    let box = try AES.GCM.seal(JSONSerialization.data(withJSONObject: body), using: connection.sendKey!, nonce: nonce, authenticating: Data(header.utf8))
    let frame = "\(header)..\(encode(Data(box.nonce))).\(encode(box.ciphertext)).\(encode(box.tag))"
    connection.events.continuation.yield(.text(text: try text(["message": ["frame": frame]])))
  }

  /// A data event of the open subscription. False while no subscription is open.
  func event(token: String, operation: String, result: [String: Any]) throws -> Bool {
    guard let id = try connection(token).subscriptions[operation] else { return false }
    try emit(token: token, body: ["t": "next", "id": id, "result": result])
    return true
  }

  func event(operation: String, result: String) throws -> Bool {
    guard let token = lastOpened, let value = try JSONSerialization.jsonObject(with: Data(result.utf8)) as? [String: Any] else { return false }
    return try event(token: token, operation: operation, result: value)
  }

  /// A data event of the first open subscription.
  func event(token: String, result: [String: Any]) throws -> Bool {
    guard let operation = try connection(token).subscriptions.keys.sorted().first else { return false }
    return try event(token: token, operation: operation, result: result)
  }

  /// The socket breaks.
  func drop(token: String) throws {
    try connection(token).events.continuation.yield(.closed(code: 1006, reason: "dropped"))
  }

  func emitOld(token: String) throws {
    let id = try connection(token).subscriptions.values.first!
    try emit(token: token, body: ["t": "next", "id": id, "result": ["data": ["bulletinAdded": ["body": "old", "occurredAt": "2026-10-18T00:00:00Z"]]]])
  }

  func cableNext(token: String) async -> EncryptedSocketEvent? {
    guard let connection = try? connection(token) else { return nil }
    return await nextElement(of: connection.events.stream)
  }
  func cableClose(token: String) async {
    lock.withLock { connections[token] }?.events.continuation.finish()
    closed.continuation.yield(token)
  }
  func query(request: TransportRequest) async throws -> String { throw DbError.Invalid(detail: "unexpected plain query") }
  func mutate(request: TransportRequest) async throws -> String { throw DbError.Invalid(detail: "unexpected plain mutation") }
  func subscribe(request: TransportRequest, sink: SubscriptionSink) async throws { throw DbError.Invalid(detail: "unexpected plain subscription") }
  func unsubscribe(token: String) {}
  func put(url: String, headers: [String: String], file: PutFile, identity: TransferIdentity, progress: ByteProgress) async throws -> PutReply { throw DbError.Invalid(detail: "unexpected upload") }
}
