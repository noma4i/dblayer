import CryptoKit
import Foundation
import Security
@testable import DBlayer

/// An encrypted server that opens every sealed request with the fixture key and seals the answer, so tests run the real encrypted session path and fake only the HTTP exchange.
final class SealedServer: EncryptedHttp, @unchecked Sendable {
  static let serverKey = "MIIBojANBgkqhkiG9w0BAQEFAAOCAY8AMIIBigKCAYEAtzb67l/cXul2uArFqeZXsMf4tImrLcaHb3uiVNp8mjjJrFK/zwUEEazm3pmriAbj1nBzSxQLuq3jNUeFHXoQxjwWmTdv+PoAypz4ZII6Dz6rknk290/ddmimc1Z4Mx0AkT4ffAA9WHiDtLPF6tKZWNsWF/SpJps9PWrIKNOKY78ZfCgfegrWfKwoNYExyZ0IuUUgG0lsRjEEE/U59GpbNBqdKtw7KaIGK47QRk6HWOkCQtyqqjsseOJLwdI0k6jDNmcH6DulPNmar9AbwsmpngD55P8y4JdH49Eq3Nr040gmg0Y1p1FZm5PbKeC4WgZZdmg1bxjKAa+MYP0fROi4dFz05/81fqFb6DWaXencb0p/Iz+IUfQiI/0EYNv1SfdTUHzGyHFAmQHeCUIKLSkNNg3uF/jqKthFecCnWEfTUWRWFsN/8/Nb65a1UI/L1hP/qd+T6xKfchqVBEf767yeHdgsy3GeuqTsgXUPkuA1DO9HPP1xWqa6orUNWkimSwxDAgMBAAE="
  static let pin = "fab4f816eb33917161942684230405ae47ad4df5b456bd973085599b1364342a"
  static let origin = "https://api.example.com"

  private let lock = NSLock()
  private let privateKey: SecKey
  private var received: [(path: String, body: [String: Any])] = []
  private var routed: [(operation: String?, purpose: NetworkPurpose)] = []
  private var refusing = false
  private var holding: String?
  private var parked: [CheckedContinuation<Void, Never>] = []
  private let arrivals = AsyncStream<Void>.makeStream()
  private let lostAnswers = AsyncStream<Void>.makeStream()
  private let deduplicate: Bool
  private var cached: [String: (Data, String)] = [:]
  private var executed: [[String: Any]] = []
  private var lostFile: String?
  private var lost = false
  private let answer: @Sendable (String, [String: Any]) -> [String: Any]

  /// `answer` maps the path and the request body of a signed-in request to the answer body.
  init(deduplicate: Bool = false, answer: @escaping @Sendable (String, [String: Any]) -> [String: Any]) throws {
    self.deduplicate = deduplicate
    self.answer = answer
    let fixtureURL = URL(fileURLWithPath: #filePath).deletingLastPathComponent().deletingLastPathComponent().deletingLastPathComponent().deletingLastPathComponent().appendingPathComponent("fixtures/encrypted-http/kat.json")
    let fixture = try JSONSerialization.jsonObject(with: Data(contentsOf: fixtureURL)) as! [String: Any]
    let pem = (fixture["SERVER_PRIVATE_KEY_PEM"] as! String).split(separator: "\n").filter { !$0.hasPrefix("-----") }.joined()
    var error: Unmanaged<CFError>?
    let attributes: [CFString: Any] = [kSecAttrKeyType: kSecAttrKeyTypeRSA, kSecAttrKeyClass: kSecAttrKeyClassPrivate]
    privateKey = SecKeyCreateWithData(Data(base64Encoded: pem)! as CFData, attributes as CFDictionary, &error)!
  }

  var requests: [(path: String, body: [String: Any])] { lock.withLock { received } }
  var routes: [(operation: String?, purpose: NetworkPurpose)] { lock.withLock { routed } }
  var effects: [[String: Any]] { lock.withLock { executed } }
  func loseAnswer(filename: String) { lock.withLock { lostFile = filename } }
  func lostAnswer() async { var iterator = lostAnswers.stream.makeAsyncIterator(); _ = await iterator.next() }
  var hasLostAnswer: Bool { lock.withLock { lost } }

  func pathRequests(_ path: String) -> Int { requests.filter { $0.path == path }.count }

  func operationRequests(_ operation: String) -> Int { requests.filter { ($0.body["operationName"] as? String) == operation }.count }

  /// While true every request is recorded and refused with HTTP 500, so a test that forbids background reads finds them in `requests`.
  func refuseEverything(_ refusing: Bool) { lock.withLock { self.refusing = refusing } }

  /// While set, a request for this operation reaches the server and waits there, so a test ends a call while its request is in flight. `nil` lets every waiting request answer.
  func hold(operation: String?) {
    let released = lock.withLock { () -> [CheckedContinuation<Void, Never>] in
      holding = operation
      guard operation == nil else { return [] }
      defer { parked = [] }
      return parked
    }
    released.forEach { $0.resume() }
  }

  /// How many requests wait in the hold.
  var parkedCount: Int { lock.withLock { parked.count } }

  func heldRequest() async {
    var iterator = arrivals.stream.makeAsyncIterator()
    _ = await iterator.next()
  }

  func post(path: String, body: String, purpose: NetworkPurpose) async throws -> EncryptedReply {
    let request = try open(body)
    let sent = request["body"] as! [String: Any]
    lock.withLock { received.append((path, sent)); routed.append((sent["operationName"] as? String, purpose)) }
    if lock.withLock({ refusing }) { return EncryptedReply(status: 500, body: "") }
    let requestId = request["requestId"] as! String
    var stable = request
    stable.removeValue(forKey: "issuedAt")
    let digest = try JSONSerialization.data(withJSONObject: stable, options: [.sortedKeys])
    if deduplicate, let prior = lock.withLock({ cached[requestId] }) {
      guard prior.0 == digest else { throw DbError.Invalid(detail: "request ID reused with different content") }
      return EncryptedReply(status: 200, body: prior.1)
    }
    let reply = answer(path, sent)
    lock.withLock { executed.append(sent) }
    if let operation = sent["operationName"] as? String, lock.withLock({ holding == operation }) {
      await withCheckedContinuation { continuation in
        let wait = lock.withLock { () -> Bool in
          guard holding == operation else { return false }
          parked.append(continuation)
          return true
        }
        if wait { arrivals.continuation.yield(()) }
        if !wait { continuation.resume() }
      }
    }
    let sealed = try seal(request, reply)
    let lose = lock.withLock { () -> Bool in
      if deduplicate { cached[requestId] = (digest, sealed) }
      let filename = (sent["variables"] as? [String: Any])?["filename"] as? String
      guard filename != nil && filename == lostFile else { return false }
      lostFile = nil
      lost = true
      return true
    }
    if lose { lostAnswers.continuation.yield(()); throw DbError.Network(detail: "lost answer") }
    return EncryptedReply(status: 200, body: sealed)
  }

  func open(_ token: String) throws -> [String: Any] {
    let parts = token.split(separator: ".", omittingEmptySubsequences: false).map(String.init)
    var error: Unmanaged<CFError>?
    let cek = SecKeyCreateDecryptedData(privateKey, .rsaEncryptionOAEPSHA1, Self.decode(parts[1]) as CFData, &error)! as Data
    let box = try AES.GCM.SealedBox(nonce: AES.GCM.Nonce(data: Self.decode(parts[2])), ciphertext: Self.decode(parts[3]), tag: Self.decode(parts[4]))
    let plain = try AES.GCM.open(box, using: SymmetricKey(data: cek), authenticating: Data(parts[0].utf8))
    return try JSONSerialization.jsonObject(with: plain) as! [String: Any]
  }

  private func seal(_ request: [String: Any], _ body: [String: Any]) throws -> String {
    let header = Self.encode(Data(#"{"alg":"dir","enc":"A256GCM"}"#.utf8))
    var answer: [String: Any] = ["requestId": request["requestId"]!, "body": body]
    answer["sequence"] = request["sequence"] is NSNull ? NSNull() : request["sequence"]
    let plain = try JSONSerialization.data(withJSONObject: answer)
    let box = try AES.GCM.seal(plain, using: SymmetricKey(data: Self.decode(request["responseKey"] as! String)), nonce: AES.GCM.Nonce(data: Data(repeating: 7, count: 12)), authenticating: Data(header.utf8))
    return "\(header)..\(Self.encode(Data(box.nonce)))" + ".\(Self.encode(box.ciphertext)).\(Self.encode(box.tag))"
  }

  private static func decode(_ text: String) -> Data {
    let base = text.replacingOccurrences(of: "-", with: "+").replacingOccurrences(of: "_", with: "/")
    return Data(base64Encoded: base + String(repeating: "=", count: (4 - base.count % 4) % 4))!
  }

  private static func encode(_ data: Data) -> String {
    data.base64EncodedString().replacingOccurrences(of: "+", with: "-").replacingOccurrences(of: "/", with: "_").replacingOccurrences(of: "=", with: "")
  }
}
