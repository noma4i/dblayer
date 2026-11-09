import CryptoKit
import DBlayerTesting
import Foundation
import Security
import DBlayer

/// An encrypted server that opens every sealed request with the fixture key and seals the answer, so tests run the real encrypted session path and fake only the HTTP exchange.
public final class SealedServer: ScriptedHttp, @unchecked Sendable {
  public static let serverKey = "MIIBojANBgkqhkiG9w0BAQEFAAOCAY8AMIIBigKCAYEAtzb67l/cXul2uArFqeZXsMf4tImrLcaHb3uiVNp8mjjJrFK/zwUEEazm3pmriAbj1nBzSxQLuq3jNUeFHXoQxjwWmTdv+PoAypz4ZII6Dz6rknk290/ddmimc1Z4Mx0AkT4ffAA9WHiDtLPF6tKZWNsWF/SpJps9PWrIKNOKY78ZfCgfegrWfKwoNYExyZ0IuUUgG0lsRjEEE/U59GpbNBqdKtw7KaIGK47QRk6HWOkCQtyqqjsseOJLwdI0k6jDNmcH6DulPNmar9AbwsmpngD55P8y4JdH49Eq3Nr040gmg0Y1p1FZm5PbKeC4WgZZdmg1bxjKAa+MYP0fROi4dFz05/81fqFb6DWaXencb0p/Iz+IUfQiI/0EYNv1SfdTUHzGyHFAmQHeCUIKLSkNNg3uF/jqKthFecCnWEfTUWRWFsN/8/Nb65a1UI/L1hP/qd+T6xKfchqVBEf767yeHdgsy3GeuqTsgXUPkuA1DO9HPP1xWqa6orUNWkimSwxDAgMBAAE="
  public static let pin = "fab4f816eb33917161942684230405ae47ad4df5b456bd973085599b1364342a"
  public static let origin = "https://api.example.com"

  private static let privateKeyBody = "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"

  private let lock = NSLock()
  private let privateKey: SecKey
  private var received: [(path: String, body: [String: Any])] = []
  private var routed: [(operation: String?, purpose: NetworkPurpose)] = []
  private var refusing = false
  private var holding: String?
  private var parked: [CheckedContinuation<Void, Never>] = []
  private let arrivals = AsyncStream<Void>.makeStream()
  private let lostAnswers = AsyncStream<Void>.makeStream()
  private let contract: ServerContract = {
    let contract = ServerContract.adopting()
    contract.cacheEverything()
    return contract
  }()
  private var executed: [[String: Any]] = []
  private var lostFile: String?
  private var lost = false
  private let answer: @Sendable (String, [String: Any]) -> [String: Any]
  private let bridge: (@Sendable (String, [String: Any]) async throws -> [String: Any])?
  @TaskLocal public static var purpose: NetworkPurpose = .standard

  /// The server contract decides repeats of a pair, a second body under one pair, a missing sequence number. `answer` maps the path and the request body of a signed-in request to the answer body.
  public init(answer: @escaping @Sendable (String, [String: Any]) -> [String: Any]) throws {
    self.answer = answer
    self.bridge = nil
    var error: Unmanaged<CFError>?
    let attributes: [CFString: Any] = [kSecAttrKeyType: kSecAttrKeyTypeRSA, kSecAttrKeyClass: kSecAttrKeyClassPrivate]
    privateKey = SecKeyCreateWithData(Data(base64Encoded: Self.privateKeyBody)! as CFData, attributes as CFDictionary, &error)!
  }

  /// A server whose answers come from an async closure, such as a mock transport behind the encrypted session.
  public init(bridge: @escaping @Sendable (String, [String: Any]) async throws -> [String: Any]) throws {
    self.answer = { _, _ in [:] }
    self.bridge = bridge
    var error: Unmanaged<CFError>?
    let attributes: [CFString: Any] = [kSecAttrKeyType: kSecAttrKeyTypeRSA, kSecAttrKeyClass: kSecAttrKeyClassPrivate]
    privateKey = SecKeyCreateWithData(Data(base64Encoded: Self.privateKeyBody)! as CFData, attributes as CFDictionary, &error)!
  }

  public var requests: [(path: String, body: [String: Any])] { lock.withLock { received } }
  public var routes: [(operation: String?, purpose: NetworkPurpose)] { lock.withLock { routed } }
  public var effects: [[String: Any]] { lock.withLock { executed } }
  public func loseAnswer(filename: String) { lock.withLock { lostFile = filename } }
  public func lostAnswer() async { var arrivals = lostAnswers.stream.makeAsyncIterator()
    _ = await arrivals.next() }
  public var hasLostAnswer: Bool { lock.withLock { lost } }

  public func pathRequests(_ path: String) -> Int { requests.filter { $0.path == path }.count }

  public func operationRequests(_ operation: String) -> Int { requests.filter { ($0.body["operationName"] as? String) == operation }.count }

  /// While true every request is recorded and refused with HTTP 500, so a test that forbids background reads finds them in `requests`.
  public func refuseEverything(_ refusing: Bool) { lock.withLock { self.refusing = refusing } }

  /// While set, a request for this operation reaches the server and waits there, so a test ends a call while its request is in flight. `nil` lets every waiting request answer.
  public func hold(operation: String?) {
    let released = lock.withLock { () -> [CheckedContinuation<Void, Never>] in
      holding = operation
      guard operation == nil else { return [] }
      defer { parked = [] }
      return parked
    }
    released.forEach { $0.resume() }
  }

  /// How many requests wait in the hold.
  public var parkedCount: Int { lock.withLock { parked.count } }

  public func heldRequest() async {
    var held = arrivals.stream.makeAsyncIterator()
    _ = await held.next()
  }

  private static func wireExtensions(_ extensions: GraphqlExtensions?) throws -> [String: Any] {
    guard let extensions else { return [:] }
    var wire: [String: Any] = [:]
    for (key, text) in extensions.fields {
      wire[key] = try JSONSerialization.jsonObject(with: Data(text.utf8), options: [.fragmentsAllowed])
    }
    if let code = extensions.code { wire["code"] = code }
    if let disposition = extensions.transactionDisposition { wire["transactionDisposition"] = transactionDispositionText(disposition: disposition) }
    return wire
  }

  public func post(path: String, body: String, purpose: NetworkPurpose) async throws -> EncryptedReply {
    let request = try open(body)
    let sent = request["body"] as! [String: Any]
    lock.withLock { received.append((path, sent)); routed.append((sent["operationName"] as? String, purpose)) }
    if lock.withLock({ refusing }) { return EncryptedReply(status: 500, body: "") }
    let wire = String(decoding: try JSONSerialization.data(withJSONObject: request, options: [.sortedKeys]), as: UTF8.self)
    if path.hasPrefix("/api/auth/"), request["sequence"] as? Int == 1 { contract.setLast(sequence: 0) }
    switch contract.admit(scope: "sealed", requestJson: wire) {
    case .stored(let sealed): return EncryptedReply(status: 200, body: sealed)
    case .twoBodies: throw DbError.Invalid(detail: "request ID reused with different content")
    case .refused(let refusal): return EncryptedReply(status: 200, body: try seal(request, try JSONSerialization.jsonObject(with: Data(refusal.utf8)) as! [String: Any], spending: false))
    case .fresh: break
    }
    let reply: [String: Any]
    if let bridge {
      do { reply = try await Self.$purpose.withValue(purpose) { try await bridge(path, sent) } } catch let error as DbError {
        switch error {
        case .Http(let status, _): return EncryptedReply(status: status, body: "")
        case .Session(let code, _): reply = ["error": Self.wireName(code)]
        case .Graphql(let messages, let extensions):
          reply = ["errors": try messages.map { ["message": $0, "extensions": try Self.wireExtensions(extensions)] as [String: Any] }]
        default: throw error
        }
      }
    } else {
      reply = answer(path, sent)
    }
    lock.withLock { executed.append(sent) }
    TestPulse.shared.pulse()
    if let operation = sent["operationName"] as? String, lock.withLock({ holding == operation }) {
      await withCheckedContinuation { continuation in
        let wait = lock.withLock { () -> Bool in
          guard holding == operation else { return false }
          parked.append(continuation)
          return true
        }
        if wait {
          arrivals.continuation.yield(())
          TestPulse.shared.pulse()
        }
        if !wait { continuation.resume() }
      }
    }
    let sealed = try seal(request, reply)
    contract.settle(scope: "sealed", requestJson: wire, token: sealed)
    let lose = lock.withLock { () -> Bool in
      let filename = (sent["variables"] as? [String: Any])?["filename"] as? String
      guard filename != nil && filename == lostFile else { return false }
      lostFile = nil
      lost = true
      return true
    }
    if lose { lostAnswers.continuation.yield(()); throw DbError.Network(detail: "lost answer") }
    return EncryptedReply(status: 200, body: sealed)
  }

  private static func wireName(_ code: SessionCode) -> String {
    "\(code)".map { $0.isUppercase ? "_\($0)" : String($0).uppercased() }.joined()
  }

  public func open(_ token: String) throws -> [String: Any] {
    let parts = token.split(separator: ".", omittingEmptySubsequences: false).map(String.init)
    var error: Unmanaged<CFError>?
    let cek = SecKeyCreateDecryptedData(privateKey, .rsaEncryptionOAEPSHA1, Self.decode(parts[1]) as CFData, &error)! as Data
    let box = try AES.GCM.SealedBox(nonce: AES.GCM.Nonce(data: Self.decode(parts[2])), ciphertext: Self.decode(parts[3]), tag: Self.decode(parts[4]))
    let plain = try AES.GCM.open(box, using: SymmetricKey(data: cek), authenticating: Data(parts[0].utf8))
    return try JSONSerialization.jsonObject(with: plain) as! [String: Any]
  }

  public func seal(_ request: [String: Any], _ body: [String: Any], spending: Bool = true) throws -> String {
    let header = Self.encode(Data(#"{"alg":"dir","enc":"A256GCM"}"#.utf8))
    var answer: [String: Any] = ["requestId": request["requestId"]!, "body": body]
    answer["sequence"] = request["sequence"] is NSNull || !spending ? NSNull() : request["sequence"]
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
