import CryptoKit
import DBlayerTesting
import Foundation
import Network
import Security
import Testing
@testable import DBlayer

private final class BootstrapPeer: @unchecked Sendable {
  let kat: [String: Any]
  let key: SecKey
  let encrypted: EncryptedConfig

  init() throws {
    let url = URL(fileURLWithPath: #filePath).deletingLastPathComponent().deletingLastPathComponent().deletingLastPathComponent().deletingLastPathComponent().appendingPathComponent("fixtures/bootstrap-config/bootstrap_kat.json")
    kat = try JSONSerialization.jsonObject(with: Data(contentsOf: url)) as! [String: Any]
    let pem = kat["rsa_private_pem"] as! String
    let der = Data(base64Encoded: pem.split(separator: "\n").filter { !$0.hasPrefix("-") }.joined())!
    key = SecKeyCreateWithData(der as CFData, [kSecAttrKeyType: kSecAttrKeyTypeRSA, kSecAttrKeyClass: kSecAttrKeyClassPrivate] as CFDictionary, nil)!
    let publicKey = SecKeyCopyExternalRepresentation(SecKeyCopyPublicKey(key)!, nil)! as Data
    func asn(_ tag: UInt8, _ data: Data) -> Data { Data([tag, 0x82, UInt8(data.count >> 8), UInt8(data.count & 255)]) + data }
    let algorithm = Data([0x30, 0x0d, 0x06, 0x09, 0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x01, 0x01, 0x05, 0x00])
    encrypted = EncryptedConfig(origin: kat["audience"] as! String, serverKey: asn(0x30, algorithm + asn(3, Data([0]) + publicKey)).base64EncodedString(), pin: kat["kid"] as! String, endpoints: EncryptedEndpoints(graphql: "/api/graphql", cable: "/api/cable", logout: "/api/logout", bootstrap: kat["path"] as! String, sessionOperations: ["otp": EncryptedSessionOperation(path: "/api/auth/otp", class: .authenticated), "confirm": EncryptedSessionOperation(path: "/api/auth/confirm", class: .authenticated), "social": EncryptedSessionOperation(path: "/api/auth/social", class: .authenticated), "ticket": EncryptedSessionOperation(path: "/api/ticket", class: .sessionless)]), protocolLabel: "example-ws-v1", cablePurpose: "ws.hello", cableChannel: "EncryptedGraphqlChannel")
  }

  func decode(_ text: String) -> Data {
    let base = text.replacingOccurrences(of: "-", with: "+").replacingOccurrences(of: "_", with: "/")
    return Data(base64Encoded: base + String(repeating: "=", count: (4 - base.count % 4) % 4))!
  }
  func encode(_ data: Data) -> String { data.base64EncodedString().replacingOccurrences(of: "+", with: "-").replacingOccurrences(of: "/", with: "_").replacingOccurrences(of: "=", with: "") }

  func answer(_ token: String) throws -> String {
    let parts = token.split(separator: ".", omittingEmptySubsequences: false).map(String.init)
    let cek = SecKeyCreateDecryptedData(key, .rsaEncryptionOAEPSHA1, decode(parts[1]) as CFData, nil)! as Data
    let box = try AES.GCM.SealedBox(nonce: AES.GCM.Nonce(data: decode(parts[2])), ciphertext: decode(parts[3]), tag: decode(parts[4]))
    let plain = try JSONSerialization.jsonObject(with: AES.GCM.open(box, using: SymmetricKey(data: cek), authenticating: Data(parts[0].utf8))) as! [String: Any]
    #expect(Set(plain.keys) == Set(["audience", "kid", "method", "path", "requestId", "issuedAt", "responseKey", "body"]))
    let issued = plain["issuedAt"] as! Double
    #expect((-30...120).contains(Date().timeIntervalSince1970 - issued))
    let body = plain["body"] as! [String: Any]
    let quote: (Any) throws -> String = { String(decoding: try JSONSerialization.data(withJSONObject: $0, options: [.fragmentsAllowed, .withoutEscapingSlashes]), as: UTF8.self) }
    let build = try body["build"].map { ",\"build\":\(try quote($0))" } ?? ""
    let bodyText = try "{\"platform\":\(quote(body["platform"]!)),\"version\":\(quote(body["version"]!))\(build)}"
    let signed = try plain.keys.filter { $0 != "issuedAt" }.sorted().map { key -> String in
      let value = key == "body" ? bodyText : try quote(plain[key]!)
      return "\"\(key)\":\(value)"
    }.joined(separator: ",")
    let digest = SHA256.hash(data: Data("{\(signed)}".utf8)).map { String(format: "%02x", $0) }.joined()
    let responseKat = kat["response"] as! [String: Any]
    let base = try JSONSerialization.jsonObject(with: Data((responseKat["plaintext"] as! String).utf8)) as! [String: Any]
    var config = base["body"] as! [String: Any]
    config["updateRequired"] = true
    config["future"] = true
    var response: [String: Any] = ["requestId": plain["requestId"]!, "sequence": NSNull()]
    response["requestDigest"] = digest
    response["body"] = config
    let header = encode(Data(#"{"alg":"dir","enc":"A256GCM"}"#.utf8))
    let sealed = try AES.GCM.seal(JSONSerialization.data(withJSONObject: response), using: SymmetricKey(data: decode(plain["responseKey"] as! String)), nonce: AES.GCM.Nonce(data: Data(repeating: 7, count: 12)), authenticating: Data(header.utf8))
    return "\(header)..\(encode(Data(sealed.nonce))).\(encode(sealed.ciphertext)).\(encode(sealed.tag))"
  }
}

private final class BootstrapHttp: ScriptedHttp, @unchecked Sendable {
  let peer: BootstrapPeer
  let kind: String
  private let lock = NSLock()
  private var count = 0
  var calls: Int { lock.withLock { count } }
  init(_ peer: BootstrapPeer, _ kind: String) { self.peer = peer; self.kind = kind }
  func send(request: HttpRequest) async -> RawOutcome {
    if kind == "timeout" {
      lock.withLock { count += 1 }
      return .failed(failure: .scripted(.timeout, "timed out"))
    }
    return await sendEncrypted(request: request)
  }
  func post(path: String, body: String, purpose: NetworkPurpose) async throws -> EncryptedReply {
    lock.withLock { count += 1 }
    #expect(path == peer.encrypted.endpoints.bootstrap)
    return kind == "http" ? EncryptedReply(status: 400, body: "") : EncryptedReply(status: 200, body: try peer.answer(body))
  }
}

private final class HeldBootstrapHttp: ScriptedHttp, @unchecked Sendable {
  let signal: AsyncStream<String>.Continuation
  init(_ signal: AsyncStream<String>.Continuation) { self.signal = signal }
  func post(path: String, body: String, purpose: NetworkPurpose) async throws -> EncryptedReply {
    signal.yield("started")
    do {
      try await parkUntilCancelled()
    } catch {
      signal.yield(error is CancellationError ? "cancelled" : "other error")
      throw error
    }
    throw DbError.Network(detail: "HTTP did not finish")
  }
}

final class BootstrapListener: @unchecked Sendable {
  let listener: NWListener
  private let lock = NSLock()
  private var received: [String] = []
  var requests: [String] { lock.withLock { received } }
  private var stopped = false
  var disconnected: Bool { lock.withLock { stopped } }
  private let holdBody: Bool
  private let holdHeaders: Bool
  private let responseStatus: String?
  init(responseStatus: String? = nil, holdBody: Bool = false, holdHeaders: Bool = false) throws {
    self.responseStatus = responseStatus
    self.holdBody = holdBody
    self.holdHeaders = holdHeaders
    listener = try NWListener(using: .tcp, on: .any)
  }
  func start() async throws -> URL {
    listener.newConnectionHandler = { [self] connection in
      connection.start(queue: .global())
      receive(connection, Data())
    }
    try await startListening(listener)
    let port = try #require(listener.port)
    return URL(string: "http://127.0.0.1:\(port.rawValue)")!
  }
  private func receive(_ connection: NWConnection, _ previous: Data) {
    connection.receive(minimumIncompleteLength: 1, maximumLength: 65536) { [self] data, _, done, _ in
      let data = previous + (data ?? Data())
      let text = String(decoding: data, as: UTF8.self)
      if let end = text.range(of: "\r\n\r\n") {
        let header = String(text[..<end.lowerBound])
        let length = header.components(separatedBy: "\r\n").first { $0.lowercased().hasPrefix("content-length:") }.flatMap { Int($0.split(separator: ":")[1].trimmingCharacters(in: .whitespaces)) } ?? 0
        if text[end.upperBound...].utf8.count < length { receive(connection, data); return }
        lock.withLock { received.append(text) }
        TestPulse.shared.pulse()
        if holdHeaders {
          watchDisconnect(connection)
          return
        }
        if holdBody {
          connection.send(content: Data("HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\n".utf8), completion: .contentProcessed { _ in
            self.watchDisconnect(connection)
          })
          return
        }
        let status = responseStatus ?? (requests.count == 1 ? "302 Found\r\nLocation: /redirect" : "200 OK")
        connection.send(content: Data("HTTP/1.1 \(status)\r\nContent-Length: 4\r\nConnection: close\r\n\r\nbusy".utf8), completion: .contentProcessed { _ in connection.cancel() })
      } else if !done { receive(connection, data) }
    }
  }
  private func watchDisconnect(_ connection: NWConnection) {
    connection.receive(minimumIncompleteLength: 1, maximumLength: 1) { _, _, done, error in
      self.lock.withLock { self.stopped = done || error != nil }
      TestPulse.shared.pulse()
      connection.cancel()
    }
  }
  deinit { listener.cancel() }
}

struct BootstrapConfigTests {
  @Test
  func nativeHttpCancellationClosesTheConnectionDuringResponseBody() async throws {
    let peer = try BootstrapPeer()
    let server = try BootstrapListener(holdBody: true)
    let origin = try await server.start()
    var config = peer.encrypted
    config.origin = origin.absoluteString
    let task = Task { try await DB.serverConfig(encrypted: config, platform: .ios, version: "1.2", http: UrlSessionTransport()) }
    await eventually("native HTTP received") { server.requests.count == 1 }
    task.cancel()
    await eventually("native HTTP disconnected", timeout: .seconds(2)) { server.disconnected }
    if case .success = await task.result { Issue.record("cancelled bootstrap succeeded") }
  }

  @Test
  func taskCancellationReachesTheCoreAndCancelsForeignHttp() async throws {
    let peer = try BootstrapPeer()
    let (events, signal) = AsyncStream<String>.makeStream()
    let cursor = Cursor(events)
    let http = HeldBootstrapHttp(signal)
    let task = Task { try await DB.serverConfig(encrypted: peer.encrypted, platform: .ios, version: "1.2", http: http) }
    #expect(await cursor.next("HTTP started") == "started")
    task.cancel()
    if case .success = await task.result { Issue.record("cancelled bootstrap succeeded") }
    #expect(await cursor.next("HTTP cancelled") == "cancelled")
    signal.finish()
  }

  @Test
  func bootstrapTimeoutWaitsForCancellationAndReportsTheAttempt() async throws {
    let peer = try BootstrapPeer()
    let http = BootstrapHttp(peer, "timeout")
    let retries = RetryProgress()
    let changes = Cursor(retries.changes)
    #expect(await changes.next("the snapshot at connect") == RetrySnapshot(waiting: []))
    let task = Task { try await DB.serverConfig(encrypted: peer.encrypted, platform: .ios, version: "1.2", http: http, retries: retries) }
    let attempt = try #require(await changes.next("the first retry")?.waiting.first)
    #expect(attempt.attempt == 1 && (800...1000).contains(attempt.delayMs) && attempt.kind == .bootstrap && attempt.owner == .bootstrap)
    task.cancel()
    if case .success = await task.result { Issue.record("cancelled bootstrap succeeded") }
    #expect(await changes.next("the cancelled call leaving") == RetrySnapshot(waiting: []))
    #expect(http.calls == 1)
  }

  /// bridge_prop_bootstrap_reader (P-BR-10): a reader that does not pull while the call retries and leaves gets the final snapshot as its next value, not the snapshots between.
  @Test
  func bridge_prop_bootstrap_reader_aSlowReaderGetsTheFinalSnapshotAndNothingIsBuffered() async throws {
    let peer = try BootstrapPeer()
    let http = BootstrapHttp(peer, "timeout")
    let retries = RetryProgress()
    let slow = Cursor(retries.changes)
    let fast = Cursor(retries.changes)
    let idle = RetrySnapshot(waiting: [])
    #expect(await slow.next("the snapshot at connect") == idle)
    #expect(await fast.next("the snapshot at connect") == idle)
    let task = Task { try await DB.serverConfig(encrypted: peer.encrypted, platform: .ios, version: "1.2", http: http, retries: retries) }
    #expect(await fast.next("the first retry")?.waiting.first?.owner == .bootstrap)
    task.cancel()
    _ = await task.result
    #expect(await fast.next("the cancelled call leaving") == idle)
    #expect(await slow.next("the final snapshot of the slow reader") == idle)
  }

  @Test(arguments: [AppPlatform.ios, .android], ["ok", "http", "pin"])
  func configBeforeDatabaseUsesTheCoreAndKeepsTheClassOfEveryOutcomeTheWrapperTypes(platform: AppPlatform, kind: String) async throws {
    let peer = try BootstrapPeer()
    let http = BootstrapHttp(peer, kind)
    var encrypted = peer.encrypted
    if kind == "pin" { encrypted.pin = "wrong" }
    do {
      let value = try await DB.serverConfig(encrypted: encrypted, platform: platform, version: "3.8.0", build: "120", http: http)
      #expect(kind == "ok")
      #expect(value.configVersion == 1 && value.minAppVersion == "3.7.2")
      #expect(value.updateRequired && value.trackingPromptEnabled && value.flagsJson == "{}")
      #expect(value.storeUrl!.contains("apps.apple.com"))
    } catch let error as DbError {
      switch (kind, error) {
      case ("pin", .Security(.serverKeyMismatch, _)), ("http", .Http(400, _)): break
      default: Issue.record("\(kind): \(error)")
      }
    }
    #expect(http.calls == (kind == "pin" ? 0 : 1))
  }

  @Test(arguments: [false, true])
  func actualHttpOmitsSecretsAndRefusesRedirects(customSession: Bool) async throws {
    let server = try BootstrapListener()
    let origin = try await server.start()
    let config = URLSessionConfiguration.ephemeral
    config.httpAdditionalHeaders = ["Authorization": "secret", "Cookie": "secret", "X-Payload-Encryption": "old", "X-Request-Signature": "old"]
    let session = URLSession(configuration: config)
    defer { session.invalidateAndCancel() }
    let reply = await UrlSessionTransport(session: customSession ? session : nil).send(request: probeRequest(origin, path: "/api/bootstrap", headers: ["User-Agent": DBlayer.userAgent(application: nil)]))
    #expect(reply == .response(status: 302, etag: nil, body: "busy"))
    #expect(server.requests.count == 1)
    let request = try #require(server.requests.first)
    #expect(request.hasPrefix("POST /api/bootstrap HTTP/1.1\r\n"))
    let headers = request.components(separatedBy: "\r\n").dropFirst().prefix(while: { !$0.isEmpty }).map { $0.lowercased() }
    #expect(headers.contains("content-type: application/jose"))
    let names = headers.map { String($0.split(separator: ":")[0]) }
    #expect(headers.contains("accept: ") || headers.contains("accept:"))
    #expect(headers.contains("accept-language: ") || headers.contains("accept-language:"))
    let userAgent = request.components(separatedBy: "\r\n").dropFirst().prefix(while: { !$0.isEmpty })
      .filter { $0.lowercased().hasPrefix("user-agent:") }
      .map { $0.split(separator: ":", maxSplits: 1, omittingEmptySubsequences: false)[1].trimmingCharacters(in: .whitespaces) }
    #expect(userAgent == [DBlayer.userAgent(application: nil)])
    let applicationHeaders = names.filter { !["host", "content-length", "transfer-encoding", "connection", "user-agent", "accept-encoding", "accept", "accept-language"].contains($0) }
    #expect(applicationHeaders == ["content-type"])
    for field in ["authorization:", "cookie:", "x-payload-encryption:", "x-request-signature:"] {
      #expect(!headers.contains { $0.hasPrefix(field) })
    }
  }
}
