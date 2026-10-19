import CryptoKit
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
    let url = URL(fileURLWithPath: #filePath).deletingLastPathComponent().deletingLastPathComponent().deletingLastPathComponent().deletingLastPathComponent().appendingPathComponent("fixtures/v3-config/bootstrap_kat.json")
    kat = try JSONSerialization.jsonObject(with: Data(contentsOf: url)) as! [String: Any]
    let pem = kat["rsa_private_pem"] as! String
    let der = Data(base64Encoded: pem.split(separator: "\n").filter { !$0.hasPrefix("-") }.joined())!
    key = SecKeyCreateWithData(der as CFData, [kSecAttrKeyType: kSecAttrKeyTypeRSA, kSecAttrKeyClass: kSecAttrKeyClassPrivate] as CFDictionary, nil)!
    let publicKey = SecKeyCopyExternalRepresentation(SecKeyCopyPublicKey(key)!, nil)! as Data
    func asn(_ tag: UInt8, _ data: Data) -> Data { Data([tag, 0x82, UInt8(data.count >> 8), UInt8(data.count & 255)]) + data }
    let algorithm = Data([0x30, 0x0d, 0x06, 0x09, 0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x01, 0x01, 0x05, 0x00])
    encrypted = EncryptedConfig(origin: kat["audience"] as! String, serverKey: asn(0x30, algorithm + asn(3, Data([0]) + publicKey)).base64EncodedString(), pin: kat["kid"] as! String)
  }

  func decode(_ text: String) -> Data {
    let base = text.replacingOccurrences(of: "-", with: "+").replacingOccurrences(of: "_", with: "/")
    return Data(base64Encoded: base + String(repeating: "=", count: (4 - base.count % 4) % 4))!
  }
  func encode(_ data: Data) -> String { data.base64EncodedString().replacingOccurrences(of: "+", with: "-").replacingOccurrences(of: "/", with: "_").replacingOccurrences(of: "=", with: "") }

  func answer(_ token: String, kind: String) throws -> String {
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
    if kind == "format" { config["configVersion"] = 2 }
    let responseBody: [String: Any] = kind == "request" ? ["error": "INVALID_REQUEST"] : config
    var response: [String: Any] = ["requestId": plain["requestId"]!, "sequence": NSNull()]
    response["requestDigest"] = kind == "tamper" ? "wrong" : digest
    response["body"] = responseBody
    let header = encode(Data(#"{"alg":"dir","enc":"A256GCM"}"#.utf8))
    let sealed = try AES.GCM.seal(JSONSerialization.data(withJSONObject: response), using: SymmetricKey(data: decode(plain["responseKey"] as! String)), nonce: AES.GCM.Nonce(data: Data(repeating: 7, count: 12)), authenticating: Data(header.utf8))
    return "\(header)..\(encode(Data(sealed.nonce))).\(encode(sealed.ciphertext)).\(encode(sealed.tag))"
  }
}

private final class BootstrapHttp: EncryptedHttp, @unchecked Sendable {
  let peer: BootstrapPeer
  let kind: String
  private let lock = NSLock()
  private var count = 0
  var calls: Int { lock.withLock { count } }
  init(_ peer: BootstrapPeer, _ kind: String) { self.peer = peer; self.kind = kind }
  func post(path: String, body: String) async throws -> EncryptedReply {
    lock.withLock { count += 1 }
    #expect(path == "/api/v3/bootstrap")
    return kind == "service" ? EncryptedReply(status: 503, body: "") : EncryptedReply(status: 200, body: try peer.answer(body, kind: kind))
  }
}

private final class BootstrapListener: @unchecked Sendable {
  let listener: NWListener
  private let lock = NSLock()
  private var received: [String] = []
  var requests: [String] { lock.withLock { received } }
  init() throws { listener = try NWListener(using: .tcp, on: .any) }
  func start() async throws -> URL {
    listener.newConnectionHandler = { [self] connection in
      connection.start(queue: .global())
      receive(connection, Data())
    }
    listener.start(queue: .global())
    await eventually("bootstrap HTTP listener ready") { self.listener.state == .ready }
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
        let status = requests.count == 1 ? "302 Found\r\nLocation: /redirect" : "200 OK"
        connection.send(content: Data("HTTP/1.1 \(status)\r\nContent-Length: 4\r\nConnection: close\r\n\r\nbusy".utf8), completion: .contentProcessed { _ in connection.cancel() })
      } else if !done { receive(connection, data) }
    }
  }
  deinit { listener.cancel() }
}

struct BootstrapConfigTests {
  @Test
  func configBeforeDatabaseUsesTheCoreAndPreservesEveryOutcome() async throws {
    let peer = try BootstrapPeer()
    for platform in [AppPlatform.ios, .android] {
      for kind in ["ok", "tamper", "request", "format", "service", "pin"] {
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
          let classified = TransportFailure.classifyEncryptedHttp(error)
          switch (kind, classified) {
          case ("pin", .Security(.serverKeyMismatch, _)), ("tamper", .Security(.responseAuthentication, _)),
               ("request", .Config(.invalidRequest, _)), ("format", .Config(.unsupportedVersion, _)),
               ("service", .Http(503, _)): break
          default: Issue.record("\(kind): \(error), classified \(classified)")
          }
        }
        #expect(http.calls == (kind == "pin" ? 0 : 1))
      }
    }
  }

  @Test
  func actualHttpOmitsSecretsAndRefusesRedirects() async throws {
    let server = try BootstrapListener()
    let origin = try await server.start()
    let config = URLSessionConfiguration.ephemeral
    config.httpAdditionalHeaders = ["Authorization": "secret", "Cookie": "secret", "X-Payload-Encryption": "old", "X-Request-Signature": "old"]
    let session = URLSession(configuration: config)
    defer { session.invalidateAndCancel() }
    let reply = try await UrlSessionEncryptedHttp(origin: origin, session: session).post(path: "/api/v3/bootstrap", body: "jwe")
    #expect(reply.status == 302 && reply.body == "busy")
    #expect(server.requests.count == 1)
    let request = try #require(server.requests.first)
    #expect(request.hasPrefix("POST /api/v3/bootstrap HTTP/1.1\r\n"))
    let headers = request.components(separatedBy: "\r\n").dropFirst().prefix(while: { !$0.isEmpty }).map { $0.lowercased() }
    #expect(headers.contains("content-type: application/jose"))
    let names = headers.map { String($0.split(separator: ":")[0]) }
    #expect(headers.contains("accept: ") || headers.contains("accept:"))
    #expect(headers.contains("accept-language: ") || headers.contains("accept-language:"))
    let userAgent = headers.filter { $0.hasPrefix("user-agent:") }
    #expect(userAgent == ["user-agent: "])
    let applicationHeaders = names.filter { !["host", "content-length", "transfer-encoding", "connection", "user-agent", "accept-encoding", "accept", "accept-language"].contains($0) }
    #expect(applicationHeaders == ["content-type"])
    for field in ["authorization:", "cookie:", "x-payload-encryption:", "x-request-signature:"] {
      #expect(!headers.contains { $0.hasPrefix(field) })
    }
  }
}
