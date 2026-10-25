import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

private let serverKey = "MIIBojANBgkqhkiG9w0BAQEFAAOCAY8AMIIBigKCAYEAtzb67l/cXul2uArFqeZXsMf4tImrLcaHb3uiVNp8mjjJrFK/zwUEEazm3pmriAbj1nBzSxQLuq3jNUeFHXoQxjwWmTdv+PoAypz4ZII6Dz6rknk290/ddmimc1Z4Mx0AkT4ffAA9WHiDtLPF6tKZWNsWF/SpJps9PWrIKNOKY78ZfCgfegrWfKwoNYExyZ0IuUUgG0lsRjEEE/U59GpbNBqdKtw7KaIGK47QRk6HWOkCQtyqqjsseOJLwdI0k6jDNmcH6DulPNmar9AbwsmpngD55P8y4JdH49Eq3Nr040gmg0Y1p1FZm5PbKeC4WgZZdmg1bxjKAa+MYP0fROi4dFz05/81fqFb6DWaXencb0p/Iz+IUfQiI/0EYNv1SfdTUHzGyHFAmQHeCUIKLSkNNg3uF/jqKthFecCnWEfTUWRWFsN/8/Nb65a1UI/L1hP/qd+T6xKfchqVBEf767yeHdgsy3GeuqTsgXUPkuA1DO9HPP1xWqa6orUNWkimSwxDAgMBAAE="
private let serverPin = "fab4f816eb33917161942684230405ae47ad4df5b456bd973085599b1364342a"

private final class RecordedHttp: EncryptedHttp, @unchecked Sendable {
  private let lock = NSLock()
  private var recorded: [(path: String, body: String)] = []
  let status: UInt16

  init(status: UInt16) { self.status = status }

  var posts: [(path: String, body: String)] { lock.withLock { recorded } }

  func post(path: String, body: String, upload: Bool) async throws -> EncryptedReply {
    lock.withLock { recorded.append((path, body)) }
    return EncryptedReply(status: status, body: "")
  }

}

private final class PlainResponseProtocol: URLProtocol, @unchecked Sendable {
  override class func canInit(with request: URLRequest) -> Bool { true }
  override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }
  override func startLoading() {
    client?.urlProtocol(self, didReceive: URLResponse(url: request.url!, mimeType: nil, expectedContentLength: 0, textEncodingName: nil), cacheStoragePolicy: .notAllowed)
    client?.urlProtocolDidFinishLoading(self)
  }
  override func stopLoading() {}
}

private final class HeldCredential: CredentialStore, @unchecked Sendable {
  private let lock = NSLock()
  private var value: String?

  var current: String? { lock.withLock { value } }
  func get() throws -> String? { current }
  func set(credential: String) throws { lock.withLock { value = credential } }
  func clear() throws { lock.withLock { value = nil } }
}

extension BridgeProbeTests {
struct EncryptedSessionTests {
  static let scriptId = "encrypted-http"
  static let origin = URL(string: "https://api.example.com")!

  @Test
  func oversizedSessionCallsFailLocallyAndLeaveTheNextCallAvailable() async throws {
    let http = RecordedHttp(status: 400)
    let database = try await openDatabase(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: MockTransport(), encrypted: EncryptedConfig(origin: Self.origin.absoluteString, serverKey: serverKey, pin: serverPin, endpoints: EncryptedEndpoints(graphql: "/api/graphql", cable: "/api/cable", logout: "/api/logout", bootstrap: "/api/bootstrap", sessionOperations: ["otp": EncryptedSessionOperation(path: "/api/auth/otp", sessionless: false), "confirm": EncryptedSessionOperation(path: "/api/auth/confirm", sessionless: false), "social": EncryptedSessionOperation(path: "/api/auth/social", sessionless: false), "ticket": EncryptedSessionOperation(path: "/api/ticket", sessionless: true)]), protocolLabel: "example-ws-v1", cablePurpose: "ws.hello", cableChannel: "EncryptedGraphqlChannel"), credentials: HeldCredential(), http: http)
    do {
      _ = try await database.sessionCall(operation: "otp", bodyJson: "{\"phone\":\"\(String(repeating: "x", count: 260_000))\"}")
      Issue.record("expected a local size refusal")
    } catch DbError.RequestTooLarge(let size, let limit) {
      #expect(size > limit && limit == 256 * 1024)
    }
    #expect(http.posts.isEmpty)
    #expect(try await database.sessionResume() == nil)
    do {
      _ = try await database.sessionCall(operation: "otp", bodyJson: #"{"phone":"+61400000000"}"#)
      Issue.record("expected the scripted HTTP refusal")
    } catch DbError.Http(let status, _) {
      #expect(status == 400)
    }
    #expect(http.posts.count == 1)
  }



  @Test
  func aResponseThatIsNotHttpHasStatusZero() async throws {
    let configuration = URLSessionConfiguration.ephemeral
    configuration.protocolClasses = [PlainResponseProtocol.self]
    let http = UrlSessionEncryptedHttp(origin: Self.origin, session: URLSession(configuration: configuration))

    #expect(try await http.post(path: "/api/bootstrap", body: "x", upload: false).status == 0)
  }

  @Test
  func aTransportFailureIsANetworkError() async throws {
    ScriptedURLProtocol.reset(scriptId: Self.scriptId)
    let http = UrlSessionEncryptedHttp(origin: Self.origin, session: ScriptedURLProtocol.session(scriptId: Self.scriptId))

    do {
      _ = try await http.post(path: "/api/logout", body: "x", upload: false)
      Issue.record("the failed transport did not throw")
    } catch DbError.Network {
    } catch {
      Issue.record("the failure surfaced as \(error), not as a network error")
    }
  }

  @Test
  func theCoreSendsSealedRequestsThroughTheAppHooksAndKeepsTheCredentialInTheAppStore() async throws {
    let http = RecordedHttp(status: 400)
    let credentials = HeldCredential()
    let database = try await openDatabase(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: MockTransport(), encrypted: EncryptedConfig(origin: Self.origin.absoluteString, serverKey: serverKey, pin: serverPin, endpoints: EncryptedEndpoints(graphql: "/api/graphql", cable: "/api/cable", logout: "/api/logout", bootstrap: "/api/bootstrap", sessionOperations: ["otp": EncryptedSessionOperation(path: "/api/auth/otp", sessionless: false), "confirm": EncryptedSessionOperation(path: "/api/auth/confirm", sessionless: false), "social": EncryptedSessionOperation(path: "/api/auth/social", sessionless: false), "ticket": EncryptedSessionOperation(path: "/api/ticket", sessionless: true)]), protocolLabel: "example-ws-v1", cablePurpose: "ws.hello", cableChannel: "EncryptedGraphqlChannel"), credentials: credentials, http: http)

    await #expect(throws: DbError.self) { try await database.sessionCall(operation: "ticket", bodyJson: "{}") }
    #expect(credentials.current == nil)
    #expect(try await database.sessionResume() == nil)
    await #expect(throws: DbError.self) { try await database.sessionCall(operation: "otp", bodyJson: #"{"phone":"+61400000000"}"#) }
    await #expect(throws: DbError.self) { try await database.sessionCall(operation: "otp", bodyJson: #"{"phone":"+61411111111"}"#) }

    #expect(try await database.sessionResume() == nil)
    let posts = http.posts
    #expect(posts.map(\.path) == ["/api/ticket", "/api/auth/otp", "/api/auth/otp"])
    #expect(posts[0].body.split(separator: ".", omittingEmptySubsequences: false).count == 5)
    #expect(credentials.current?.count == 43)
    try database.sessionReset()
    #expect(credentials.current == nil)
  }

}
}
