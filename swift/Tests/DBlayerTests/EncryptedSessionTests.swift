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

  func post(path: String, body: String) async throws -> EncryptedReply {
    lock.withLock { recorded.append((path, body)) }
    return EncryptedReply(status: status, body: "")
  }

  func get(path: String) async throws -> EncryptedReply { EncryptedReply(status: 503, body: "") }
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

@Suite(.serialized)
struct EncryptedSessionTests {
  static let scriptId = "encrypted-http"
  static let origin = URL(string: "https://api.example.com")!

  @Test
  func postsTheJoseTextToTheOriginPathAndReturnsAnyStatusUnchanged() async throws {
    ScriptedURLProtocol.reset(scriptId: Self.scriptId)
    ScriptedURLProtocol.push(scriptId: Self.scriptId, status: 503, headers: [:], body: Data("busy".utf8), url: Self.origin)
    let http = UrlSessionEncryptedHttp(origin: Self.origin, session: ScriptedURLProtocol.session(scriptId: Self.scriptId))

    let reply = try await http.post(path: "/api/v3/graphql", body: "a.b.c.d.e")

    #expect((reply.status, reply.body) == (503, "busy"))
    let request = try #require(ScriptedURLProtocol.recordedRequests(scriptId: Self.scriptId).first)
    #expect(request.url?.absoluteString == "https://api.example.com/api/v3/graphql")
    #expect(request.httpMethod == "POST")
    #expect(request.value(forHTTPHeaderField: "Content-Type") == "application/jose")
    #expect(request.value(forHTTPHeaderField: "Authorization") == nil && request.value(forHTTPHeaderField: "Cookie") == nil)
    #expect(ScriptedURLProtocol.wireBodies(scriptId: Self.scriptId).first == Data("a.b.c.d.e".utf8))
  }

  @Test
  func readsThePublicConfigWithAGetThatCarriesNoBody() async throws {
    ScriptedURLProtocol.reset(scriptId: Self.scriptId)
    ScriptedURLProtocol.push(scriptId: Self.scriptId, status: 200, headers: [:], body: Data("{}".utf8), url: Self.origin)
    let http = UrlSessionEncryptedHttp(origin: Self.origin, session: ScriptedURLProtocol.session(scriptId: Self.scriptId))

    let reply = try await http.get(path: "/api/v3/config")

    #expect((reply.status, reply.body) == (200, "{}"))
    let request = try #require(ScriptedURLProtocol.recordedRequests(scriptId: Self.scriptId).first)
    #expect(request.httpMethod == "GET" && request.url?.path == "/api/v3/config" && request.value(forHTTPHeaderField: "Content-Type") == nil)
  }

  @Test
  func aResponseThatIsNotHttpHasStatusZero() async throws {
    let configuration = URLSessionConfiguration.ephemeral
    configuration.protocolClasses = [PlainResponseProtocol.self]
    let http = UrlSessionEncryptedHttp(origin: Self.origin, session: URLSession(configuration: configuration))

    #expect(try await http.get(path: "/api/v3/config").status == 0)
  }

  @Test
  func aTransportFailureIsANetworkError() async throws {
    ScriptedURLProtocol.reset(scriptId: Self.scriptId)
    let http = UrlSessionEncryptedHttp(origin: Self.origin, session: ScriptedURLProtocol.session(scriptId: Self.scriptId))

    await #expect(throws: DbError.self) { try await http.post(path: "/api/v3/logout", body: "x") }
  }

  @Test
  func aTransportWithoutAnOperationRunRefusesQueriesAndMutations() async throws {
    let transport = DatabaseTransport(cable: { nil })
    let request = TransportRequest(document: "query Q { q }", operationName: "Q", variablesJson: "{}")

    await #expect(throws: DbError.self) { try await transport.query(request: request) }
    await #expect(throws: DbError.self) { try await transport.mutate(request: request) }
  }

  @Test
  func theCoreSendsSealedRequestsThroughTheAppHooksAndKeepsTheCredentialInTheAppStore() async throws {
    let http = RecordedHttp(status: 400)
    let credentials = HeldCredential()
    let database = try Database.openEncrypted(
      config: Config(path: nil, dataVersion: nil, cacheRows: 32),
      modelsJson: modelsJSON,
      encrypted: EncryptedConfig(origin: Self.origin.absoluteString, serverKey: serverKey, pin: serverPin),
      http: http,
      credentials: credentials,
      transport: DatabaseTransport(cable: { nil })
    )

    await #expect(throws: DbError.self) { try await database.sessionCall(operation: "otp", bodyJson: #"{"phone":"+61400000000"}"#) }
    await #expect(throws: DbError.self) { try await database.sessionCall(operation: "otp", bodyJson: #"{"phone":"+61411111111"}"#) }
    await #expect(throws: DbError.self) { try await database.serverConfig() }

    let posts = http.posts
    #expect(posts.map(\.path) == ["/api/v3/auth/otp"])
    #expect(posts[0].body.split(separator: ".", omittingEmptySubsequences: false).count == 5)
    #expect(credentials.current?.count == 43)
    try database.sessionReset()
    #expect(credentials.current == nil)
  }
}
