import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

private final class PathCredential: CredentialStore, @unchecked Sendable {
  private let lock = NSLock()
  private var value: String?
  func get() throws -> String? { lock.withLock { value } }
  func set(credential: String) throws { lock.withLock { value = credential } }
  func clear() throws { lock.withLock { value = nil } }
}

private enum PathProbe: GraphQLOperation {
  typealias Variables = String
  static let operationName = "PathProbe"
  static let document = "query PathProbe { __typename }"
}

private enum SessionProbe: GraphQLOperation {
  typealias Variables = String
  static let operationName = "SessionProfile"
  static let document = "query SessionProfile { sessionProfile { id } }"
}

private struct PathLogin: Encodable, Sendable { let account: String }

private final class RefusingHttp: ScriptedHttp, Sendable {
  let posts: AsyncStream<Void>.Continuation
  init(_ posts: AsyncStream<Void>.Continuation) { self.posts = posts }
  func post(path: String, body: String, purpose: NetworkPurpose) async throws -> EncryptedReply {
    posts.yield()
    throw DbError.Invalid(detail: "unexpected HTTP request")
  }
}

extension BridgeProbeTests {
struct ReachabilityTests {
  @Test @MainActor
  func thePathStateGatesTheOpenDatabaseAndTheBootstrapRequest() async throws {
    await DB.closeForCleanup()
    let path = FakePath()
    defer { Task { @MainActor in try? await DB.close() } }
    let url = URL(fileURLWithPath: #filePath).deletingLastPathComponent().deletingLastPathComponent().deletingLastPathComponent().deletingLastPathComponent().appendingPathComponent("fixtures/encrypted-http/kat.json")
    let fixture = try JSONDecoder().decode([String: String].self, from: Data(contentsOf: url))
    let encrypted = EncryptedConfig(origin: "https://example.com", serverKey: try #require(fixture["PUBLIC_KEY_DER_B64"]), pin: try #require(fixture["PIN"]), endpoints: EncryptedEndpoints(graphql: "/graphql", cable: "/cable", logout: "/logout", bootstrap: "/bootstrap", sessionOperations: [:]), protocolLabel: "test-v1", cablePurpose: "hello", cableChannel: "TestChannel")
    let (posts, continuation) = AsyncStream<Void>.makeStream()
    let bootstrapHttp = RefusingHttp(continuation)
    let server = try SealedServer { requestPath, body in
      if requestPath == "/api/auth/login_link" { return ["sessionId": "stand"] }
      if body["operationName"] as? String == "SessionProfile" { return ["data": ["sessionProfile": ["id": "stand"]]] }
      return ["data": [String: Any]()]
    }
    let signedIn = EncryptedConfig(origin: encrypted.origin, serverKey: encrypted.serverKey, pin: encrypted.pin, endpoints: EncryptedEndpoints(graphql: "/graphql", cable: "/cable", logout: "/logout", bootstrap: "/bootstrap", sessionOperations: ["login_link": EncryptedSessionOperation(path: "/api/auth/login_link", class: .authenticated)]), protocolLabel: "test-v1", cablePurpose: "hello", cableChannel: "TestChannel")
    try await DB.openTesting(transport: MockTransport(), modelsJSON: "[]", operations: [PathProbe.self, SessionProbe.self], operationReturns: ["PathProbe": rawReturns, "SessionProfile": rawReturns], encrypted: signedIn, credentials: PathCredential(), http: server, observers: SystemObservers(path: path))
    path.set(true)
    try await DB.sessionCall("login_link", body: PathLogin(account: "A"))
    try await DB.call("SessionProfile", variables: [:])
    #expect(!(await refusal().contains("needs the network")))

    path.set(false)
    #expect(await refusal().contains("needs the network"))
    let bootstrap = Task { try? await fetchServerConfig(encrypted: encrypted, platform: .ios, version: "1.2", build: nil, http: bootstrapHttp, retries: nil, observers: SystemObservers(path: path)) }
    path.set(true)
    #expect(await Cursor(posts).next("the bootstrap request after the path became satisfied") != nil)
    bootstrap.cancel()
    #expect(!(await refusal().contains("needs the network")))
    path.set(false)
    #expect(await refusal().contains("needs the network"))
  }

  private func refusal() async -> String {
    do {
      try await DB.call("PathProbe", variables: [:])
      return ""
    } catch {
      return "\(error)".lowercased()
    }
  }
}
}
