import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

private final class PathCredential: CredentialStore, Sendable {
  func get() throws -> String? { nil }
  func set(credential: String) throws {}
  func clear() throws {}
}

private enum PathProbe: GraphQLOperation {
  typealias Variables = String
  static let operationName = "PathProbe"
  static let document = "query PathProbe { __typename }"
}

private final class RefusingHttp: EncryptedHttp, Sendable {
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
    let http = RefusingHttp(continuation)
    try await Reachability.$source.withValue(path) {
      try await DB.open(encrypted: encrypted, credentials: PathCredential(), transport: MockTransport(), modelsJSON: "[]", operations: [PathProbe.self], http: http)
    }

    #expect(await refusal().contains("offline"))
    let bootstrap = Task { try? await Reachability.$source.withValue(path) { try await DB.serverConfig(encrypted: encrypted, platform: .ios, version: "1.2", http: http) } }
    path.set(true)
    #expect(await Cursor(posts).next("the bootstrap request after the path became satisfied") != nil)
    bootstrap.cancel()
    #expect(!(await refusal().contains("offline")))
    path.set(false)
    #expect(await refusal().contains("offline"))
  }

  private func refusal() async -> String {
    do {
      try await DB.run("PathProbe", variables: [:])
      return ""
    } catch {
      return "\(error)".lowercased()
    }
  }
}
}
