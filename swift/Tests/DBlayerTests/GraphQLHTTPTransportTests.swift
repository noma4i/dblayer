import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

@Suite(.serialized)
struct GraphQLHTTPTransportTests {
  @Test
  func sessionTimeouts() async throws {
    ScriptedURLProtocol.reset(scriptId: Self.scriptId)
    let url = try #require(URL(string: "https://example.test/"))
    ScriptedURLProtocol.push(scriptId: Self.scriptId, status: 200, headers: [:], body: Data(), url: url)
    let configuration = ScriptedURLProtocol.session(scriptId: Self.scriptId).configuration
    let session = GraphQLHTTPTransport.makeSession(configuration: configuration)
    defer { session.invalidateAndCancel() }
    let transport = makeTransport(url: url, token: "tok", session: session)

    _ = try await transport.send(
      operationName: "Ping",
      documentHash: "abc123",
      document: nil,
      variablesJSON: Data("{}".utf8)
    )

    let request = try #require(ScriptedURLProtocol.recordedRequests(scriptId: Self.scriptId).first)
    #expect(request.timeoutInterval == 30)
    #expect(session.configuration.timeoutIntervalForRequest == 30)
    #expect(session.configuration.timeoutIntervalForResource == 60)
  }

  @Test(arguments: [nil, "query Ping { ping }"] as [String?])
  func sendsSignedEncryptedPost(document: String?) async throws {
    ScriptedURLProtocol.reset(scriptId: Self.scriptId)

    let url = try #require(URL(string: "https://example.test/"))
    let expectedBody = Data(#"{"data":{"ping":"pong"}}"#.utf8)
    ScriptedURLProtocol.push(scriptId: Self.scriptId, status: 200, headers: [:], body: expectedBody, url: url)
    let transport = makeTransport(url: url, token: "tok")

    let result = try await transport.send(
      operationName: "Ping",
      documentHash: "abc123",
      document: document,
      variablesJSON: Data("{}".utf8)
    )

    #expect(result.status == 200)
    #expect(result.body == expectedBody)
    let recordedRequest = try #require(ScriptedURLProtocol.recordedRequests(scriptId: Self.scriptId).first)
    #expect(recordedRequest.httpMethod == "POST")
    #expect(recordedRequest.value(forHTTPHeaderField: "X-API-Version") == "2")
    #expect(recordedRequest.value(forHTTPHeaderField: "X-Payload-Encryption") == "1")
    #expect(recordedRequest.value(forHTTPHeaderField: "x-device-uuid") == "device-1")
    #expect(!(recordedRequest.value(forHTTPHeaderField: "X-Request-Signature") ?? "").isEmpty)
    let requestBody = try #require(String(bytes: ScriptedURLProtocol.body(of: recordedRequest), encoding: .utf8))
    #expect(requestBody.contains("\"query\"") == (document != nil))

    let wireBody = try #require(ScriptedURLProtocol.wireBodies(scriptId: Self.scriptId).first)
    let envelope = try JSONDecoder().decode(PayloadEnvelope.self, from: wireBody)
    #expect(envelope.envelopeVersion == PayloadEnvelope.version)
    let rawBody = try #require(String(data: wireBody, encoding: .utf8))
    #expect(!rawBody.contains(#""operationName""#))
  }

  @Test
  func tokenSupplierReadsTheCurrentTokenOncePerRequest() async throws {
    ScriptedURLProtocol.reset(scriptId: Self.scriptId)

    let url = try #require(URL(string: "https://example.test/"))
    ScriptedURLProtocol.push(scriptId: Self.scriptId, status: 200, headers: [:], body: Data(), url: url)
    ScriptedURLProtocol.push(scriptId: Self.scriptId, status: 200, headers: [:], body: Data(), url: url)
    let tokens = TokenSequence()
    let transport = GraphQLHTTPTransport.make(
      url: url,
      inputs: TransportInputs(
        appVersion: "1.0",
        appBuild: "1",
        deviceUUID: "device-1",
        tokenSupplier: { tokens.next() }
      ),
      session: ScriptedURLProtocol.session(scriptId: Self.scriptId),
      signer: RequestSigner(secret: "secret"),
      clock: ServerClock()
    )

    for _ in 0..<2 {
      _ = try await transport.send(
        operationName: "Ping",
        documentHash: "abc123",
        document: nil,
        variablesJSON: Data("{}".utf8)
      )
    }

    let requests = ScriptedURLProtocol.recordedRequests(scriptId: Self.scriptId)
    #expect(tokens.reads == 2)
    #expect(requests.map { $0.value(forHTTPHeaderField: "authorization") } == ["Bearer token-1", "Bearer token-2"])
  }

  @Test
  func decryptsEncryptedResponse() async throws {
    ScriptedURLProtocol.reset(scriptId: Self.scriptId)

    let url = try #require(URL(string: "https://example.test/"))
    let plaintext = Data(#"{"data":{"ping":"pong"}}"#.utf8)
    let envelope = try PayloadCipher(secret: "secret").seal(plaintext)
    let encryptedBody = try JSONEncoder().encode(envelope)
    ScriptedURLProtocol.push(
      scriptId: Self.scriptId,
      status: 200,
      headers: ["x-payload-encryption": "1"],
      body: encryptedBody,
      url: url
    )
    let transport = makeTransport(url: url, token: "tok")

    let result = try await transport.send(
      operationName: "Ping",
      documentHash: "abc123",
      document: nil,
      variablesJSON: Data("{}".utf8)
    )

    #expect(result.body == plaintext)
  }

  @Test
  func authorizationFollowsOperationVisibility() async throws {
    ScriptedURLProtocol.reset(scriptId: Self.scriptId)

    let url = try #require(URL(string: "https://example.test/"))
    ScriptedURLProtocol.push(scriptId: Self.scriptId, status: 200, headers: [:], body: Data(), url: url)
    ScriptedURLProtocol.push(scriptId: Self.scriptId, status: 200, headers: [:], body: Data(), url: url)
    let transport = makeTransport(url: url, token: "tok")

    _ = try await transport.send(
      operationName: "myProfile",
      documentHash: "abc123",
      document: "query myProfile { me }",
      variablesJSON: Data("{}".utf8)
    )
    _ = try await transport.send(
      operationName: "AuthSignIn",
      documentHash: "abc123",
      document: "mutation AuthSignIn { authSignIn }",
      variablesJSON: Data("{}".utf8)
    )

    let requests = ScriptedURLProtocol.recordedRequests(scriptId: Self.scriptId)
    #expect(requests.map { $0.value(forHTTPHeaderField: "authorization") } == ["Bearer tok", nil])
  }

  @Test
  func privateOperationWithoutTokenThrows() async throws {
    ScriptedURLProtocol.reset(scriptId: Self.scriptId)

    let url = try #require(URL(string: "https://example.test/"))
    ScriptedURLProtocol.push(scriptId: Self.scriptId, status: 200, headers: [:], body: Data(), url: url)
    let transport = makeTransport(url: url, token: nil)

    await #expect(throws: GraphQLTransportError.unauthenticated) {
      _ = try await transport.send(
        operationName: "myProfile",
        documentHash: "abc123",
        document: "query myProfile { me }",
        variablesJSON: Data("{}".utf8)
      )
    }

    #expect(ScriptedURLProtocol.recordedRequests(scriptId: Self.scriptId).isEmpty)
  }
}

private extension GraphQLHTTPTransportTests {
  static let scriptId = "graphql-http-transport"

  func makeTransport(url: URL, token: String?, session: URLSession? = nil) -> GraphQLHTTPTransport {
    GraphQLHTTPTransport(
      url: url,
      session: session ?? ScriptedURLProtocol.session(scriptId: Self.scriptId),
      signer: RequestSigner(secret: "secret"),
      clock: ServerClock(),
      appVersion: "1.0",
      appBuild: "1",
      deviceUUID: "device-1",
      tokenSupplier: { token }
    )
  }

}

private final class TokenSequence: @unchecked Sendable {
  private let lock = NSLock()
  private var count = 0

  var reads: Int {
    lock.lock()
    defer { lock.unlock() }
    return count
  }

  func next() -> String? {
    lock.lock()
    defer { lock.unlock() }
    count += 1
    return "token-\(count)"
  }
}
