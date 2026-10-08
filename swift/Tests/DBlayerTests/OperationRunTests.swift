import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

@Suite(.serialized)
struct OperationRunTests {
  @Test(arguments: [
    (#"{"data":{"authSignIn":{"userId":42}}}"#, 0),
    (#"{"data":{"authSignIn":{"userId":42}},"errors":[]}"#, 0),
    (#"{"data":{"authSignIn":null},"errors":[{"message":"Rejected secret-message"}]}"#, 1)
  ])
  func returnsMutationBodyAndReportsGraphQLErrorsWithData(body: String, failureCount: Int) async throws {
    let scriptId = "operation-run-success-\(failureCount)-\(body.count)"
    ScriptedURLProtocol.reset(scriptId: scriptId)
    defer { ScriptedURLProtocol.reset(scriptId: scriptId) }
    let expectedBody = Data(body.utf8)

    let (transport, url) = try makeScriptedTransport(scriptId: scriptId)
    ScriptedURLProtocol.push(scriptId: scriptId, status: 200, headers: [:], body: expectedBody, url: url)
    let failures = FailureCollector()
    let run = OperationRun(transport: transport, hashes: PersistedQueryHashes(), onFailure: failures.record)
    let result = try await run.send(
      operationName: "AuthSignIn",
      document: "mutation AuthSignIn { authSignIn { userId } }",
      variables: ["email": "secret-variable@example.com"],
      kind: .mutation
    )

    #expect(result == expectedBody)
    #expect(failures.all.count == failureCount)
    #expect(failures.all.allSatisfy { $0 == RequestFailure(operationName: "AuthSignIn", status: 200, errorClass: "graphQL") })
  }

  @Test(arguments: [
    (401, "", GraphQLTransportError.unauthenticated, "unauthenticated"),
    (500, "", GraphQLTransportError.server(status: 500), "server"),
    (200, #"{"errors":[{"message":"Rejected"}]}"#, GraphQLTransportError.graphQL(messages: ["Rejected"]), "graphQL")
  ])
  func rejectsFailedResponseAndReportsOneFailure(
    status: Int,
    body: String,
    error: GraphQLTransportError,
    errorClass: String
  ) async throws {
    let scriptId = "operation-run-reject-\(status)"
    ScriptedURLProtocol.reset(scriptId: scriptId)
    defer { ScriptedURLProtocol.reset(scriptId: scriptId) }

    let (transport, url) = try makeScriptedTransport(scriptId: scriptId)
    ScriptedURLProtocol.push(scriptId: scriptId, status: status, headers: [:], body: Data(body.utf8), url: url)
    let failures = FailureCollector()
    let run = OperationRun(transport: transport, hashes: PersistedQueryHashes(), onFailure: failures.record)

    await #expect(throws: error) {
      _ = try await run.send(
        operationName: "AuthSignIn",
        document: "mutation AuthSignIn { authSignIn { userId } }",
        variables: ["email": "secret-variable@example.com"],
        kind: .mutation
      )
    }

    #expect(failures.all == [RequestFailure(operationName: "AuthSignIn", status: status, errorClass: errorClass)])
    #expect(!String(describing: failures.all).contains("secret"))
  }

  @Test(arguments: [false, true])
  func persistedQueryNotFoundRetriesWithQuery(repeatedMiss: Bool) async throws {
    let scriptId = "operation-run-retry-success"
    ScriptedURLProtocol.reset(scriptId: scriptId)
    defer { ScriptedURLProtocol.reset(scriptId: scriptId) }

    let (transport, url) = try makeScriptedTransport(scriptId: scriptId)
    let missBody = Data(#"{"errors":[{"message":"PersistedQueryNotFound"}]}"#.utf8)
    ScriptedURLProtocol.push(scriptId: scriptId, status: 200, headers: [:], body: missBody, url: url)
    let expectedBody = repeatedMiss
      ? Data(#"{"errors":[{"message":"PersistedQueryNotFound"}]}"#.utf8)
      : Data(#"{"data":{"authSignIn":{"userId":42}}}"#.utf8)
    ScriptedURLProtocol.push(scriptId: scriptId, status: 200, headers: [:], body: expectedBody, url: url)
    let run = OperationRun(transport: transport, hashes: PersistedQueryHashes())

    let send = {
      try await run.send(
        operationName: "AuthSignIn",
        document: "mutation AuthSignIn { authSignIn { userId } }",
        variables: ["email": "user@example.com"],
        kind: .mutation
      )
    }
    if repeatedMiss {
      await #expect(throws: GraphQLTransportError.graphQL(messages: ["PersistedQueryNotFound"])) {
        _ = try await send()
      }
    } else {
      #expect(try await send() == expectedBody)
    }
    let requests = ScriptedURLProtocol.recordedRequests(scriptId: scriptId)
    #expect(requests.count == 2)
    let firstRequest = try #require(requests.first)
    let secondRequest = try #require(requests.dropFirst().first)
    let firstBody = try #require(String(data: ScriptedURLProtocol.body(of: firstRequest), encoding: .utf8))
    let secondBody = try #require(String(data: ScriptedURLProtocol.body(of: secondRequest), encoding: .utf8))
    #expect(!firstBody.contains(#""query""#))
    #expect(secondBody.contains(#""query""#))
  }

  @Test
  func signsCanonicalVariables() async throws {
    let scriptId = "operation-run-canonical-variables"
    ScriptedURLProtocol.reset(scriptId: scriptId)
    defer { ScriptedURLProtocol.reset(scriptId: scriptId) }

    let (transport, url) = try makeScriptedTransport(scriptId: scriptId)
    let expectedBody = Data(#"{"data":{"authSignIn":{"userId":42}}}"#.utf8)
    ScriptedURLProtocol.push(
      scriptId: scriptId,
      status: 200,
      headers: [:],
      body: expectedBody,
      url: url
    )
    let run = OperationRun(transport: transport, hashes: PersistedQueryHashes())

    _ = try await run.send(
      operationName: "AuthSignIn",
      document: "mutation AuthSignIn { authSignIn { userId } }",
      variables: UnsortedVariables(zeta: "2", alpha: "1"),
      kind: .mutation
    )

    let request = try #require(ScriptedURLProtocol.recordedRequests(scriptId: scriptId).first)
    let requestBody = try #require(String(data: ScriptedURLProtocol.body(of: request), encoding: .utf8))
    #expect(requestBody.contains(#""variables":{"alpha":"1","zeta":"2"}"#))

    let timestamp = try #require(request.value(forHTTPHeaderField: "X-Request-Timestamp"))
    let nonce = try #require(request.value(forHTTPHeaderField: "X-Request-Nonce"))
    let timestampValue = try #require(Int(timestamp))
    let expectedSignature = RequestSigner(secret: "secret").sign(
      SignatureInput(
        operationName: "AuthSignIn",
        variablesJSON: Data(#"{"alpha":"1","zeta":"2"}"#.utf8),
        identity: "device-1",
        deviceUUID: "device-1",
        now: Date(timeIntervalSince1970: TimeInterval(timestampValue)),
        nonce: nonce
      )
    )
    #expect(request.value(forHTTPHeaderField: "X-Request-Signature") == expectedSignature.signature)
  }
}

extension OperationRunTests {
  @Test(arguments: [
    ("AuthSignIn", "mutation AuthSignIn { authSignIn { userId } }", GraphQLOperationKind.mutation),
    ("MyProfile", "query MyProfile { myProfile { id } }", GraphQLOperationKind.query)
  ])
  func retriesExpiredSignature(name: String, document: String, kind: GraphQLOperationKind) async throws {
    let scriptId = "operation-run-signature-expired-\(name)"
    ScriptedURLProtocol.reset(scriptId: scriptId)
    defer { ScriptedURLProtocol.reset(scriptId: scriptId) }
    let expectedBody = Data(#"{"data":{"authSignIn":{"userId":42}}}"#.utf8)
    let run = try makeSignatureRun(scriptId: scriptId, code: "SIGNATURE_EXPIRED", responseBody: expectedBody)

    let body = try await run.send(
      operationName: name,
      document: document,
      variables: ["email": "user@example.com"],
      kind: kind
    )

    #expect(body == expectedBody)
    #expect(ScriptedURLProtocol.recordedRequests(scriptId: scriptId).count == 2)
  }

  @Test
  func rejectsReplaySignatureForMutation() async throws {
    let scriptId = "operation-run-signature-replay-mutation"
    ScriptedURLProtocol.reset(scriptId: scriptId)
    defer { ScriptedURLProtocol.reset(scriptId: scriptId) }
    let run = try makeSignatureRun(scriptId: scriptId, code: "SIGNATURE_REPLAY", responseBody: nil)

    await #expect(throws: GraphQLTransportError.graphQL(messages: ["Signature error"])) {
      _ = try await run.send(
        operationName: "AuthSignIn",
        document: "mutation AuthSignIn { authSignIn { userId } }",
        variables: ["email": "user@example.com"],
        kind: .mutation
      )
    }

    #expect(ScriptedURLProtocol.recordedRequests(scriptId: scriptId).count == 1)
  }
}

private final class FailureCollector: @unchecked Sendable {
  private let lock = NSLock()
  private var failures: [RequestFailure] = []

  var all: [RequestFailure] {
    lock.withLock { failures }
  }

  func record(_ failure: RequestFailure) {
    lock.withLock { failures.append(failure) }
  }
}

private struct UnsortedVariables: Encodable {
  let zeta: String
  let alpha: String
}

private extension OperationRunTests {
  func makeSignatureRun(
    scriptId: String,
    code: String,
    responseBody: Data?
  ) throws -> OperationRun {
    let (transport, url) = try makeScriptedTransport(scriptId: scriptId)
    let signatureBody = Data(#"{"errors":[{"message":"Signature error","extensions":{"code":"\#(code)"}}]}"#.utf8)
    ScriptedURLProtocol.push(scriptId: scriptId, status: 403, headers: [:], body: signatureBody, url: url)
    if let responseBody {
      ScriptedURLProtocol.push(scriptId: scriptId, status: 200, headers: [:], body: responseBody, url: url)
    }
    return OperationRun(transport: transport, hashes: PersistedQueryHashes())
  }
}

func makeScriptedTransport(scriptId: String) throws -> (GraphQLHTTPTransport, URL) {
  let graphqlURL = URL(string: "https://api.yupi.social/graphql")!
  let transport = GraphQLHTTPTransport.make(
    url: graphqlURL,
    inputs: TransportInputs(
      appVersion: "1.0",
      appBuild: "1",
      deviceUUID: "device-1",
      token: "token"
    ),
    session: ScriptedURLProtocol.session(scriptId: scriptId),
    signer: RequestSigner(secret: "secret"),
    clock: ServerClock()
  )
  return (transport, graphqlURL)
}
