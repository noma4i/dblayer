import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

@Suite(.serialized)
struct TransportInputsTests {
  @Test
  func makeSendsToConfiguredGraphQLURL() async throws {
    let scriptId = "transport-inputs"
    ScriptedURLProtocol.reset(scriptId: scriptId)
    defer { ScriptedURLProtocol.reset(scriptId: scriptId) }

    let graphqlURL = URL(string: "https://api.yupi.social/graphql")!
    ScriptedURLProtocol.push(
      scriptId: scriptId,
      status: 200,
      headers: [:],
      body: Data(#"{"data":{"ping":"pong"}}"#.utf8),
      url: graphqlURL
    )

    let transport = GraphQLHTTPTransport.make(
      url: graphqlURL,
      inputs: TransportInputs(appVersion: "1.0", appBuild: "1", deviceUUID: "device-1", token: "token"),
      session: ScriptedURLProtocol.session(scriptId: scriptId),
      signer: RequestSigner(secret: "secret"),
      clock: ServerClock()
    )

    _ = try await transport.send(
      operationName: "Ping",
      documentHash: "abc123",
      document: "query Ping { ping }",
      variablesJSON: Data("{}".utf8)
    )

    let request = try #require(ScriptedURLProtocol.recordedRequests(scriptId: scriptId).first)
    #expect(request.url == graphqlURL)
  }
}
