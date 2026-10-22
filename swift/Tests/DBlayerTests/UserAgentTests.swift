import Foundation
import Testing
@testable import DBlayer

private final class AgentProgress: ByteProgress, @unchecked Sendable {
  override func sent(bytes: UInt64) {}
  override func isCancelled() -> Bool { false }
}

private final class AgentSink: SubscriptionSink, @unchecked Sendable {
  override func ready() {}
  override func failed(error: DbError) {}
  override func closed() {}
}

struct UserAgentTests {
  @Test(arguments: ["encrypted", "graphql", "put", "encryptedSocket", "graphqlSocket"], [nil, "Example/3"] as [String?])
  func everyRequestIdentifiesTheLibrary(path: String, application: String?) async throws {
    let server = try BootstrapListener(responseStatus: "400 Bad Request")
    defer { server.listener.cancel() }
    let origin = try await server.start()
    let headers = application.map { ["uSeR-aGeNt": $0] } ?? [:]
    let session = URLSession(configuration: .ephemeral)
    defer { session.invalidateAndCancel() }
    let transport = UrlSessionTransport(endpoint: origin, headers: { headers }, session: session, cablePath: "/api/cable")
    let operation = TransportRequest(document: "query Probe { id }", operationName: "Probe", variablesJson: "{}")
    switch path {
    case "encrypted":
      _ = try await UrlSessionEncryptedHttp(origin: origin, session: session).post(path: "/api/session", body: "jwe")
    case "graphql":
      _ = try? await transport.query(request: operation)
    case "put":
      _ = try await transport.put(url: origin.absoluteString, headers: headers, bytes: Data([1, 2]), progress: AgentProgress(noHandle: .init()))
    case "encryptedSocket":
      _ = try? await transport.openCable()
    case "graphqlSocket":
      let token = try? await transport.subscribe(request: operation, sink: AgentSink(noHandle: .init()))
      await eventually("GraphQL handshake received") { !server.requests.isEmpty }
      if let token { transport.unsubscribe(token: token) }
    default: break
    }
    let request = try #require(server.requests.first)
    let agents = request.components(separatedBy: "\r\n").dropFirst().prefix(while: { !$0.isEmpty }).filter { $0.lowercased().hasPrefix("user-agent:") }.map { $0.split(separator: ":", maxSplits: 1, omittingEmptySubsequences: false)[1].trimmingCharacters(in: .whitespaces) }
    let supportsHeaders = ["graphql", "put", "graphqlSocket", "encryptedSocket"].contains(path)
    #expect(agents == [userAgent(application: supportsHeaders ? application : nil)])
  }


}
