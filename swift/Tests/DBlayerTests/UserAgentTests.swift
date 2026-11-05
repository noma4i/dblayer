import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

private final class AgentSink: SubscriptionSink, @unchecked Sendable {
  private let lock = NSLock()
  private var opened: String?
  var token: String? { lock.withLock { opened } }
  override func ready() {}
  override func failed(error: DbError) {}
  override func closed() {}
  override func opened(token: String) throws { lock.withLock { opened = token } }
}

struct UserAgentTests {
  @Test(arguments: ["encrypted", "graphql", "encryptedSocket", "graphqlSocket"], [nil, "Example/3"] as [String?])
  func everyRequestIdentifiesTheLibrary(path: String, application: String?) async throws {
    let server = try BootstrapListener(responseStatus: "400 Bad Request")
    defer { server.listener.cancel() }
    let origin = try await server.start()
    let headers = application.map { ["uSeR-aGeNt": $0] } ?? [:]
    let session = URLSession(configuration: .ephemeral)
    defer { session.invalidateAndCancel() }
    let transport = UrlSessionTransport(endpoint: origin, headers: { headers }, session: session)
    let operation = TransportRequest(document: "query Probe { id }", operationName: "Probe", variablesJson: "{}", purpose: .standard)
    switch path {
    case "encrypted":
      _ = try await UrlSessionEncryptedHttp(origin: origin, session: session).post(path: "/api/session", body: "jwe", purpose: .standard)
    case "graphql":
      _ = try? await transport.query(request: operation)
    case "encryptedSocket":
      _ = try? await transport.openCable(lease: RecordingLease(noHandle: .init()), url: cableURL(origin))
    case "graphqlSocket":
      let sink = AgentSink(noHandle: .init())
      _ = try? await transport.subscribe(request: operation, sink: sink)
      await eventually("GraphQL handshake received") { !server.requests.isEmpty }
      if let token = sink.token { transport.unsubscribe(token: token) }
    default: break
    }
    let request = try #require(server.requests.first)
    let agents = request.components(separatedBy: "\r\n").dropFirst().prefix(while: { !$0.isEmpty }).filter { $0.lowercased().hasPrefix("user-agent:") }.map { $0.split(separator: ":", maxSplits: 1, omittingEmptySubsequences: false)[1].trimmingCharacters(in: .whitespaces) }
    let supportsHeaders = ["graphql", "graphqlSocket", "encryptedSocket"].contains(path)
    #expect(agents == [userAgent(application: supportsHeaders ? application : nil)])
  }
}
