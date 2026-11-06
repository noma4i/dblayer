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

private final class ChangingHeaders: @unchecked Sendable {
  private let lock = NSLock()
  private var current = "first"
  func set(_ value: String) { lock.withLock { current = value } }
  func read() -> [String: String] { lock.withLock { ["X-Application": current, "User-Agent": "Reader"] } }
}

private final class PutBytes: ByteProgress, @unchecked Sendable {
  override func isCancelled() -> Bool { false }
  override func sent(bytes: UInt64) {}
}

struct UserAgentTests {
  @Test(arguments: ["encrypted", "graphql", "encryptedSocket", "graphqlSocket"], [nil, "", "Reader"] as [String?])
  func life_prop_user_agent(path: String, application: String?) async throws {
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
      _ = try await UrlSessionEncryptedHttp(origin: origin, headers: { headers }, session: session).post(path: "/api/session", body: "jwe", purpose: .standard)
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
    #expect(agents == [userAgent(application: application)])
  }

  @Test
  func life_prop_headers_per_request() async throws {
    let server = try BootstrapListener(responseStatus: "400 Bad Request")
    defer { server.listener.cancel() }
    let origin = try await server.start()
    let values = ChangingHeaders()
    let http = UrlSessionEncryptedHttp(origin: origin, headers: { values.read() })
    let transport = UrlSessionTransport(endpoint: origin, headers: { values.read() })
    _ = try await http.post(path: "/api/session", body: "jwe", purpose: .standard)
    values.set("second")
    _ = try await http.post(path: "/api/session", body: "jwe", purpose: .standard)
    _ = try? await transport.openCable(lease: RecordingLease(noHandle: .init()), url: cableURL(origin))
    let path = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
    try Data("file".utf8).write(to: path)
    defer { try? FileManager.default.removeItem(at: path) }
    _ = try await transport.put(url: origin.appendingPathComponent("upload").absoluteString, headers: ["X-Presigned": "yes"], file: PutFile(path: path.path, offset: 0, length: 4), identity: TransferIdentity(operationId: 1, itemId: "a", step: .put), progress: PutBytes(noHandle: .init()))
    let requests = server.requests.map { $0.lowercased() }
    #expect(requests.count == 4)
    #expect(requests[0].contains("x-application: first"))
    #expect(requests[1].contains("x-application: second"))
    #expect(requests[2].contains("x-application: second"))
    #expect(requests[3].contains("x-presigned: yes"))
    #expect(!requests[3].contains("x-application:"))
  }
}
