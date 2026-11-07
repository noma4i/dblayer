import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

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
  @Test(arguments: ["encrypted", "graphql", "encryptedSocket", "put"])
  func life_prop_user_agent(path: String) async throws {
    let server = try BootstrapListener(responseStatus: "400 Bad Request")
    defer { server.listener.cancel() }
    let origin = try await server.start()
    let headers = ["uSeR-aGeNt": "Core/1 DBlayer/x", "X-Core": "yes"]
    let session = URLSession(configuration: .ephemeral)
    defer { session.invalidateAndCancel() }
    let transport = UrlSessionTransport(session: session)
    switch path {
    case "encrypted":
      _ = await UrlSessionTransport(session: session).send(request: probeRequest(origin, headers: headers))
    case "graphql":
      _ = await transport.send(request: probeRequest(origin, path: "/graphql", headers: headers))
    case "encryptedSocket":
      _ = await transport.openCable(url: cableURL(origin), headers: headers, connectMs: 10_000, events: RecordingEvents())
    default:
      let file = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
      try Data("file".utf8).write(to: file)
      defer { try? FileManager.default.removeItem(at: file) }
      _ = await transport.put(target: PutTarget(url: origin.appendingPathComponent("upload").absoluteString, headers: headers, connectMs: 10_000, requestMs: 30_000), file: PutFile(path: file.path, offset: 0, length: 4), identity: TransferIdentity(operationId: 1, itemId: "a", step: .put), progress: PutBytes(noHandle: .init()))
    }
    let request = try #require(server.requests.first)
    let fields = request.components(separatedBy: "\r\n").dropFirst().prefix(while: { !$0.isEmpty }).map { $0.lowercased() }
    #expect(fields.filter { $0.hasPrefix("user-agent:") } == ["user-agent: core/1 dblayer/x"])
    #expect(fields.contains("x-core: yes"))
  }

  @Test
  func life_prop_headers_per_request() {
    let values = ChangingHeaders()
    let http = UrlSessionTransport(headers: { values.read() })
    let transport = UrlSessionTransport(headers: { values.read() })
    #expect([http.appHeaders()["X-Application"], transport.appHeaders()["X-Application"]] == ["first", "first"])
    values.set("second")
    #expect([http.appHeaders()["X-Application"], transport.appHeaders()["X-Application"]] == ["second", "second"])
  }

  @Test
  func life_put_connection_failure_is_unreachable() async throws {
    let session = URLSession(configuration: .ephemeral)
    defer { session.invalidateAndCancel() }
    let transport = UrlSessionTransport(session: session)
    let path = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
    try Data("file".utf8).write(to: path)
    defer { try? FileManager.default.removeItem(at: path) }
    let outcome = await transport.put(target: PutTarget(url: "http://127.0.0.1:1/upload", headers: [:], connectMs: 10_000, requestMs: 30_000), file: PutFile(path: path.path, offset: 0, length: 4), identity: TransferIdentity(operationId: 1, itemId: "a", step: .put), progress: PutBytes(noHandle: .init()))
    let failure = try #require(rawFailure(outcome))
    #expect((failure.domain, failure.code) == (NSURLErrorDomain, Int64(URLError.cannotConnectToHost.rawValue)))
  }
}
