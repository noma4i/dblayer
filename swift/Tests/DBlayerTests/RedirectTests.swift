import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

private final class PutNoBytes: ByteProgress, @unchecked Sendable {
  override func isCancelled() -> Bool { false }
  override func sent(bytes: UInt64) {}
}

struct RedirectTests {
  @Test(arguments: ["query", "encrypted", "put"])
  func life_prop_no_redirect(path: String) async throws {
    let server = try BootstrapListener(responseStatus: "302 Found\r\nLocation: /redirect")
    defer { server.listener.cancel() }
    let origin = try await server.start()
    let session = URLSession(configuration: .ephemeral)
    defer { session.invalidateAndCancel() }
    let transport = UrlSessionTransport(endpoint: origin, session: session)
    switch path {
    case "query":
      do {
        _ = try await transport.query(request: TransportRequest(document: "query Probe { id }", operationName: "Probe", variablesJson: "{}", purpose: .standard))
        Issue.record("a redirect answer returned")
      } catch let DbError.Http(status, _) {
        #expect(status == 302)
      }
    case "encrypted":
      let reply = try await UrlSessionEncryptedHttp(origin: origin, session: session).post(path: "/api/session", body: "jwe", purpose: .standard)
      #expect(reply.status == 302)
    default:
      let file = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
      try Data("file".utf8).write(to: file)
      defer { try? FileManager.default.removeItem(at: file) }
      let reply = try await transport.put(url: origin.appendingPathComponent("upload").absoluteString, headers: [:], file: PutFile(path: file.path, offset: 0, length: 4), identity: TransferIdentity(operationId: 1, itemId: "a", step: .put), progress: PutNoBytes(noHandle: .init()))
      #expect(reply.status == 302)
    }
    #expect(server.requests.count == 1)
  }
}
