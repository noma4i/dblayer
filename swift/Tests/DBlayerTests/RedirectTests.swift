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
    let transport = UrlSessionTransport(session: session)
    switch path {
    case "query":
      #expect(responseStatus(await transport.send(request: probeRequest(origin, path: "/graphql"))) == 302)
    case "encrypted":
      #expect(responseStatus(await UrlSessionTransport(session: session).send(request: probeRequest(origin))) == 302)
    default:
      let file = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
      try Data("file".utf8).write(to: file)
      defer { try? FileManager.default.removeItem(at: file) }
      let reply = await transport.put(target: PutTarget(url: origin.appendingPathComponent("upload").absoluteString, headers: [:], connectMs: 10_000, requestMs: 30_000), file: PutFile(path: file.path, offset: 0, length: 4), identity: TransferIdentity(operationId: 1, itemId: "a", step: .put), progress: PutNoBytes(noHandle: .init()))
      #expect(responseStatus(reply) == 302)
    }
    #expect(server.requests.count == 1)
  }
}

struct PutPartTests {
  @Test
  func an_upload_sends_the_part_the_core_names() async throws {
    let server = try BootstrapListener(responseStatus: "200 OK\r\nETag: \"part\"")
    defer { server.listener.cancel() }
    let origin = try await server.start()
    let session = URLSession(configuration: .ephemeral)
    defer { session.invalidateAndCancel() }
    let file = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
    try Data("0123456789".utf8).write(to: file)
    defer { try? FileManager.default.removeItem(at: file) }
    let outcome = await UrlSessionTransport(session: session).put(target: PutTarget(url: origin.appendingPathComponent("part").absoluteString, headers: [:], connectMs: 10_000, requestMs: 30_000), file: PutFile(path: file.path, offset: 2, length: 4), identity: TransferIdentity(operationId: 1, itemId: "a", step: .part(number: 1)), progress: PutNoBytes(noHandle: .init()))
    #expect(outcome == .response(status: 200, etag: "\"part\"", body: "busy"))
    #expect(try #require(server.requests.first).hasSuffix("\r\n\r\n2345"))
  }
}
