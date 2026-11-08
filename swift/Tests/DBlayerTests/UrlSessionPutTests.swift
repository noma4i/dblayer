import Foundation
import Testing
@testable import DBlayer

private final class AnswerProtocol: URLProtocol, @unchecked Sendable {
  nonisolated(unsafe) static var body = Data()
  override class func canInit(with request: URLRequest) -> Bool { true }
  override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }
  override func startLoading() {
    let response = HTTPURLResponse(url: request.url!, statusCode: 200, httpVersion: nil, headerFields: nil)!
    client?.urlProtocol(self, didReceive: response, cacheStoragePolicy: .notAllowed)
    client?.urlProtocol(self, didLoad: Self.body)
    client?.urlProtocolDidFinishLoading(self)
  }
  override func stopLoading() {}
}

private final class NoProgress: ByteProgress, @unchecked Sendable {
  init() { super.init(noHandle: .init()) }
  required init(unsafeFromHandle handle: UInt64) { fatalError("not a core object") }
  override func sent(bytes: UInt64) {}
  override func isCancelled() -> Bool { false }
}

struct UrlSessionPutTests {
  /// bridge_prop_raw_io (P-BR-08): the body of the PUT answer reaches the core as it is, the empty one too.
  @Test(arguments: ["stored", ""])
  func thePutAnswerBodyReachesTheCore(body: String) async throws {
    let configuration = URLSessionConfiguration.ephemeral
    configuration.protocolClasses = [AnswerProtocol.self]
    AnswerProtocol.body = Data(body.utf8)
    let file = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
    try Data("0123".utf8).write(to: file)
    defer { try? FileManager.default.removeItem(at: file) }
    let transport = UrlSessionTransport(session: URLSession(configuration: configuration))
    let outcome = await transport.put(target: PutTarget(url: "https://example.com/part", headers: [:], connectMs: 10_000, requestMs: 30_000), file: PutFile(path: file.path, offset: 0, length: 4), identity: TransferIdentity(operationId: 1, itemId: "i", step: .put), progress: NoProgress())
    #expect(outcome == .response(status: 200, etag: nil, body: body))
  }
}
