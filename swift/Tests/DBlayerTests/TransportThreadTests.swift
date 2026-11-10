import Foundation
import Testing
@testable import DBlayer

@Suite
struct TransportThreadTests {
  @Test(arguments: [false, true]) @MainActor
  func urlSessionTransportLoadsOffTheCallerThread(mutation: Bool) async throws {
    let url = try #require(URL(string: "https://example.test/\(UUID().uuidString)"))
    let session = ThreadProbeProtocol.session()
    defer { session.invalidateAndCancel() }
    let transport = UrlSessionTransport(headers: { [:] }, session: session)

    let body = #"{"query":"\#(mutation ? "mutation" : "query") Ping { ping }","operationName":"Ping","variables":{}}"#
    let request = HttpRequest(author: .session, requestId: 0, attempt: 1, url: url.absoluteString, method: "POST", headers: ["Content-Type": "application/json"], body: body, connectMs: 10_000, requestMs: 30_000, purpose: .standard, responseLimit: 16 * 1024 * 1024)
    let outcome = await transport.send(request: request)

    #expect(outcome == .response(status: 200, etag: nil, body: #"{"data":{"ping":"pong"}}"#))
    #expect(ThreadProbeProtocol.loadedOnMain(url: url) == [false])
  }
}

private final class ThreadProbeProtocol: URLProtocol, @unchecked Sendable {
  private static let lock = NSLock()
  private nonisolated(unsafe) static var main: [URL: [Bool]] = [:]

  static func loadedOnMain(url: URL) -> [Bool] {
    lock.lock()
    defer { lock.unlock() }
    return main.removeValue(forKey: url) ?? []
  }

  static func session() -> URLSession {
    let configuration = URLSessionConfiguration.ephemeral
    configuration.protocolClasses = [ThreadProbeProtocol.self]
    return URLSession(configuration: configuration)
  }

  override class func canInit(with request: URLRequest) -> Bool { true }
  override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }

  override func startLoading() {
    Self.lock.lock()
    if let url = request.url { Self.main[url, default: []].append(Thread.isMainThread) }
    Self.lock.unlock()
    let body = Data(#"{"data":{"ping":"pong"}}"#.utf8)
    guard let url = request.url, let response = HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: [:]) else { return }
    client?.urlProtocol(self, didReceive: response, cacheStoragePolicy: .notAllowed)
    client?.urlProtocol(self, didLoad: body)
    client?.urlProtocolDidFinishLoading(self)
  }

  override func stopLoading() {}
}
