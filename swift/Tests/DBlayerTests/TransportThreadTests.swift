import Foundation
import Testing
@testable import DBlayer

@Suite(.serialized)
struct TransportThreadTests {
  @Test @MainActor
  func graphQLTransportLoadsOffTheCallerThread() async throws {
    let url = try #require(URL(string: "https://api.yupi.social/"))
    let session = ThreadProbeProtocol.session()
    defer { session.invalidateAndCancel() }
    ThreadProbeProtocol.reset()
    let transport = GraphQLHTTPTransport(
      url: url,
      session: session,
      signer: RequestSigner(secret: "secret"),
      clock: ServerClock(),
      appVersion: "1.0",
      appBuild: "1",
      deviceUUID: "device-1",
      tokenSupplier: { "tok" }
    )

    _ = try await transport.send(operationName: "Ping", documentHash: "abc123", document: nil, variablesJSON: Data("{}".utf8))

    #expect(ThreadProbeProtocol.loadedOnMain == [false])
  }

  @Test @MainActor
  func urlSessionTransportLoadsOffTheCallerThread() async throws {
    let url = try #require(URL(string: "https://api.yupi.social/"))
    let session = ThreadProbeProtocol.session()
    defer { session.invalidateAndCancel() }
    ThreadProbeProtocol.reset()
    let transport = UrlSessionTransport(endpoint: url, headers: { [:] }, session: session)

    _ = try await transport.query(request: TransportRequest(document: "query Ping { ping }", operationName: "Ping", variablesJson: "{}"))

    #expect(ThreadProbeProtocol.loadedOnMain == [false])
  }
}

private final class ThreadProbeProtocol: URLProtocol, @unchecked Sendable {
  private static let lock = NSLock()
  private nonisolated(unsafe) static var main: [Bool] = []

  static var loadedOnMain: [Bool] {
    lock.lock()
    defer { lock.unlock() }
    return main
  }

  static func reset() {
    lock.lock()
    defer { lock.unlock() }
    main = []
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
    Self.main.append(Thread.isMainThread)
    Self.lock.unlock()
    let body = Data(#"{"data":{"ping":"pong"}}"#.utf8)
    guard let url = request.url, let response = HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: [:]) else { return }
    client?.urlProtocol(self, didReceive: response, cacheStoragePolicy: .notAllowed)
    client?.urlProtocol(self, didLoad: body)
    client?.urlProtocolDidFinishLoading(self)
  }

  override func stopLoading() {}
}
