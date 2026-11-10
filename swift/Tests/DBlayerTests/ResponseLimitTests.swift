import Foundation
import Testing
@testable import DBlayer

private final class LimitProtocol: URLProtocol, @unchecked Sendable {
  override class func canInit(with request: URLRequest) -> Bool { true }
  override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }
  override func startLoading() {
    let url = request.url!
    let parts = url.lastPathComponent.split(separator: "-")
    let length = Int(parts[0])!
    let headers = parts[1] == "length" ? ["Content-Length": String(length)] : [:]
    let response = HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: headers)!
    client?.urlProtocol(self, didReceive: response, cacheStoragePolicy: .notAllowed)
    for _ in 0..<length { client?.urlProtocol(self, didLoad: Data([97])) }
    client?.urlProtocolDidFinishLoading(self)
  }
  override func stopLoading() {}
}

struct ResponseLimitTests {
  @Test
  func boundary_http_response_limit_c9() async throws {
    for length in [16, 17] {
      for mode in ["length", "chunked"] {
        let config = URLSessionConfiguration.ephemeral
        config.protocolClasses = [LimitProtocol.self]
        let session = URLSession(configuration: config)
        defer { session.invalidateAndCancel() }
        let url = "https://example.test/\(UUID().uuidString)/\(length)-\(mode)"
        let request = HttpRequest(author: .session, requestId: 0, attempt: 1, url: url, method: "POST", headers: [:], body: "{}", connectMs: 10_000, requestMs: 30_000, purpose: .standard, responseLimit: 16)
        let outcome = await UrlSessionTransport(session: session).send(request: request)
        if length == 16 { #expect(outcome == .response(status: 200, etag: nil, body: String(repeating: "a", count: 16))) }
        else {
          let failure = try #require(rawFailure(outcome))
          #expect((failure.domain, failure.code) == ("DBlayer.ResponseLimit", 1))
        }
      }
    }
  }
}
