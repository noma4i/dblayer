import Foundation
import Testing
@testable import DBlayer

@Suite(.serialized)
struct TransportHeadersTests {
  @Test(arguments: [nil, "", "tok"] as [String?])
  func containsEveryRequiredHeader(token: String?) {
    let headers = TransportHeaders.make(
      appVersion: "3.7.0",
      appBuild: "42",
      deviceUUID: "device-1",
      token: token,
      signed: makeSignedHeaders()
    )

    #expect(headers["X-API-Version"] == "2")
    #expect(headers["x-app-version"] == "3.7.0")
    #expect(headers["x-app-build"] == "42")
    #expect(headers["x-app-platform"] == "ios")
    #expect(headers["x-device-uuid"] == "device-1")
    #expect(headers["X-Payload-Encryption"] == "1")
    #expect(headers["X-Request-Signature-Version"] == "2")
    #expect(headers["X-Request-Signature"] == "sig")
    #expect(headers["X-Request-Timestamp"] == "1700000000")
    #expect(headers["X-Request-Nonce"] == "nonce123")
    let bearer = token.flatMap { $0.isEmpty ? nil : "Bearer \($0)" }
    #expect(headers["authorization"] == bearer)
    #expect(headers.count == (bearer == nil ? 10 : 11))
  }
}

private extension TransportHeadersTests {
  func makeSignedHeaders() -> SignedHeaders {
    SignedHeaders(version: "2", signature: "sig", timestamp: "1700000000", nonce: "nonce123")
  }
}
