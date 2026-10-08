import Foundation
import Testing
@testable import DBlayer

@Suite(.serialized)
struct RequestSignerTests {
  @Test
  func signatureMatchesServerVector() {
    let signer = RequestSigner(secret: "test_secret")
    let headers = signer.sign(
      SignatureInput(
        operationName: "TestOperation",
        variablesJSON: Data("{}".utf8),
        identity: "test_bearer_token",
        deviceUUID: "9B3D2C1A-0000-4000-8000-000000000001",
        now: Date(timeIntervalSince1970: 1_700_000_000),
        nonce: "00112233445566778899aabbccddeeff"
      )
    )

    #expect(headers.signature == "2652e11a3ed0441719e4c732124e5871e8bfa668699c9392e7ec6e6531213452")
    #expect(headers.timestamp == "1700000000")
    #expect(headers.version == "2")
    #expect(headers.nonce == "00112233445566778899aabbccddeeff")
  }

  @Test
  func variablesAndNonceChangeSignature() {
    let base = Self.sign(variables: "{}", nonce: "00112233445566778899aabbccddeeff")

    #expect(base.signature != Self.sign(variables: "{\"a\":1}", nonce: base.nonce).signature)
    #expect(base.signature != Self.sign(variables: "{}", nonce: "ffeeddccbbaa99887766554433221100").signature)
  }

  @Test
  func nonceHasSixteenRandomBytes() {
    let first = RequestSigner.makeNonce()
    let second = RequestSigner.makeNonce()

    #expect(first.count == 32)
    #expect(first.allSatisfy { $0.isHexDigit && !$0.isUppercase })
    #expect(first != second)
  }

  @Test
  func payloadCipherUsesSignerSecret() throws {
    let envelope = try RequestSigner(secret: "test-secret").payloadCipher().seal(
      Data(#"{"a":1}"#.utf8),
      iv: Data([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11])
    )

    #expect(envelope.ciphertextBytes == Data([165, 245, 199, 42, 56, 247, 88]))
    #expect(envelope.tagBytes == Data([125, 233, 161, 19, 223, 128, 121, 220, 1, 49, 120, 222, 65, 50, 151, 167]))
  }

  private static func sign(variables: String, nonce: String) -> SignedHeaders {
    RequestSigner(secret: "test_secret").sign(
      SignatureInput(
        operationName: "TestOperation",
        variablesJSON: Data(variables.utf8),
        identity: "test_bearer_token",
        deviceUUID: "9B3D2C1A-0000-4000-8000-000000000001",
        now: Date(timeIntervalSince1970: 1_700_000_000),
        nonce: nonce
      )
    )
  }
}
