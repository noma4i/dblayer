import DBlayer
import Foundation
import Testing
@testable import DBlayer

@Suite(.serialized)
struct PayloadEnvelopeTests {
  @Test
  func encodesBytesAsLowercaseHexAndRejectsBrokenHex() throws {
    let envelope = PayloadEnvelope(
      initializationVector: Data((1 ... 12).map(UInt8.init)),
      ciphertext: Data([52, 239]),
      tag: Data(repeating: 204, count: 16)
    )

    #expect(envelope.envelopeVersion == 1)
    #expect(envelope.initializationVector == "0102030405060708090a0b0c")
    #expect(envelope.ciphertext == "34ef")
    #expect(envelope.tag == String(repeating: "cc", count: 16))
    #expect(envelope.ivBytes == Data((1 ... 12).map(UInt8.init)))
    #expect(envelope.ciphertextBytes == Data([52, 239]))
    #expect(envelope.tagBytes == Data(repeating: 204, count: 16))

    let encoded = try JSONEncoder().encode(envelope)
    let json = try JSONDecoder().decode([String: JSONValue].self, from: encoded)
    #expect(Set(json.keys) == Set(["v", "iv", "ct", "tag"]))

    let oddLength = PayloadEnvelope(initializationVector: "", ciphertext: "abc", tag: "")
    let invalidCharacter = PayloadEnvelope(initializationVector: "", ciphertext: "zz", tag: "")

    #expect(oddLength.ciphertextBytes == nil)
    #expect(invalidCharacter.ciphertextBytes == nil)
  }

  @Test
  func errorMessagesMatchV2() {
    let decryptionFailed = PayloadTransportError.responseDecryptionFailed(status: 403)
    let encryptionInvalid = PayloadTransportError.responseEncryptionInvalid(status: 403)

    #expect(decryptionFailed.code == "PAYLOAD_RESPONSE_DECRYPTION_FAILED")
    #expect(encryptionInvalid.code == "PAYLOAD_RESPONSE_ENCRYPTION_INVALID")
    #expect(decryptionFailed.message == "PAYLOAD_RESPONSE_DECRYPTION_FAILED (HTTP 403)")
    #expect(encryptionInvalid.message == "PAYLOAD_RESPONSE_ENCRYPTION_INVALID (HTTP 403)")
  }
}
