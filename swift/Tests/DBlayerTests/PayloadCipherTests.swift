import Foundation
import Testing
@testable import DBlayer

struct PayloadCipherTests {
  @Test
  func sealWithFixedIVMatchesServerVector() throws {
    let cipher = PayloadCipher(secret: "test-secret")
    let envelope = try cipher.seal(
      Data(#"{"a":1}"#.utf8),
      iv: Data([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11])
    )

    #expect(envelope.ivBytes == Data([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]))
    #expect(envelope.ciphertextBytes == Data([165, 245, 199, 42, 56, 247, 88]))
    #expect(envelope.tagBytes == Data([125, 233, 161, 19, 223, 128, 121, 220, 1, 49, 120, 222, 65, 50, 151, 167]))
  }

  @Test
  func openReturnsPlaintextAndRejectsTamperedOrUnsupportedEnvelopes() throws {
    let cipher = PayloadCipher(secret: "test-secret")
    let vector = Data([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11])
    let ciphertext = Data([165, 245, 199, 42, 56, 247, 88])
    let tag = Data([125, 233, 161, 19, 223, 128, 121, 220, 1, 49, 120, 222, 65, 50, 151, 167])
    let changedTag = Data([125, 233, 161, 19, 223, 128, 121, 220, 1, 49, 120, 222, 65, 50, 151, 168])
    let unsupported = PayloadEnvelope(
      envelopeVersion: 2,
      initializationVector: "000102030405060708090a0b",
      ciphertext: "a5f5c72a38f758",
      tag: "7de9a113df8079dc013178de413297a7"
    )

    #expect(
      try cipher.open(PayloadEnvelope(initializationVector: vector, ciphertext: ciphertext, tag: tag))
        == Data(#"{"a":1}"#.utf8)
    )
    #expect(throws: PayloadCipher.Failure.authenticationFailed) {
      try cipher.open(PayloadEnvelope(initializationVector: vector, ciphertext: ciphertext, tag: changedTag))
    }
    #expect(throws: PayloadCipher.Failure.unsupportedVersion) {
      try cipher.open(unsupported)
    }
  }
}
