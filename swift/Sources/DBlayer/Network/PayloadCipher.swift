import CryptoKit
import Foundation

public struct PayloadCipher: Sendable {
  public enum Failure: Error, Equatable, Sendable {
    case unsupportedVersion
    case malformedEnvelope
    case authenticationFailed
  }

  private let key: SymmetricKey

  public init(secret: String) {
    key = HKDF<SHA256>.deriveKey(
      inputKeyMaterial: SymmetricKey(data: Data(secret.utf8)),
      salt: Data(),
      info: Data("payload-enc-v1".utf8),
      outputByteCount: 32
    )
  }

  public func seal(_ plaintext: Data, iv initializationVector: Data) throws -> PayloadEnvelope {
    guard initializationVector.count == 12 else {
      throw Failure.malformedEnvelope
    }

    do {
      let nonce = try AES.GCM.Nonce(data: initializationVector)
      let sealedBox = try AES.GCM.seal(plaintext, using: key, nonce: nonce)
      return PayloadEnvelope(
        initializationVector: initializationVector,
        ciphertext: sealedBox.ciphertext,
        tag: sealedBox.tag
      )
    } catch {
      throw Failure.malformedEnvelope
    }
  }

  public func seal(_ plaintext: Data) throws -> PayloadEnvelope {
    let initializationVector = Data((0 ..< 12).map { _ in UInt8.random(in: UInt8.min ... UInt8.max) })
    return try seal(plaintext, iv: initializationVector)
  }

  public func open(_ envelope: PayloadEnvelope) throws -> Data {
    guard envelope.envelopeVersion == PayloadEnvelope.version else {
      throw Failure.unsupportedVersion
    }
    guard
      let initializationVector = envelope.ivBytes,
      initializationVector.count == 12,
      let ciphertext = envelope.ciphertextBytes,
      let tag = envelope.tagBytes,
      tag.count == 16
    else {
      throw Failure.malformedEnvelope
    }

    do {
      let nonce = try AES.GCM.Nonce(data: initializationVector)
      let sealedBox = try AES.GCM.SealedBox(nonce: nonce, ciphertext: ciphertext, tag: tag)
      return try AES.GCM.open(sealedBox, using: key)
    } catch {
      throw Failure.authenticationFailed
    }
  }
}
