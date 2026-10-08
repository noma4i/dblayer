import Foundation

public struct PayloadEnvelope: Codable, Equatable, Sendable {
  public let envelopeVersion: Int
  public let initializationVector: String
  public let ciphertext: String
  public let tag: String

  public static let version = 1

  private enum CodingKeys: String, CodingKey {
    case envelopeVersion = "v"
    case initializationVector = "iv"
    case ciphertext = "ct"
    case tag
  }

  public init(envelopeVersion: Int = 1, initializationVector: String, ciphertext: String, tag: String) {
    self.envelopeVersion = envelopeVersion
    self.initializationVector = initializationVector
    self.ciphertext = ciphertext
    self.tag = tag
  }

  public init(initializationVector: Data, ciphertext: Data, tag: Data) {
    self.init(
      initializationVector: Self.hexString(for: initializationVector),
      ciphertext: Self.hexString(for: ciphertext),
      tag: Self.hexString(for: tag)
    )
  }

  public var ivBytes: Data? {
    Self.decodeHex(initializationVector)
  }

  public var ciphertextBytes: Data? {
    Self.decodeHex(ciphertext)
  }

  public var tagBytes: Data? {
    Self.decodeHex(tag)
  }

  private static func hexString(for data: Data) -> String {
    data.map { String(format: "%02x", $0) }.joined()
  }

  private static func decodeHex(_ value: String) -> Data? {
    let bytes = Array(value.utf8)
    guard bytes.count.isMultiple(of: 2) else {
      return nil
    }

    var decoded = [UInt8]()
    decoded.reserveCapacity(bytes.count / 2)

    for index in stride(from: 0, to: bytes.count, by: 2) {
      guard let high = hexValue(bytes[index]), let low = hexValue(bytes[index + 1]) else {
        return nil
      }
      decoded.append((high << 4) | low)
    }

    return Data(decoded)
  }

  private static func hexValue(_ byte: UInt8) -> UInt8? {
    switch byte {
    case 48 ... 57:
      byte - 48
    case 97 ... 102:
      byte - 87
    default:
      nil
    }
  }
}

public enum PayloadTransportError: Error, Equatable, Sendable {
  case responseDecryptionFailed(status: Int)
  case responseEncryptionInvalid(status: Int)

  public var code: String {
    switch self {
    case .responseDecryptionFailed:
      "PAYLOAD_RESPONSE_DECRYPTION_FAILED"
    case .responseEncryptionInvalid:
      "PAYLOAD_RESPONSE_ENCRYPTION_INVALID"
    }
  }

  public var message: String {
    switch self {
    case let .responseDecryptionFailed(status), let .responseEncryptionInvalid(status):
      "\(code) (HTTP \(status))"
    }
  }
}
