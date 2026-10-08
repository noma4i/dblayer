import CryptoKit
import Foundation

public struct SignedHeaders: Sendable, Equatable {
  public let version: String
  public let signature: String
  public let timestamp: String
  public let nonce: String

  public init(version: String, signature: String, timestamp: String, nonce: String) {
    self.version = version
    self.signature = signature
    self.timestamp = timestamp
    self.nonce = nonce
  }
}

public struct SignatureInput: Sendable, Equatable {
  public let operationName: String
  public let variablesJSON: Data
  public let identity: String
  public let deviceUUID: String
  public let now: Date
  public let nonce: String

  public init(
    operationName: String,
    variablesJSON: Data,
    identity: String,
    deviceUUID: String,
    now: Date,
    nonce: String
  ) {
    self.operationName = operationName
    self.variablesJSON = variablesJSON
    self.identity = identity
    self.deviceUUID = deviceUUID
    self.now = now
    self.nonce = nonce
  }
}

public struct RequestSigner: Sendable {
  private let secret: String

  public init(secret: String) {
    self.secret = secret
  }

  public func payloadCipher() -> PayloadCipher {
    PayloadCipher(secret: secret)
  }

  public static let version = "2"

  public static func makeNonce() -> String {
    let bytes = (0 ..< 16).map { _ in UInt8.random(in: 0 ... 255) }
    return bytes.map { String(format: "%02x", $0) }.joined()
  }

  public func sign(_ input: SignatureInput) -> SignedHeaders {
    let timestamp = Int(input.now.timeIntervalSince1970.rounded(.down))
    let variablesHash = SHA256.hash(data: input.variablesJSON)
      .map { String(format: "%02x", $0) }
      .joined()
    let canonicalString = [
      Self.version,
      input.operationName,
      variablesHash,
      String(timestamp),
      input.nonce,
      input.deviceUUID
    ].joined(separator: "|")
    let signedData = "\(input.identity)|\(canonicalString)"
    let signature = HMAC<SHA256>.authenticationCode(
      for: Data(signedData.utf8),
      using: SymmetricKey(data: Data(secret.utf8))
    )
    .map { String(format: "%02x", $0) }
    .joined()

    return SignedHeaders(
      version: Self.version,
      signature: signature,
      timestamp: String(timestamp),
      nonce: input.nonce
    )
  }
}
