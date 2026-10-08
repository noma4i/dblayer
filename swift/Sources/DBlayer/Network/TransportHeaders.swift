import Foundation

public enum TransportHeaders {
  public static let apiVersion = "2"
  public static let platform = "ios"

  public static func make(
    appVersion: String,
    appBuild: String,
    deviceUUID: String,
    token: String?,
    signed: SignedHeaders
  ) -> [String: String] {
    var headers: [String: String] = [
      "X-API-Version": apiVersion,
      "x-app-version": appVersion,
      "x-app-build": appBuild,
      "x-app-platform": platform,
      "x-device-uuid": deviceUUID,
      "X-Payload-Encryption": "1",
      "X-Request-Signature-Version": signed.version,
      "X-Request-Signature": signed.signature,
      "X-Request-Timestamp": signed.timestamp,
      "X-Request-Nonce": signed.nonce
    ]

    if let token, !token.isEmpty {
      headers["authorization"] = "Bearer \(token)"
    }

    return headers
  }
}
