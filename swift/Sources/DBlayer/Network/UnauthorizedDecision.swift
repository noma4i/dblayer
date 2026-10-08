import Foundation

public enum UnauthorizedDecision: Sendable {
  public static let logoutOperationName = "myProfile"

  public static func logsOut(operationName: String, status: Int?, codes: [String], messages: [String]) -> Bool {
    guard operationName == logoutOperationName else {
      return false
    }
    if status == 401 || status == 403 {
      return true
    }
    if codes.contains("UNAUTHENTICATED") {
      return true
    }
    return messages.contains { message in
      let lowercased = message.lowercased()
      return lowercased.contains("unauthorized") || lowercased.contains("unauthenticated")
    }
  }

  public static func reportsFatal(status: Int?) -> Bool {
    guard let status else {
      return false
    }
    return [500, 502, 503, 504].contains(status)
  }

  public static func isSecurityCode(_ code: String) -> Bool {
    [
      "SIGNATURE_VERSION_UNSUPPORTED",
      "SIGNATURE_MISSING",
      "SIGNATURE_INVALID",
      "SIGNATURE_EXPIRED",
      "PAYLOAD_ENCRYPTION_REQUIRED",
      "PAYLOAD_DECRYPTION_FAILED"
    ].contains(code)
  }
}
