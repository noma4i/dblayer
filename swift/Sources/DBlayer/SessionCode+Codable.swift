import Foundation

extension SessionCode: Codable {
  public init(from decoder: any Decoder) throws {
    let name = try decoder.singleValueContainer().decode(String.self)
    switch name {
    case "sequenceConflict": self = .sequenceConflict
    case "replayedRequest": self = .replayedRequest
    case "rateLimited": self = .rateLimited
    case "accountUnavailable": self = .accountUnavailable
    case "unauthenticated": self = .unauthenticated
    case "sessionEnded": self = .sessionEnded
    case "invalidCode": self = .invalidCode
    case "invalidPhone": self = .invalidPhone
    case "invalidProvider": self = .invalidProvider
    case "providerRejected": self = .providerRejected
    case "alreadyAuthenticated": self = .alreadyAuthenticated
    case "serverKeyMismatch": self = .serverKeyMismatch
    case "stateLost": self = .stateLost
    case "noCredential": self = .noCredential
    case "other": self = .other
    default:
      throw DecodingError.dataCorrupted(.init(codingPath: decoder.codingPath, debugDescription: "unknown session code \(name)"))
    }
  }

  public func encode(to encoder: any Encoder) throws {
    var field = encoder.singleValueContainer()
    try field.encode(name)
  }

  private var name: String {
    switch self {
    case .sequenceConflict: "sequenceConflict"
    case .replayedRequest: "replayedRequest"
    case .rateLimited: "rateLimited"
    case .accountUnavailable: "accountUnavailable"
    case .unauthenticated: "unauthenticated"
    case .sessionEnded: "sessionEnded"
    case .invalidCode: "invalidCode"
    case .invalidPhone: "invalidPhone"
    case .invalidProvider: "invalidProvider"
    case .providerRejected: "providerRejected"
    case .alreadyAuthenticated: "alreadyAuthenticated"
    case .serverKeyMismatch: "serverKeyMismatch"
    case .stateLost: "stateLost"
    case .noCredential: "noCredential"
    case .other: "other"
    }
  }
}
