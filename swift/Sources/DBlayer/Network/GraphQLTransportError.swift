import Foundation

public enum GraphQLTransportError: Error, Equatable, Sendable {
  case unauthenticated
  case payloadTransport(code: String, status: Int)
  case server(status: Int)
  case graphQL(messages: [String])

  public static func classify(status: Int, errorMessages: [String]) -> GraphQLTransportError? {
    if (500 ... 599).contains(status) {
      return .server(status: status)
    }
    if status == 401 {
      return .unauthenticated
    }
    if !errorMessages.isEmpty {
      return .graphQL(messages: errorMessages)
    }
    if status == 403 {
      return .unauthenticated
    }
    if (200 ... 299).contains(status) {
      return nil
    }
    return .server(status: status)
  }

  public var errorClass: String {
    switch self {
    case .unauthenticated: "unauthenticated"
    case .payloadTransport: "payloadTransport"
    case .server: "server"
    case .graphQL: "graphQL"
    }
  }

  public var message: String {
    switch self {
    case .unauthenticated:
      "UNAUTHENTICATED"
    case let .payloadTransport(code, status):
      "\(code) (HTTP \(status))"
    case let .server(status):
      "SERVER_ERROR (HTTP \(status))"
    case let .graphQL(messages):
      messages.joined(separator: ", ")
    }
  }
}
