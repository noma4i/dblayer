public enum CableCommand: Sendable, Equatable {
  case subscribe(identifier: String)
  case unsubscribe(identifier: String)
  case message(identifier: String, data: String)

  public var json: String {
    switch self {
    case let .subscribe(identifier):
      "{\"command\":\"subscribe\",\"identifier\":\(Self.quoted(identifier))}"
    case let .unsubscribe(identifier):
      "{\"command\":\"unsubscribe\",\"identifier\":\(Self.quoted(identifier))}"
    case let .message(identifier, data):
      "{\"command\":\"message\",\"identifier\":\(Self.quoted(identifier)),\"data\":\(Self.quoted(data))}"
    }
  }

  public static func identifier(channel: String, parameters: [(String, String)]) -> String {
    let channelField = "\"channel\":\(quoted(channel))"
    let parameterFields = parameters.map { key, value in
      "\(quoted(key)):\(quoted(value))"
    }
    return "{\(([channelField] + parameterFields).joined(separator: ","))}"
  }

  private static func quoted(_ value: String) -> String {
    let data = CanonicalJSON.encode(.string(value))
    guard let string = String(bytes: data, encoding: .utf8) else {
      preconditionFailure("CanonicalJSON produced invalid UTF-8")
    }
    return string
  }
}
