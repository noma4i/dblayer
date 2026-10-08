

public enum GraphQLSubscriptionEvent: Sendable, Equatable {
  case ready
  case payload(JSONValue)
  case errors([String])
  case completed
}

public enum GraphQLChannel: Sendable {
  public static let channelName = "GraphqlChannel"
  public static let apiVersion = "2"

  public static func identifier(channelId: String) -> String {
    CableCommand.identifier(
      channel: channelName,
      parameters: [("channelId", channelId), ("api_version", apiVersion)]
    )
  }

  public static func requestId(channelId: String, attempt: Int) -> String {
    "\(channelId):\(attempt)"
  }

  public static func execute(
    channelId: String,
    attempt: Int,
    document: String,
    operationName: String
  ) -> CableCommand {
    let data = [
      "{\"action\":",
      encoded(.string("execute")),
      ",\"requestId\":",
      encoded(.string(requestId(channelId: channelId, attempt: attempt))),
      ",\"query\":",
      encoded(.string(document)),
      ",\"variables\":",
      encoded(.object([:])),
      ",\"operationName\":",
      encoded(.string(operationName)),
      "}"
    ].joined()
    return .message(identifier: identifier(channelId: channelId), data: data)
  }

  public static func event(from payload: JSONValue, requestId: String) -> GraphQLSubscriptionEvent? {
    guard case let .object(fields) = payload else {
      return nil
    }
    if case .bool(false) = fields["more"] {
      return .completed
    }
    if case .bool(true) = fields["subscriptionReady"] {
      if case let .string(payloadRequestId) = fields["requestId"], payloadRequestId == requestId {
        return .ready
      }
    }
    guard case let .object(result) = fields["result"] else {
      return nil
    }
    if case let .object(data) = result["data"], !data.isEmpty {
      return .payload(.object(data))
    }
    guard case let .array(errors) = result["errors"], !errors.isEmpty else {
      return nil
    }
    let messages = errors.compactMap { error -> String? in
      guard case let .object(fields) = error else {
        return nil
      }
      guard case let .string(message) = fields["message"] else {
        return nil
      }
      return message
    }
    return .errors(messages)
  }

  private static func encoded(_ value: JSONValue) -> String {
    let data = CanonicalJSON.encode(value)
    guard let string = String(bytes: data, encoding: .utf8) else {
      preconditionFailure("CanonicalJSON produced invalid UTF-8")
    }
    return string
  }
}
