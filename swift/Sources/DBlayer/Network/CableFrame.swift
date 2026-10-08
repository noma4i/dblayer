
import Foundation

public enum CableFrame: Sendable, Equatable {
  case welcome
  case ping(Date)
  case confirmed(identifier: String)
  case rejected(identifier: String)
  case disconnect(reason: String, reconnect: Bool)
  case message(identifier: String, payload: JSONValue)
  case closed

  public init?(json: String) {
    guard let fields = Self.decodeFields(json), let frame = Self.frame(for: fields) else {
      return nil
    }
    self = frame
  }

  private static func decodeFields(_ json: String) -> [String: JSONValue]? {
    guard let data = json.data(using: .utf8) else {
      return nil
    }
    return try? JSONValue.decodeObject(data)
  }

  private static func frame(for fields: [String: JSONValue]) -> CableFrame? {
    guard let typeValue = fields["type"] else {
      return messageFrame(fields: fields)
    }
    guard case let .string(type) = typeValue else {
      return nil
    }
    return serverFrame(type: type, fields: fields)
  }

  private static func serverFrame(type: String, fields: [String: JSONValue]) -> CableFrame? {
    switch type {
    case "welcome":
      .welcome
    case "ping":
      pingFrame(fields: fields)
    case "confirm_subscription":
      subscriptionFrame(fields: fields, confirmed: true)
    case "reject_subscription":
      subscriptionFrame(fields: fields, confirmed: false)
    case "disconnect":
      disconnectFrame(fields: fields)
    default:
      nil
    }
  }

  private static func pingFrame(fields: [String: JSONValue]) -> CableFrame? {
    guard case let .number(seconds) = fields["message"] else {
      return nil
    }
    return .ping(Date(timeIntervalSince1970: seconds))
  }

  private static func subscriptionFrame(fields: [String: JSONValue], confirmed: Bool) -> CableFrame? {
    guard case let .string(identifier) = fields["identifier"] else {
      return nil
    }
    return confirmed ? .confirmed(identifier: identifier) : .rejected(identifier: identifier)
  }

  private static func disconnectFrame(fields: [String: JSONValue]) -> CableFrame? {
    guard case let .string(reason) = fields["reason"], let reconnect = reconnectValue(fields["reconnect"]) else {
      return nil
    }
    return .disconnect(reason: reason, reconnect: reconnect)
  }

  private static func reconnectValue(_ value: JSONValue?) -> Bool? {
    guard let value else {
      return true
    }
    guard case let .bool(reconnect) = value else {
      return nil
    }
    return reconnect
  }

  private static func messageFrame(fields: [String: JSONValue]) -> CableFrame? {
    guard case let .string(identifier) = fields["identifier"], let payload = fields["message"] else {
      return nil
    }
    return .message(identifier: identifier, payload: payload)
  }
}
