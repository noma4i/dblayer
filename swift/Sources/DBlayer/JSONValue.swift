import Foundation

public enum JSONValue: Codable, Sendable, Equatable, Hashable {
  case null
  case bool(Bool)
  case integer(Int64)
  case number(Double)
  case string(String)
  case array([JSONValue])
  case object([String: JSONValue])

  public init(from decoder: Decoder) throws {
    let container = try decoder.singleValueContainer()
    if container.decodeNil() { self = .null }
    else if let value = try? container.decode(Bool.self) { self = .bool(value) }
    else if let value = try? container.decode(Int64.self) { self = .integer(value) }
    else if let value = try? container.decode(Double.self) { self = .number(value) }
    else if let value = try? container.decode(String.self) { self = .string(value) }
    else if let value = try? container.decode([JSONValue].self) { self = .array(value) }
    else if let value = try? container.decode([String: JSONValue].self) { self = .object(value) }
    else { throw DecodingError.dataCorrupted(DecodingError.Context(codingPath: decoder.codingPath, debugDescription: "Unsupported JSON value")) }
  }

  public func encode(to encoder: Encoder) throws {
    var container = encoder.singleValueContainer()
    switch self {
    case .null: try container.encodeNil()
    case .bool(let value): try container.encode(value)
    case .integer(let value): try container.encode(value)
    case .number(let value): try container.encode(value)
    case .string(let value): try container.encode(value)
    case .array(let value): try container.encode(value)
    case .object(let value): try container.encode(value)
    }
  }

  public static func encode<T: Encodable>(_ value: T) throws -> JSONValue {
    try decode(JSONEncoder().encode(value))
  }

  public func jsonData() throws -> Data {
    let encoder = JSONEncoder()
    encoder.outputFormatting = [.sortedKeys]
    return try encoder.encode(self)
  }

  public static func decode(_ data: Data) throws -> JSONValue {
    try JSONDecoder().decode(JSONValue.self, from: data)
  }

  public static func decodeObject(_ data: Data) throws -> [String: JSONValue] {
    guard case let .object(object) = try decode(data) else {
      throw DecodingError.typeMismatch([String: JSONValue].self, DecodingError.Context(codingPath: [], debugDescription: "JSON body is not an object"))
    }
    return object
  }

  public static func readObject(_ text: String) throws -> [String: JSONValue] { try decodeObject(Data(text.utf8)) }

  public static func read(_ text: String) throws -> JSONValue {
    try JSONDecoder().decode(JSONValue.self, from: Data(text.utf8))
  }

  public var objectValue: [String: JSONValue] {
    get throws {
      guard case .object(let value) = self else { throw DecodingError.typeMismatch([String: JSONValue].self, DecodingError.Context(codingPath: [], debugDescription: "JSON value is not an object")) }
      return value
    }
  }
  public var intValue: Int? { if case .integer(let value) = self { Int(exactly: value) } else { nil } }
  public var doubleValue: Double? {
    switch self {
    case .integer(let value): Double(value)
    case .number(let value): value
    default: nil
    }
  }
  public var boolValue: Bool? { if case .bool(let value) = self { value } else { nil } }
  public var stringValue: String? { if case .string(let value) = self { value } else { nil } }
  public var text: String { get throws { String(decoding: try jsonData(), as: UTF8.self) } }
}

extension JSONValue: ExpressibleByStringLiteral, ExpressibleByIntegerLiteral, ExpressibleByBooleanLiteral, ExpressibleByFloatLiteral {
  public init(stringLiteral value: String) { self = .string(value) }
  public init(integerLiteral value: Int) { self = .integer(Int64(value)) }
  public init(booleanLiteral value: Bool) { self = .bool(value) }
  public init(floatLiteral value: Double) { self = .number(value) }
}
