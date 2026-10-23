import Foundation

extension Cell {
  public var asText: String? { if case let .text(value) = self { value } else { nil } }
  public var asId: String? {
    switch self {
    case let .text(value): value
    case let .int(value): String(value)
    default: nil
    }
  }
  public var asBool: Bool? { if case let .bool(value) = self { value } else { nil } }
  public var asInt: Int? { if case let .int(value) = self { Int(value) } else { nil } }
  public var asDouble: Double? {
    switch self {
    case let .real(value): value
    case let .int(value): Double(value)
    default: nil
    }
  }

  public var asJSON: JSONValue {
    switch self {
    case .null: .null
    case let .bool(value): .bool(value)
    case let .int(value): .integer(value)
    case let .real(value): .number(value)
    case let .text(value): .string(value)
    case let .seq(items): .array(items.map(\.asJSON))
    case let .keyed(entries): .object(Dictionary(entries.map { ($0.key, $0.value.asJSON) }, uniquingKeysWith: { _, newer in newer }))
    }
  }
}

public struct CellReader: Sendable {
  private let cells: [Cell]

  public init(_ cells: [Cell]) { self.cells = cells }

  private func cell(_ at: Int) throws -> Cell {
    guard cells.indices.contains(at) else { throw DbError.Invalid(detail: "the row has no cell \(at)") }
    return cells[at]
  }

  private func wrong(_ at: Int) -> DbError { DbError.Invalid(detail: "the cell \(at) has the wrong type") }

  private func required<T>(_ value: T?, _ at: Int) throws -> T {
    guard let value else { throw DbError.Invalid(detail: "the cell \(at) is empty") }
    return value
  }

  public func opt<T>(_ at: Int, _ convert: KeyPath<Cell, T?>) throws -> T? {
    let cell = try cell(at)
    if cell == .null { return nil }
    guard let value = cell[keyPath: convert] else { throw wrong(at) }
    return value
  }

  public func req<T>(_ at: Int, _ convert: KeyPath<Cell, T?>) throws -> T { try required(try opt(at, convert), at) }

  public func listOpt<T>(_ at: Int, _ convert: KeyPath<Cell, T?>) throws -> [T]? {
    let cell = try cell(at)
    if cell == .null { return nil }
    guard case let .seq(items) = cell else { throw wrong(at) }
    return try items.map { item in
      guard let value = item[keyPath: convert] else { throw wrong(at) }
      return value
    }
  }

  public func list<T>(_ at: Int, _ convert: KeyPath<Cell, T?>) throws -> [T] { try required(try listOpt(at, convert), at) }

  public func objOpt<T>(_ at: Int, _ make: ([Cell]) throws -> T) throws -> T? {
    let cell = try cell(at)
    if cell == .null { return nil }
    guard case let .seq(items) = cell else { throw wrong(at) }
    return try make(items)
  }

  public func obj<T>(_ at: Int, _ make: ([Cell]) throws -> T) throws -> T { try required(try objOpt(at, make), at) }

  public func arrOpt<T>(_ at: Int, _ make: ([Cell]) throws -> T) throws -> [T]? {
    let cell = try cell(at)
    if cell == .null { return nil }
    guard case let .seq(items) = cell else { throw wrong(at) }
    return try items.map { item in
      guard case let .seq(members) = item else { throw wrong(at) }
      return try make(members)
    }
  }

  public func arr<T>(_ at: Int, _ make: ([Cell]) throws -> T) throws -> [T] { try required(try arrOpt(at, make), at) }

  public func jsonOpt(_ at: Int) throws -> JSONValue? {
    let cell = try cell(at)
    return cell == .null ? nil : cell.asJSON
  }

  public func json(_ at: Int) throws -> JSONValue { try cell(at).asJSON }
}
