import Foundation

public struct Condition: Sendable {
  let json: JSONValue
  public static func equal(_ field: String, _ value: JSONValue) -> Condition { .make("equal", field, ["value": value]) }
  public static func notEqual(_ field: String, _ value: JSONValue) -> Condition { .make("not_equal", field, ["value": value]) }
  public static func compare(_ field: String, _ op: String, _ value: JSONValue) -> Condition { .make("compare", field, ["op": .string(op), "value": value]) }
  public static func inList(_ field: String, _ values: [JSONValue]) -> Condition { .make("in_list", field, ["values": .array(values)]) }
  public static func isNull(_ field: String) -> Condition { .make("is_null", field, [:]) }
  public static func search(_ text: String, wholeWords: Bool = false) -> Condition {
    Condition(json: .object(["type": "search", "text": .string(text), "whole_words": .bool(wholeWords)]))
  }
  public static func && (lhs: Condition, rhs: Condition) -> Condition { Condition(json: .object(["type": "all_of", "clauses": .array([lhs.json, rhs.json])])) }
  public static func || (lhs: Condition, rhs: Condition) -> Condition { Condition(json: .object(["type": "any_of", "clauses": .array([lhs.json, rhs.json])])) }
  public static prefix func ! (clause: Condition) -> Condition { Condition(json: .object(["type": "not", "clause": clause.json])) }

  private static func make(_ type: String, _ field: String, _ rest: [String: JSONValue]) -> Condition {
    Condition(json: .object(rest.merging(["type": .string(type), "field": .string(field)]) { first, _ in first }))
  }
}

public struct Order: Sendable {
  public let field: String
  public let direction: String
  public init(field: String, direction: String) { self.field = field; self.direction = direction }
}

public struct Column<Model: DBModel, Value: Encodable & Sendable>: Sendable {
  public let name: String
  public init(_ name: String) { self.name = name }
  public var asc: Order { Order(field: name, direction: "asc") }
  public var desc: Order { Order(field: name, direction: "desc") }
  public var ascNullsLast: Order { Order(field: name, direction: "asc_nulls_last") }
  public var descNullsFirst: Order { Order(field: name, direction: "desc_nulls_first") }
  public var isNull: Condition { .isNull(name) }
  public func `in`(_ values: [Value]) throws -> Condition { .inList(name, try values.map(JSONValue.encode)) }

  public static func == (column: Column, value: Value) throws -> Condition { .equal(column.name, try .encode(value)) }
  public static func != (column: Column, value: Value) throws -> Condition { .notEqual(column.name, try .encode(value)) }
  public static func < (column: Column, value: Value) throws -> Condition { .compare(column.name, "less", try .encode(value)) }
  public static func <= (column: Column, value: Value) throws -> Condition { .compare(column.name, "less_or_equal", try .encode(value)) }
  public static func > (column: Column, value: Value) throws -> Condition { .compare(column.name, "greater", try .encode(value)) }
  public static func >= (column: Column, value: Value) throws -> Condition { .compare(column.name, "greater_or_equal", try .encode(value)) }
}

public struct Query<Model: DBModel>: Sendable {
  var condition: Condition?
  var orders: [Order] = []
  var limitCount: Int?
  var relation: String?
  var params: [String: JSONValue] = [:]

  public init() {}
  public init(relation: String, params: [String: JSONValue]) { self.relation = relation; self.params = params }

  public func `where`(_ condition: Condition) -> Query {
    var copy = self
    copy.condition = self.condition.map { $0 && condition } ?? condition
    return copy
  }
  public func search(_ text: String, wholeWords: Bool = false) -> Query { `where`(.search(text, wholeWords: wholeWords)) }
  public func order(_ order: Order...) -> Query { var copy = self; copy.orders += order; return copy }
  public func limit(_ count: Int) -> Query { var copy = self; copy.limitCount = count; return copy }
  var json: String {
    get throws { try json(refreshOnConnect: false) }
  }

  func json(refreshOnConnect: Bool) throws -> String {
    var object: [String: JSONValue] = ["model": .string(Model.modelName)]
    if refreshOnConnect { object["refreshOnConnect"] = .bool(true) }
    if let condition { object["where"] = condition.json }
    if !orders.isEmpty { object["order"] = .array(orders.map { .object(["field": .string($0.field), "direction": .string($0.direction)]) }) }
    if let limitCount { object["limit"] = .integer(Int64(limitCount)) }
    if let relation { object["relation"] = .string(relation); object["params"] = .object(params) }
    return try JSONValue.object(object).text
  }

  @concurrent public func cached() async throws -> Cached<[Model]>? {
    guard let peeked = try await DB.database.peekQuery(queryJson: json) else { return nil }
    return Cached(value: try peeked.rows.map { try Model.decode($0) }, revision: peeked.revision)
  }

  @concurrent public func cachedRow(_ id: String) async throws -> Model? {
    guard let row = try DB.database.peekRow(model: Model.modelName, rowId: id) else { return nil }
    return try Model.decode(row)
  }

  /// The rows of this query read from storage once. It sends no request and keeps no stream.
  @concurrent public func read() async throws -> [Model] {
    try await DB.database.readQuery(queryJson: json).map { try Model.decode($0) }
  }

  /// One row read from storage. It is `nil` when the row does not exist.
  @concurrent public func readRow(_ id: String) async throws -> Model? {
    guard let row = try await DB.database.readRow(model: Model.modelName, rowId: id) else { return nil }
    return try Model.decode(row)
  }

  @concurrent public func warm() async throws { try await DB.database.warm(queryJson: json) }

  @concurrent public func edgeValues(of id: String) async throws -> [String: JSONValue]? {
    try JSONValue.read(try await DB.database.edgeValues(model: Model.modelName, relation: relation ?? "", paramsJson: JSONValue.object(params).text, rowIds: [id])).objectValue[id]?.objectValue
  }

  @concurrent public func load() async throws { try await DB.database.load(model: Model.modelName, relation: relation ?? "", paramsJson: JSONValue.object(params).text) }
  @concurrent public func refresh() async throws { try await DB.database.refresh(model: Model.modelName, relation: relation ?? "", paramsJson: JSONValue.object(params).text) }
  /// Reads every page of the connection of this relation to its confirmed end, so that a stream opened afterwards holds all rows. `force: false` uses the current freshness and the pages that are stored, `force: true` refreshes the first page and then reads the rest. A call that runs together with another one for the same relation shares its page requests. It throws when a page fails, when `max_pages` stops it before the end, or when the query has no remote relation. It does not make the pages atomic: rows of earlier pages stay after a failure.
  @concurrent public func loadAll(force: Bool = false) async throws { try await DB.database.loadAll(model: Model.modelName, relation: relation ?? "", paramsJson: JSONValue.object(params).text, force: force) }
  @discardableResult @concurrent public func loadMore() async throws -> LoadMoreResult { try await DB.database.loadMore(model: Model.modelName, relation: relation ?? "", paramsJson: JSONValue.object(params).text) }
}

public struct Association<Owner: DBModel, Target: DBModel>: Sendable {
  public let name: String
  public init(_ name: String) { self.name = name }
}

public struct Cached<Value: Sendable>: Sendable {
  public let value: Value
  public let revision: Int64
}
