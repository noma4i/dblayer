import Foundation

public struct Condition: Sendable {
  let tree: QueryCondition
  public static func equal(_ field: String, _ value: JSONValue) -> Condition { Condition(tree: .equal(field: field, value: Cell(json: value))) }
  public static func notEqual(_ field: String, _ value: JSONValue) -> Condition { Condition(tree: .notEqual(field: field, value: Cell(json: value))) }
  public static func compare(_ field: String, _ op: CompareOp, _ value: JSONValue) -> Condition { Condition(tree: .compare(field: field, op: op, value: Cell(json: value))) }
  public static func inList(_ field: String, _ values: [JSONValue]) -> Condition { Condition(tree: .inList(field: field, values: values.map(Cell.init(json:)))) }
  public static func isNull(_ field: String) -> Condition { Condition(tree: .isNull(field: field)) }
  public static func search(_ text: String, wholeWords: Bool = false) -> Condition { Condition(tree: .search(text: text, wholeWords: wholeWords)) }
  public static func && (lhs: Condition, rhs: Condition) -> Condition { Condition(tree: .allOf(clauses: [lhs.tree, rhs.tree])) }
  public static func || (lhs: Condition, rhs: Condition) -> Condition { Condition(tree: .anyOf(clauses: [lhs.tree, rhs.tree])) }
  public static prefix func ! (clause: Condition) -> Condition { Condition(tree: .not(clause: clause.tree)) }
}

public typealias Order = QueryOrder

public struct Column<Model: DBModel, Value: Encodable & Sendable>: Sendable {
  public let name: String
  public init(_ name: String) { self.name = name }
  public var asc: Order { Order(field: name, direction: .asc) }
  public var desc: Order { Order(field: name, direction: .desc) }
  public var ascNullsLast: Order { Order(field: name, direction: .ascNullsLast) }
  public var descNullsFirst: Order { Order(field: name, direction: .descNullsFirst) }
  public var isNull: Condition { .isNull(name) }
  public func `in`(_ values: [Value]) throws -> Condition { .inList(name, try values.map(JSONValue.encode)) }

  public static func == (column: Column, value: Value) throws -> Condition { .equal(column.name, try .encode(value)) }
  public static func != (column: Column, value: Value) throws -> Condition { .notEqual(column.name, try .encode(value)) }
  public static func < (column: Column, value: Value) throws -> Condition { .compare(column.name, .less, try .encode(value)) }
  public static func <= (column: Column, value: Value) throws -> Condition { .compare(column.name, .lessOrEqual, try .encode(value)) }
  public static func > (column: Column, value: Value) throws -> Condition { .compare(column.name, .greater, try .encode(value)) }
  public static func >= (column: Column, value: Value) throws -> Condition { .compare(column.name, .greaterOrEqual, try .encode(value)) }
}

public struct Query<Model: DBModel>: Sendable {
  var conditions: [Condition] = []
  var orders: [Order] = []
  var limitCount: Int?
  var offsetCount: Int?
  var relation: String?
  var params: [String: JSONValue] = [:]

  public init() {}
  public init(relation: String, params: [String: JSONValue]) { self.relation = relation; self.params = params }

  public func `where`(_ condition: Condition) -> Query { var copy = self; copy.conditions.append(condition); return copy }
  public func search(_ text: String, wholeWords: Bool = false) -> Query { `where`(.search(text, wholeWords: wholeWords)) }
  public func order(_ order: Order...) -> Query { var copy = self; copy.orders += order; return copy }
  public func limit(_ count: Int) -> Query { var copy = self; copy.limitCount = count; return copy }
  public func offset(_ count: Int) -> Query { var copy = self; copy.offsetCount = count; return copy }

  func spec(refreshOnConnect: Bool = false) throws -> QuerySpec {
    QuerySpec(model: Model.modelName, relation: relation, paramsJson: try JSONValue.object(params).text, conditions: conditions.map(\.tree), order: orders, limit: try Self.window(limitCount, "limit"), offset: try Self.window(offsetCount, "offset"), refreshOnConnect: refreshOnConnect)
  }

  private static func window(_ count: Int?, _ name: String) throws -> UInt32? {
    guard let count else { return nil }
    guard let exact = UInt32(exactly: count) else { throw DbError.Invalid(detail: "\(name) \(count) is outside 0...\(UInt32.max)") }
    return exact
  }

  @concurrent public func cachedRow(_ id: String, in scope: AccountScope? = nil) async throws -> Model? {
    let (database, epoch) = try DB.admitted(scope)
    guard let row = try await database.peekRow(epoch: epoch, model: Model.modelName, rowId: id) else { return nil }
    return try Model.decode(row)
  }

  /// The rows of this query read from storage once. It sends no request and keeps no stream.
  @concurrent public func read(in scope: AccountScope? = nil) async throws -> [Model] {
    let (database, epoch) = try DB.admitted(scope)
    return try await database.readQuery(epoch: epoch, query: try spec()).map { try Model.decode($0) }
  }

  /// One row read from storage. It is `nil` when the row does not exist.
  @concurrent public func readRow(_ id: String, in scope: AccountScope? = nil) async throws -> Model? {
    let (database, epoch) = try DB.admitted(scope)
    guard let row = try await database.readRow(epoch: epoch, model: Model.modelName, rowId: id) else { return nil }
    return try Model.decode(row)
  }

  @concurrent public func edgeValues(of id: String, in scope: AccountScope? = nil) async throws -> [String: JSONValue]? {
    let (database, epoch) = try DB.admitted(scope)
    let rows = try await database.edgeValues(epoch: epoch, model: Model.modelName, relation: relation, paramsJson: JSONValue.object(params).text, rowIds: [id])
    return try rows.first.map { try Cell.keyed(entries: $0.values).asJSON.objectValue }
  }

  /// Starts loading the relation and returns this caller's interest at once. Callers of the same scope share one request, and the request stops when every caller cancelled its interest.
  public func beginLoad(in scope: AccountScope? = nil) throws -> LoadRequest {
    let (database, epoch) = try DB.admitted(scope)
    return try database.beginLoad(epoch: epoch, model: Model.modelName, relation: relation, paramsJson: JSONValue.object(params).text)
  }
  /// Loads the relation. Cancelling the task gives up its interest in the shared request.
  @concurrent public func load(in scope: AccountScope? = nil) async throws {
    let request = try beginLoad(in: scope)
    defer { request.cancel() }
    try await withTaskCancellationHandler { try await request.result() } onCancel: { request.cancel() }
  }
  @concurrent public func refresh(in scope: AccountScope? = nil) async throws {
    let (database, epoch) = try DB.admitted(scope)
    try await database.refresh(epoch: epoch, model: Model.modelName, relation: relation, paramsJson: JSONValue.object(params).text)
  }
  /// Reads every page of the connection of this relation to its confirmed end, so that a stream opened afterwards holds all rows. `force: false` uses the current freshness and the pages that are stored, `force: true` refreshes the first page and then reads the rest. A call that runs together with another one for the same relation shares its page requests. It throws when a page fails, when `max_pages` stops it before the end, or when the query has no remote relation. It does not make the pages atomic: rows of earlier pages stay after a failure.
  @concurrent public func loadAll(force: Bool = false, in scope: AccountScope? = nil) async throws {
    let (database, epoch) = try DB.admitted(scope)
    let request = try database.beginLoadAll(epoch: epoch, model: Model.modelName, relation: relation, paramsJson: JSONValue.object(params).text, force: force)
    defer { request.cancel() }
    try await withTaskCancellationHandler { try await request.result() } onCancel: { request.cancel() }
  }
  @discardableResult @concurrent public func loadMore(in scope: AccountScope? = nil) async throws -> LoadMoreResult {
    let (database, epoch) = try DB.admitted(scope)
    return try await database.loadMore(epoch: epoch, model: Model.modelName, relation: relation, paramsJson: JSONValue.object(params).text)
  }
}

public struct Association<Owner: DBModel, Target: DBModel>: Sendable {
  public let name: String
  public init(_ name: String) { self.name = name }
}
