import Foundation

public struct Condition: Sendable {
  let json: JSONValue
  public static func equal(_ field: String, _ value: JSONValue) -> Condition { .make("equal", field, ["value": value]) }
  public static func notEqual(_ field: String, _ value: JSONValue) -> Condition { .make("not_equal", field, ["value": value]) }
  public static func compare(_ field: String, _ op: String, _ value: JSONValue) -> Condition { .make("compare", field, ["op": .string(op), "value": value]) }
  public static func inList(_ field: String, _ values: [JSONValue]) -> Condition { .make("in_list", field, ["values": .array(values)]) }
  public static func isNull(_ field: String) -> Condition { .make("is_null", field, [:]) }
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
  public func `in`(_ values: [Value]) -> Condition { .inList(name, values.map(JSONValue.encode)) }

  public static func == (column: Column, value: Value) -> Condition { .equal(column.name, .encode(value)) }
  public static func != (column: Column, value: Value) -> Condition { .notEqual(column.name, .encode(value)) }
  public static func < (column: Column, value: Value) -> Condition { .compare(column.name, "less", .encode(value)) }
  public static func <= (column: Column, value: Value) -> Condition { .compare(column.name, "less_or_equal", .encode(value)) }
  public static func > (column: Column, value: Value) -> Condition { .compare(column.name, "greater", .encode(value)) }
  public static func >= (column: Column, value: Value) -> Condition { .compare(column.name, "greater_or_equal", .encode(value)) }
}

func listSource<Model: DBModel, Value: Sendable>(
  initial: Value, merge: (@Sendable (Value, Value) -> Value)? = nil,
  open: @escaping @Sendable (QueryListener) throws -> LiveSubscription,
  map: @escaping @Sendable (ListApplied<Model>, LiveChange) -> Value
) -> LiveSource<Value> {
  LiveSource(initial: initial, merge: merge) { emit in
    let session = ListSession<Model, Value>(open: { listener in let subscription = try open(listener); return { subscription.cancel() } }, emit: emit, map: map)
    try session.start()
    return { session.cancel() }
  }
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
  public func order(_ order: Order...) -> Query { var copy = self; copy.orders += order; return copy }
  public func limit(_ count: Int) -> Query { var copy = self; copy.limitCount = count; return copy }

  var json: String {
    var object: [String: JSONValue] = ["model": .string(Model.modelName)]
    if let condition { object["where"] = condition.json }
    if !orders.isEmpty { object["order"] = .array(orders.map { .object(["field": .string($0.field), "direction": .string($0.direction)]) }) }
    if let limitCount { object["limit"] = .number(Double(limitCount)) }
    if let relation { object["relation"] = .string(relation); object["params"] = .object(params) }
    return JSONValue.object(object).text
  }

  public var live: LiveSource<[Model]> { listSource(initial: [], open: { listener in try DB.database.liveQuery(queryJson: json, listener: listener) }) { (applied: ListApplied<Model>, _) in applied.rows } }
  public var count: LiveSource<Int> { LiveSource(initial: 0) { listener in try DB.database.liveCount(queryJson: json, listener: listener) } map: { Int($0.count ?? 0) } }
  public func find(_ id: String) -> LiveSource<Model?> {
    listSource(initial: nil, open: { listener in try DB.database.liveFind(model: Model.modelName, rowId: id, listener: listener) }) { (applied: ListApplied<Model>, _) in applied.rows.first }
  }

  public func load() async throws { try await DB.database.load(model: Model.modelName, relation: relation ?? "", paramsJson: JSONValue.object(params).text) }
  public func refresh() async throws { try await DB.database.refresh(model: Model.modelName, relation: relation ?? "", paramsJson: JSONValue.object(params).text) }
  @discardableResult public func loadMore() async throws -> Bool { try await DB.database.loadMore(model: Model.modelName, relation: relation ?? "", paramsJson: JSONValue.object(params).text) }

  /// The rows with the ids that the delivery replaced: a created record answered by the server (or matched by `correlate`) arrives under its real id.
  public var snapshots: LiveSource<Snapshot<Model>> {
    listSource(initial: Snapshot(rows: [], replaced: [:], reordered: false), merge: { Snapshot.merging($0, $1) }, open: { listener in try DB.database.liveQuery(queryJson: json, listener: listener) }) { (applied: ListApplied<Model>, change) in
      Snapshot(rows: applied.rows, replaced: change.replacedIds, reordered: applied.reordered)
    }
  }

  public var state: LiveSource<LoadState> {
    let model = Model.modelName, relation = relation ?? "", params = JSONValue.object(params).text
    return LiveSource(initial: LoadState()) { emit in
      let subscription = try DB.database.liveState(model: model, relation: relation, paramsJson: params, listener: StateObserver { emit(LoadState($0)) })
      return { subscription.cancel() }
    }
  }
}

public struct Snapshot<Model: DBModel>: Sendable {
  public let rows: [Model]
  /// Real id of a row that replaced a temporary one in this delivery, to the temporary id.
  public let replaced: [String: String]
  public let reordered: Bool

  public init(rows: [Model], replaced: [String: String], reordered: Bool = false) {
    self.rows = rows
    self.replaced = replaced
    self.reordered = reordered
  }

  /// The newer rows with the replacements of both deliveries: a chain A to B to C reads as C replaced A, and no pair is lost.
  static func merging(_ older: Snapshot, _ newer: Snapshot) -> Snapshot {
    var replaced = older.replaced
    for (real, temporary) in newer.replaced {
      replaced[real] = older.replaced[temporary] ?? temporary
      if older.replaced[temporary] != nil { replaced[temporary] = nil }
    }
    return Snapshot(rows: newer.rows, replaced: replaced, reordered: older.reordered || newer.reordered)
  }
}

final class StateObserver: StateListener, @unchecked Sendable {
  let body: @Sendable (RelationStateChange) -> Void
  init(_ body: @escaping @Sendable (RelationStateChange) -> Void) { self.body = body }
  func changed(state: RelationStateChange) { body(state) }
}

public struct LoadState: Sendable, Equatable {
  public enum Phase: String, Sendable { case idle, loading, failed }

  public var phase: Phase = .idle
  /// The running load is the first one of a list that has no rows yet.
  public var isInitialLoading = false
  /// The running load reloads a list that already has rows.
  public var isRefreshing = false
  /// The running load fetches the next page.
  public var isFetchingMore = false
  public var hasMore = false
  public var total: Int64?
  public var errorKind: String?
  public var error: String?
  public var loadedAt: Int64?

  public init() {}

  init(_ change: RelationStateChange) {
    phase = Phase(rawValue: change.phase) ?? .idle
    isInitialLoading = change.initial
    isRefreshing = change.refreshing
    isFetchingMore = change.fetchingMore
    hasMore = change.hasMore
    total = change.total
    errorKind = change.errorKind
    error = change.error
    loadedAt = change.loadedAt
  }

  public var isLoading: Bool { phase == .loading }
  public var failed: Bool { phase == .failed }
}
