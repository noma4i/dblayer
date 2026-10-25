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
  /// Rows whose searchable fields hold the words of `text`, best match first when the query has no order. The last word matches as a prefix unless `wholeWords` is true.
  public func search(_ text: String, wholeWords: Bool = false) -> Query { `where`(.search(text, wholeWords: wholeWords)) }
  public func order(_ order: Order...) -> Query { var copy = self; copy.orders += order; return copy }
  public func limit(_ count: Int) -> Query { var copy = self; copy.limitCount = count; return copy }
  /// The last `rows` rows of this query, kept live. The query needs an order and no limit, and it reads the local database only.
  public func window(tail rows: Int) -> Window<Model> { Window(query: self, tail: rows) }

  var json: String {
    get throws { try json(refreshOnConnect: false) }
  }

  private func json(refreshOnConnect: Bool) throws -> String {
    var object: [String: JSONValue] = ["model": .string(Model.modelName)]
    if refreshOnConnect { object["refreshOnConnect"] = .bool(true) }
    if let condition { object["where"] = condition.json }
    if !orders.isEmpty { object["order"] = .array(orders.map { .object(["field": .string($0.field), "direction": .string($0.direction)]) }) }
    if let limitCount { object["limit"] = .integer(Int64(limitCount)) }
    if let relation { object["relation"] = .string(relation); object["params"] = .object(params) }
    return try JSONValue.object(object).text
  }

  public var live: LiveSource<[Model]> { listSource(initial: [], open: { listener in try DB.database.liveQuery(queryJson: json, listener: listener) }) { (applied: ListApplied<Model>, _) in applied.rows } }
  public var count: LiveSource<Int> { LiveSource(initial: 0) { listener in try DB.database.liveCount(queryJson: json, listener: listener) } map: { Int($0.count ?? 0) } }
  public func find(_ id: String) -> LiveSource<Model?> {
    listSource(initial: nil, open: { listener in try DB.database.liveFind(model: Model.modelName, rowId: id, listener: listener) }) { (applied: ListApplied<Model>, _) in applied.rows.first }
  }

  /// The rows of this query from memory, or nil when the query is not warm. It throws when the cache read or the decode of a row fails. It reads no database file, and the copy and the decode of the rows run off the thread of the caller. A query is warm after `warm()` and after any `live` or `snapshots` read of it. `revision` grows with each commit that reaches the rows.
  @concurrent public func cached() async throws -> Cached<[Model]>? {
    guard let change = try await DB.database.peekQuery(queryJson: json) else { return nil }
    return Cached(value: try change.rows.map { try Model.decode($0) }, revision: change.revision)
  }

  /// One row from the row cache, or nil when it is not cached. It throws when the cache read or the decode fails. Nil does not mean that the row does not exist.
  @concurrent public func cachedRow(_ id: String) async throws -> Model? {
    guard let row = try DB.database.peekRow(model: Model.modelName, rowId: id) else { return nil }
    return try Model.decode(row)
  }

  /// Reads the query once and keeps its rows in memory, so that `cached()` answers at once. The library keeps the latest queries warm and drops the oldest.
  @concurrent public func warm() async throws { try await DB.database.warm(queryJson: json) }

  /// The values that the connection of this relation carried on the edge of one row, or nil when the row is not in the connection. It reads one row by key and never waits for a write.
  @concurrent public func edgeValues(of id: String) async throws -> [String: JSONValue]? {
    try JSONValue.read(try await DB.database.edgeValues(model: Model.modelName, relation: relation ?? "", paramsJson: JSONValue.object(params).text, rowIds: [id])).objectValue[id]?.objectValue
  }

  @concurrent public func load() async throws { try await DB.database.load(model: Model.modelName, relation: relation ?? "", paramsJson: JSONValue.object(params).text) }
  @concurrent public func refresh() async throws { try await DB.database.refresh(model: Model.modelName, relation: relation ?? "", paramsJson: JSONValue.object(params).text) }
  /// Reads every page of the connection of this relation to its confirmed end, so that `snapshots.current()` holds all rows afterwards. `force: false` uses the current freshness and the pages that are stored, `force: true` refreshes the first page and then reads the rest. A call that runs together with another one for the same relation shares its page requests. It throws when a page fails, when `max_pages` stops it before the end, or when the query has no remote relation. It does not make the pages atomic: rows of earlier pages stay after a failure.
  @concurrent public func loadAll(force: Bool = false) async throws { try await DB.database.loadAll(model: Model.modelName, relation: relation ?? "", paramsJson: JSONValue.object(params).text, force: force) }
  @discardableResult @concurrent public func loadMore() async throws -> LoadMoreResult { try await DB.database.loadMore(model: Model.modelName, relation: relation ?? "", paramsJson: JSONValue.object(params).text) }

  /// The rows with the ids that the delivery replaced: a created record answered by the server (or matched by `correlate`) arrives under its real id.
  public var snapshots: LiveSource<Snapshot<Model>> {
    snapshots(refreshOnConnect: false) { _ in }
  }

  func snapshots(refreshOnConnect: Bool, connected: @escaping @Sendable (Result<LiveSubscription, Error>) -> Void) -> LiveSource<Snapshot<Model>> {
    listSource(initial: Snapshot(rows: [], replaced: [:], reordered: false), merge: { Snapshot.merging($0, $1) }, open: { listener in
      let result = Result { try DB.database.liveQuery(queryJson: json(refreshOnConnect: refreshOnConnect), listener: listener) }
      connected(result)
      return try result.get()
    }) { (applied: ListApplied<Model>, change) in
      Snapshot(applied, change)
    }
  }
}

public struct Cached<Value: Sendable>: Sendable {
  public let value: Value
  public let revision: Int64
}

public enum LoadPhase: Sendable, Equatable { case opening, notLoaded, loading, loaded, empty, failed, closed }

extension LoadPhase {
  init(_ phase: LivePhase?) {
    switch phase {
    case .notLoaded: self = .notLoaded
    case .loading: self = .loading
    case .loaded: self = .loaded
    case .empty: self = .empty
    case .failed: self = .failed
    case .closed: self = .closed
    case nil: self = .opening
    }
  }
}

/// One delivery of a live list: the rows with the ids it replaced, the load phase of the relation and the error, as the core saw them at `seq`.
public struct Snapshot<Model: DBModel>: Sendable {
  public let rows: [Model]
  /// Real id of a row that replaced a temporary one in this delivery, to the temporary id.
  public let replaced: [String: String]
  public let reordered: Bool
  public let edits: [ListEdit<Model>]?
  public let window: WindowEdges?
  public let seq: UInt64
  public let phase: LoadPhase
  /// The running load is the first one of a list that has no rows yet.
  public let isInitialLoading: Bool
  /// The running load reloads a list that already has rows.
  public let isRefreshing: Bool
  /// The running load fetches the next page.
  public let isFetchingMore: Bool
  public let hasMore: Bool
  public let total: Int?
  public let loadedAt: Int64?
  public let error: (any Error)?
  public let errorKind: String?
  /// The ids of the rows that this build cannot decode. They are not in `rows`.
  public let undecodable: [String]

  public init(
    rows: [Model], replaced: [String: String], reordered: Bool = false, edits: [ListEdit<Model>]? = nil, window: WindowEdges? = nil, seq: UInt64 = 0, phase: LoadPhase = .opening,
    isInitialLoading: Bool = false, isRefreshing: Bool = false, isFetchingMore: Bool = false, hasMore: Bool = false, total: Int? = nil, loadedAt: Int64? = nil,
    error: (any Error)? = nil, errorKind: String? = nil, undecodable: [String] = []
  ) {
    self.rows = rows
    self.replaced = replaced
    self.reordered = reordered
    self.edits = edits
    self.window = window
    self.seq = seq
    self.phase = phase
    self.isInitialLoading = isInitialLoading
    self.isRefreshing = isRefreshing
    self.isFetchingMore = isFetchingMore
    self.hasMore = hasMore
    self.total = total
    self.loadedAt = loadedAt
    self.error = error
    self.errorKind = errorKind
    self.undecodable = undecodable
  }

  init(_ applied: ListApplied<Model>, _ change: LiveChange) {
    self.init(
      rows: applied.rows, replaced: change.replacedIds, reordered: applied.reordered, edits: applied.edits, window: change.window.map(WindowEdges.init), seq: change.seq, phase: LoadPhase(change.phase),
      isInitialLoading: change.initial, isRefreshing: change.refreshing, isFetchingMore: change.fetchingMore, hasMore: change.hasMore, total: change.total.map(Int.init), loadedAt: change.loadedAt,
      error: applied.failure ?? change.error, errorKind: applied.failure == nil ? change.errorKind : "invalid", undecodable: applied.undecodable
    )
  }

  public var isLoading: Bool { phase == .loading }
  public var failed: Bool { phase == .failed }

  /// The newer delivery with the replacements of both: a chain A to B to C reads as C replaced A, and no pair is lost.
  static func merging(_ older: Snapshot, _ newer: Snapshot) -> Snapshot {
    var replaced = older.replaced
    for (real, temporary) in newer.replaced {
      replaced[real] = older.replaced[temporary] ?? temporary
      if older.replaced[temporary] != nil { replaced[temporary] = nil }
    }
    let edits = ListEdit.composing(older.edits, newer.edits, within: newer.rows.count)
    return Snapshot(
      rows: newer.rows, replaced: replaced, reordered: older.reordered || newer.reordered, edits: edits, window: newer.window ?? older.window, seq: newer.seq, phase: newer.phase,
      isInitialLoading: newer.isInitialLoading, isRefreshing: newer.isRefreshing, isFetchingMore: newer.isFetchingMore, hasMore: newer.hasMore, total: newer.total, loadedAt: newer.loadedAt,
      error: newer.error, errorKind: newer.errorKind, undecodable: newer.undecodable
    )
  }
}
