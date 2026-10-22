import Foundation

public protocol DBModel: Sendable, Identifiable, Hashable {
  static var modelName: String { get }
  static var wireLayout: String { get }
  init(wire: WireRow) throws
  var id: String { get }
  var sync: SyncInfo { get }
  var upload: UploadInfo? { get }
}

extension DBModel {
  public func hash(into hasher: inout Hasher) { hasher.combine(id) }

  public var needsAttention: Bool { sync.state == .failed || sync.state == .deliveryUnknown }
  public var canRetry: Bool { needsAttention }
  public var errorMessage: String? { sync.error?.message }

  public static func ids(_ ids: [String]) -> Query<Self> { Query<Self>().where(.inList("id", ids.map(JSONValue.string))) }

  public static func create(values: [String: JSONValue], args: [String: JSONValue] = [:]) async throws -> Self {
    try decode(try await DB.database.create(model: modelName, valuesJson: JSONValue.object(values).text, argsJson: JSONValue.object(args).text))
  }
  public func update(values: [String: JSONValue], args: [String: JSONValue] = [:]) async throws -> Self {
    try Self.decode(try await DB.database.update(model: Self.modelName, rowId: id, patchJson: JSONValue.object(values).text, argsJson: JSONValue.object(args).text))
  }
  /// Writes a field the server never sends. A `nil` value clears it.
  public func setLocal(_ path: String, _ value: JSONValue) async throws {
    try await DB.database.setLocal(model: Self.modelName, rowId: id, path: path, valueJson: value.text)
  }
  public func act(_ action: String, values: [String: JSONValue], args: [String: JSONValue]) async throws -> Self {
    try Self.decode(try await DB.database.act(model: Self.modelName, rowId: id, action: action, patchJson: JSONValue.object(values).text, argsJson: JSONValue.object(args).text).row)
  }
  public func actAnswering<Answer: Decodable & Sendable>(_ action: String, values: [String: JSONValue], args: [String: JSONValue]) async throws -> ActionAnswer<Self, Answer> {
    let queued = try await DB.database.act(model: Self.modelName, rowId: id, action: action, patchJson: JSONValue.object(values).text, argsJson: JSONValue.object(args).text)
    return ActionAnswer(model: try Self.decode(queued.row), operation: queued.operation)
  }
  public func set(values: [String: JSONValue]) async throws {
    try await DB.database.setEphemeral(model: Self.modelName, rowId: id, valuesJson: JSONValue.object(values).text)
  }
  public func destroy() async throws { try await DB.database.destroy(model: Self.modelName, rowId: id) }
  /// Waits until the server has answered every queued change of the record, throws on a refusal or when offline.
  public func settled() async throws { try await DB.database.settled(model: Self.modelName, rowId: id) }
  /// The server id of a record created locally. Waits for the queued create to be answered, throws when it is refused or the device is offline.
  public func confirmedId() async throws -> String { try await DB.database.confirmed(model: Self.modelName, rowId: id) }

  @discardableResult public func retry() async throws -> UInt64 { try await DB.database.retry(model: Self.modelName, rowId: id) }
  public func discard() async throws { try await DB.database.discard(model: Self.modelName, rowId: id) }
  @discardableResult public static func retryAll() async throws -> UInt64 { try await DB.database.retryAll() }

  public func related<Target: DBModel>(_ name: String, as _: Target.Type) -> LiveSource<[Target]> {
    let model = Self.modelName, rowId = id
    return listSource(initial: [], open: { listener in try DB.database.liveHasMany(model: model, rowId: rowId, name: name, listener: listener) }) { (applied: ListApplied<Target>, _) in applied.rows }
  }

  public static func readNode(relation: String, id: String, force: Bool) async throws -> Self? {
    try await DB.database.node(model: modelName, relation: relation, rowId: id, force: force).map(decode)
  }
  public static func readNodes(relation: String, ids: [String]) async throws -> [Self] {
    try await DB.database.nodes(model: modelName, relation: relation, rowIds: ids).map(decode)
  }

  static func decode(_ row: WireRow) throws -> Self {
    guard row.layout == wireLayout else { throw DbError.Invalid(detail: "the generated \(modelName) does not match the library: layout \(wireLayout) against \(row.layout)") }
    return try Self(wire: row)
  }
}

/// The optimistic record of a queued action and, once the server answers, the `result` fields the action declares.
public struct ActionAnswer<Model: DBModel, Answer: Decodable & Sendable>: Sendable {
  public let model: Model
  let operation: Int64

  /// Waits until the server answers the action, throws on a refusal or when offline. The answer is read once.
  public func answer() async throws -> Answer {
    try JSONDecoder().decode(Answer.self, from: Data(try await DB.database.answer(model: Model.modelName, operation: operation).utf8))
  }
}
