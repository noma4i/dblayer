import Foundation

public protocol DBModel: Codable, Sendable, Identifiable, Hashable {
  static var modelName: String { get }
  var id: String { get }
  var sync: SyncInfo { get }
  var upload: UploadInfo? { get }
}

extension DBModel {
  public static func == (lhs: Self, rhs: Self) -> Bool { lhs.id == rhs.id && lhs.sync == rhs.sync }
  public func hash(into hasher: inout Hasher) { hasher.combine(id) }

  public var needsAttention: Bool { sync.state == .failed || sync.state == .deliveryUnknown }
  public var canRetry: Bool { needsAttention }
  public var errorMessage: String? { sync.error?.message }

  public static func ids(_ ids: [String]) -> Query<Self> { Query<Self>().where(.inList("id", ids.map(JSONValue.string))) }

  public static func create(values: [String: JSONValue]) async throws -> Self {
    try decode(try DB.database.create(model: modelName, valuesJson: JSONValue.object(values).text))
  }
  public func update(values: [String: JSONValue]) async throws -> Self {
    try Self.decode(try DB.database.update(model: Self.modelName, rowId: id, patchJson: JSONValue.object(values).text))
  }
  public func act(_ action: String, values: [String: JSONValue], args: [String: JSONValue]) async throws -> Self {
    try Self.decode(try DB.database.act(model: Self.modelName, rowId: id, action: action, patchJson: JSONValue.object(values).text, argsJson: JSONValue.object(args).text))
  }
  public func set(values: [String: JSONValue]) async throws {
    try DB.database.setEphemeral(model: Self.modelName, rowId: id, valuesJson: JSONValue.object(values).text)
  }
  public func destroy() async throws { try DB.database.destroy(model: Self.modelName, rowId: id) }

  public func retry() { Task { try? await DB.database.retry(model: Self.modelName, rowId: id) } }
  public func discard() { try? DB.database.discard(model: Self.modelName, rowId: id) }
  public static func retryAll() { Task { try? await DB.database.retryAll() } }

  public func related<Target: DBModel>(_ name: String, as _: Target.Type) -> LiveSource<[Target]> {
    let model = Self.modelName, rowId = id
    return LiveSource(initial: []) { listener in try DB.database.liveHasMany(model: model, rowId: rowId, name: name, listener: listener) } map: { $0.decodeRows() }
  }

  static func decode(_ json: String) throws -> Self { try JSONDecoder().decode(Self.self, from: Data(json.utf8)) }
}
