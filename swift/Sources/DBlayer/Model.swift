import Foundation

public protocol DBModel: Sendable, Identifiable, Hashable {
  associatedtype Files: RawRepresentable & Sendable = NoFiles where Files.RawValue == String
  static var modelName: String { get }
  static var wireLayout: String { get }
  init(wire: WireRow) throws
  var id: String { get }
  var sync: SyncInfo { get }
  var uploads: [String: UploadInfo] { get }
}

public protocol DBSingleton: DBModel { static var singleton: Query<Self> { get } }

extension DBModel {
  public func hash(into hasher: inout Hasher) { hasher.combine(id) }

  public var needsAttention: Bool { sync.state == .failed || sync.state == .deliveryUnknown }
  public var canRetry: Bool { needsAttention }
  public var errorMessage: String? { sync.error?.message }

  public static func ids(_ ids: [String]) -> Query<Self> { Query<Self>().where(.inList("id", ids.map(JSONValue.string))) }

  /// Creates a row. A model that keeps the row on the device writes it at once. A model whose create the server answers needs the network and returns the confirmed row. A command of an account passes `scope`: it is refused when that account left. Without `scope` the command belongs to the account that is signed in when it is called.
  @concurrent public static func create(values: [String: JSONValue], args: [String: JSONValue] = [:], in scope: AccountScope? = nil) async throws -> Self {
    let (database, epoch) = try DB.admitted(scope)
    return try decode(try await database.create(epoch: epoch, model: modelName, valuesJson: JSONValue.object(values).text, argsJson: JSONValue.object(args).text))
  }
  @concurrent public func update(values: [String: JSONValue], args: [String: JSONValue] = [:], in scope: AccountScope? = nil) async throws -> Self {
    let (database, epoch) = try DB.admitted(scope)
    return try Self.decode(try await database.update(epoch: epoch, model: Self.modelName, rowId: id, patchJson: JSONValue.object(values).text, argsJson: JSONValue.object(args).text))
  }
  @concurrent public func setLocal(_ path: String, _ value: JSONValue, in scope: AccountScope? = nil) async throws {
    let (database, epoch) = try DB.admitted(scope)
    try await database.setLocal(epoch: epoch, model: Self.modelName, rowId: id, path: path, valueJson: value.text)
  }
  @concurrent public func act(_ action: String, values: [String: JSONValue], args: [String: JSONValue], in scope: AccountScope? = nil) async throws -> Self {
    let (database, epoch) = try DB.admitted(scope)
    return try Self.decode(try await database.act(epoch: epoch, model: Self.modelName, rowId: id, action: action, patchJson: JSONValue.object(values).text, argsJson: JSONValue.object(args).text).row)
  }
  @concurrent public func actAnswering<Answer: Decodable & Sendable>(_ action: String, values: [String: JSONValue], args: [String: JSONValue], in scope: AccountScope? = nil) async throws -> ActionAnswer<Self, Answer> {
    let (database, epoch) = try DB.admitted(scope)
    let queued = try await database.act(epoch: epoch, model: Self.modelName, rowId: id, action: action, patchJson: JSONValue.object(values).text, argsJson: JSONValue.object(args).text)
    return ActionAnswer(model: try Self.decode(queued.row), operation: queued.operation, epoch: epoch, database: database)
  }
  @concurrent public func set(values: [String: JSONValue], in scope: AccountScope? = nil) async throws {
    let (database, epoch) = try DB.admitted(scope)
    try await database.setEphemeral(epoch: epoch, model: Self.modelName, rowId: id, valuesJson: JSONValue.object(values).text)
  }
  @concurrent public func destroy(in scope: AccountScope? = nil) async throws {
    let (database, epoch) = try DB.admitted(scope)
    try await database.destroy(epoch: epoch, model: Self.modelName, rowId: id)
  }
  /// Waits until the server has answered the queued changes of the record, or of the declared `action` when given, throws on a refusal or when offline.
  @concurrent public func settled(action: String?, in scope: AccountScope? = nil) async throws {
    let (database, epoch) = try DB.admitted(scope)
    try await database.settled(epoch: epoch, model: Self.modelName, rowId: id, action: action)
  }
  @concurrent public func attach(_ field: Files, data: Data, name: String, mime: String, in scope: AccountScope? = nil) async throws -> FileInfo {
    let (database, epoch) = try DB.admitted(scope)
    return try FileInfo.parse(try await database.attach(epoch: epoch, model: Self.modelName, rowId: id, field: field.rawValue, bytes: data, name: name, mime: mime))
  }
  @concurrent public func file(_ field: Files, in scope: AccountScope? = nil) async throws -> FileInfo? {
    let (database, epoch) = try DB.admitted(scope)
    return try await database.file(epoch: epoch, model: Self.modelName, rowId: id, field: field.rawValue).map(FileInfo.parse)
  }
  @concurrent public func read(_ field: Files, in scope: AccountScope? = nil) async throws -> Data? {
    let (database, epoch) = try DB.admitted(scope)
    return try await database.read(epoch: epoch, model: Self.modelName, rowId: id, field: field.rawValue)
  }
  @concurrent public func url(_ field: Files, in scope: AccountScope? = nil) async throws -> URL? {
    let (database, epoch) = try DB.admitted(scope)
    return try await database.url(epoch: epoch, model: Self.modelName, rowId: id, field: field.rawValue).map { URL(fileURLWithPath: $0) }
  }
  @discardableResult @concurrent public func detach(_ field: Files, in scope: AccountScope? = nil) async throws -> Bool {
    let (database, epoch) = try DB.admitted(scope)
    return try await database.detach(epoch: epoch, model: Self.modelName, rowId: id, field: field.rawValue)
  }
  @concurrent public func confirmedId(in scope: AccountScope? = nil) async throws -> String {
    let (database, epoch) = try DB.admitted(scope)
    return try await database.confirmed(epoch: epoch, model: Self.modelName, rowId: id)
  }

  @discardableResult @concurrent public func retry(action: String?, in scope: AccountScope? = nil) async throws -> UInt64 { try await Self.retry(id: id, action: action, in: scope) }
  @discardableResult @concurrent public static func retry(id: String, action: String?, in scope: AccountScope? = nil) async throws -> UInt64 {
    let (database, epoch) = try DB.admitted(scope)
    return try await database.retry(epoch: epoch, model: modelName, rowId: id, action: action)
  }
  @concurrent public func discard(action: String?, in scope: AccountScope? = nil) async throws {
    let (database, epoch) = try DB.admitted(scope)
    try await database.discard(epoch: epoch, model: Self.modelName, rowId: id, action: action)
  }

  @concurrent public static func readNode(relation: String, id: String, force: Bool, in scope: AccountScope? = nil) async throws -> Self? {
    let (database, epoch) = try DB.admitted(scope)
    return try await database.node(epoch: epoch, model: modelName, relation: relation, rowId: id, force: force).map(decode)
  }
  @concurrent public static func readNodes(relation: String, ids: [String], in scope: AccountScope? = nil) async throws -> [Self] {
    let (database, epoch) = try DB.admitted(scope)
    return try await database.nodes(epoch: epoch, model: modelName, relation: relation, rowIds: ids).map(decode)
  }

  static func decode(_ row: WireRow) throws -> Self {
    guard row.layout == wireLayout else { throw DbError.Invalid(detail: "the generated \(modelName) does not match the library: layout \(wireLayout) against \(row.layout)") }
    return try Self(wire: row)
  }
}

public struct ActionAnswer<Model: DBModel, Answer: Decodable & Sendable>: Sendable {
  public let model: Model
  let operation: Int64
  let epoch: UInt64
  let database: Database

  @concurrent public func answer() async throws -> Answer {
    try JSONDecoder().decode(Answer.self, from: Data(try await database.answer(epoch: epoch, model: Model.modelName, operation: operation).utf8))
  }
}

public struct ModelEvent<Model: DBModel>: Sendable {
  public let area: String
  public let kind: String
  public let id: String
  public let before: Model?
  public let after: Model?
}

public enum ModelEventItem<Model: DBModel>: Sendable {
  case event(ModelEvent<Model>)
  case missed(UInt64)
  case failed(any Error)
}
