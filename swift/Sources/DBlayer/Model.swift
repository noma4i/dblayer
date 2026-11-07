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

  public var errorMessage: String? { sync.error?.message }

  public static func ids(_ ids: [String]) -> Query<Self> { Query<Self>().where(.inList("id", ids.map(JSONValue.string))) }

  @concurrent public static func create(values: [String: JSONValue], args: [String: JSONValue] = [:], in scope: AccountScope? = nil) async throws -> MutationReceipt<Self> {
    let (database, epoch) = try DB.admitted(scope)
    return try MutationReceipt(database: database, wire: await database.create(epoch: epoch, model: modelName, valuesJson: JSONValue.object(values).text, argsJson: JSONValue.object(args).text))
  }
  @concurrent public func update(values: [String: JSONValue], args: [String: JSONValue] = [:], in scope: AccountScope? = nil) async throws -> MutationReceipt<Self> {
    let (database, epoch) = try DB.admitted(scope)
    return try MutationReceipt(database: database, wire: await database.update(epoch: epoch, model: Self.modelName, rowId: id, patchJson: JSONValue.object(values).text, argsJson: JSONValue.object(args).text))
  }
  @concurrent public func setLocal(_ path: String, _ value: JSONValue, in scope: AccountScope? = nil) async throws {
    let (database, epoch) = try DB.admitted(scope)
    try await database.setLocal(epoch: epoch, model: Self.modelName, rowId: id, path: path, valueJson: value.text)
  }
  @concurrent public func act(_ action: String, values: [String: JSONValue], args: [String: JSONValue], in scope: AccountScope? = nil) async throws -> MutationReceipt<Self> {
    let (database, epoch) = try DB.admitted(scope)
    return try MutationReceipt(database: database, wire: await database.act(epoch: epoch, model: Self.modelName, rowId: id, action: action, patchJson: JSONValue.object(values).text, argsJson: JSONValue.object(args).text))
  }
  @concurrent public func set(values: [String: JSONValue], in scope: AccountScope? = nil) async throws {
    let (database, epoch) = try DB.admitted(scope)
    try await database.setEphemeral(epoch: epoch, model: Self.modelName, rowId: id, valuesJson: JSONValue.object(values).text)
  }
  @discardableResult @concurrent public func destroy(in scope: AccountScope? = nil) async throws -> MutationReceipt<Self> {
    let (database, epoch) = try DB.admitted(scope)
    return try MutationReceipt(database: database, wire: await database.destroy(epoch: epoch, model: Self.modelName, rowId: id))
  }
  @concurrent public func attach(_ field: Files, data: Data, name: String, mime: String, in scope: AccountScope? = nil) async throws -> FileInfo {
    let (database, epoch) = try DB.admitted(scope)
    return FileInfo(wire: try await database.attach(epoch: epoch, model: Self.modelName, rowId: id, field: field.rawValue, file: FileBytes(bytes: data, name: name, mime: mime)))
  }
  @concurrent public func file(_ field: Files, in scope: AccountScope? = nil) async throws -> FileInfo? {
    let (database, epoch) = try DB.admitted(scope)
    return try await database.file(epoch: epoch, model: Self.modelName, rowId: id, field: field.rawValue).map(FileInfo.init(wire:))
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
  @discardableResult @concurrent public func retry(action: String?, source: UploadRequest? = nil, in scope: AccountScope? = nil) async throws -> MutationReceipt<Self> { try await Self.retry(id: id, action: action, source: source, in: scope) }
  @discardableResult @concurrent public static func retry(id: String, action: String?, source: UploadRequest? = nil, in scope: AccountScope? = nil) async throws -> MutationReceipt<Self> {
    let (database, epoch) = try DB.admitted(scope)
    return try MutationReceipt(database: database, wire: await database.retry(epoch: epoch, model: modelName, rowId: id, action: action, source: source?.wire))
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

public enum OperationOutcome<Model: DBModel>: Sendable {
  case confirmed(value: Model?, answerJSON: String?)
  case rejected(failure: WireFailure)
  case interrupted

  public var confirmedValue: Model? {
    guard case let .confirmed(value, _) = self else { return nil }
    return value
  }
}

public struct MutationReceipt<Model: DBModel>: Sendable {
  public let optimisticValue: Model?
  public let revision: Int64
  public let epoch: UInt64
  public let operation: Int64?
  let database: Database
  let wire: WireOperationReceipt

  init(database: Database, wire: WireOperationReceipt) throws {
    self.database = database
    self.wire = wire
    self.revision = wire.revision
    self.epoch = wire.epoch
    self.operation = wire.operation
    self.optimisticValue = try wire.optimisticValue.map(Model.decode)
  }

  @concurrent public func outcome() async throws -> OperationOutcome<Model> {
    switch try await database.outcome(receipt: wire) {
    case let .confirmed(value, answerJSON): return .confirmed(value: try value.map(Model.decode), answerJSON: answerJSON)
    case let .rejected(failure): return .rejected(failure: failure)
    case .interrupted: return .interrupted
    }
  }
}

public struct ItemReceipt<Model: DBModel>: Sendable {
  public let item: ItemId
  public let optimisticValue: Model
  /// The receipt of this item: its own operation, revision and outcome.
  public let receipt: MutationReceipt<Model>
}

public struct OperationReceipt<Model: DBModel>: Sendable {
  public let epoch: UInt64
  public let items: [ItemReceipt<Model>]

  init(database: Database, wire: WireBatchReceipt) throws {
    self.epoch = wire.epoch
    self.items = try wire.items.map { ItemReceipt(item: $0.item, optimisticValue: try Model.decode($0.optimisticValue), receipt: try MutationReceipt(database: database, wire: $0.receipt)) }
  }
}

public struct ModelEvent<Model: DBModel>: Sendable {
  public let position: UInt64
  public let area: String
  public let kind: String
  public let id: String
  /// The row before the event landed. It is `nil` for a created row.
  public let before: Model?
  /// The row after the event landed. It is `nil` for a destroyed row.
  public let after: Model?
}

public enum ModelEventItem<Model: DBModel>: Sendable {
  /// The source of the events is open: every event after this item reaches the stream.
  case ready
  case event(ModelEvent<Model>)
  /// The events at these positions of the stream are lost. `first` and `last` are included.
  case lost(first: UInt64, last: UInt64)
  /// The core lost the order of the events: the reader reads the rows again.
  case gap
  /// The stream ended: the core reports why. Nothing follows.
  case ended(ReadEnd)
}
