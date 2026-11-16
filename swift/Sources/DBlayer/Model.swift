import Foundation

public protocol DBModel: Sendable, Identifiable, Hashable {
  associatedtype Files: RawRepresentable & Sendable = NoFiles where Files.RawValue == String
  static var modelName: String { get }
  static var wireLayout: String { get }
  init(wire: WireRow) throws
  var id: String { get }
  var sync: SyncInfo { get }
  var uploads: [String: UploadInfo] { get }
  /// The revision of the store the row was read at.
  var rowRevision: Int64 { get }
  /// The bytes the core estimates the row holds.
  var estimatedBytes: UInt64 { get }
}

public protocol DBSingleton: DBModel { static var singleton: Query<Self> { get } }

extension DBSingleton {
  /// Calls an action on the type: it works on the stored row, or sends the mutation when there is none and the answer writes the row.
  nonisolated(nonsending) public static func actOnSingleton(_ action: String, values: [String: JSONValue], args: [String: JSONValue], in scope: AccountScope? = nil) async throws -> MutationReceipt<Self> {
    try await DB.ordered(scope) { root, database, epoch in
      return try MutationReceipt(database: root, wire: await database.act(epoch: epoch, model: modelName, rowId: "singleton", action: action, patchJson: JSONValue.object(values).text, argsJson: JSONValue.object(args).text))
    }
  }
}

extension DBModel {
  public func hash(into hasher: inout Hasher) { hasher.combine(id) }

  public var errorMessage: String? { sync.error?.message }

  public static func ids(_ ids: [String]) -> Query<Self> { Query<Self>().where(.inList("id", ids.map(JSONValue.string))) }

  nonisolated(nonsending) public static func create(values: [String: JSONValue], args: [String: JSONValue] = [:], in scope: AccountScope? = nil) async throws -> MutationReceipt<Self> {
    try await DB.ordered(scope) { root, database, epoch in
      return try MutationReceipt(database: root, wire: await database.create(epoch: epoch, model: modelName, valuesJson: JSONValue.object(values).text, argsJson: JSONValue.object(args).text))
    }
  }
  nonisolated(nonsending) public func update(values: [String: JSONValue], args: [String: JSONValue] = [:], in scope: AccountScope? = nil) async throws -> MutationReceipt<Self> {
    try await DB.ordered(scope) { root, database, epoch in
      return try MutationReceipt(database: root, wire: await database.update(epoch: epoch, model: Self.modelName, rowId: id, patchJson: JSONValue.object(values).text, argsJson: JSONValue.object(args).text))
    }
  }
  nonisolated(nonsending) public func setLocal(_ path: String, _ value: JSONValue, in scope: AccountScope? = nil) async throws {
    try await DB.ordered(scope) { root, database, epoch in
      try await database.setLocal(epoch: epoch, model: Self.modelName, rowId: id, path: path, valueJson: value.text)
    }
  }
  nonisolated(nonsending) public func act(_ action: String, values: [String: JSONValue], args: [String: JSONValue], in scope: AccountScope? = nil) async throws -> MutationReceipt<Self> {
    try await DB.ordered(scope) { root, database, epoch in
      return try MutationReceipt(database: root, wire: await database.act(epoch: epoch, model: Self.modelName, rowId: id, action: action, patchJson: JSONValue.object(values).text, argsJson: JSONValue.object(args).text))
    }
  }
  nonisolated(nonsending) public func set(values: [String: JSONValue], in scope: AccountScope? = nil) async throws {
    try await DB.ordered(scope) { root, database, epoch in
      try await database.setEphemeral(epoch: epoch, model: Self.modelName, rowId: id, valuesJson: JSONValue.object(values).text)
    }
  }
  @discardableResult nonisolated(nonsending) public func destroy(in scope: AccountScope? = nil) async throws -> MutationReceipt<Self> {
    try await DB.ordered(scope) { root, database, epoch in
      return try MutationReceipt(database: root, wire: await database.destroy(epoch: epoch, model: Self.modelName, rowId: id))
    }
  }
  nonisolated(nonsending) public func attach(_ field: Files, source: URL, name: String, mime: String, in scope: AccountScope? = nil) async throws -> FileInfo {
    try await DB.ordered(scope) { root, database, epoch in
      return FileInfo(wire: try await database.attachFile(epoch: epoch, model: Self.modelName, rowId: id, field: field.rawValue, sourcePath: source.absoluteString, name: name, mime: mime))
    }
  }
  @concurrent public func file(_ field: Files, in scope: AccountScope? = nil) async throws -> FileInfo? {
    let (database, epoch) = try DB.admitted(scope)
    return try await database.file(epoch: epoch, model: Self.modelName, rowId: id, field: field.rawValue).map(FileInfo.init(wire:))
  }
  @concurrent public func url(_ field: Files, in scope: AccountScope? = nil) async throws -> URL? {
    let (database, epoch) = try DB.admitted(scope)
    return try await database.url(epoch: epoch, model: Self.modelName, rowId: id, field: field.rawValue).map { URL(fileURLWithPath: $0) }
  }
  @discardableResult nonisolated(nonsending) public func detach(_ field: Files, in scope: AccountScope? = nil) async throws -> Bool {
    try await DB.ordered(scope) { root, database, epoch in
      return try await database.detach(epoch: epoch, model: Self.modelName, rowId: id, field: field.rawValue)
    }
  }
  @discardableResult nonisolated(nonsending) public func retry(action: String?, source: UploadSource? = nil, in scope: AccountScope? = nil) async throws -> MutationReceipt<Self> { try await Self.retry(id: id, action: action, source: source, in: scope) }
  @discardableResult nonisolated(nonsending) public static func retry(id: String, action: String?, source: UploadSource? = nil, in scope: AccountScope? = nil) async throws -> MutationReceipt<Self> {
    try await DB.ordered(scope) { root, database, epoch in
      return try MutationReceipt(database: root, wire: await database.retry(epoch: epoch, model: modelName, rowId: id, action: action, source: source?.wire))
    }
  }
  nonisolated(nonsending) public func discard(action: String?, in scope: AccountScope? = nil) async throws {
    try await DB.ordered(scope) { root, database, epoch in
      try await database.discard(epoch: epoch, model: Self.modelName, rowId: id, action: action)
    }
  }

  nonisolated(nonsending) public static func readNode(relation: String, id: String, force: Bool, in scope: AccountScope? = nil) async throws -> Self? {
    try await DB.ordered(scope) { root, database, epoch in
      return try await database.node(epoch: epoch, model: modelName, relation: relation, rowId: id, force: force).map(decode)
    }
  }
  nonisolated(nonsending) public static func readNodes(relation: String, ids: [String], in scope: AccountScope? = nil) async throws -> [Self] {
    try await DB.ordered(scope) { root, database, epoch in
      return try await database.nodes(epoch: epoch, model: modelName, relation: relation, rowIds: ids).map(decode)
    }
  }

  static func decode(_ row: WireRow) throws -> Self {
    guard row.layout == wireLayout else { throw DbError.Invalid(detail: "the generated \(modelName) does not match the library: layout \(wireLayout) against \(row.layout)") }
    return try Self(wire: row)
  }
}

public struct Confirmed<Model: DBModel>: Sendable {
  public let value: Model?
  public let answerJson: String?
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

  /// The one outcome of the receipt: the value, or the error of a refusal (`DbError.Rejected`, `kept` = the failed intent stays) or of the end of the wait.
  @concurrent public func confirmed(discardOnFailure: Bool = false) async throws -> Confirmed<Model> {
    let confirmed = try await database.confirmed(receipt: wire, discardOnFailure: discardOnFailure)
    return Confirmed(value: try confirmed.value.map(Model.decode), answerJson: confirmed.answerJson)
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
  case ended(EndCause)
}
