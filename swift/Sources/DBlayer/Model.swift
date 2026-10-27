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

extension DBModel {
  public func hash(into hasher: inout Hasher) { hasher.combine(id) }

  public var needsAttention: Bool { sync.state == .failed || sync.state == .deliveryUnknown }
  public var canRetry: Bool { needsAttention }
  public var errorMessage: String? { sync.error?.message }

  public static func ids(_ ids: [String]) -> Query<Self> { Query<Self>().where(.inList("id", ids.map(JSONValue.string))) }

  @concurrent public static func create(values: [String: JSONValue], args: [String: JSONValue] = [:]) async throws -> Self {
    try decode(try await DB.database.create(model: modelName, valuesJson: JSONValue.object(values).text, argsJson: JSONValue.object(args).text))
  }
  @concurrent public func update(values: [String: JSONValue], args: [String: JSONValue] = [:]) async throws -> Self {
    try Self.decode(try await DB.database.update(model: Self.modelName, rowId: id, patchJson: JSONValue.object(values).text, argsJson: JSONValue.object(args).text))
  }
  @concurrent public func setLocal(_ path: String, _ value: JSONValue) async throws {
    try await DB.database.setLocal(model: Self.modelName, rowId: id, path: path, valueJson: value.text)
  }
  @concurrent public func act(_ action: String, values: [String: JSONValue], args: [String: JSONValue]) async throws -> Self {
    try Self.decode(try await DB.database.act(model: Self.modelName, rowId: id, action: action, patchJson: JSONValue.object(values).text, argsJson: JSONValue.object(args).text).row)
  }
  @concurrent public func actAnswering<Answer: Decodable & Sendable>(_ action: String, values: [String: JSONValue], args: [String: JSONValue]) async throws -> ActionAnswer<Self, Answer> {
    let queued = try await DB.database.act(model: Self.modelName, rowId: id, action: action, patchJson: JSONValue.object(values).text, argsJson: JSONValue.object(args).text)
    return ActionAnswer(model: try Self.decode(queued.row), operation: queued.operation)
  }
  @concurrent public func set(values: [String: JSONValue]) async throws {
    try await DB.database.setEphemeral(model: Self.modelName, rowId: id, valuesJson: JSONValue.object(values).text)
  }
  @concurrent public func destroy() async throws { try await DB.database.destroy(model: Self.modelName, rowId: id) }
  /// Waits until the server has answered the queued changes of the record, or of the declared `action` when given, throws on a refusal or when offline.
  @concurrent public func settled(action: String?) async throws { try await DB.database.settled(model: Self.modelName, rowId: id, action: action) }
  @concurrent public func attach(_ field: Files, data: Data, name: String, mime: String) async throws -> FileInfo {
    try FileInfo.parse(try await DB.database.attach(model: Self.modelName, rowId: id, field: field.rawValue, bytes: data, name: name, mime: mime))
  }
  @concurrent public func file(_ field: Files) async throws -> FileInfo? {
    try await DB.database.file(model: Self.modelName, rowId: id, field: field.rawValue).map(FileInfo.parse)
  }
  @concurrent public func read(_ field: Files) async throws -> Data? { try await DB.database.read(model: Self.modelName, rowId: id, field: field.rawValue) }
  @concurrent public func url(_ field: Files) async throws -> URL? {
    try await DB.database.url(model: Self.modelName, rowId: id, field: field.rawValue).map { URL(fileURLWithPath: $0) }
  }
  @discardableResult @concurrent public func detach(_ field: Files) async throws -> Bool { try await DB.database.detach(model: Self.modelName, rowId: id, field: field.rawValue) }
  @concurrent public func confirmedId() async throws -> String { try await DB.database.confirmed(model: Self.modelName, rowId: id) }

  @discardableResult @concurrent public func retry(action: String?) async throws -> UInt64 { try await Self.retry(id: id, action: action) }
  @discardableResult @concurrent public static func retry(id: String, action: String?) async throws -> UInt64 { try await DB.database.retry(model: modelName, rowId: id, action: action) }
  @concurrent public func discard(action: String?) async throws { try await DB.database.discard(model: Self.modelName, rowId: id, action: action) }

  public func related<Target: DBModel>(_ name: String, as _: Target.Type) -> LiveSource<[Target]> {
    Query<Self>().where(.equal("id", .string(id))).related(Association<Self, Target>(name)).live
  }

  @concurrent public static func readNode(relation: String, id: String, force: Bool) async throws -> Self? {
    try await DB.database.node(model: modelName, relation: relation, rowId: id, force: force).map(decode)
  }
  @concurrent public static func readNodes(relation: String, ids: [String]) async throws -> [Self] {
    try await DB.database.nodes(model: modelName, relation: relation, rowIds: ids).map(decode)
  }

  @concurrent public static func openEvents() async throws -> LiveStream<ModelEventItem<Self>> {
    let feed = try await DB.database.openModelEvents(model: modelName)
    let first = eventStream(feed)
    guard !Task.isCancelled else { first.cancel(); throw CancellationError() }
    return LiveStream(first: first) {
      do { return eventStream(try await DB.database.openModelEvents(model: modelName)) }
      catch {
        let latest = Latest<ModelEventItem<Self>>(merge: nil)
        latest.put(.failed(error))
        return LiveStream(receive: {
          let item = await latest.next()
          latest.close()
          return item
        }, cancel: { latest.close() })
      }
    }
  }

  private static func eventStream(_ feed: ModelEventFeed) -> LiveStream<ModelEventItem<Self>> {
    return LiveStream(receive: {
      let item: WireModelEventItem?
      do { item = try await feed.next() } catch {
        feed.cancel()
        return .failed(error)
      }
      switch item {
      case .none: return nil
      case .some(.missed(let count)): return .missed(count)
      case .some(.event(let wire)):
        do {
          return .event(ModelEvent(area: wire.area, kind: wire.kind, id: wire.id, before: try wire.before.map(decode), after: try wire.after.map(decode)))
        } catch {
          feed.cancel()
          return .failed(error)
        }
      }
    }, cancel: { feed.cancel() })
  }

  static func decode(_ row: WireRow) throws -> Self {
    guard row.layout == wireLayout else { throw DbError.Invalid(detail: "the generated \(modelName) does not match the library: layout \(wireLayout) against \(row.layout)") }
    return try Self(wire: row)
  }
}

public struct ActionAnswer<Model: DBModel, Answer: Decodable & Sendable>: Sendable {
  public let model: Model
  let operation: Int64

  @concurrent public func answer() async throws -> Answer {
    try JSONDecoder().decode(Answer.self, from: Data(try await DB.database.answer(model: Model.modelName, operation: operation).utf8))
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
