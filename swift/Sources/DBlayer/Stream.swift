import Foundation
import Observation

/// The values of one stream in order, taken from the core one by one. The stream lives while its reader lives: when the reader stops or the task that iterates it is cancelled, the stream ends in the core. A stream that ends by itself delivers one `.ended` value last.
public struct ValueStream: AsyncSequence, Sendable {
  public typealias Element = ReadValue
  public let epoch: UInt64
  let reader: ReadStream
  let database: Database
  let load: LoadTarget?

  struct LoadTarget: Sendable {
    let model: String
    let relation: String
    let paramsJson: String
  }

  public func makeAsyncIterator() -> AsyncIterator { AsyncIterator(reader: reader) }

  public struct AsyncIterator: AsyncIteratorProtocol {
    let reader: ReadStream

    public mutating func next() async throws -> ReadValue? { try await reader.pull() }
  }

  /// Loads the relation of the stream again. Throws `Interrupted` when the session of the stream ended.
  public func refresh() async throws {
    let load = try target
    try await database.refresh(epoch: epoch, model: load.model, relation: load.relation, paramsJson: load.paramsJson)
  }

  public func loadMore() async throws -> LoadMoreResult {
    let load = try target
    return try await database.loadMore(epoch: epoch, model: load.model, relation: load.relation, paramsJson: load.paramsJson)
  }

  /// Stops reading: the stream ends in the core and nothing is delivered after this returns.
  public func cancel() async { await reader.cancel() }

  private var target: LoadTarget {
    get throws {
      guard let load else { throw DbError.Invalid(detail: "the stream has no relation to load") }
      return load
    }
  }
}

extension Database {
  fileprivate func openValueStream(epoch: UInt64, request: ReadRequest, load: ValueStream.LoadTarget? = nil) -> ValueStream {
    ValueStream(epoch: epoch, reader: subscribeValues(epoch: epoch, request: request), database: self, load: load)
  }
}

/// What a screen shows of a stream of rows: a value the screen owns, built only from deliveries. It holds no lock and no reference to the core.
public struct ListProjection<Model: DBModel>: Sendable {
  public enum Applied: Sendable, Equatable { case changed, resync, ignored }

  public private(set) var rows: [Model] = []
  public private(set) var state: ReadState?
  public private(set) var revision: Int64?
  public private(set) var ended: ReadEnd?
  public private(set) var failure: String?
  /// How many rows were decoded so far. A change of one row decodes one row.
  public private(set) var decoded = 0
  /// How many deltas were applied. A delta decodes the rows it carries and no other.
  public private(set) var deltas = 0
  /// The steps of the last delivery from the rows before to the rows after. `nil` after a snapshot: the screen reads the whole list.
  public private(set) var lastEdits: [ListEdit<Model>]? = nil
  /// Server id to the temporary id the row had when the screen first showed it, so a screen keeps the identity of a row that the server confirmed.
  public private(set) var temporaryIds: [String: String] = [:]
  public init() {}

  public var phase: ReadPhase? { state?.phase }

  /// The error the screen shows: the one the core reports for the stream, else a row the platform could not decode.
  public var error: DbError? { state?.error ?? failure.map { DbError.Invalid(detail: $0) } }

  public mutating func fail(_ error: Error) { failure = String(describing: error) }

  public mutating func apply(_ value: ReadValue) -> Applied {
    switch value {
    case let .snapshot(revision, wire, state):
      failure = nil
      rows = decode(wire).compactMap { $0 }
      self.revision = revision
      self.state = state
      lastEdits = nil
      return .changed
    case let .state(_, state):
      self.state = state
      lastEdits = []
      return .changed
    case let .ended(cause):
      ended = cause
      lastEdits = []
      return .changed
    case let .delta(base, revision, size, wire, indexes, removed, placed, replaced, renamed, state):
      guard let held = self.revision else { return .ignored }
      guard base == held else { return gap() }
      let carried = decode(wire)
      guard carried.allSatisfy({ $0 != nil }) else { return gap() }
      guard let edits = rebuild(carried: zip(carried.compactMap { $0 }, indexes).map { ($0, Int($1)) }, removed: removed, placed: placed, replaced: replaced, size: Int(size)) else { return gap() }
      for pair in replaced + renamed { temporaryIds[pair.to] = temporaryIds[pair.from] ?? pair.from; temporaryIds[pair.from] = nil }
      lastEdits = edits.count <= rows.count ? edits : nil
      deltas += 1
      self.revision = revision
      self.state = state
      return .changed
    default:
      return .ignored
    }
  }

  /// The projection holds no revision until the next snapshot: deltas wait for it.
  private mutating func gap() -> Applied {
    revision = nil
    return .resync
  }

  private mutating func decode(_ wire: [WireRow]) -> [Model?] {
    wire.map { row in
      do {
        let model = try Model.decode(row)
        decoded += 1
        return model
      } catch {
        failure = String(describing: error)
        return nil
      }
    }
  }

  /// Changes of values at their own place touch the carried rows only. A change of order works on a copy and leaves the rows as they were when it cannot apply.
  private mutating func rebuild(carried: [(Model, Int)], removed: [String], placed: [RowPlace], replaced: [RowRename], size: Int) -> [ListEdit<Model>]? {
    if removed.isEmpty && placed.isEmpty && replaced.isEmpty {
      guard size == rows.count, carried.allSatisfy({ $0.1 < rows.count && rows[$0.1].id == $0.0.id }) else { return nil }
      for (model, index) in carried { rows[index] = model }
      return carried.map { .rewrite($0.1, $0.0) }
    }
    var work = rows
    var edits: [ListEdit<Model>] = []
    let byId = Dictionary(carried.map { ($0.0.id, $0.0) }, uniquingKeysWith: { _, newer in newer })
    var landed = Set<String>()
    for pair in replaced {
      guard let at = work.firstIndex(where: { $0.id == pair.from }), let model = byId[pair.to] else { return nil }
      work[at] = model
      edits.append(.rewrite(at, model))
      landed.insert(pair.to)
    }
    if !removed.isEmpty {
      let gone = Set(removed)
      let found = work.indices.filter { gone.contains(work[$0].id) }
      guard found.count == gone.count else { return nil }
      for index in found.reversed() {
        edits.append(.remove(index, work[index].id))
        work.remove(at: index)
      }
    }
    var taken: [String: Model] = [:]
    for place in placed {
      if let at = work.firstIndex(where: { $0.id == place.id }) {
        taken[place.id] = work[at]
        edits.append(.remove(at, place.id))
        work.remove(at: at)
      }
    }
    for place in placed.sorted(by: { $0.index < $1.index }) {
      guard let model = taken[place.id] ?? byId[place.id], Int(place.index) <= work.count else { return nil }
      work.insert(model, at: Int(place.index))
      edits.append(.insert(Int(place.index), model))
      if taken[place.id] == nil { landed.insert(place.id) }
    }
    for (model, index) in carried {
      guard index < work.count else { return nil }
      if landed.contains(model.id), work[index].id == model.id { continue }
      work[index] = model
      edits.append(.rewrite(index, model))
    }
    guard work.count == size else { return nil }
    rows = work
    return edits
  }
}

/// The screen model of a stream of rows: Swift Observation over a projection. It reads the stream until it is released, and releasing it ends the stream in the core.
@MainActor @Observable
public final class Projected<Model: DBModel> {
  public private(set) var projection = ListProjection<Model>()
  public var rows: [Model] { projection.rows }
  public var value: Model? { projection.rows.first }
  public var state: ReadState? { projection.state }
  public var phase: ReadPhase? { projection.phase }
  public var error: DbError? { projection.error }
  public var isEnded: Bool { projection.ended != nil }
  @ObservationIgnored public let stream: ValueStream
  @ObservationIgnored var reading: Task<Void, Never>?
  @ObservationIgnored private var pending: [ListEdit<Model>]?

  public init(_ stream: ValueStream) {
    self.stream = stream
    reading = Task { [weak self] in
      do {
        for try await value in stream {
          guard let self else { return }
          self.receive(value)
        }
      } catch {
        self?.projection.fail(error)
      }
    }
  }

  private func receive(_ value: ReadValue) {
    let applied = projection.apply(value)
    if applied == .changed { pending = ListEdit.composing(pending, projection.lastEdits, within: projection.rows.count) }
  }

  /// The steps since the last call, to apply to a list the screen keeps. `nil`: the screen reads `rows` again.
  public func takeEdits() -> [ListEdit<Model>]? {
    defer { pending = [] }
    return pending
  }

  public func refresh() async throws { try await stream.refresh() }
  public func loadMore() async throws -> LoadMoreResult { try await stream.loadMore() }

  isolated deinit { reading?.cancel() }
}

/// The screen model of a count stream.
@MainActor @Observable
public final class ProjectedCount {
  public private(set) var count: Int?
  public private(set) var state: ReadState?
  /// Why the stream ended by itself.
  public private(set) var ended: ReadEnd?
  public var error: DbError? { state?.error }
  @ObservationIgnored public let stream: ValueStream
  @ObservationIgnored private var reading: Task<Void, Never>?

  public init(_ stream: ValueStream) {
    self.stream = stream
    reading = Task { [weak self] in
      do {
        for try await value in stream {
          guard let self else { return }
          self.receive(value)
        }
      } catch {}
    }
  }

  private func receive(_ value: ReadValue) {
    switch value {
    case let .count(_, count, state): self.count = Int(count); self.state = state
    case let .state(_, state): self.state = state
    case let .ended(cause): ended = cause
    default: break
    }
  }

  isolated deinit { reading?.cancel() }
}

/// The events of a model, after the stream opened. The stream ends when the reader stops or the session of its account ends.
public struct EventStream<Model: DBModel>: AsyncSequence, Sendable {
  public typealias Element = ModelEventItem<Model>
  public let stream: ValueStream

  public struct AsyncIterator: AsyncIteratorProtocol {
    var inner: ValueStream.AsyncIterator

    public mutating func next() async throws -> ModelEventItem<Model>? {
      while let value = try await inner.next() {
        switch value {
        case let .state(_, state):
          if let error = state.error { return .failed(error) }
        case .ready: return .ready
        case let .ended(cause):
          if cause == .invalid { return .failed(DbError.Invalid(detail: "the events stream could not open")) }
          return nil
        case let .lost(first, last): return .lost(first: first, last: last)
        case .gap: return .gap
        case let .event(position, area, kind, id, before, after):
          do { return .event(ModelEvent(position: position, area: area, kind: kind, id: id, before: try before.map(Model.decode), after: try after.map(Model.decode))) } catch { return .failed(error) }
        default: continue
        }
      }
      return nil
    }
  }

  public func makeAsyncIterator() -> AsyncIterator { AsyncIterator(inner: stream.makeAsyncIterator()) }
}

/// The status of a transfer at one change.
public struct TransferProgress: Sendable, Equatable {
  public let revision: Int64
  public let state: String
  public let progress: UInt64
  public let error: String?
}

/// The status of a transfer while it runs, by the key `model:row:action`.
public struct UploadStatusStream: AsyncSequence, Sendable {
  public typealias Element = TransferProgress
  public let stream: ValueStream

  public struct AsyncIterator: AsyncIteratorProtocol {
    var inner: ValueStream.AsyncIterator

    public mutating func next() async throws -> TransferProgress? {
      while let value = try await inner.next() {
        switch value {
        case let .status(revision, state, progress, error): return TransferProgress(revision: revision, state: state, progress: progress, error: error)
        case .ended: return nil
        default: continue
        }
      }
      return nil
    }
  }

  public func makeAsyncIterator() -> AsyncIterator { AsyncIterator(inner: stream.makeAsyncIterator()) }
}

/// The file of a media command and the declared upload action that sends it.
public struct UploadCommand: Sendable {
  public var action: String
  public var request: UploadRequest

  public init(action: String, request: UploadRequest) {
    (self.action, self.request) = (action, request)
  }
}

/// The streams and commands of one signed-in account. The root of the app makes one after the sign-in and recreates its screens when the account changes: a scope whose session ended opens streams that end at once.
public struct AccountScope: Sendable {
  public let epoch: UInt64
  let database: Database

  public init() throws {
    let database = try DB.database
    self.database = database
    epoch = database.sessionEpoch()
  }

  /// `refreshOnConnect` loads the relation again when the stream opens, also when it is fresh. The first rows stay visible.
  public func stream<Model: DBModel>(_ query: Query<Model>, refreshOnConnect: Bool = false) throws -> ValueStream {
    database.openValueStream(epoch: epoch, request: .list(queryJson: try query.json(refreshOnConnect: refreshOnConnect)), load: try query.loadTarget)
  }

  public func count<Model: DBModel>(_ query: Query<Model>) throws -> ValueStream {
    database.openValueStream(epoch: epoch, request: .count(queryJson: try query.json))
  }

  /// One row by id. `fetch` tells when the library loads it from the server.
  public func entity<Model: DBModel>(_: Model.Type, _ id: String, fetch: ReadFetch = .never) -> ValueStream {
    database.openValueStream(epoch: epoch, request: .entity(model: Model.modelName, id: id, fetch: fetch))
  }

  public func singleton<Model: DBSingleton>(_: Model.Type) throws -> ValueStream { try stream(Model.singleton) }

  public func events<Model: DBModel>(_: Model.Type) -> EventStream<Model> {
    EventStream<Model>(stream: database.openValueStream(epoch: epoch, request: .events(model: Model.modelName)))
  }

  /// The status of the transfer of `uploads[action]` of a row.
  public func uploadStatus<Model: DBModel>(_ row: Model, action: String) -> UploadStatusStream {
    UploadStatusStream(stream: database.openValueStream(epoch: epoch, request: .blob(key: "\(Model.modelName):\(row.id):\(action)")))
  }

  /// Writes the model locally and queues it for the server. It works without a network and after the app closed. Only a durable create action (sending a message) and a declared upload action work this way; another model is refused. Throws `Interrupted` when the session of this scope ended.
  @concurrent public func send<Model: DBModel>(_: Model.Type, values: [String: JSONValue], args: [String: JSONValue] = [:], upload: UploadCommand? = nil) async throws -> MutationReceipt<Model> {
    let receipt = try await database.send(request: CommandRequest(epoch: epoch, model: Model.modelName, valuesJson: JSONValue.object(values).text, argsJson: JSONValue.object(args).text, upload: try upload.map { CommandUpload(action: $0.action, request: try $0.request.wire) }))
    return try MutationReceipt(database: database, wire: receipt)
  }

  /// Sends the file of an existing record through its declared upload action.
  @concurrent public func upload<Model: DBModel>(_ row: Model, action: String, _ request: UploadRequest) async throws -> MutationReceipt<Model> {
    let receipt = try await database.upload(epoch: epoch, model: Model.modelName, rowId: row.id, action: action, request: request.wire)
    return try MutationReceipt(database: database, wire: receipt)
  }

  @concurrent public func uploadBatch<Model: DBModel>(_: Model.Type, items: [UploadItem], request: UploadBatchRequest) async throws -> OperationReceipt<Model> {
    let receipt = try await database.uploadCreateBatch(epoch: epoch, model: Model.modelName, items: items.map { try $0.wire }, request: request.wire)
    return try OperationReceipt(database: database, wire: receipt)
  }
}

extension DB {
  public static func account() throws -> AccountScope { try AccountScope() }

}
