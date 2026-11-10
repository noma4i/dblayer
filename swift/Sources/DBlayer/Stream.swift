import Foundation
import Observation

/// The values of one stream in order, taken from the core one by one. The stream lives while its reader lives: when the reader stops or the task that iterates it is cancelled, the stream ends in the core. A stream that ends by itself delivers one `.ended` value last.
public struct ValueStream: AsyncSequence, Sendable {
  public typealias Element = ReadValue
  public let epoch: UInt64
  let reader: ReadStream

  public func makeAsyncIterator() -> AsyncIterator { AsyncIterator(lease: ReaderLease(reader)) }

  public struct AsyncIterator: AsyncIteratorProtocol {
    let lease: ReaderLease

    public mutating func next() async -> ReadValue? { await lease.next() }
  }

  /// Loads the relation of the stream again. Throws `NoSession` when the session ended and `Invalid` when the stream has no relation.
  public func refresh() async throws { try await reader.refresh() }

  public func loadMore() async throws -> LoadMoreResult { try await reader.loadMore() }

  /// Stops reading: the stream ends in the core and nothing is delivered after this returns.
  public func cancel() async {
    await reader.cancel()
  }
}

/// The one reader a stream has: the core refuses a second claim with `Invalid`, and a reader that leaves ends the interest of the stream while the `ValueStream` is held.
final class ReaderLease: @unchecked Sendable {
  private enum Claim { case unclaimed, held(UInt64), over }
  private let reader: ReadStream
  private var claim = Claim.unclaimed

  init(_ reader: ReadStream) { self.reader = reader }

  func next() async -> ReadValue? {
    if case .unclaimed = claim {
      switch await reader.claimReader() {
      case let .claimed(id): claim = .held(id)
      case let .ended(cause):
        claim = .over
        return .ended(cause: cause)
      }
    }
    guard case let .held(id) = claim else { return nil }
    let reader = reader
    return await withTaskCancellationHandler { await reader.pull(reader: id) } onCancel: { Task { await reader.cancel() } }
  }

  deinit {
    if case let .held(id) = claim { reader.releaseReader(reader: id) }
  }
}

extension Database {
  fileprivate func openValueStream(epoch: UInt64, request: ReadRequest) -> ValueStream {
    ValueStream(epoch: epoch, reader: subscribeValues(epoch: epoch, request: request))
  }
}

/// One delivery of a stream of rows with its rows decoded: the platform value that `ListProjection` mirrors.
public enum ListDelivery<Model: DBModel>: Sendable {
  case snapshot(revision: Int64, rows: [Model], edits: [ListEdit<Model>], state: ReadState)
  case delta(base: Int64, revision: Int64, edits: [ListEdit<Model>], state: ReadState)
  case state(revision: Int64, state: ReadState)
  case ended(EndCause)

  /// Decodes the rows of a value of a stream of rows. `nil` for a value that carries no list. Throws when a row cannot be decoded: the reader then calls `ReadStream.resync()`.
  public init?(_ value: ReadValue) throws {
    switch value {
    case let .snapshot(revision, wire, edits, state): self = .snapshot(revision: revision, rows: try wire.map(Model.decode), edits: try edits.map(Self.step), state: state)
    case let .delta(base, revision, edits, state): self = .delta(base: base, revision: revision, edits: try edits.map(Self.step), state: state)
    case let .state(revision, state): self = .state(revision: revision, state: state)
    case let .ended(cause): self = .ended(cause)
    default: return nil
    }
  }

  private static func step(_ edit: RowEdit) throws -> ListEdit<Model> {
    switch edit {
    case let .insert(index, row): .insert(Int(index), try Model.decode(row))
    case let .rewrite(index, row, temporary): .rewrite(Int(index), try Model.decode(row), temporary: temporary)
    case let .remove(index, id): .remove(Int(index), id)
    case let .move(from, to): .move(Int(from), Int(to))
    }
  }
}

/// What a screen shows of a stream of rows: a value the screen owns, mirrored from the deliveries of the core. It holds no lock and no reference to the core.
public struct ListProjection<Model: DBModel>: Sendable {
  public private(set) var rows: [Model] = []
  public private(set) var state: ReadState?
  public private(set) var revision: Int64?
  /// The revision the last delta continues from.
  public private(set) var base: Int64?
  public private(set) var ended: EndCause?
  /// The steps of the last delivery from the rows before to the rows after, as the core gives them for a snapshot and a delta alike.
  public private(set) var lastEdits: [ListEdit<Model>] = []
  public init() {}

  public var phase: ReadPhase? { state?.phase }

  /// The error the core reports for the stream.
  public var error: DbError? { state?.error }

  public mutating func apply(_ delivery: ListDelivery<Model>) {
    switch delivery {
    case let .snapshot(revision, rows, edits, state):
      lastEdits = edits
      self.rows = rows
      self.revision = revision
      base = nil
      self.state = state
    case let .delta(base, revision, edits, state):
      rows.replay(edits)
      self.base = base
      self.revision = revision
      self.state = state
      lastEdits = edits
    case let .state(revision, state):
      self.revision = revision
      self.state = state
      lastEdits = []
    case let .ended(cause):
      ended = cause
      lastEdits = []
    }
  }

  /// Decodes and applies a value. Returns `false` for a value that carries no list. Throws when a row cannot be decoded, the projection is then unchanged.
  @discardableResult public mutating func apply(_ value: ReadValue) throws -> Bool {
    guard let delivery = try ListDelivery<Model>(value) else { return false }
    apply(delivery)
    return true
  }
}

/// The screen model of a stream of rows: Swift Observation over a projection. It reads the stream until it is released, and releasing it ends the stream in the core. Rows are decoded off the main actor, the main actor only publishes them.
@MainActor @Observable
public final class Projected<Model: DBModel> {
  public private(set) var projection = ListProjection<Model>()
  public var rows: [Model] { projection.rows }
  public var value: Model? { projection.rows.first }
  public var state: ReadState? { projection.state }
  public var phase: ReadPhase? { projection.phase }
  public var error: DbError? { projection.error }
  public var isEnded: Bool { projection.ended != nil }
  /// The steps of the last delivery, to apply to a list the screen keeps.
  public var edits: [ListEdit<Model>] { projection.lastEdits }
  @ObservationIgnored public let stream: ValueStream
  @ObservationIgnored var reading: Task<Void, Never>?

  public init(_ stream: ValueStream) {
    self.stream = stream
    reading = Task.detached { [weak self] in
      for await value in stream {
        let delivery: ListDelivery<Model>?
        do { delivery = try ListDelivery(value) } catch {
          stream.reader.resync()
          continue
        }
        guard let delivery, let self else { continue }
        await self.receive(delivery)
      }
    }
  }

  private func receive(_ delivery: ListDelivery<Model>) { projection.apply(delivery) }

  public func refresh() async throws { try await stream.refresh() }
  public func loadMore() async throws -> LoadMoreResult { try await stream.loadMore() }

  isolated deinit {
    reading?.cancel()
    let stream = stream
    Task { await stream.cancel() }
  }
}

/// The screen model of a count stream.
@MainActor @Observable
public final class ProjectedCount {
  public private(set) var count: UInt64?
  public private(set) var revision: Int64?
  public private(set) var state: ReadState?
  /// Why the stream ended by itself.
  public private(set) var ended: EndCause?
  public var error: DbError? { state?.error }
  @ObservationIgnored public let stream: ValueStream
  @ObservationIgnored private var reading: Task<Void, Never>?

  public init(_ stream: ValueStream) {
    self.stream = stream
    reading = Task { [weak self] in
      for await value in stream {
        guard let self else { return }
        self.receive(value)
      }
    }
  }

  private func receive(_ value: ReadValue) {
    switch value {
    case let .count(revision, count, state): (self.count, self.revision, self.state) = (count, revision, state)
    case let .state(revision, state): (self.revision, self.state) = (revision, state)
    case let .ended(cause): ended = cause
    default: break
    }
  }

  isolated deinit {
    reading?.cancel()
    let stream = stream
    Task { await stream.cancel() }
  }
}

/// The events of a model, after the stream opened. The stream ends when the reader stops or the session of its account ends.
public struct EventStream<Model: DBModel>: AsyncSequence, Sendable {
  public typealias Element = ModelEventItem<Model>
  public let stream: ValueStream

  public struct AsyncIterator: AsyncIteratorProtocol {
    var inner: ValueStream.AsyncIterator

    public mutating func next() async throws -> ModelEventItem<Model>? {
      while let value = await inner.next() {
        switch value {
        case .ready: return .ready
        case let .ended(cause): return .ended(cause)
        case let .lost(first, last): return .lost(first: first, last: last)
        case .gap: return .gap
        case let .event(position, area, kind, id, before, after):
          return .event(ModelEvent(position: position, area: area, kind: kind, id: id, before: try before.map(Model.decode), after: try after.map(Model.decode)))
        default: continue
        }
      }
      return nil
    }
  }

  public func makeAsyncIterator() -> AsyncIterator { AsyncIterator(inner: stream.makeAsyncIterator()) }
}

public enum ChangeItem: Sendable, Equatable {
  case ready
  case changed(revision: Int64, rows: [ChangedRow])
  case ended(EndCause)
}

public struct ChangeStream: AsyncSequence, Sendable {
  public typealias Element = ChangeItem
  public let stream: ValueStream

  public struct AsyncIterator: AsyncIteratorProtocol {
    var inner: ValueStream.AsyncIterator

    public mutating func next() async -> ChangeItem? {
      while let value = await inner.next() {
        switch value {
        case .ready: return .ready
        case let .changes(revision, rows): return .changed(revision: revision, rows: rows)
        case let .ended(cause): return .ended(cause)
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
  public let waitReason: String?
  public let progress: UInt64
  public let error: String?
  public let localUrl: String?
  public let processing: WireProcessing?
}

/// One item of the status of a transfer: a change, or the end of the stream with the cause of the core.
public enum TransferItem: Sendable, Equatable {
  case progress(TransferProgress)
  case ended(EndCause)
}

/// The status of a transfer while it runs, by the key `model:row:action`. The last item is `.ended`.
public struct UploadStatusStream: AsyncSequence, Sendable {
  public typealias Element = TransferItem
  public let stream: ValueStream

  public struct AsyncIterator: AsyncIteratorProtocol {
    var inner: ValueStream.AsyncIterator

    public mutating func next() async -> TransferItem? {
      while let value = await inner.next() {
        switch value {
        case let .status(revision, state, waitReason, progress, error, localUrl, processing): return .progress(TransferProgress(revision: revision, state: state, waitReason: waitReason, progress: progress, error: error, localUrl: localUrl, processing: processing))
        case let .ended(cause): return .ended(cause)
        default: continue
        }
      }
      return nil
    }
  }

  public func makeAsyncIterator() -> AsyncIterator { AsyncIterator(inner: stream.makeAsyncIterator()) }
}

/// One item of the queue of operations: the whole queue, or the end of the stream with the cause of the core.
public enum OperationQueueItem: Sendable {
  case queue(revision: Int64, items: [OperationInfo])
  case ended(EndCause)
}

/// The queue of operations of the account: the whole queue at open and after every change, then `.ended`. Each `OperationInfo` names the arguments of `retry(model:id:action:)` and `discard(model:id:action:)`.
public struct OperationQueueStream: AsyncSequence, Sendable {
  public typealias Element = OperationQueueItem
  public let stream: ValueStream

  public struct AsyncIterator: AsyncIteratorProtocol {
    var inner: ValueStream.AsyncIterator

    public mutating func next() async -> OperationQueueItem? {
      while let value = await inner.next() {
        switch value {
        case let .operations(revision, items): return .queue(revision: revision, items: items)
        case let .ended(cause): return .ended(cause)
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
    epoch = try database.admit(accountEpoch: nil)
  }

  /// `refreshOnConnect` loads the relation again when the stream opens, also when it is fresh. The first rows stay visible.
  public func stream<Model: DBModel>(_ query: Query<Model>, refreshOnConnect: Bool = false) throws -> ValueStream {
    database.openValueStream(epoch: epoch, request: .list(query: try query.spec(refreshOnConnect: refreshOnConnect)))
  }

  public func count<Model: DBModel>(_ query: Query<Model>) throws -> ValueStream {
    database.openValueStream(epoch: epoch, request: .count(query: try query.spec()))
  }

  /// One row by id. `fetch` tells when the library loads it from the server.
  public func entity<Model: DBModel>(_: Model.Type, _ id: String, fetch: ReadFetch = .never) -> ValueStream {
    database.openValueStream(epoch: epoch, request: .entity(model: Model.modelName, id: id, fetch: fetch))
  }

  public func singleton<Model: DBSingleton>(_: Model.Type) throws -> ValueStream { try stream(Model.singleton) }

  public func events<Model: DBModel>(_: Model.Type) -> EventStream<Model> {
    EventStream<Model>(stream: database.openValueStream(epoch: epoch, request: .events(model: Model.modelName)))
  }

  public func changes() -> ChangeStream {
    ChangeStream(stream: database.openValueStream(epoch: epoch, request: .changes))
  }

  /// The status of the transfer of `uploads[action]` of a row.
  public func uploadStatus<Model: DBModel>(_ row: Model, action: String) -> UploadStatusStream {
    UploadStatusStream(stream: database.openValueStream(epoch: epoch, request: .blob(model: Model.modelName, id: row.id, action: action)))
  }

  /// The operations that wait for the server, are on their way or were refused.
  public func operations() -> OperationQueueStream {
    OperationQueueStream(stream: database.openValueStream(epoch: epoch, request: .operations))
  }

  /// Sends a refused operation again, by the fields of its `OperationInfo`. Throws `NotFound` when the operation is gone.
  @concurrent public func retry(model: String, id: String, action: String?) async throws {
    _ = try await database.retry(epoch: epoch, model: model, rowId: id, action: action, source: nil)
  }

  /// Drops an operation that is not on its way, by the fields of its `OperationInfo`.
  @concurrent public func discard(model: String, id: String, action: String?) async throws {
    try await database.discard(epoch: epoch, model: model, rowId: id, action: action)
  }

  /// Writes the model locally and queues it for the server. It works without a network and after the app closed. A model with a `create` operation and a declared upload action work this way; another model is refused. Throws `NoSession` when the session of this scope ended.
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

extension OperationInfo {
  /// The value `uploads[action]` of the active transfer this entry shows: the same value a row shows. `nil` for an operation that is no transfer.
  public var uploadInfo: UploadInfo? { upload.map(UploadInfo.init(wire:)) }
}
