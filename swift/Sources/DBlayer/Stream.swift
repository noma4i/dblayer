import Foundation
import Observation

/// The deliveries of one stream in order. The stream lives while this value is read: when the reader stops or the task that iterates it is cancelled, the stream ends in the core. A stream that ends by itself delivers one `.ended` value last.
public struct ValueStream: AsyncSequence, Sendable {
  public typealias Element = StreamDelivery
  public let id: UInt64
  public let epoch: UInt64
  private let deliveries: AsyncStream<StreamDelivery>
  private let database: Database

  fileprivate init(id: UInt64, epoch: UInt64, deliveries: AsyncStream<StreamDelivery>, database: Database) {
    self.id = id
    self.epoch = epoch
    self.deliveries = deliveries
    self.database = database
  }

  public func makeAsyncIterator() -> AsyncIterator { AsyncIterator(deliveries) }

  public struct AsyncIterator: AsyncIteratorProtocol {
    private var inner: AsyncStream<StreamDelivery>.AsyncIterator
    private var held: StreamDelivery?
    private var lastSeq: UInt64 = 0

    init(_ deliveries: AsyncStream<StreamDelivery>) { inner = deliveries.makeAsyncIterator() }

    public mutating func next() async -> StreamDelivery? {
      if let delivery = held {
        held = nil
        return delivery
      }
      guard let delivery = await inner.next() else { return nil }
      let lost = delivery.seq > lastSeq + 1 ? delivery.seq - lastSeq - 1 : 0
      lastSeq = delivery.seq
      guard lost > 0 else { return delivery }
      held = delivery
      return StreamDelivery(stream: delivery.stream, epoch: delivery.epoch, seq: delivery.seq, value: .missed(count: lost))
    }
  }

  /// The projection could not apply a delta. The core sends one new snapshot.
  public func resync() { database.resyncStream(stream: id) }

  /// Loads the relation of the stream again. Throws `Interrupted` when the session of the stream ended.
  public func refresh() async throws { try await database.refreshStream(stream: id) }

  public func loadMore() async throws -> LoadMoreResult { try await database.loadMoreStream(stream: id) }

  /// Returns when the first load of the stream settled.
  public func loaded() async throws { try await database.streamLoaded(stream: id) }

  /// Stops reading: the stream ends in the core and no new delivery is admitted after this returns. A delivery already admitted may still finish, and the call does not wait for it.
  public func cancel() { database.cancelStream(stream: id) }
}

/// A reader that falls behind keeps the newest deliveries only. The count of the dropped ones reaches the reader as `.missed`, and a list projection that misses a delta asks the core for a new snapshot. The core calls the sink one delivery at a time.
private let streamBuffer = 256

final class Forward: ValueSink, @unchecked Sendable {
  private let continuation: AsyncStream<StreamDelivery>.Continuation
  init(_ continuation: AsyncStream<StreamDelivery>.Continuation) { self.continuation = continuation }
  func delivered(delivery: StreamDelivery) {
    continuation.yield(delivery)
    if case .ended = delivery.value { continuation.finish() }
  }
}

extension Database {
  fileprivate func openValueStream(epoch: UInt64, request: StreamRequest) -> ValueStream {
    let (deliveries, continuation) = AsyncStream<StreamDelivery>.makeStream(bufferingPolicy: .bufferingNewest(streamBuffer))
    let id = subscribe(epoch: epoch, request: request, sink: Forward(continuation))
    continuation.onTermination = { [self] _ in cancelStream(stream: id) }
    return ValueStream(id: id, epoch: epoch, deliveries: deliveries, database: self)
  }
}

/// What a screen shows of a stream of rows: a value the screen owns, built only from deliveries. It holds no lock and no reference to the core.
public struct ListProjection<Model: DBModel>: Sendable {
  public enum Applied: Sendable, Equatable { case changed, resync, ignored }

  public private(set) var rows: [Model] = []
  public private(set) var state: StreamState?
  public private(set) var revision: Int64?
  public private(set) var epoch: UInt64?
  public private(set) var ended: StreamEnd?
  public private(set) var failure: String?
  /// How many rows were decoded so far. A change of one row decodes one row.
  public private(set) var decoded = 0
  /// How many deltas were applied. A delta decodes the rows it carries and no other.
  public private(set) var deltas = 0
  /// The steps of the last delivery from the rows before to the rows after. `nil` after a snapshot: the screen reads the whole list.
  public private(set) var lastEdits: [ListEdit<Model>]? = nil
  /// Server id to the temporary id the row had when the screen first showed it, so a screen keeps the identity of a row that the server confirmed.
  public private(set) var temporaryIds: [String: String] = [:]
  private var awaitingSnapshot = false
  /// The sequence number of the last delivery read. A jump means that deliveries were dropped before the reader.
  private var lastSeq: UInt64?

  public init() {}

  public var phase: LivePhase? { state?.phase }

  /// The error the screen shows: the one the core reports for the stream, else a row the platform could not decode.
  public var error: DbError? { state?.error ?? failure.map { DbError.Invalid(detail: $0) } }

  public mutating func apply(_ delivery: StreamDelivery) -> Applied {
    if let epoch, delivery.epoch != epoch { return .ignored }
    if case .missed = delivery.value { return .ignored }
    epoch = delivery.epoch
    let jumped = lastSeq.map { delivery.seq > $0 + 1 } ?? false
    lastSeq = delivery.seq
    switch delivery.value {
    case let .snapshot(revision, wire, state):
      failure = nil
      rows = decode(wire).compactMap { $0 }
      self.revision = revision
      self.state = state
      awaitingSnapshot = false
      lastEdits = nil
      return .changed
    case let .state(state):
      self.state = state
      lastEdits = []
      return jumped ? gap() : .changed
    case let .ended(reason):
      ended = reason
      lastEdits = []
      return .changed
    case let .delta(base, revision, size, wire, indexes, removed, placed, replaced, renamed, state):
      if awaitingSnapshot { return jumped ? .resync : .ignored }
      guard !jumped, base == self.revision else { return gap() }
      let carried = decode(wire)
      guard carried.allSatisfy({ $0 != nil }) else { return gap() }
      guard let edits = rebuild(carried: zip(carried.compactMap { $0 }, indexes).map { ($0, Int($1)) }, removed: removed, placed: placed, replaced: replaced, size: Int(size)) else { return gap() }
      for pair in renamed { temporaryIds[pair.to] = temporaryIds[pair.from] ?? pair.from; temporaryIds[pair.from] = nil }
      lastEdits = edits.count <= rows.count ? edits : nil
      deltas += 1
      self.revision = revision
      self.state = state
      return .changed
    default:
      return .ignored
    }
  }

  private mutating func gap() -> Applied {
    awaitingSnapshot = true
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
  private mutating func rebuild(carried: [(Model, Int)], removed: [String], placed: [Placement], replaced: [Replacement], size: Int) -> [ListEdit<Model>]? {
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
  public var state: StreamState? { projection.state }
  public var phase: LivePhase? { projection.phase }
  public var error: DbError? { projection.error }
  public var isEnded: Bool { projection.ended != nil }
  @ObservationIgnored public let stream: ValueStream
  @ObservationIgnored private var reading: Task<Void, Never>?
  @ObservationIgnored private var pending: [ListEdit<Model>]?

  public init(_ stream: ValueStream) {
    self.stream = stream
    reading = Task { [weak self] in
      for await delivery in stream {
        guard let self else { return }
        self.receive(delivery)
      }
    }
  }

  private func receive(_ delivery: StreamDelivery) {
    let applied = projection.apply(delivery)
    if applied == .resync { stream.resync() }
    if applied == .changed { pending = ListEdit.composing(pending, projection.lastEdits, within: projection.rows.count) }
  }

  /// The steps since the last call, to apply to a list the screen keeps. `nil`: the screen reads `rows` again.
  public func takeEdits() -> [ListEdit<Model>]? {
    defer { pending = [] }
    return pending
  }

  public func refresh() async throws { try await stream.refresh() }
  public func loadMore() async throws -> LoadMoreResult { try await stream.loadMore() }
  public func loaded() async throws { try await stream.loaded() }

  isolated deinit {
    reading?.cancel()
    stream.cancel()
  }
}

/// The screen model of a count stream.
@MainActor @Observable
public final class ProjectedCount {
  public private(set) var count: Int?
  public private(set) var state: StreamState?
  /// Why the stream ended by itself.
  public private(set) var ended: StreamEnd?
  public var error: DbError? { state?.error }
  @ObservationIgnored public let stream: ValueStream
  @ObservationIgnored private var reading: Task<Void, Never>?
  @ObservationIgnored private var epoch: UInt64?

  public init(_ stream: ValueStream) {
    self.stream = stream
    reading = Task { [weak self] in
      for await delivery in stream {
        guard let self else { return }
        self.receive(delivery)
      }
    }
  }

  private func receive(_ delivery: StreamDelivery) {
    if let epoch, delivery.epoch != epoch { return }
    epoch = delivery.epoch
    switch delivery.value {
    case let .count(_, count, state): self.count = Int(count); self.state = state
    case let .state(state): self.state = state
    case let .ended(reason): ended = reason
    default: break
    }
  }

  isolated deinit {
    reading?.cancel()
    stream.cancel()
  }
}

/// The events of a model, after the stream opened. The stream ends when the reader stops or the session of its account ends.
public struct EventStream<Model: DBModel>: AsyncSequence, Sendable {
  public typealias Element = ModelEventItem<Model>
  public let stream: ValueStream

  public struct AsyncIterator: AsyncIteratorProtocol {
    var inner: ValueStream.AsyncIterator
    let stream: ValueStream
    var reported = false

    public mutating func next() async -> ModelEventItem<Model>? {
      while let delivery = await inner.next() {
        switch delivery.value {
        case let .state(state):
          if let error = state.error, !reported { reported = true; return .failed(error) }
        case let .ended(reason):
          if reason == .invalid, !reported { reported = true; return .failed(DbError.Invalid(detail: "the events stream could not open")) }
          return nil
        case let .missed(count): return .missed(count)
        case let .event(wire):
          do { return .event(ModelEvent(area: wire.area, kind: wire.kind, id: wire.id, before: try wire.before.map(Model.decode), after: try wire.after.map(Model.decode))) }
          catch {
            stream.cancel()
            return .failed(error)
          }
        default: continue
        }
      }
      return nil
    }
  }

  public func makeAsyncIterator() -> AsyncIterator { AsyncIterator(inner: stream.makeAsyncIterator(), stream: stream) }
}

/// The status of a media upload while it runs.
public struct UploadStatusStream: AsyncSequence, Sendable {
  public typealias Element = WireBlobSnapshot
  public let stream: ValueStream

  public struct AsyncIterator: AsyncIteratorProtocol {
    var inner: ValueStream.AsyncIterator

    public mutating func next() async -> WireBlobSnapshot? {
      while let delivery = await inner.next() {
        switch delivery.value {
        case let .status(blob): return blob
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
    database.openValueStream(epoch: epoch, request: .query(queryJson: try query.json(refreshOnConnect: refreshOnConnect)))
  }

  public func count<Model: DBModel>(_ query: Query<Model>) throws -> ValueStream {
    database.openValueStream(epoch: epoch, request: .count(queryJson: try query.json))
  }

  public func related<Model: DBModel, Target: DBModel>(_ query: Query<Model>, _ association: Association<Model, Target>) throws -> ValueStream {
    database.openValueStream(epoch: epoch, request: .related(queryJson: try query.json, name: association.name))
  }

  /// One row by id. `fetch` tells when the library loads it from the server.
  public func entity<Model: DBModel>(_: Model.Type, _ id: String, fetch: EntityFetch = .none) -> ValueStream {
    database.openValueStream(epoch: epoch, request: .entity(model: Model.modelName, id: id, fetch: fetch))
  }

  public func singleton<Model: DBSingleton>(_: Model.Type) throws -> ValueStream { try stream(Model.singleton) }

  public func events<Model: DBModel>(_: Model.Type) -> EventStream<Model> {
    EventStream(stream: database.openValueStream(epoch: epoch, request: .events(model: Model.modelName)))
  }

  public func uploadStatus(_ id: String) -> UploadStatusStream {
    UploadStatusStream(stream: database.openValueStream(epoch: epoch, request: .blob(id: id)))
  }

  /// Writes the model locally and queues it for the server. It works without a network and after the app closed. Only a durable create action (sending a message) and a declared upload action work this way; another model is refused. Throws `Interrupted` when the session of this scope ended.
  @concurrent public func send<Model: DBModel>(_: Model.Type, values: [String: JSONValue], args: [String: JSONValue] = [:], upload: UploadCommand? = nil) async throws -> Sent<Model> {
    let receipt = try await database.send(request: CommandRequest(epoch: epoch, model: Model.modelName, valuesJson: JSONValue.object(values).text, argsJson: JSONValue.object(args).text, upload: try upload.map { CommandUpload(action: $0.action, request: try $0.request.wire) }))
    return Sent(row: try Model.decode(receipt.row), receipt: receipt, database: database)
  }

  /// Sends the file of an existing record through its declared upload action.
  @concurrent public func upload<Model: DBModel, Answer: Decodable & Sendable>(_ row: Model, action: String, _ request: UploadRequest) async throws -> ActionAnswer<Model, Answer> {
    let operation = try await database.upload(epoch: epoch, model: Model.modelName, rowId: row.id, action: action, request: request.wire)
    return ActionAnswer(model: row, operation: operation, epoch: epoch, database: database)
  }

  @concurrent public func uploadBatch<Model: DBModel>(_: Model.Type, items: [UploadItem], request: UploadBatchRequest) async throws -> [Model] {
    try await database.uploadCreateBatch(epoch: epoch, model: Model.modelName, items: items.map { try $0.wire }, request: request.wire).map(Model.decode)
  }
}

public struct Sent<Model: DBModel>: Sendable {
  public let row: Model
  fileprivate let receipt: CommandReceipt
  fileprivate let database: Database

  public func status() throws -> CommandStatus { try database.commandStatus(receipt: receipt) }

  /// Waits for the one outcome: delivered, failed, or ended with the session. It waits while the network is away.
  @concurrent public func outcome() async throws -> CommandStatus { try await database.commandOutcome(receipt: receipt) }
}

extension DB {
  public static func account() throws -> AccountScope { try AccountScope() }

  public static func streamCensus() throws -> StreamCensus { try database.streamCensus() }
}
