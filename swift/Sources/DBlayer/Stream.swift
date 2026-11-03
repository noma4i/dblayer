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

  public func makeAsyncIterator() -> AsyncStream<StreamDelivery>.AsyncIterator { deliveries.makeAsyncIterator() }

  /// The projection could not apply a delta. The core sends one new snapshot.
  public func resync() { database.resyncStream(stream: id) }

  /// Loads the relation of the stream again. Throws `Interrupted` when the session of the stream ended.
  public func refresh() async throws { try await database.refreshStream(stream: id) }

  public func loadMore() async throws -> LoadMoreResult { try await database.loadMoreStream(stream: id) }

  /// Returns when the first load of the stream settled.
  public func loaded() async throws { try await database.streamLoaded(stream: id) }
}

private final class Forward: ValueSink, @unchecked Sendable {
  private let continuation: AsyncStream<StreamDelivery>.Continuation
  init(_ continuation: AsyncStream<StreamDelivery>.Continuation) { self.continuation = continuation }
  func delivered(delivery: StreamDelivery) {
    continuation.yield(delivery)
    if case .ended = delivery.value { continuation.finish() }
  }
}

extension Database {
  fileprivate func openValueStream(epoch: UInt64, request: StreamRequest) -> ValueStream {
    let (deliveries, continuation) = AsyncStream<StreamDelivery>.makeStream(bufferingPolicy: .unbounded)
    let id = subscribe(epoch: epoch, request: request, sink: Forward(continuation))
    continuation.onTermination = { [self] _ in cancelStream(stream: id) }
    return ValueStream(id: id, epoch: epoch, deliveries: deliveries, database: self)
  }
}

/// What a screen shows of a stream: a value the screen owns, built only from deliveries. It holds no lock and no reference to the core.
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

  public init() {}

  public var phase: LivePhase? { state?.phase }

  public mutating func apply(_ delivery: StreamDelivery) -> Applied {
    if let epoch, delivery.epoch != epoch { return .ignored }
    epoch = delivery.epoch
    switch delivery.value {
    case let .snapshot(revision, wire, state):
      rows = decode(wire).compactMap { $0 }
      self.revision = revision
      self.state = state
      return .changed
    case let .state(state):
      self.state = state
      return .changed
    case let .ended(reason):
      ended = reason
      return .changed
    case let .delta(base, revision, size, wire, indexes, removed, placed, replaced, _, state):
      guard base == self.revision else { return .resync }
      let carried = decode(wire)
      guard carried.allSatisfy({ $0 != nil }) else { return .resync }
      guard let next = Self.rebuild(rows, carried: zip(carried.compactMap { $0 }, indexes).map { ($0, Int($1)) }, removed: removed, placed: placed, replaced: replaced, size: Int(size)) else { return .resync }
      rows = next
      deltas += 1
      self.revision = revision
      self.state = state
      return .changed
    }
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

  private static func rebuild(_ rows: [Model], carried: [(Model, Int)], removed: [String], placed: [Placement], replaced: [Replacement], size: Int) -> [Model]? {
    var work = rows
    let byId = Dictionary(carried.map { ($0.0.id, $0.0) }, uniquingKeysWith: { _, newer in newer })
    var landed = Set<String>()
    for pair in replaced {
      guard let at = work.firstIndex(where: { $0.id == pair.from }), let model = byId[pair.to] else { return nil }
      work[at] = model
      landed.insert(pair.to)
    }
    if !removed.isEmpty {
      let gone = Set(removed)
      guard work.filter({ gone.contains($0.id) }).count == gone.count else { return nil }
      work.removeAll { gone.contains($0.id) }
    }
    var taken: [String: Model] = [:]
    for place in placed {
      if let at = work.firstIndex(where: { $0.id == place.id }) { taken[place.id] = work.remove(at: at) }
    }
    for place in placed.sorted(by: { $0.index < $1.index }) {
      guard let model = taken[place.id] ?? byId[place.id], Int(place.index) <= work.count else { return nil }
      work.insert(model, at: Int(place.index))
      if taken[place.id] == nil { landed.insert(place.id) }
    }
    for (model, index) in carried {
      guard index < work.count else { return nil }
      if landed.contains(model.id), work[index].id == model.id { continue }
      work[index] = model
    }
    return work.count == size ? work : nil
  }
}

/// The screen model of a stream: Swift Observation over a projection. It reads the stream until it is released, and releasing it ends the stream in the core.
@MainActor @Observable
public final class Projected<Model: DBModel> {
  public private(set) var projection = ListProjection<Model>()
  public var rows: [Model] { projection.rows }
  public var value: Model? { projection.rows.first }
  @ObservationIgnored public let stream: ValueStream
  @ObservationIgnored private var reading: Task<Void, Never>?

  public init(_ stream: ValueStream) {
    self.stream = stream
    reading = Task { [weak self] in
      for await delivery in stream {
        guard let self else { return }
        if self.projection.apply(delivery) == .resync { stream.resync() }
      }
    }
  }

  public func refresh() async throws { try await stream.refresh() }
  public func loadMore() async throws -> LoadMoreResult { try await stream.loadMore() }
  public func loaded() async throws { try await stream.loaded() }

  isolated deinit { reading?.cancel() }
}

/// The streams and commands of one signed-in account. The root of the app makes one after the sign-in and recreates its screens when the account changes: a scope whose session ended opens streams that end at once.
public struct AccountScope: Sendable {
  public let epoch: UInt64
  private let database: Database

  public init() throws {
    let database = try DB.database
    self.database = database
    epoch = database.sessionEpoch()
  }

  public func stream<Model: DBModel>(_ query: Query<Model>) throws -> ValueStream {
    database.openValueStream(epoch: epoch, request: .query(queryJson: try query.json))
  }

  public func entity<Model: DBModel>(_: Model.Type, _ id: String) -> ValueStream {
    database.openValueStream(epoch: epoch, request: .entity(model: Model.modelName, id: id))
  }

  public func singleton<Model: DBSingleton>(_: Model.Type) throws -> ValueStream { try stream(Model.singleton) }

  /// Writes the model locally and queues it for the server. It works without a network and after the app closed. Throws `Interrupted` when the session of this scope ended.
  @concurrent public func send<Model: DBModel>(_: Model.Type, values: [String: JSONValue], args: [String: JSONValue] = [:]) async throws -> Sent<Model> {
    let receipt = try await database.send(request: CommandRequest(epoch: epoch, model: Model.modelName, valuesJson: JSONValue.object(values).text, argsJson: JSONValue.object(args).text))
    return Sent(row: try Model.decode(receipt.row), receipt: receipt, database: database)
  }
}

public struct Sent<Model: DBModel>: Sendable {
  public let row: Model
  fileprivate let receipt: CommandReceipt
  fileprivate let database: Database

  public func status() throws -> CommandStatus { try database.commandStatus(receipt: receipt) }

  /// Waits for the one outcome: delivered, failed, or ended with the session.
  @concurrent public func outcome() async throws -> CommandStatus { try await database.commandOutcome(receipt: receipt) }
}

extension DB {
  public static func account() throws -> AccountScope { try AccountScope() }

  public static func streamCensus() throws -> StreamCensus { try database.streamCensus() }
}
