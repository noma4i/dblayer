import Foundation
import Observation
import SwiftUI

final class ChangeListener: QueryListener, @unchecked Sendable {
  let body: @Sendable (LiveChange) -> Void
  init(_ body: @escaping @Sendable (LiveChange) -> Void) { self.body = body }
  func changed(change: LiveChange) { body(change) }
}

/// The newest value waits for a slow reader. Values that arrive meanwhile are merged into it, so the queue never grows.
final class Latest<Value: Sendable>: @unchecked Sendable {
  private let lock = NSLock()
  private var pending: Value?
  private var waiter: CheckedContinuation<Value?, Never>?
  private var closed = false
  private let merge: (@Sendable (Value, Value) -> Value)?

  init(merge: (@Sendable (Value, Value) -> Value)?) { self.merge = merge }

  var hasPending: Bool { lock.withLock { pending != nil } }

  func put(_ value: Value) {
    let waiting: CheckedContinuation<Value?, Never>? = lock.withLock {
      guard !closed else { return nil }
      if let waiter { self.waiter = nil; return waiter }
      pending = pending.flatMap { older in merge.map { $0(older, value) } } ?? value
      return nil
    }
    waiting?.resume(returning: value)
  }

  func next() async -> Value? {
    await withCheckedContinuation { (continuation: CheckedContinuation<Value?, Never>) in
      let ready: Value?? = lock.withLock {
        if let value = pending { pending = nil; return .some(value) }
        if closed { return .some(nil) }
        waiter = continuation
        return nil
      }
      if let ready { continuation.resume(returning: ready) }
    }
  }

  func close() {
    let waiting = lock.withLock { () -> CheckedContinuation<Value?, Never>? in
      closed = true
      pending = nil
      defer { waiter = nil }
      return waiter
    }
    waiting?.resume(returning: nil)
  }
}

/// Cancels the core subscription when the stream is dropped, read to its end or cancelled.
final class Attachment: @unchecked Sendable {
  private let lock = NSLock()
  private var cancelAction: (@Sendable () -> Void)?
  init(_ cancel: @escaping @Sendable () -> Void) { cancelAction = cancel }
  func cancel() { lock.withLock { let action = cancelAction; cancelAction = nil; return action }?() }
  deinit { cancel() }
}

/// The values of a live source in order. The subscription ends when the stream and its iterators are dropped or the reading task is cancelled.
public struct LiveStream<Value: Sendable>: AsyncSequence, Sendable {
  public typealias Element = Value
  let latest: Latest<Value>
  let attachment: Attachment?

  static var ended: LiveStream {
    let latest = Latest<Value>(merge: nil)
    latest.close()
    return LiveStream(latest: latest, attachment: nil)
  }

  public struct AsyncIterator: AsyncIteratorProtocol {
    let latest: Latest<Value>
    let attachment: Attachment?
    public mutating func next() async -> Value? {
      await withTaskCancellationHandler { await latest.next() } onCancel: { latest.close(); attachment?.cancel() }
    }
  }

  public func makeAsyncIterator() -> AsyncIterator { AsyncIterator(latest: latest, attachment: attachment) }
}

public struct LiveSource<Value: Sendable>: Sendable {
  public let initial: Value
  let subscribe: @Sendable (@escaping @Sendable (Value) -> Void) throws -> @Sendable () -> Void
  let merge: (@Sendable (Value, Value) -> Value)?

  init(initial: Value, merge: (@Sendable (Value, Value) -> Value)? = nil, subscribe: @escaping @Sendable (@escaping @Sendable (Value) -> Void) throws -> @Sendable () -> Void) {
    self.initial = initial; self.merge = merge; self.subscribe = subscribe
  }

  init(initial: Value, merge: (@Sendable (Value, Value) -> Value)? = nil, attach: @escaping @Sendable (QueryListener) throws -> LiveSubscription, map: @escaping @Sendable (LiveChange) -> Value) {
    self.init(initial: initial, merge: merge) { emit in
      let subscription = try attach(ChangeListener { emit(map($0)) })
      return { subscription.cancel() }
    }
  }

  /// The one way to read a live value. The subscription and its first read run off the caller's thread. Cancelling the caller before, during or after the attach leaves no subscription. A slow reader gets the newest value, merged with the ones it missed.
  public func stream() async -> LiveStream<Value> {
    guard !Task.isCancelled else { return .ended }
    let latest = Latest<Value>(merge: merge)
    let attached = await Task.detached(priority: .userInitiated) { [subscribe] in try? subscribe { latest.put($0) } }.value
    guard let attached else { return .ended }
    let attachment = Attachment(attached)
    guard !Task.isCancelled else { attachment.cancel(); return .ended }
    return LiveStream(latest: latest, attachment: attachment)
  }

  public func current() async -> Value {
    for await value in await stream() { return value }
    return initial
  }
}

@MainActor
final class LiveBox<Value: Sendable>: ObservableObject {
  @Published var value: Value
  private var task: Task<Void, Never>?

  init(_ source: LiveSource<Value>) {
    value = source.initial
    task = Task { [weak self] in
      for await next in await source.stream() { self?.value = next }
    }
  }

  deinit { task?.cancel() }
}

@MainActor
@propertyWrapper
public struct Live<Value: Sendable>: DynamicProperty {
  @StateObject private var box: LiveBox<Value>
  public init(_ source: LiveSource<Value>) { _box = StateObject(wrappedValue: LiveBox(source)) }
  public var wrappedValue: Value { box.value }
}

extension Live {
  public init<Model: DBModel>(_ query: Query<Model>) where Value == [Model] { self.init(query.live) }
}

public struct LiveView<Value: Sendable, Content: View>: View {
  @Live private var value: Value
  private let content: (Value) -> Content
  public init(_ source: LiveSource<Value>, @ViewBuilder content: @escaping (Value) -> Content) {
    _value = Live(source); self.content = content
  }
  public init<Model: DBModel>(_ query: Query<Model>, @ViewBuilder content: @escaping ([Model]) -> Content) where Value == [Model] {
    self.init(query.live, content: content)
  }
  public var body: some View { content(value) }
}

struct ListUpdate<Model: DBModel>: Sendable {
  var snapshot: Snapshot<Model>?
  var state: LoadState?

  static func merging(_ older: ListUpdate, _ newer: ListUpdate) -> ListUpdate {
    ListUpdate(snapshot: older.snapshot.flatMap { old in newer.snapshot.map { Snapshot.merging(old, $0) } ?? old } ?? newer.snapshot, state: newer.state ?? older.state)
  }
}

/// A live query for view models: the rows, the load state of the relation, and the calls that move them.
@MainActor
@Observable
public final class LiveList<Model: DBModel> {
  public private(set) var rows: [Model] = []
  public private(set) var ids: [String] = []
  /// The ids that the last delivery of `rows` replaced: real id to temporary id.
  public private(set) var replaced: [String: String] = [:]
  public private(set) var state = LoadState()
  public private(set) var failure: String?
  @ObservationIgnored private let query: Query<Model>
  @ObservationIgnored private var pending: [ListEdit<Model>]?
  @ObservationIgnored private var tasks: [Task<Void, Never>] = []
  @ObservationIgnored private var initialLoad: Task<Void, Error>?

  public convenience init(_ query: Query<Model>, refreshOnConnect: Bool = false) {
    let (connections, continuation) = AsyncThrowingStream<LiveSubscription, Error>.makeStream(bufferingPolicy: .bufferingNewest(1))
    let snapshots = query.snapshots(refreshOnConnect: refreshOnConnect) { result in
      switch result {
      case .success(let subscription): continuation.yield(subscription); continuation.finish()
      case .failure(let error): continuation.finish(throwing: error)
      }
    }
    self.init(query, snapshots: snapshots, state: query.state)
    initialLoad = Task {
      for try await subscription in connections { try await subscription.waitForInitialLoad() }
    }
  }

  init(_ query: Query<Model>, snapshots: LiveSource<Snapshot<Model>>, state: LiveSource<LoadState>) {
    self.query = query
    let source = LiveSource<ListUpdate<Model>>(initial: ListUpdate(), merge: { ListUpdate.merging($0, $1) }) { emit in
      let rows = try snapshots.subscribe { emit(ListUpdate(snapshot: $0)) }
      let load = try? state.subscribe { emit(ListUpdate(state: $0)) }
      return { rows(); load?() }
    }
    tasks = [
      Task { [weak self] in
        for await next in await source.stream() {
          guard let self else { return }
          if let snapshot = next.snapshot {
            replaced = snapshot.replaced
            if snapshot.failure != failure { failure = snapshot.failure }
            pending = ListEdit.composing(pending, snapshot.edits, within: snapshot.rows.count)
            rows = snapshot.rows
            if snapshot.reordered { ids = snapshot.rows.map(\.id) }
          }
          if let loaded = next.state { self.state = loaded }
        }
      }
    ]
  }

  public func takeEdits() -> [ListEdit<Model>]? {
    defer { pending = [] }
    return pending
  }

  public func refresh() async throws { try await query.refresh() }

  public func waitForInitialLoad() async throws {
    let load = initialLoad
    try await withTaskCancellationHandler {
      try await load?.value
    } onCancel: {
      load?.cancel()
    }
  }

  @discardableResult
  public func loadMore() async throws -> Bool { try await query.loadMore() }

  isolated deinit { tasks.forEach { $0.cancel() }; initialLoad?.cancel() }
}
