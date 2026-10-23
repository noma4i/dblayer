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
  let receive: @Sendable () async -> Value?
  let cancel: @Sendable () -> Void
  let attachment: Attachment?
  let pending: @Sendable () -> Bool
  var hasPending: Bool { pending() }

  init(latest: Latest<Value>, attachment: Attachment?) {
    receive = { await latest.next() }
    cancel = { latest.close(); attachment?.cancel() }
    pending = { latest.hasPending }
    self.attachment = attachment
  }

  init(receive: @escaping @Sendable () async -> Value?, cancel: @escaping @Sendable () -> Void) {
    self.receive = receive
    pending = { false }
    let attachment = Attachment(cancel)
    self.attachment = attachment
    self.cancel = { attachment.cancel() }
  }

  static var ended: LiveStream { LiveStream(receive: { nil }, cancel: {}) }

  public struct AsyncIterator: AsyncIteratorProtocol {
    let stream: LiveStream
    public mutating func next() async -> Value? {
      let stream = stream
      return await withTaskCancellationHandler { await stream.receive() } onCancel: { stream.cancel() }
    }
  }

  public func makeAsyncIterator() -> AsyncIterator { AsyncIterator(stream: self) }

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
  public func stream() async throws -> LiveStream<Value> {
    guard !Task.isCancelled else { return .ended }
    let latest = Latest<Value>(merge: merge)
    let attached = try await Task.detached(priority: .userInitiated) { [subscribe] in try subscribe { latest.put($0) } }.value
    let attachment = Attachment(attached)
    guard !Task.isCancelled else { attachment.cancel(); return .ended }
    return LiveStream(latest: latest, attachment: attachment)
  }

  public func current() async throws -> Value {
    for await value in try await stream() { return value }
    return initial
  }
}

@MainActor
final class LiveBox<Value: Sendable>: ObservableObject {
  @Published var value: Value
  @Published var failure: Error?
  private var task: Task<Void, Never>?

  init(_ source: LiveSource<Value>) {
    value = source.initial
    task = Task { [weak self] in
      do {
        for await next in try await source.stream() { self?.value = next }
      } catch { self?.failure = error }
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
  /// The error that ended the subscription, nil while it runs.
  public var projectedValue: Error? { box.failure }
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

/// A live query for view models: the newest snapshot of the list and the calls that move it.
@MainActor
@Observable
public final class LiveList<Model: DBModel> {
  public private(set) var snapshot = Snapshot<Model>(rows: [], replaced: [:])
  public private(set) var rows: [Model] = []
  public private(set) var ids: [String] = []
  /// The ids that the last delivery of `rows` replaced: real id to temporary id.
  public private(set) var replaced: [String: String] = [:]
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
    self.init(query, snapshots: snapshots)
    initialLoad = Task {
      for try await subscription in connections { try await subscription.waitForInitialLoad() }
    }
  }

  init(_ query: Query<Model>, snapshots: LiveSource<Snapshot<Model>>) {
    self.query = query
    tasks = [
      Task { [weak self] in
        let stream: LiveStream<Snapshot<Model>>
        do { stream = try await snapshots.stream() } catch {
          self?.snapshot = Snapshot(rows: [], replaced: [:], phase: .failed, error: error.localizedDescription, errorKind: "subscription")
          return
        }
        for await next in stream {
          guard let self else { return }
          replaced = next.replaced
          pending = ListEdit.composing(pending, next.edits, within: next.rows.count)
          rows = next.rows
          if next.reordered { ids = next.rows.map(\.id) }
          snapshot = next
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
  public func loadMore() async throws -> LoadMoreResult { try await query.loadMore() }

  isolated deinit { tasks.forEach { $0.cancel() }; initialLoad?.cancel() }
}
