import Foundation
import Observation
import SwiftUI

final class ChangeListener: QueryListener, @unchecked Sendable {
  let body: @Sendable (LiveChange) -> Void
  init(_ body: @escaping @Sendable (LiveChange) -> Void) { self.body = body }
  func changed(change: LiveChange) { body(change) }
}

final class Latest<Value: Sendable>: @unchecked Sendable {
  private enum Put {
    case done
    case resume(CheckedContinuation<Value?, Never>)
    case merge(Value, Int)
  }

  private let lock = NSLock()
  private var pending: Value?
  private var version = 0
  private var waiter: CheckedContinuation<Value?, Never>?
  private var closed = false
  private let merge: (@Sendable (Value, Value) -> Value)?

  init(merge: (@Sendable (Value, Value) -> Value)?) { self.merge = merge }

  var hasPending: Bool { lock.withLock { pending != nil } }

  func put(_ value: Value) {
    while true {
      var retired: Value?
      let step = lock.withLock { () -> Put in
        guard !closed else { return .done }
        if let waiter { self.waiter = nil; return .resume(waiter) }
        guard let older = pending, merge != nil else { retired = pending; pending = value; version += 1; return .done }
        return .merge(older, version)
      }
      withExtendedLifetime(retired) {}
      switch step {
      case .done: return
      case .resume(let waiting): waiting.resume(returning: value); return
      case .merge(let older, let seen):
        let combined = merge?(older, value) ?? value
        let landed = lock.withLock { () -> Bool in
          guard !closed else { return true }
          guard version == seen else { return false }
          retired = pending
          pending = combined
          version += 1
          return true
        }
        withExtendedLifetime(retired) {}
        if landed { return }
      }
    }
  }

  func next() async -> Value? {
    await withCheckedContinuation { (continuation: CheckedContinuation<Value?, Never>) in
      let ready: Value?? = lock.withLock {
        if let value = pending { pending = nil; version += 1; return .some(value) }
        if closed { return .some(nil) }
        waiter = continuation
        return nil
      }
      if let ready { continuation.resume(returning: ready) }
    }
  }

  func close() {
    let (waiting, dropped) = lock.withLock { () -> (CheckedContinuation<Value?, Never>?, Value?) in
      closed = true
      version += 1
      defer { waiter = nil; pending = nil }
      return (waiter, pending)
    }
    waiting?.resume(returning: nil)
    withExtendedLifetime(dropped) {}
  }
}

final class Attachment: @unchecked Sendable {
  private let lock = NSLock()
  private var cancelAction: (@Sendable () -> Void)?
  init(_ cancel: @escaping @Sendable () -> Void) { cancelAction = cancel }
  func cancel() { lock.withLock { let action = cancelAction; cancelAction = nil; return action }?() }
  deinit { cancel() }
}

public struct LiveStream<Value: Sendable>: AsyncSequence, Sendable {
  public typealias Element = Value
  let receive: @Sendable () async -> Value?
  let cancel: @Sendable () -> Void
  let attachment: Attachment?
  let pending: @Sendable () -> Bool
  private let iterator: (@Sendable () -> AsyncIterator)?
  var hasPending: Bool { pending() }

  init(latest: Latest<Value>, attachment: Attachment?) {
    iterator = nil
    receive = { await latest.next() }
    cancel = { latest.close(); attachment?.cancel() }
    pending = { latest.hasPending }
    self.attachment = attachment
  }

  init(receive: @escaping @Sendable () async -> Value?, cancel: @escaping @Sendable () -> Void) {
    iterator = nil
    self.receive = receive
    pending = { false }
    let attachment = Attachment(cancel)
    self.attachment = attachment
    self.cancel = { attachment.cancel() }
  }

  static var ended: LiveStream { LiveStream(receive: { nil }, cancel: {}) }

  init(first: LiveStream, reopen: @escaping @Sendable () async -> LiveStream) {
    let readers = StreamReaders(first)
    receive = { nil }
    cancel = { readers.cancel() }
    attachment = nil
    pending = { false }
    iterator = { AsyncIterator(stream: readers.take(), open: reopen) }
  }

  public struct AsyncIterator: AsyncIteratorProtocol {
    var stream: LiveStream?
    var open: (@Sendable () async -> LiveStream)? = nil
    public mutating func next() async -> Value? {
      guard !Task.isCancelled else { stream?.cancel(); return nil }
      if stream == nil, let open {
        stream = await open()
        self.open = nil
      }
      guard let stream else { return nil }
      return await withTaskCancellationHandler { await stream.receive() } onCancel: { stream.cancel() }
    }
  }

  public func makeAsyncIterator() -> AsyncIterator { iterator?() ?? AsyncIterator(stream: self) }

}

private final class StreamReaders<Value: Sendable>: @unchecked Sendable {
  private let lock = NSLock()
  private var first: LiveStream<Value>?
  private weak var attachment: Attachment?

  init(_ first: LiveStream<Value>) { self.first = first; attachment = first.attachment }
  func take() -> LiveStream<Value>? { lock.withLock { defer { first = nil }; return first } }
  func cancel() {
    let (held, live) = lock.withLock { () -> (LiveStream<Value>?, Attachment?) in
      defer { first = nil }
      return (first, attachment)
    }
    live?.cancel()
    withExtendedLifetime(held) {}
  }
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
  private(set) var task: Task<Void, Never>?

  init(_ source: LiveSource<Value>) {
    value = source.initial
    task = Task { [weak self] in
      do {
        for await next in try await source.stream() { self?.value = next }
      } catch is CancellationError {
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
  public var projectedValue: Error? { box.failure }
}

extension Live {
  public init<Model: DBModel>(_ query: Query<Model>) where Value == [Model] { self.init(query.live) }
  public init<Target: DBModel>(_ related: RelatedQuery<Target>) where Value == [Target] { self.init(related.live) }
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
  public init<Target: DBModel>(_ related: RelatedQuery<Target>, @ViewBuilder content: @escaping ([Target]) -> Content) where Value == [Target] {
    self.init(related.live, content: content)
  }
  public var body: some View { content(value) }
}

@MainActor
@Observable
public final class LiveList<Model: DBModel> {
  public private(set) var snapshot = Snapshot<Model>(rows: [], replaced: [:])
  public private(set) var rows: [Model] = []
  public private(set) var ids: [String] = []
  public private(set) var replaced: [String: String] = [:]
  @ObservationIgnored private let reload: @Sendable () async throws -> Void
  @ObservationIgnored private let more: @Sendable () async throws -> LoadMoreResult
  @ObservationIgnored let updates = ListUpdates<Model>()
  @ObservationIgnored private(set) var tasks: [Task<Void, Never>] = []
  @ObservationIgnored private var initialLoad: Task<Void, Error>?

  public convenience init(_ query: Query<Model>, refreshOnConnect: Bool = false) {
    self.init(refresh: { try await query.refresh() }, loadMore: { try await query.loadMore() }, refreshOnConnect: refreshOnConnect) { connected in
      query.snapshots(refreshOnConnect: refreshOnConnect, connected: connected)
    }
  }

  public convenience init(_ related: RelatedQuery<Model>, refreshOnConnect: Bool = false) {
    self.init(refresh: related.refresh, loadMore: related.loadMore, refreshOnConnect: refreshOnConnect) { connected in
      related.snapshots(refreshOnConnect: refreshOnConnect, connected: connected)
    }
  }

  private convenience init(
    refresh: @escaping @Sendable () async throws -> Void, loadMore: @escaping @Sendable () async throws -> LoadMoreResult, refreshOnConnect: Bool,
    snapshots open: (@escaping @Sendable (Result<LiveSubscription, Error>) -> Void) -> LiveSource<Snapshot<Model>>
  ) {
    let (connections, continuation) = AsyncThrowingStream<LiveSubscription, Error>.makeStream(bufferingPolicy: .bufferingNewest(1))
    let snapshots = open { result in
      switch result {
      case .success(let subscription): continuation.yield(subscription); continuation.finish()
      case .failure(let error): continuation.finish(throwing: error)
      }
    }
    self.init(refresh: refresh, loadMore: loadMore, snapshots: snapshots)
    initialLoad = Task {
      for try await subscription in connections { try await subscription.waitForInitialLoad() }
    }
  }

  convenience init(_ query: Query<Model>, snapshots: LiveSource<Snapshot<Model>>) {
    self.init(refresh: { try await query.refresh() }, loadMore: { try await query.loadMore() }, snapshots: snapshots)
  }

  private init(refresh: @escaping @Sendable () async throws -> Void, loadMore: @escaping @Sendable () async throws -> LoadMoreResult, snapshots: LiveSource<Snapshot<Model>>) {
    reload = refresh
    more = loadMore
    let updates = updates
    tasks = [
      Task.detached { [weak self] in
        let stream: LiveStream<Snapshot<Model>>
        do { stream = try await snapshots.stream() } catch is CancellationError { return } catch {
          await self?.fail(error)
          return
        }
        for await next in stream {
          let ids = next.reordered ? next.rows.map(\.id) : nil
          updates.publish(next)
          guard let self else { return }
          await self.show(next, ids: ids)
        }
      }
    ]
  }

  private func fail(_ error: Error) {
    snapshot = Snapshot(rows: [], replaced: [:], phase: .failed, error: error, errorKind: "subscription")
  }

  private func show(_ next: Snapshot<Model>, ids: [String]?) {
    replaced = next.replaced
    rows = next.rows
    if let ids { self.ids = ids }
    snapshot = next
  }

  public func takeUpdate() -> ListUpdate<Model> { updates.take() }

  public func refresh() async throws { try await reload() }

  public func waitForInitialLoad() async throws {
    let load = initialLoad
    try await withTaskCancellationHandler {
      try await load?.value
    } onCancel: {
      load?.cancel()
    }
  }

  @discardableResult
  public func loadMore() async throws -> LoadMoreResult { try await more() }

  isolated deinit { tasks.forEach { $0.cancel() }; initialLoad?.cancel() }
}
