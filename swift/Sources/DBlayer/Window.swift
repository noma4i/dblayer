import Foundation
import Observation

public struct WindowEdges: Sendable, Equatable {
  public let hasBefore: Bool
  public let hasAfter: Bool
  public let total: Int?

  init(_ edge: LiveWindowEdge) {
    hasBefore = edge.hasBefore
    hasAfter = edge.hasAfter
    total = edge.total.map(Int.init)
  }
}

struct WeakSubscription: @unchecked Sendable {
  weak var value: LiveSubscription?
}

final class WindowControl: @unchecked Sendable {
  private let lock = NSLock()
  private var subscriptions: [WeakSubscription] = []
  private var rows: Int
  private let tail: Int
  let max: Int?

  init(tail: Int, max: Int?) { rows = tail; self.tail = tail; self.max = max }

  var size: Int { lock.withLock { rows } }

  func attach(_ next: LiveSubscription) {
    lock.withLock {
      subscriptions.removeAll { $0.value == nil }
      subscriptions.append(WeakSubscription(value: next))
    }
  }
  func shown(_ count: Int) {
    lock.withLock {
      rows = Swift.max(rows, count)
      if let max, rows > max { rows = tail }
    }
  }

  func grow(before: Int, after: Int) {
    let current = lock.withLock { subscriptions.compactMap(\.value) }
    for subscription in current {
      if before > 0 { subscription.expandBefore(rows: UInt32(clamping: before)) }
      if after > 0 { subscription.expandAfter(rows: UInt32(clamping: after)) }
    }
  }
}

public struct Window<Model: DBModel>: Sendable {
  let query: Query<Model>
  let control: WindowControl

  init(query: Query<Model>, tail: Int, max: Int? = nil) {
    self.query = query
    control = WindowControl(tail: tail, max: max)
  }

  /// Delivers the rows of the window with `edits` in window positions, the edges and the total of the query. A row outside the window is not decoded.
  public var snapshots: LiveSource<Snapshot<Model>> {
    let query = query, control = control
    return listSource(initial: Snapshot(rows: [], replaced: [:], reordered: false), merge: { Snapshot.merging($0, $1) }, open: { listener in
      let subscription = try DB.database.liveWindow(queryJson: query.json, tail: UInt32(clamping: control.size), max: control.max.map { UInt32(clamping: $0) }, listener: listener)
      control.attach(subscription)
      return subscription
    }) { (applied: ListApplied<Model>, change) in
      control.shown(applied.rows.count)
      return Snapshot(applied, change)
    }
  }

  public func expand(before rows: Int) { control.grow(before: rows, after: 0) }
  public func expand(after rows: Int) { control.grow(before: 0, after: rows) }

  public func related<Target: DBModel>(_ association: Association<Model, Target>) -> RelatedWindow<Target> {
    RelatedWindow(name: association.name, sourceJson: { try query.json }, control: control)
  }
}

public struct RelatedWindow<Target: DBModel>: Sendable {
  let name: String
  let sourceJson: @Sendable () throws -> String
  let control: WindowControl

  private func connect(_ listener: QueryListener) throws -> LiveSubscription {
    let subscription = try DB.database.liveWindowRelated(queryJson: try sourceJson(), tail: UInt32(clamping: control.size), max: control.max.map { UInt32(clamping: $0) }, name: name, listener: listener)
    control.attach(subscription)
    return subscription
  }

  public var live: LiveSource<[Target]> {
    listSource(initial: [], open: { listener in try connect(listener) }) { (applied: ListApplied<Target>, _) in applied.rows }
  }

  public var snapshots: LiveSource<Snapshot<Target>> {
    listSource(initial: Snapshot(rows: [], replaced: [:], reordered: false), merge: { Snapshot.merging($0, $1) }, open: { listener in try connect(listener) }) { (applied: ListApplied<Target>, change) in Snapshot(applied, change) }
  }
}

@MainActor
@Observable
public final class LiveWindow<Model: DBModel> {
  public private(set) var snapshot = Snapshot<Model>(rows: [], replaced: [:])
  public var rows: [Model] { snapshot.rows }
  public var ids: [String] { snapshot.rows.map(\.id) }
  public var replaced: [String: String] { snapshot.replaced }
  public var hasBefore: Bool { snapshot.window?.hasBefore ?? false }
  public var hasAfter: Bool { snapshot.window?.hasAfter ?? false }
  public var total: Int? { snapshot.window?.total }
  public var failure: (any Error)? { snapshot.error }
  @ObservationIgnored private let window: Window<Model>
  @ObservationIgnored let updates = ListUpdates<Model>()
  @ObservationIgnored private var task: Task<Void, Never>?

  public convenience init(_ window: Window<Model>) { self.init(window, snapshots: window.snapshots) }

  init(_ window: Window<Model>, snapshots: LiveSource<Snapshot<Model>>) {
    self.window = window
    let updates = updates
    task = Task.detached { [weak self] in
      let stream: LiveStream<Snapshot<Model>>
      do { stream = try await snapshots.stream() } catch { await self?.fail(error); return }
      for await snapshot in stream {
        updates.publish(snapshot)
        guard let self else { return }
        await self.show(snapshot)
      }
    }
  }

  private func fail(_ error: Error) {
    show(Snapshot(rows: snapshot.rows, replaced: snapshot.replaced, window: snapshot.window, phase: snapshot.rows.isEmpty ? .failed : .loaded, error: error, errorKind: "subscription"))
  }

  private func show(_ snapshot: Snapshot<Model>) {
    self.snapshot = snapshot
  }

  public func takeUpdate() -> ListUpdate<Model> { updates.take() }

  public func expand(before rows: Int) { window.expand(before: rows) }
  public func expand(after rows: Int) { window.expand(after: rows) }

  isolated deinit { task?.cancel() }
}
