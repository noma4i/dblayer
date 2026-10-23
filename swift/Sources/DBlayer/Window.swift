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

final class WindowControl: @unchecked Sendable {
  private let lock = NSLock()
  private var subscription: LiveSubscription?
  private var rows: Int

  init(tail: Int) { rows = tail }

  var size: Int { lock.withLock { rows } }

  func attach(_ next: LiveSubscription) { lock.withLock { subscription = next } }
  func shown(_ count: Int) { lock.withLock { rows = max(rows, count) } }

  func grow(before: Int, after: Int) {
    let current = lock.withLock { subscription }
    if before > 0 { current?.expandBefore(rows: UInt32(clamping: before)) }
    if after > 0 { current?.expandAfter(rows: UInt32(clamping: after)) }
  }
}

public struct Window<Model: DBModel>: Sendable {
  let query: Query<Model>
  let control: WindowControl

  init(query: Query<Model>, tail: Int) {
    self.query = query
    control = WindowControl(tail: tail)
  }

  /// Delivers the rows of the window with `edits` in window positions, the edges and the total of the query. A row outside the window is not decoded.
  public var snapshots: LiveSource<Snapshot<Model>> {
    let json = query.json, control = control
    return listSource(initial: Snapshot(rows: [], replaced: [:], reordered: false), merge: { Snapshot.merging($0, $1) }, open: { listener in
      let subscription = try DB.database.liveWindow(queryJson: json, tail: UInt32(clamping: control.size), listener: listener)
      control.attach(subscription)
      return subscription
    }) { (applied: ListApplied<Model>, change) in
      control.shown(applied.rows.count)
      return Snapshot(applied, change)
    }
  }

  public func expand(before rows: Int) { control.grow(before: rows, after: 0) }
  public func expand(after rows: Int) { control.grow(before: 0, after: rows) }
}

@MainActor
@Observable
public final class LiveWindow<Model: DBModel> {
  public private(set) var rows: [Model] = []
  public private(set) var ids: [String] = []
  public private(set) var replaced: [String: String] = [:]
  public private(set) var hasBefore = false
  public private(set) var hasAfter = false
  public private(set) var total: Int?
  public private(set) var failure: String?
  @ObservationIgnored private let window: Window<Model>
  @ObservationIgnored private var pending: [ListEdit<Model>]?
  @ObservationIgnored private var task: Task<Void, Never>?

  public init(_ window: Window<Model>) {
    self.window = window
    task = Task { [weak self] in
      let stream: LiveStream<Snapshot<Model>>
      do { stream = try await window.snapshots.stream() } catch { self?.failure = error.localizedDescription; return }
      for await snapshot in stream {
        guard let self else { return }
        replaced = snapshot.replaced
        if snapshot.error != failure { failure = snapshot.error }
        if let edges = snapshot.window { (hasBefore, hasAfter, total) = (edges.hasBefore, edges.hasAfter, edges.total) }
        pending = ListEdit.composing(pending, snapshot.edits, within: snapshot.rows.count)
        rows = snapshot.rows
        if snapshot.reordered { ids = snapshot.rows.map(\.id) }
      }
    }
  }

  public func takeEdits() -> [ListEdit<Model>]? {
    defer { pending = [] }
    return pending
  }

  public func expand(before rows: Int) { window.expand(before: rows) }
  public func expand(after rows: Int) { window.expand(after: rows) }

  isolated deinit { task?.cancel() }
}
