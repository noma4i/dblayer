import Foundation
import os

private let decodeLog = Logger(subsystem: "DBlayer", category: "decode")

struct ListApplied<Model: DBModel> {
  let rows: [Model]
  let reordered: Bool
  let desynced: Bool
}

final class ListFeed<Model: DBModel>: @unchecked Sendable {
  private let lock = NSLock()
  private let decoder = JSONDecoder()
  private var rows: [Model] = []
  private var decoded = 0
  private var dropped = 0
  private var desynced = 0

  var decodedRows: Int { lock.withLock { decoded } }
  var desyncs: Int { lock.withLock { desynced } }

  func reset() { lock.withLock { rows = []; dropped = 0 } }

  func apply(_ change: LiveChange) -> ListApplied<Model> {
    lock.withLock {
      if change.error != nil { return ListApplied(rows: rows, reordered: false, desynced: false) }
      var failed = 0
      let carried: [(row: Model, index: Int)] = change.rows.enumerated().compactMap { position, text in
        decoded += 1
        do {
          let row = try decoder.decode(Model.self, from: Data(text.utf8))
          return (row, change.full ? position : Int(change.rowIndexes[position]))
        } catch {
          failed += 1
          decodeLog.error("Row of \(Model.modelName, privacy: .public) failed to decode: \(String(describing: error), privacy: .public)")
          return nil
        }
      }
      if change.full {
        rows = carried.map(\.row)
        dropped = failed
        return ListApplied(rows: rows, reordered: true, desynced: false)
      }
      let reordered = !change.removed.isEmpty || !change.placed.isEmpty || !change.replaced.isEmpty
      guard failed == 0, applyDelta(change, carried) else {
        desynced += 1
        return ListApplied(rows: rows, reordered: reordered, desynced: true)
      }
      return ListApplied(rows: rows, reordered: reordered, desynced: false)
    }
  }

  private func applyDelta(_ change: LiveChange, _ carried: [(row: Model, index: Int)]) -> Bool {
    let byId = Dictionary(carried.map { ($0.row.id, $0.row) }, uniquingKeysWith: { _, newer in newer })
    for pair in change.replaced {
      guard let at = rows.firstIndex(where: { $0.id == pair.from }), let row = byId[pair.to] else { return false }
      rows[at] = row
    }
    let before = rows.count
    if !change.removed.isEmpty {
      let gone = Set(change.removed)
      rows.removeAll { gone.contains($0.id) }
      if before - rows.count != gone.count { return false }
    }
    var taken: [String: Model] = [:]
    for place in change.placed {
      if let at = rows.firstIndex(where: { $0.id == place.id }) { taken[place.id] = rows.remove(at: at) }
    }
    for place in change.placed {
      guard let row = taken[place.id] ?? byId[place.id], Int(place.index) <= rows.count else { return false }
      rows.insert(row, at: Int(place.index))
    }
    for (row, index) in carried {
      guard index < rows.count else { return false }
      rows[index] = row
    }
    return rows.count == Int(change.size) - dropped
  }
}

final class ListSession<Model: DBModel, Value: Sendable>: @unchecked Sendable {
  private let lock = NSLock()
  private let feed = ListFeed<Model>()
  private let open: @Sendable (QueryListener) throws -> @Sendable () -> Void
  private let emit: @Sendable (Value) -> Void
  private let map: @Sendable (ListApplied<Model>, LiveChange) -> Value
  private var cancelSubscription: (@Sendable () -> Void)?
  private var cancelled = false
  private var generation = 0
  private var reopens = 0

  var reopened: Int { lock.withLock { reopens } }
  var desyncs: Int { feed.desyncs }

  init(
    open: @escaping @Sendable (QueryListener) throws -> @Sendable () -> Void,
    emit: @escaping @Sendable (Value) -> Void,
    map: @escaping @Sendable (ListApplied<Model>, LiveChange) -> Value
  ) {
    self.open = open; self.emit = emit; self.map = map
  }

  func start() throws {
    let current = lock.withLock { generation }
    let made = try open(ChangeListener { [weak self] change in self?.receive(change, generation: current) })
    let keep = lock.withLock { () -> Bool in
      guard !cancelled, generation == current else { return false }
      cancelSubscription = made
      return true
    }
    if !keep { made() }
  }

  func cancel() {
    let current = lock.withLock { () -> (@Sendable () -> Void)? in
      cancelled = true
      defer { cancelSubscription = nil }
      return cancelSubscription
    }
    current?()
  }

  private func receive(_ change: LiveChange, generation current: Int) {
    guard lock.withLock({ !cancelled && generation == current }) else { return }
    let applied = feed.apply(change)
    if applied.desynced { reopen(from: current); return }
    emit(map(applied, change))
  }

  private func reopen(from current: Int) {
    let old = lock.withLock { () -> (@Sendable () -> Void)?? in
      guard !cancelled, generation == current else { return nil }
      generation += 1
      reopens += 1
      defer { cancelSubscription = nil }
      return .some(cancelSubscription)
    }
    guard let old else { return }
    feed.reset()
    old?()
    Task.detached(priority: .userInitiated) { [self] in try? start() }
  }
}

extension LiveChange {
  var replacedIds: [String: String] {
    Dictionary(replaced.map { ($0.to, $0.from) }, uniquingKeysWith: { first, _ in first })
  }
}
