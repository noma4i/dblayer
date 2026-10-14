import Foundation
import os

private let decodeLog = Logger(subsystem: "DBlayer", category: "decode")

struct ListApplied<Model: DBModel> {
  let rows: [Model]
  let reordered: Bool
  let desynced: Bool
  var failure: String?
}

final class ListFeed<Model: DBModel>: @unchecked Sendable {
  private struct Slot {
    let id: String
    let row: Model?
  }

  private let lock = NSLock()
  private var slots: [Slot] = []
  private var decoded = 0
  private var undecodable = 0
  private var desynced = 0

  var decodedRows: Int { lock.withLock { decoded } }
  var undecodableRows: Int { lock.withLock { undecodable } }
  var desyncs: Int { lock.withLock { desynced } }
  var rows: [Model] { lock.withLock { slots.compactMap(\.row) } }

  func apply(_ change: LiveChange) -> ListApplied<Model> {
    lock.withLock {
      if change.error != nil { return ListApplied(rows: slots.compactMap(\.row), reordered: false, desynced: false) }
      let carried: [(slot: Slot, index: Int)] = change.rows.enumerated().map { position, wire in
        decoded += 1
        let index = change.full ? position : Int(change.rowIndexes[position])
        do {
          let row = try Model.decode(wire)
          return (Slot(id: row.id, row: row), index)
        } catch {
          undecodable += 1
          decodeLog.error("Row of \(Model.modelName, privacy: .public) failed to decode: \(String(describing: error), privacy: .public)")
          return (Slot(id: wire.id, row: nil), index)
        }
      }
      if change.full {
        slots = carried.map(\.slot)
        return ListApplied(rows: slots.compactMap(\.row), reordered: true, desynced: false)
      }
      let reordered = !change.removed.isEmpty || !change.placed.isEmpty || !change.replaced.isEmpty
      var next = slots
      guard applyDelta(change, carried, to: &next) else {
        desynced += 1
        return ListApplied(rows: slots.compactMap(\.row), reordered: reordered, desynced: true)
      }
      slots = next
      return ListApplied(rows: slots.compactMap(\.row), reordered: reordered, desynced: false)
    }
  }

  private func applyDelta(_ change: LiveChange, _ carried: [(slot: Slot, index: Int)], to slots: inout [Slot]) -> Bool {
    let byId = Dictionary(carried.map { ($0.slot.id, $0.slot) }, uniquingKeysWith: { _, newer in newer })
    for pair in change.replaced {
      guard let at = slots.firstIndex(where: { $0.id == pair.from }), let slot = byId[pair.to] else { return false }
      slots[at] = slot
    }
    if !change.removed.isEmpty {
      let gone = Set(change.removed)
      let before = slots.count
      slots.removeAll { gone.contains($0.id) }
      if before - slots.count != gone.count { return false }
    }
    var taken: [String: Slot] = [:]
    for place in change.placed {
      if let at = slots.firstIndex(where: { $0.id == place.id }) { taken[place.id] = slots.remove(at: at) }
    }
    for place in change.placed {
      guard let slot = taken[place.id] ?? byId[place.id], Int(place.index) <= slots.count else { return false }
      slots.insert(slot, at: Int(place.index))
    }
    for (slot, index) in carried {
      guard index < slots.count else { return false }
      slots[index] = slot
    }
    return slots.count == Int(change.size)
  }
}

final class ListSession<Model: DBModel, Value: Sendable>: @unchecked Sendable {
  private let lock = NSLock()
  private let feed = ListFeed<Model>()
  private let open: @Sendable (QueryListener) throws -> @Sendable () -> Void
  private let emit: @Sendable (Value) -> Void
  private let map: @Sendable (ListApplied<Model>, LiveChange) -> Value
  private let maxReopens: Int
  private var cancelSubscription: (@Sendable () -> Void)?
  private var cancelled = false
  private var stalled = false
  private var generation = 0
  private var reopens = 0
  private var reopensInRow = 0

  var reopened: Int { lock.withLock { reopens } }
  var desyncs: Int { feed.desyncs }
  var isStalled: Bool { lock.withLock { stalled } }

  init(
    maxReopens: Int = 2,
    open: @escaping @Sendable (QueryListener) throws -> @Sendable () -> Void,
    emit: @escaping @Sendable (Value) -> Void,
    map: @escaping @Sendable (ListApplied<Model>, LiveChange) -> Value
  ) {
    self.maxReopens = maxReopens; self.open = open; self.emit = emit; self.map = map
  }

  func start() throws {
    let current = lock.withLock { generation }
    let made = try open(ChangeListener { [weak self] change in self?.receive(change, generation: current) })
    let keep = lock.withLock { () -> Bool in
      guard !cancelled, !stalled, generation == current else { return false }
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
    guard lock.withLock({ !cancelled && !stalled && generation == current }) else { return }
    let applied = feed.apply(change)
    if applied.desynced { reopen(from: current); return }
    if !change.full { lock.withLock { reopensInRow = 0 } }
    emit(map(applied, change))
  }

  private enum Step {
    case ignore
    case stall
    case restart((@Sendable () -> Void)?)
  }

  private func reopen(from current: Int) {
    let step = lock.withLock { () -> Step in
      guard !cancelled, !stalled, generation == current else { return .ignore }
      guard reopensInRow < maxReopens else { return .stall }
      generation += 1
      reopens += 1
      reopensInRow += 1
      defer { cancelSubscription = nil }
      return .restart(cancelSubscription)
    }
    switch step {
    case .ignore: return
    case .stall: stall("the list is out of sync with the database and a new subscription did not fix it")
    case .restart(let old):
      old?()
      Task.detached(priority: .userInitiated) { [self] in await restart() }
    }
  }

  private func restart() async {
    while true {
      do { try start(); return } catch {
        let again = lock.withLock { () -> Bool in
          guard !cancelled, !stalled, reopensInRow < maxReopens else { return false }
          reopensInRow += 1
          return true
        }
        if !again { stall("the subscription could not be opened again: \(error)"); return }
      }
    }
  }

  private func stall(_ reason: String) {
    let current = lock.withLock { () -> (@Sendable () -> Void)? in
      guard !cancelled, !stalled else { return nil }
      stalled = true
      defer { cancelSubscription = nil }
      return cancelSubscription
    }
    current?()
    decodeLog.error("List of \(Model.modelName, privacy: .public) stopped: \(reason, privacy: .public)")
    let applied = ListApplied<Model>(rows: feed.rows, reordered: false, desynced: false, failure: reason)
    emit(map(applied, LiveChange(revision: 0, full: false, size: 0, rows: [], rowIndexes: [], removed: [], placed: [], replaced: [], renamed: [], count: nil, error: reason)))
  }
}

extension LiveChange {
  var replacedIds: [String: String] {
    Dictionary(renamed.map { ($0.to, $0.from) }, uniquingKeysWith: { first, _ in first })
  }
}
