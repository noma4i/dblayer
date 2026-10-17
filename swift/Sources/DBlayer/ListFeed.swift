import Foundation
import os

private let decodeLog = Logger(subsystem: "DBlayer", category: "decode")

struct ListApplied<Model: DBModel> {
  let rows: [Model]
  let reordered: Bool
  let desynced: Bool
  var failure: String?
  var edits: [ListEdit<Model>]?
}

final class ListFeed<Model: DBModel>: @unchecked Sendable {
  private struct Slot {
    let id: String
    let row: Model?
  }

  private enum Undo {
    case restore(Int, Slot)
    case reinsert(Int, Slot)
    case drop(Int)
  }

  private let lock = NSLock()
  private var slots: [Slot] = []
  private var loaded: [Model] = []
  private var missing = 0
  private var decoded = 0
  private var undecodable = 0
  private var desynced = 0

  var decodedRows: Int { lock.withLock { decoded } }
  var undecodableRows: Int { lock.withLock { undecodable } }
  var desyncs: Int { lock.withLock { desynced } }
  var rows: [Model] { lock.withLock { current } }

  private var current: [Model] { missing == 0 ? loaded : slots.compactMap(\.row) }

  func apply(_ change: LiveChange) -> ListApplied<Model> {
    lock.withLock {
      if change.error != nil { return ListApplied(rows: current, reordered: false, desynced: false) }
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
        missing = slots.reduce(0) { $0 + ($1.row == nil ? 1 : 0) }
        loaded = missing == 0 ? slots.compactMap(\.row) : []
        return ListApplied(rows: current, reordered: true, desynced: false)
      }
      let reordered = !change.removed.isEmpty || !change.placed.isEmpty || !change.replaced.isEmpty
      let held = missing == 0
      var edits: [ListEdit<Model>] = []
      guard applyDelta(change, carried, edits: &edits) else {
        desynced += 1
        return ListApplied(rows: current, reordered: reordered, desynced: true)
      }
      guard held, missing == 0, edits.count <= slots.count else {
        loaded = missing == 0 ? slots.compactMap(\.row) : []
        return ListApplied(rows: current, reordered: reordered, desynced: false)
      }
      loaded.replay(edits)
      return ListApplied(rows: loaded, reordered: reordered, desynced: false, edits: edits)
    }
  }

  private func applyDelta(_ change: LiveChange, _ carried: [(slot: Slot, index: Int)], edits: inout [ListEdit<Model>]) -> Bool {
    var undo: [Undo] = []
    let missingBefore = missing
    func fail() -> Bool {
      for step in undo.reversed() {
        switch step {
        case .restore(let index, let slot): slots[index] = slot
        case .reinsert(let index, let slot): slots.insert(slot, at: index)
        case .drop(let index): slots.remove(at: index)
        }
      }
      missing = missingBefore
      return false
    }
    func hold(_ slot: Slot) { if slot.row == nil { missing += 1 } }
    func release(_ slot: Slot) { if slot.row == nil { missing -= 1 } }
    func write(_ index: Int, _ slot: Slot) {
      undo.append(.restore(index, slots[index]))
      release(slots[index])
      slots[index] = slot
      hold(slot)
      if let row = slot.row { edits.append(.rewrite(index, row)) }
    }
    func take(_ index: Int) -> Slot {
      let slot = slots.remove(at: index)
      undo.append(.reinsert(index, slot))
      release(slot)
      edits.append(.remove(index, slot.id))
      return slot
    }
    func put(_ index: Int, _ slot: Slot) {
      slots.insert(slot, at: index)
      undo.append(.drop(index))
      hold(slot)
      if let row = slot.row { edits.append(.insert(index, row)) }
    }
    let byId = Dictionary(carried.map { ($0.slot.id, $0.slot) }, uniquingKeysWith: { _, newer in newer })
    var landed = Set<String>()
    for pair in change.replaced {
      guard let at = slots.firstIndex(where: { $0.id == pair.from }), let slot = byId[pair.to] else { return fail() }
      write(at, slot)
      landed.insert(pair.to)
    }
    if !change.removed.isEmpty {
      let gone = Set(change.removed)
      let found = slots.indices.filter { gone.contains(slots[$0].id) }
      if found.count != gone.count { return fail() }
      for index in found.reversed() { _ = take(index) }
    }
    var taken: [String: Slot] = [:]
    for place in change.placed {
      if let at = slots.firstIndex(where: { $0.id == place.id }) { taken[place.id] = take(at) }
    }
    for place in change.placed {
      guard let slot = taken[place.id] ?? byId[place.id], Int(place.index) <= slots.count else { return fail() }
      put(Int(place.index), slot)
      if taken[place.id] == nil { landed.insert(place.id) }
    }
    for (slot, index) in carried {
      guard index < slots.count else { return fail() }
      if landed.contains(slot.id), slots[index].id == slot.id { continue }

      write(index, slot)
    }
    return slots.count == Int(change.size) ? true : fail()
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
    emit(map(applied, LiveChange(revision: 0, full: false, size: 0, rows: [], rowIndexes: [], removed: [], placed: [], replaced: [], renamed: [], count: nil, error: .Invalid(detail: reason))))
  }
}

extension LiveChange {
  var replacedIds: [String: String] {
    Dictionary(renamed.map { ($0.to, $0.from) }, uniquingKeysWith: { first, _ in first })
  }
}
