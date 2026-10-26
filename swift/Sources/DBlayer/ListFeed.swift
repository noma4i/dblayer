import Foundation
import os

private let decodeLog = Logger(subsystem: "DBlayer", category: "decode")

struct ListApplied<Model: DBModel> {
  let rows: [Model]
  let reordered: Bool
  let desynced: Bool
  var failure: DbError?
  var edits: [ListEdit<Model>]?
  var undecodable: [String] = []
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
  private var lastDecodeFailure: DbError?
  private var desynced = 0
  private var copied = 0

  var decodedRows: Int { lock.withLock { decoded } }
  var undecodableRows: Int { lock.withLock { undecodable } }
  var desyncs: Int { lock.withLock { desynced } }
  var copiedRows: Int { lock.withLock { copied } }
  var rows: [Model] { lock.withLock { current } }

  private var current: [Model] { missing == 0 ? loaded : slots.compactMap(\.row) }
  private var decodeFailure: DbError? { missing == 0 ? nil : lastDecodeFailure }
  private func materialized() -> [Model] {
    guard missing == 0 else { return [] }
    copied += slots.count
    return slots.compactMap(\.row)
  }
  private var missingIds: [String] { missing == 0 ? [] : slots.filter { $0.row == nil }.map(\.id) }

  private static func invalid(_ error: Error) -> DbError {
    if let error = error as? DbError, case .Invalid = error { return error }
    return .Invalid(detail: String(describing: error))
  }

  func apply(_ change: LiveChange) -> ListApplied<Model> {
    lock.withLock {
      let carried: [(slot: Slot, index: Int)] = change.rows.enumerated().map { position, wire in
        decoded += 1
        let index = change.full ? position : Int(change.rowIndexes[position])
        do {
          let row = try Model.decode(wire)
          return (Slot(id: row.id, row: row), index)
        } catch {
          undecodable += 1
          lastDecodeFailure = Self.invalid(error)
          decodeLog.error("Row of \(Model.modelName, privacy: .public) failed to decode: \(String(describing: error), privacy: .public)")
          return (Slot(id: wire.id, row: nil), index)
        }
      }
      if change.full {
        slots = carried.map(\.slot)
        missing = slots.reduce(0) { $0 + ($1.row == nil ? 1 : 0) }
        loaded = materialized()
        return ListApplied(rows: current, reordered: true, desynced: false, failure: decodeFailure, undecodable: missingIds)
      }
      let reordered = !change.removed.isEmpty || !change.placed.isEmpty || !change.replaced.isEmpty
      let held = missing == 0
      var edits: [ListEdit<Model>] = []
      guard applyDelta(change, carried, edits: &edits) else {
        desynced += 1
        return ListApplied(rows: current, reordered: reordered, desynced: true, failure: decodeFailure, undecodable: missingIds)
      }
      guard held, missing == 0, edits.count <= slots.count else {
        loaded = materialized()
        return ListApplied(rows: current, reordered: reordered, desynced: false, failure: decodeFailure, undecodable: missingIds)
      }
      copied += loaded.count
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

struct ListHandle: Sendable {
  let cancel: @Sendable () -> Void
  let reportDesync: @Sendable () -> Void
  let initialLoad: @Sendable () async throws -> Void

  init(cancel: @escaping @Sendable () -> Void, reportDesync: @escaping @Sendable () -> Void, initialLoad: @escaping @Sendable () async throws -> Void = {}) {
    self.cancel = cancel; self.reportDesync = reportDesync; self.initialLoad = initialLoad
  }

  init(_ subscription: LiveSubscription) {
    self.init(cancel: { subscription.cancel() }, reportDesync: { subscription.reportDesync() }, initialLoad: { try await subscription.waitForInitialLoad() })
  }
}

final class ListSession<Model: DBModel, Value: Sendable>: @unchecked Sendable {
  private let lock = NSLock()
  private let feed = ListFeed<Model>()
  private let open: @Sendable (QueryListener) throws -> ListHandle
  private let emit: @Sendable (Value) -> Void
  private let map: @Sendable (ListApplied<Model>, LiveChange) -> Value
  private var handle: ListHandle?
  private var cancelled = false
  private var desyncPending = false

  var desyncs: Int { feed.desyncs }

  init(
    open: @escaping @Sendable (QueryListener) throws -> ListHandle,
    emit: @escaping @Sendable (Value) -> Void,
    map: @escaping @Sendable (ListApplied<Model>, LiveChange) -> Value
  ) {
    self.open = open; self.emit = emit; self.map = map
  }

  func start() throws {
    let made = try open(ChangeListener { [weak self] change in self?.receive(change) })
    let pending = lock.withLock { () -> Bool? in
      guard !cancelled else { return nil }
      handle = made
      return desyncPending
    }
    guard let pending else { made.cancel(); return }
    if pending { made.reportDesync() }
  }

  func waitForInitialLoad() async throws {
    try await lock.withLock { handle }?.initialLoad()
  }

  func cancel() {
    let current = lock.withLock { () -> ListHandle? in
      cancelled = true
      defer { handle = nil }
      return handle
    }
    current?.cancel()
  }

  private func receive(_ change: LiveChange) {
    guard lock.withLock({ !cancelled }) else { return }
    let applied = feed.apply(change)
    if applied.desynced {
      let current = lock.withLock { () -> ListHandle? in
        if handle == nil { desyncPending = true }
        return handle
      }
      current?.reportDesync()
      return
    }
    emit(map(applied, change))
  }
}

extension LiveChange {
  var replacedIds: [String: String] {
    Dictionary(renamed.map { ($0.to, $0.from) }, uniquingKeysWith: { first, _ in first })
  }
}
