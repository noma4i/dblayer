import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

final class ListEditSink: @unchecked Sendable {
  var emit: (@Sendable (Snapshot<Moment>) -> Void)?
}

struct ListEditTests {
  static func moment(_ id: String, text: String? = nil) -> WireRow {
    let cells: [Cell] = [.text(value: id), .null, .text(value: text ?? id), .bool(value: false), .keyed(entries: []), .null, .text(value: "x")]
    return WireRow(id: id, revision: 1, estimatedBytes: 1, layout: Moment.wireLayout, cells: cells, sync: WireSync(state: .synced, actions: [], dirtyFields: [], error: nil, attempts: 0, since: 0), upload: nil)
  }

  static func change(
    full: Bool = false, size: Int, rows: [WireRow] = [], indexes: [UInt32] = [], removed: [String] = [],
    placed: [Placement] = [], replaced: [Replacement] = []
  ) -> LiveChange {
    LiveChange(
      revision: 1, full: full, size: UInt32(size), rows: rows, rowIndexes: indexes, removed: removed, placed: placed,
      replaced: replaced, renamed: replaced, count: nil, error: nil
    )
  }

  static func snapshot(_ feed: ListFeed<Moment>, _ change: LiveChange) -> Snapshot<Moment> {
    let applied = feed.apply(change)
    return Snapshot(rows: applied.rows, replaced: change.replacedIds, reordered: applied.reordered, failure: applied.failure, edits: applied.edits)
  }

  static func texts(_ rows: [Moment]) -> [String] { rows.map { "\($0.id):\($0.text ?? "")" } }

  @Test(arguments: [20, 200, 2000])
  func oneUpdateOneAppendAndOneRemoveAreOneEditEachAtAnyListSize(size: Int) throws {
    let feed = ListFeed<Moment>()
    _ = feed.apply(Self.change(full: true, size: size, rows: (0 ..< size).map { Self.moment("m\($0)") }))
    let middle = size / 2

    let update = feed.apply(Self.change(size: size, rows: [Self.moment("m\(middle)", text: "new")], indexes: [UInt32(middle)]))
    let append = feed.apply(Self.change(size: size + 1, rows: [Self.moment("x")], indexes: [UInt32(size)], placed: [Placement(id: "x", index: UInt32(size))]))
    let remove = feed.apply(Self.change(size: size, removed: ["m1"]))

    #expect(update.edits?.count == 1, "update at \(size)")
    #expect(append.edits?.count == 1, "append at \(size)")
    #expect(remove.edits?.count == 1, "remove at \(size)")
    guard case .rewrite(middle, let row)? = update.edits?.first else {
      Issue.record("update is not a rewrite")
      return
    }
    #expect(row.text == "new")
  }

  @Test
  func severalDeliveriesMergedReplayToTheFinalRowsWhateverTheyDidToOneId() throws {
    let steps: [(name: String, changes: [LiveChange])] = [
      ("update then move then remove", [
        Self.change(size: 13, rows: [Self.moment("a", text: "a1")], indexes: [0]),
        Self.change(size: 13, rows: [Self.moment("a", text: "a2")], indexes: [12], placed: [Placement(id: "a", index: 12)]),
        Self.change(size: 12, removed: ["a"])
      ]),
      ("the same id updated three times", [
        Self.change(size: 13, rows: [Self.moment("b", text: "b1")], indexes: [1]),
        Self.change(size: 13, rows: [Self.moment("b", text: "b2")], indexes: [1]),
        Self.change(size: 13, rows: [Self.moment("b", text: "b3")], indexes: [1])
      ]),
      ("temporary row becomes real then reorders", [
        Self.change(size: 14, rows: [Self.moment("t1")], indexes: [13], placed: [Placement(id: "t1", index: 13)]),
        Self.change(size: 14, rows: [Self.moment("r1")], indexes: [13], replaced: [Replacement(from: "t1", to: "r1")]),
        Self.change(size: 14, rows: [Self.moment("r1", text: "r1x")], indexes: [0], placed: [Placement(id: "r1", index: 0)])
      ])
    ]
    for step in steps {
      let feed = ListFeed<Moment>()
      let ids = ["a", "b", "c"] + (0 ..< 10).map { "f\($0)" }
      let base = feed.apply(Self.change(full: true, size: ids.count, rows: ids.map { Self.moment($0) })).rows
      let deliveries = step.changes.map { Self.snapshot(feed, $0) }
      let merged = deliveries.dropFirst().reduce(deliveries[0]) { Snapshot.merging($0, $1) }
      var mirror = base
      mirror.replay(try #require(merged.edits, "\(step.name) has edits"))

      #expect(Self.texts(mirror) == Self.texts(feed.rows), "\(step.name)")
      #expect(Self.texts(mirror) == Self.texts(merged.rows), "\(step.name)")
    }
  }

  @Test
  func severalRowsRemovedByOneDeliveryReplayToTheRowsThatStay() throws {
    let feed = ListFeed<Moment>()
    let base = feed.apply(Self.change(full: true, size: 6, rows: ["a", "b", "c", "d", "e", "f"].map { Self.moment($0) })).rows

    let removed = Self.snapshot(feed, Self.change(size: 4, removed: ["b", "d"]))
    var mirror = base
    mirror.replay(try #require(removed.edits))

    #expect(mirror.map(\.id) == ["a", "c", "e", "f"])
  }

  @Test
  func aWholeListAndTooManyEditsAreDeliveredWithoutEdits() {
    let feed = ListFeed<Moment>()
    let whole = Self.snapshot(feed, Self.change(full: true, size: 2, rows: ["a", "b"].map { Self.moment($0) }))
    let one = Self.snapshot(feed, Self.change(size: 2, rows: [Self.moment("a", text: "1")], indexes: [0]))
    let two = Self.snapshot(feed, Self.change(size: 2, rows: [Self.moment("b", text: "2")], indexes: [1]))
    let three = Self.snapshot(feed, Self.change(size: 2, rows: [Self.moment("a", text: "3")], indexes: [0]))

    #expect(whole.edits == nil)
    #expect(Snapshot.merging(whole, one).edits == nil)
    #expect(Snapshot.merging(one, two).edits?.count == 2)
    #expect(Snapshot.merging(Snapshot.merging(one, two), three).edits == nil, "3 edits on 2 rows cost more than the rows")
  }

  @Test
  func aRowThatDoesNotDecodeTurnsTheEditsOffUntilTheReaderTakesTheWholeList() {
    let feed = ListFeed<Moment>()
    var broken = Self.moment("b")
    broken.cells = []
    _ = feed.apply(Self.change(full: true, size: 3, rows: [Self.moment("a"), broken, Self.moment("c")]))

    let update = feed.apply(Self.change(size: 3, rows: [Self.moment("c", text: "c1")], indexes: [2]))

    #expect(update.edits == nil)
    #expect(update.rows.map(\.id) == ["a", "c"])
  }

  @MainActor
  @Test
  func theListHandsOutTheEditsOfEveryDeliveryTheReaderMissed() async throws {
    let sink = ListEditSink()
    let snapshots = LiveSource<Snapshot<Moment>>(initial: Snapshot(rows: [], replaced: [:]), merge: { Snapshot.merging($0, $1) }) { emit in
      sink.emit = emit
      return {}
    }
    let list = LiveList<Moment>(Moment.recent, snapshots: snapshots, state: LiveSource(initial: LoadState()) { _ in {} })
    let feed = ListFeed<Moment>()
    func deliver(_ change: LiveChange) async {
      for _ in 0 ..< 200 where sink.emit == nil { try? await Task.sleep(for: .milliseconds(10)) }
      let snapshot = Self.snapshot(feed, change)
      sink.emit?(snapshot)
      for _ in 0 ..< 200 where list.rows.count != snapshot.rows.count || Self.texts(list.rows) != Self.texts(snapshot.rows) {
        try? await Task.sleep(for: .milliseconds(10))
      }
    }

    await deliver(Self.change(full: true, size: 3, rows: ["a", "b", "c"].map { Self.moment($0) }))
    #expect(list.takeEdits() == nil, "the first read takes the whole list")
    await deliver(Self.change(size: 3, rows: [Self.moment("a", text: "a1")], indexes: [0]))
    await deliver(Self.change(size: 3, rows: [Self.moment("c", text: "c1")], indexes: [2]))
    var mirror = ["a", "b", "c"].map { try? Moment.decode(Self.moment($0)) }.compactMap { $0 }
    mirror.replay(try #require(list.takeEdits()))

    #expect(Self.texts(mirror) == Self.texts(list.rows))
    #expect(list.takeEdits()?.isEmpty == true, "a reader that is up to date gets no steps")
  }
}
