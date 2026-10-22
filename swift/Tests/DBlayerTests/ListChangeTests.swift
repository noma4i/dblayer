import Foundation
import Testing
import DBlayerTesting
@testable import DBlayer

extension BridgeProbeTests {
  @Suite struct ListChangeTests {
    @Test func aSlowConsumerReceivesABoundedReplacementWithEveryCurrentRow() async throws {
      DB.database = try await Database.open(config: Config(path: nil, dataVersion: nil, cacheRows: 64), modelsJson: modelsJSON, transport: nil)
      let query = Query<Moment>().order(Moment.createdAt.desc)
      let cursor = Cursor(await query.changes.stream())
      for number in 0..<192 { _ = try await Moment.create(text: "row\(number)") }
      var rows: [Moment] = []
      var sawReplacement = false
      for _ in 0...192 {
        guard let change = await cursor.next("the queued changes") else { break }
        if change.kind == .replaced {
          rows = change.inserted.map(\.row)
          sawReplacement = sawReplacement || change.inserted.count >= 64
        }
        else { for row in change.inserted { rows.insert(row.row, at: row.index) } }
        if change.size == 192 { break }
      }
      #expect(sawReplacement, "overflow never delivered a replacement snapshot")
      #expect(rows == (await query.live.current()))
    }
    final class Decodes: @unchecked Sendable {
      let lock = NSLock()
      var value = 0
      func add() { lock.withLock { value += 1 } }
      func read() -> Int { lock.withLock { value } }
    }

    struct CountedMoment: DBModel {
      static let decodes = Decodes()
      static let modelName = Moment.modelName
      static let wireLayout = Moment.wireLayout
      let value: Moment
      var id: String { value.id }
      var sync: SyncInfo { value.sync }
      var upload: UploadInfo? { value.upload }
      init(wire: WireRow) throws { Self.decodes.add(); value = try Moment(wire: wire) }
    }

    @Test func changesWork() {
      for run in 1...5 {
        let initial = ListChange<CountedMoment>(LiveChange(revision: 1, full: true, size: 2000, rows: (0..<2000).map { Self.wire("\($0)") }, rowIndexes: [], removed: [], placed: [], replaced: [], renamed: [], count: nil, error: nil, inserted: (0..<2000).map(UInt32.init)))
        #expect(initial.inserted.count == 2000)
        let before = CountedMoment.decodes.read()
        let delta = LiveChange(revision: 2, full: false, size: 2001, rows: [Self.wire("new")], rowIndexes: [2000], removed: [], placed: [Placement(id: "new", index: 2000)], replaced: [], renamed: [], count: nil, error: nil, inserted: [0])
        let next = ListChange<CountedMoment>(delta)
        #expect(next.inserted.count == 1)
        #expect(next.inserted.first?.index == 2000)
        #expect(CountedMoment.decodes.read() - before == 1)
        print("delta-work swift run=\(run) ffi=\(delta.rows.count) decoded=\(CountedMoment.decodes.read() - before) consumer=\(next.inserted.count)")
      }
    }

    @Test func changesDeliverOnlyChangedRowsAndRestart() async throws {
      DB.database = try await Database.open(config: Config(path: nil, dataVersion: nil, cacheRows: 64), modelsJson: modelsJSON, transport: nil)
      let query = Query<Moment>().order(Moment.createdAt.desc)
      for number in 0..<2000 { _ = try await Moment.create(text: "row\(number)") }
      let cursor = Cursor(await query.changes.stream())
      let initial = await cursor.next("initial inserts")
      #expect(initial?.inserted.count == 2000)
      var row: Moment?
      for run in 1...5 {
        row = try await Moment.create(text: "new\(run)")
        let next = await cursor.next("one insertion")
        #expect(next?.inserted.map(\.row.id) == [row!.id])
        #expect(next?.updated.isEmpty == true)
        #expect(next?.size == 2000 + run)
        #expect((next?.revision ?? 0) > (initial?.revision ?? 0))
        print("changes-work swift run=\(run) consumer=\(next?.inserted.count ?? -1)")
      }
      _ = try await row!.update(text: "edited")
      let updated = await cursor.next("one update")
      #expect(updated?.updated.first?.row.text == "edited")
      try await row!.destroy()
      let removed = await cursor.next("one removal")
      #expect(removed?.removed == [row!.id])
      let restarted = Cursor(await query.changes.stream())
      #expect(await restarted.next("restart inserts")?.inserted.count == 2004)
    }

    @Test func changesKeepAllBatchesAndCancel() async throws {
      let transport = MockTransport()
      let nodes = (0..<2501).map { "{\"__typename\":\"Moment\",\"id\":\"\($0)\",\"uuid\":\"\($0)\",\"likedByMe\":false,\"media\":{},\"createdAt\":\"2026-10-08T00:00:00.000Z\"}" }.joined(separator: ",")
      transport.enqueue("MomentsFeed", reply: "{\"data\":{\"feed\":{\"nodes\":[\(nodes)],\"pageInfo\":{\"hasNextPage\":false}}}}")
      DB.database = try await Database.open(config: Config(path: nil, dataVersion: nil, cacheRows: 64), modelsJson: modelsJSON, transport: transport)
      try DB.database.registerOperations(defsJson: #"[{"name":"MomentsFeed","document":"query MomentsFeed { feed { nodes { id uuid likedByMe media createdAt } pageInfo { hasNextPage endCursor } } }"}]"#)
      let query = Query<Moment>()
      var stream: LiveStream<ListChange<Moment>>? = await query.changes.stream()
      var cursor: Cursor<LiveStream<ListChange<Moment>>>? = Cursor(stream!)
      #expect(await cursor!.next("empty initial")?.inserted.isEmpty == true)
      try await Moment.feed.load()
      let batch = await cursor!.next("2501 inserts in one delivery")
      #expect(Set(batch?.inserted.map(\.row.id) ?? []) == Set((0..<2501).map(String.init)))
      cursor = nil
      stream = nil
      await eventually("changes subscription released") { DB.subscriptionCensus().subscriptions == 0 }
    }
    static func wire(_ id: String) -> WireRow {
      WireRow(id: id, revision: 1, estimatedBytes: 1, layout: Moment.wireLayout,
        cells: [.text(value: id), .null, .text(value: id), .bool(value: false), .keyed(entries: []), .null, .text(value: "x")],
        sync: WireSync(state: .synced, actions: [], dirtyFields: [], error: nil, attempts: 0, since: 0), upload: nil)
    }

    @Test func snapshotWork() {
      for run in 1...5 {
        let feed = ListFeed<Moment>()
        _ = feed.apply(LiveChange(revision: 1, full: true, size: 2000, rows: (0..<2000).map { Self.wire("\($0)") }, rowIndexes: [], removed: [], placed: [], replaced: [], renamed: [], count: nil, error: nil))
        let before = feed.decodedRows
        let delta = LiveChange(revision: 2, full: false, size: 2001, rows: [Self.wire("new")], rowIndexes: [2000], removed: [], placed: [Placement(id: "new", index: 2000)], replaced: [], renamed: [], count: nil, error: nil)
        let snapshot = feed.apply(delta)
        print("snapshot-work swift run=\(run) ffi=\(delta.rows.count) decoded=\(feed.decodedRows - before) consumer=\(snapshot.rows.count) known-scan=\(snapshot.rows.count)")
        #expect(snapshot.rows.count == 2001)
        #expect(feed.decodedRows - before == 1)
      }
    }
  }
}
