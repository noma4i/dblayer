import Foundation
import Testing
import DBlayerTesting
@testable import DBlayer

extension BridgeProbeTests {
  @Suite struct ListChangeTests {
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

    enum Surface: Sendable { case listChange, listFeed }

    @Test(arguments: [Surface.listChange, .listFeed], [20, 200, 2000])
    func aDeltaOfOneRowDecodesOneRowAtEveryListSize(surface: Surface, size: Int) {
      let rows = (0..<size).map { Self.wire("\($0)") }
      let delta = LiveChange(revision: 2, full: false, size: UInt32(size + 1), rows: [Self.wire("new")], rowIndexes: [UInt32(size)], removed: [], placed: [Placement(id: "new", index: UInt32(size))], replaced: [], renamed: [], count: nil, error: nil, inserted: [0])
      switch surface {
      case .listChange:
        let initial = ListChange<CountedMoment>(LiveChange(revision: 1, full: true, size: UInt32(size), rows: rows, rowIndexes: [], removed: [], placed: [], replaced: [], renamed: [], count: nil, error: nil, inserted: (0..<size).map(UInt32.init)))
        #expect(initial.kind == .replaced && initial.inserted.count == size)
        let before = CountedMoment.decodes.read()
        let next = ListChange<CountedMoment>(delta)
        #expect(next.kind == .delta && next.inserted.count == 1 && next.inserted.first?.index == size)
        #expect(CountedMoment.decodes.read() - before == 1)
      case .listFeed:
        let feed = ListFeed<Moment>()
        _ = feed.apply(LiveChange(revision: 1, full: true, size: UInt32(size), rows: rows, rowIndexes: [], removed: [], placed: [], replaced: [], renamed: [], count: nil, error: nil))
        let before = feed.decodedRows
        let snapshot = feed.apply(delta)
        #expect(snapshot.rows.count == size + 1)
        #expect(feed.decodedRows - before == 1)
      }
    }

    @Test func aChangeStreamDeliversOneInsertionAndARestartedStreamStartsWithTheWholeList() async throws {
      try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 64), modelsJson: modelsJSON, transport: nil)
      let query = Query<Moment>().order(Moment.createdAt.desc)
      for number in 0..<20 { _ = try await Moment.create(text: "row\(number)") }
      let cursor = Cursor(try await query.changes.stream())
      let initial = await cursor.next("initial inserts")
      #expect(initial?.inserted.count == 20)
      let row = try await Moment.create(text: "new")
      let next = await cursor.next("one insertion")
      #expect(next?.inserted.map(\.row.id) == [row.id])
      #expect(next?.size == 21)
      #expect((next?.revision ?? 0) > (initial?.revision ?? 0))
      let restarted = Cursor(try await query.changes.stream())
      let again = await restarted.next("restart inserts")
      #expect(again?.kind == .replaced && again?.inserted.count == 21)
    }

    @Test func changesKeepAllBatchesAndCancel() async throws {
      let transport = MockTransport()
      let nodes = (0..<2501).map { "{\"__typename\":\"Moment\",\"id\":\"\($0)\",\"uuid\":\"\($0)\",\"likedByMe\":false,\"media\":{},\"createdAt\":\"2026-10-08T00:00:00.000Z\"}" }.joined(separator: ",")
      transport.enqueue("MomentsFeed", reply: "{\"data\":{\"feed\":{\"nodes\":[\(nodes)],\"pageInfo\":{\"hasNextPage\":false}}}}")
      try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 64), modelsJson: modelsJSON, transport: transport)
      try DB.database.registerOperations(defsJson: #"[{"name":"MomentsFeed","document":"query MomentsFeed { feed { nodes { id uuid likedByMe media createdAt } pageInfo { hasNextPage endCursor } } }"}]"#)
      let query = Query<Moment>()
      var stream: LiveStream<ListChange<Moment>>? = try await query.changes.stream()
      var cursor: Cursor<LiveStream<ListChange<Moment>>>? = Cursor(stream!)
      #expect(await cursor!.next("empty initial")?.inserted.isEmpty == true)
      try await Moment.feed.load()
      let batch = await cursor!.next("2501 inserts in one delivery")
      #expect(Set(batch?.inserted.map(\.row.id) ?? []) == Set((0..<2501).map(String.init)))
      cursor = nil
      stream = nil
      await eventually("changes subscription released") { (try? DB.subscriptionCensus().subscriptions) == 0 }
    }
    static func wire(_ id: String) -> WireRow {
      WireRow(id: id, revision: 1, estimatedBytes: 1, layout: Moment.wireLayout,
        cells: [.text(value: id), .null, .text(value: id), .bool(value: false), .keyed(entries: []), .null, .text(value: "x")],
        sync: WireSync(state: .synced, actions: [], dirtyFields: [], error: nil, attempts: 0, since: 0), upload: nil)
    }
  }
}
