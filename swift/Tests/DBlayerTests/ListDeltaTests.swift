import Foundation
import Testing
@testable import DBlayer

extension BridgeProbeTests {
  fileprivate static let byText = Query<Moment>().order(Order(field: "text", direction: "asc"))

  fileprivate static func open() throws {
    DB.database = try Database.open(config: Config(path: nil, dataVersion: nil, cacheRows: 64), modelsJson: modelsJSON, transport: nil)
  }

  fileprivate static func watching(_ query: Query<Moment>, feed: ListFeed<Moment>) async -> LiveStream<[Moment]> {
    let json = query.json
    let source = LiveSource<[Moment]>(initial: []) { emit in
      let subscription = try DB.database.liveQuery(queryJson: json, listener: ChangeListener { emit(feed.apply($0).rows) })
      return { subscription.cancel() }
    }
    return await source.stream()
  }

  @Test
  func inv_observe_17_anUpdateOfOneRowDecodesOneRowAtEveryListSize() async throws {
    var decoded: [Int] = []
    for count in [20, 200, 2000] {
      try Self.open()
      var created: [Moment] = []
      for number in 0 ..< count { created.append(try await Moment.create(text: String(format: "t%05d", number))) }
      let feed = ListFeed<Moment>()
      var changes = await Self.watching(Self.byText, feed: feed).makeAsyncIterator()
      #expect(await changes.next()?.count == count)
      let before = feed.decodedRows
      _ = try await created[count / 2].update(text: String(format: "t%05dx", count / 2))
      let next = await changes.next()
      #expect(next?.count == count && next?[count / 2].text == String(format: "t%05dx", count / 2))
      decoded.append(feed.decodedRows - before)
    }
    #expect(decoded == [1, 1, 1], "rows decoded for 1 update at 20, 200, 2000 rows")
  }

  @Test(.timeLimit(.minutes(1)))
  func inv_observe_15_theFeedRebuildsTheListThatAFreshQueryReturns() async throws {
    try Self.open()
    let feed = ListFeed<Moment>()
    var changes = await Self.watching(Self.byText, feed: feed).makeAsyncIterator()
    #expect(await changes.next()?.isEmpty == true)
    var rows: [Moment] = []
    var seed: UInt64 = 0x9E3779B97F4A7C15
    func below(_ bound: Int) -> Int {
      seed ^= seed << 13; seed ^= seed >> 7; seed ^= seed << 17
      return Int(seed % UInt64(bound))
    }
    for step in 0 ..< 120 {
      switch (rows.isEmpty ? 0 : below(3)) {
      case 0: rows.append(try await Moment.create(text: String(format: "n%02d-%03d", below(40), step)))
      case 1:
        let at = below(rows.count)
        rows[at] = try await rows[at].update(text: String(format: "n%02d-%03d", below(40), step))
      default: try await rows.remove(at: below(rows.count)).destroy()
      }
      let fresh = await Self.byText.live.current()
      var seen = await changes.next() ?? []
      for _ in 0 ..< 50 where seen != fresh {
        guard let next = await changes.next() else { break }
        seen = next
      }
      #expect(seen.map(\.id) == fresh.map(\.id), "step \(step)")
    }
  }

  @Test
  func inv_observe_15_aSnapshotMarksAReorderOnlyWhenTheIdsChange() async throws {
    try Self.open()
    let first = try await Moment.create(text: "a")
    _ = try await Moment.create(text: "b")
    var snapshots = await Self.byText.snapshots.stream().makeAsyncIterator()
    #expect(await snapshots.next()?.rows.count == 2)
    _ = try await first.update(text: "ax")
    let edited = await snapshots.next()
    #expect(edited?.reordered == false && edited?.rows.first?.text == "ax")
    _ = try await first.update(text: "z")
    let moved = await snapshots.next()
    #expect(moved?.reordered == true && moved?.rows.map(\.text) == ["b", "z"])
  }

  private static func moment(_ id: String) -> String {
    let row = Moment(id: id, uuid: id, text: id, media: .object([:]), createdAt: "x")
    return String(decoding: (try? JSONEncoder().encode(row)) ?? Data(), as: UTF8.self)
  }

  private static func wire(full: Bool = false, size: UInt32, rows: [String] = [], indexes: [UInt32] = [], removed: [String] = [], placed: [Placement] = []) -> LiveChange {
    LiveChange(revision: 1, full: full, size: size, rows: rows.map(moment), rowIndexes: indexes, removed: removed, placed: placed, replaced: [], count: nil, error: nil)
  }

  @Test
  func inv_observe_15_aDeliveryThatDoesNotFitTheRowsIsRefusedAndTheFeedCountsIt() {
    let feed = ListFeed<Moment>()
    #expect(feed.apply(Self.wire(full: true, size: 2, rows: ["a", "b"])).desynced == false)
    #expect(feed.apply(Self.wire(size: 3, placed: [Placement(id: "zz", index: 0)])).desynced)
    #expect(feed.apply(Self.wire(size: 2, rows: ["a"], indexes: [5])).desynced)
    #expect(feed.apply(Self.wire(size: 9, rows: ["a"], indexes: [0])).desynced)
    #expect(feed.desyncs == 3)
  }

  @Test(.timeLimit(.minutes(1)))
  func inv_observe_15_aDeliveryThatDoesNotFitOpensTheSubscriptionAgainForTheWholeList() async throws {
    let (listeners, openings) = AsyncStream<QueryListener>.makeStream()
    let (values, sink) = AsyncStream<[String]>.makeStream()
    let session = ListSession<Moment, [String]>(
      open: { listener in openings.yield(listener); return {} },
      emit: { sink.yield($0) },
      map: { applied, _ in applied.rows.map(\.id) }
    )
    try session.start()
    var opened = listeners.makeAsyncIterator()
    var seen = values.makeAsyncIterator()
    let first = try #require(await opened.next())
    first.changed(change: Self.wire(full: true, size: 2, rows: ["a", "b"]))
    #expect(await seen.next() == ["a", "b"])
    first.changed(change: Self.wire(size: 3, placed: [Placement(id: "zz", index: 0)]))
    let second = try #require(await opened.next())
    second.changed(change: Self.wire(full: true, size: 3, rows: ["a", "b", "c"]))
    #expect(await seen.next() == ["a", "b", "c"])
    #expect(session.desyncs == 1 && session.reopened == 1)
  }
}
