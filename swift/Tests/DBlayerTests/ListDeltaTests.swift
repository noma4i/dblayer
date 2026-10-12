import Foundation
import Testing
@testable import DBlayer

/// Runs inside the serialized bridge suite: every test here replaces the global database.
extension BridgeProbeTests {
  fileprivate static let byText = Query<Moment>().order(Order(field: "text", direction: "asc"))

  fileprivate static func open() throws {
    DB.database = try Database.open(config: Config(path: nil, dataVersion: nil, cacheRows: 64), modelsJson: modelsJSON, transport: nil)
  }

  /// The list of a query as the wrapper keeps it, read through the same feed that `Query.live` uses.
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
      while seen != fresh { seen = await changes.next() ?? [] }
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
}
