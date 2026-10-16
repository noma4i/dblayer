import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

extension BridgeProbeTests {
  fileprivate static let byText = Query<Moment>().order(Order(field: "text", direction: "asc"))

  fileprivate static func openWindowBase() throws {
    DB.database = try Database.open(config: Config(path: nil, dataVersion: nil, cacheRows: 64), modelsJson: modelsJSON, transport: nil)
  }

  @Test
  func aWindowDecodesOnlyItsRowsAtEveryListSize() async throws {
    var costs: [[Int]] = []
    for count in [20, 200, 2000] {
      try Self.openWindowBase()
      var created: [Moment] = []
      for number in 0 ..< count { created.append(try await Moment.create(text: String(format: "t%05d", number))) }
      let feed = ListFeed<Moment>()
      let source = LiveSource<[Moment]>(initial: []) { emit in
        let subscription = try DB.database.liveWindow(queryJson: Self.byText.json, tail: 10, listener: ChangeListener { emit(feed.apply($0).rows) })
        return { subscription.cancel() }
      }
      let changes = Cursor(await source.stream())
      #expect(await changes.first("the window") { $0.count == 10 } != nil)
      let atStart = feed.decodedRows
      let edited = String(format: "t%05dx", count - 1)
      _ = try await created[count - 1].update(text: edited)
      #expect(await changes.first("the edited row") { $0.last?.text == edited } != nil)
      let afterInside = feed.decodedRows
      _ = try await created[0].update(text: "t00000x")
      let again = String(format: "t%05dy", count - 1)
      _ = try await created[count - 1].update(text: again)
      #expect(await changes.first("the second edit") { $0.last?.text == again } != nil)
      costs.append([atStart, afterInside - atStart, feed.decodedRows - afterInside])
    }
    #expect(costs == [[10, 1, 1], [10, 1, 1], [10, 1, 1]], "rows decoded for the window, 1 edit inside, 1 edit before plus 1 edit inside, at 20, 200, 2000 rows")
  }

  @Test @MainActor
  func aLiveWindowFollowsTheTailAndExpandsBefore() async throws {
    try Self.openWindowBase()
    for number in 0 ..< 30 { _ = try await Moment.create(text: String(format: "t%05d", number)) }
    let live = LiveWindow(Self.byText.window(tail: 5))
    await eventually("the window") { live.rows.count == 5 }
    #expect(live.rows.map(\.text) == (25 ..< 30).map { String(format: "t%05d", $0) })
    #expect(live.hasBefore && !live.hasAfter && live.total == 30)
    var held = live.rows
    _ = live.takeEdits()

    _ = try await Moment.create(text: "t00030")
    await eventually("the new tail row") { live.rows.last?.text == "t00030" }
    held.replay(live.takeEdits() ?? [])
    #expect(held.map(\.id) == live.rows.map(\.id))
    #expect(live.total == 31)

    live.expand(before: 3)
    await eventually("the wider window") { live.rows.count == 8 }
    held.replay(live.takeEdits() ?? [])
    #expect(held.map(\.id) == live.rows.map(\.id))
    live.expand(before: 100)
    await eventually("every row") { live.rows.count == 31 }
    #expect(!live.hasBefore && !live.hasAfter && live.total == 31)
  }
}
