import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

extension BridgeProbeTests {
  fileprivate static let byText = Query<Moment>().order(Order(field: "text", direction: "asc"))

  fileprivate static func openWindowBase() async throws {
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 64), modelsJson: modelsJSON, transport: nil)
  }

  @Test @MainActor
  func aLiveWindowFollowsTheTailAndExpandsBefore() async throws {
    try await Self.openWindowBase()
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
