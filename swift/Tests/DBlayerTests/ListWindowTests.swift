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
    _ = live.takeUpdate()

    _ = try await Moment.create(text: "t00030")
    await eventually("the new tail row") { live.rows.last?.text == "t00030" }
    held.replay(live.takeUpdate().edits ?? [])
    #expect(held.map(\.id) == live.rows.map(\.id))
    #expect(live.total == 31)

    live.expand(before: 3)
    await eventually("the wider window") { live.rows.count == 8 }
    held.replay(live.takeUpdate().edits ?? [])
    #expect(held.map(\.id) == live.rows.map(\.id))
    live.expand(before: 100)
    await eventually("every row") { live.rows.count == 31 }
    #expect(!live.hasBefore && !live.hasAfter && live.total == 31)
  }

  @Test @MainActor
  func aRelatedWindowReadsTheSendersOfTheWindowRowsOnly() async throws {
    try await Self.openWindowBase()
    for (id, name) in [("u1", "Ann"), ("u2", "Bob"), ("u3", "Cy")] { _ = try await User.create(values: ["id": .string(id), "name": .string(name)]) }
    for (number, user) in ["u2", "u2", "u1", "u3"].enumerated() {
      _ = try await Moment.create(values: ["text": .string(String(format: "t%05d", number)), "userId": .string(user)])
    }
    let senders = Self.byText.window(tail: 2).related(Moment.Relations.author)
    #expect(try await senders.live.current().map(\.name) == ["Ann", "Cy"])
  }

  @Test @MainActor
  func aWindowThatGrowsPastMaxShrinksToItsFirstSize() async throws {
    try await Self.openWindowBase()
    for number in 0 ..< 30 { _ = try await Moment.create(text: String(format: "t%05d", number)) }
    let live = LiveWindow(Self.byText.window(tail: 5, max: 8))
    await eventually("the window") { live.rows.count == 5 }
    live.expand(before: 3)
    await eventually("the widest window") { live.rows.count == 8 }
    live.expand(before: 1)
    await eventually("the shrunk window") { live.rows.count == 5 }
    #expect(live.rows.map(\.text) == (25 ..< 30).map { String(format: "t%05d", $0) })
  }
}
