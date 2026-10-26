import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

extension BridgeProbeTests {
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func advancingTheTestClockExpiresAMemoryRowWithoutWaiting() async throws {
    DB.close()
    defer { DB.close() }
    let transport = MockTransport()
    let clock = try await DB.openTesting(transport: transport, modelsJSON: modelsJSON)
    try DB.database.registerOperations(defsJson: #"[{"name":"BulletinAdded","document":"subscription BulletinAdded { bulletinAdded { body occurredAt } }","rootPath":"bulletinAdded"}]"#)
    let notices = Cursor(try await Notice.recent.live.stream())
    #expect(await notices.next()?.isEmpty == true)
    await eventually("the notice subscription opens") { transport.calls.count == 1 }
    try transport.emit("BulletinAdded", json: #"{"data":{"bulletinAdded":{"body":"hello","occurredAt":"2026-10-18T00:00:00Z"}}}"#)
    #expect(await notices.first("the notice row") { !$0.isEmpty } != nil)

    clock.advance(by: 61)

    #expect(await notices.first("the notice expires") { $0.isEmpty } != nil)
  }

  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func writingEqualValuesRenewsAMemoryRowAndStaysSilent() async throws {
    DB.close()
    defer { DB.close() }
    let clock = try await DB.openTesting(transport: MockTransport(), modelsJSON: modelsJSON)
    let lobby = Cursor(try await Presence.where(Presence.room == "lobby").live.stream())
    #expect(await lobby.next()?.isEmpty == true)
    let presence = try await Presence.create(userId: "u1", room: "lobby")
    #expect(await lobby.first("the presence row") { !$0.isEmpty } != nil)

    clock.advance(by: 40)
    _ = try await presence.update(room: "lobby")
    clock.advance(by: 40)

    #expect(try await Presence.find(presence.id).current() != nil)
    clock.advance(by: 21)
    #expect(await lobby.next("the expiry, not a repeat of the row")?.isEmpty == true)
  }
}
