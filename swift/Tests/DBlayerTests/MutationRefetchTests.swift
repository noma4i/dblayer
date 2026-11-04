import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

extension BridgeProbeTests {
  /// K33 (A1): a call that waits for the answer of a mutation returns with that answer while a load of the list the mutation refetches is held. The list is read again once that load ends.
  @MainActor
  @Test(.timeLimit(.minutes(2)))
  func aMutationReturnsWithItsAnswerWhileALoadOfItsRefetchedListIsHeld() async throws {
    let transport = MockTransport()
    let (club, joined, _) = try await Self.openClubWithMembers(transport)
    let stream = try DB.account().stream(joined)
    defer { stream.cancel() }
    let cursor = Cursor(stream)
    try await stream.loaded()
    let current = ["u1", "u2", "u3", "u4"]
    let loadHold = transport.hold("ClubMembers", reply: membersReply(current))
    let addHold = transport.hold("AddClubMember", reply: clubAnswer("addClubMember"))
    let rereadHold = transport.hold("ClubMembers", reply: membersReply(current + ["u9"]))
    defer { loadHold.release(); addHold.release(); rereadHold.release() }
    let refreshing = Task { try await stream.refresh() }
    try #require(await entered(loadHold), "the old list did not reach the transport")
    let adding = Task { try await club.addMember(userIds: ["u9"]) }
    try #require(await entered(addHold), "the add did not reach the transport")
    let receipt = try await valueBeforeDeadline(adding) { loadHold.release(); addHold.release(); rereadHold.release() }
    addHold.release()
    let outcome = Task { try await receipt.outcome() }
    let result = try await valueBeforeDeadline(outcome) { loadHold.release(); rereadHold.release() }
    if case .confirmed = result {} else { Issue.record("the add was not confirmed") }
    loadHold.release()
    try await valueBeforeDeadline(refreshing) { loadHold.release(); rereadHold.release() }
    try #require(await entered(rereadHold), "the automatic reread did not reach the transport")
    rereadHold.release()
    var projection = ListProjection<Person>()
    let deadline = ContinuousClock.now + .seconds(10)
    while ContinuousClock.now < deadline && projection.rows.map(\.id) != current + ["u9"] {
      let remaining = deadline - ContinuousClock.now
      guard let delivery = await cursor.next("the reread delivery", timeout: remaining) else { break }
      _ = projection.apply(delivery)
    }
    #expect(projection.rows.map(\.id) == current + ["u9"])
  }
}
