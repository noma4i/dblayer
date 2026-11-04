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
    let list = ListReader(joined)
    try await list.waitForInitialLoad()
    let current = ["u1", "u2", "u3", "u4"]
    let loadHold = transport.hold("ClubMembers", reply: membersReply(current))
    let addHold = transport.hold("AddClubMember", reply: clubAnswer("addClubMember"))
    let rereadHold = transport.hold("ClubMembers", reply: membersReply(current + ["u9"]))
    let refreshing = Task { try await list.refresh() }
    await loadHold.entered()
    let adding = Task { try await club.addMember(userIds: ["u9"]) }
    await addHold.entered()
    addHold.release()
    let returned = await finishes("the add to return with its answer while the list load is held") { _ = try? await adding.value }
    loadHold.release()
    rereadHold.release()
    #expect(returned, "the add waited for the held load of its refetched list")
    await finishes("the add and the refresh to end once the holds are released") {
      _ = try? await adding.value
      _ = try? await refreshing.value
    }
    try await adding.value
    try await refreshing.value

    await eventually("the list is read again after the held load") { list.ids == current + ["u9"] }
  }
}
