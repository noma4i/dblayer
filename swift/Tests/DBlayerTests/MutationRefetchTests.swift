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
    let before = transport.calls.filter { $0.operationName == "ClubMembers" }.count
    let loadHold = transport.hold("ClubMembers", reply: membersReply(current))
    let mutationHold = transport.hold("RemoveClubMember", reply: clubAnswer("removeClubMember"))
    let rereadHold = transport.hold("ClubMembers", reply: membersReply(Array(current.dropLast()) + ["u9"]))
    let (signals, signal) = AsyncStream<Int>.makeStream()
    let cursor = Cursor(signals)
    defer { signal.finish(); loadHold.release(); mutationHold.release(); rereadHold.release() }
    let refreshing = Task { try await list.refresh() }
    defer { refreshing.cancel() }
    let oldEntered = Task { await loadHold.entered(); signal.yield(1) }
    defer { oldEntered.cancel() }
    guard await cursor.next("the old load", timeout: .seconds(20)) == 1 else { Issue.record("the old load did not start"); return }
    let removing = Task { try await club.removeMember(userIds: ["u4"]) }
    defer { removing.cancel() }
    let mutationEntered = Task { await mutationHold.entered(); signal.yield(2) }
    defer { mutationEntered.cancel() }
    guard await cursor.next("the mutation", timeout: .seconds(20)) == 2 else { Issue.record("the mutation did not start"); return }
    mutationHold.release()
    let mutationFinished = Task {
      do {
        let receipt = try await removing.value
        if case .confirmed = try await receipt.outcome() { signal.yield(3) }
        else { signal.yield(8) }
      } catch { signal.yield(8) }
    }
    defer { mutationFinished.cancel() }
    guard await cursor.next("the mutation outcome", timeout: .seconds(20)) == 3 else { Issue.record("the mutation was not confirmed"); return }
    #expect(transport.calls.filter { $0.operationName == "ClubMembers" }.count == before + 1)
    loadHold.release()
    let rereadEntered = Task { await rereadHold.entered(); signal.yield(4) }
    defer { rereadEntered.cancel() }
    guard await cursor.next("the reread", timeout: .seconds(20)) == 4 else { Issue.record("the reread did not start"); return }
    #expect(transport.calls.filter { $0.operationName == "ClubMembers" }.count == before + 2)
    rereadHold.release()
    let refreshFinished = Task { _ = await refreshing.result; signal.yield(5) }
    defer { refreshFinished.cancel() }
    guard await cursor.next("the old load ends", timeout: .seconds(20)) == 5 else { Issue.record("the old load did not end"); return }
    try await refreshing.value
    guard await observe("the list shows the reread", { list.ids == Array(current.dropLast()) + ["u9"] }) else { return }
    #expect(transport.calls.filter { $0.operationName == "ClubMembers" }.count == before + 2)
  }
}
