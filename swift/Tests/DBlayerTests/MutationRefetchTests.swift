import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

private let refetchDefs = #"[{"name":"MomentsFeed","document":"query MomentsFeed($first: Int, $after: String) { feed(first: $first, after: $after) { edges { impressionScore impressionUuid rank node { id uuid likedByMe media createdAt } } pageInfo { hasNextPage endCursor } } }"},{"name":"LikeMoment","document":"mutation LikeMoment($momentId: ID!) { likeMoment(momentId: $momentId) { id uuid likedByMe media createdAt } }"}]"#
private let feedReply = #"{"data":{"feed":{"edges":[{"impressionUuid":"imp-1","impressionScore":5,"node":{"__typename":"Moment","id":"1","uuid":"a","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}],"pageInfo":{"hasNextPage":false,"endCursor":"c"}}}}"#
private let likeReply = #"{"data":{"likeMoment":{"__typename":"Moment","id":"1","uuid":"a","likedByMe":true,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}"#

extension BridgeProbeTests {
  /// A call that waits for the answer of a mutation does not wait for a load of the list the mutation refetches that was already in flight. The list is read again once that load ends.
  @MainActor
  @Test(.timeLimit(.minutes(2)))
  func aMutationSettlesWhileALoadOfItsRefetchedListIsHeld() async throws {
    let transport = MockTransport()
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 64), modelsJson: modelsJSON, transport: transport)
    try DB.database.registerOperations(defsJson: refetchDefs)
    transport.enqueue("MomentsFeed", reply: feedReply)
    let list = ListReader(Moment.feed)
    try await list.waitForInitialLoad()
    let moment = try #require(try await Query<Moment>().readRow("1"))
    let hold = transport.hold("MomentsFeed", reply: feedReply)
    let refreshing = Task { try await list.refresh() }
    await hold.entered()
    let likeHold = transport.hold("LikeMoment", reply: likeReply)
    let rereadHold = transport.hold("MomentsFeed", reply: feedReply)

    _ = try await moment.like()
    let settling = Task { try await moment.settled(action: "like") }
    await likeHold.entered()
    likeHold.release()
    let returned = await finishes("the like to settle while the list load is held") { _ = try? await settling.value }
    #expect(returned)
    try await settling.value
    hold.release()
    try await refreshing.value
    await rereadHold.entered()
    rereadHold.release()

    await eventually("the list is read again after the held load") { transport.calls.filter { $0.operationName == "MomentsFeed" }.count == 3 }
  }
}
