import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

private final class FirstFrameSink: ValueSink, @unchecked Sendable {
  struct Frame { let onMain: Bool; let seq: UInt64; let ids: [String]?; let revision: Int64? }

  private let lock = NSLock()
  private var frame: Frame?

  var first: Frame? { lock.withLock { frame } }

  func delivered(delivery: StreamDelivery) {
    var ids: [String]?
    var revision: Int64?
    if case let .snapshot(rev, rows, _) = delivery.value { ids = rows.map(\.id); revision = rev }
    lock.withLock { if frame == nil { frame = Frame(onMain: Thread.isMainThread, seq: delivery.seq, ids: ids, revision: revision) } }
  }
}

@MainActor private func names(_ projected: Projected<User>) -> [String] { projected.rows.map(\.name) }

/// Spec B, first frame: the cached rows come with the first delivery, read off the calling thread. Related queries: the target rows of a link follow a change of the target and of the link.
extension BridgeProbeTests {
  /// Requirement: a stream opened from the main thread over cached rows delivers them in its very first delivery, off the calling thread, so never inside the opening call. Observable: the sequence number, the thread and the snapshot content of the first delivery, whatever its kind.
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func theFirstFrameOfAWarmStreamArrivesOffTheCallingThreadWithTheCachedRows() async throws {
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: MockTransport())
    let ann = try await User.create(name: "Ann").optimisticValue!
    let seats = [try await Presence.create(userId: ann.id, room: "r").optimisticValue!, try await Presence.create(userId: ann.id, room: "r").optimisticValue!]
    let scope = try DB.account()
    let sink = FirstFrameSink()

    let stream = try DB.database.subscribe(epoch: scope.epoch, request: .query(queryJson: try Presence.where(.equal("room", .string("r"))).json), sink: sink)
    defer { try? DB.database.cancelStream(stream: stream) }

    await eventually("the first frame arrives") { sink.first != nil }
    let frame = try #require(sink.first)
    #expect(frame.seq == 1)
    #expect(!frame.onMain, "the first delivery came on the calling thread, inside the opening call")
    #expect(frame.revision != nil, "the first delivery was not a snapshot with a revision")
    #expect(Set(frame.ids ?? []) == Set(seats.map(\.id)), "the first delivery lacked the cached rows: \(String(describing: frame.ids))")
  }

  /// Requirement: a related query returns the target rows of the link, and follows a change of a target row and a change of the link. Observable: the names the projection shows after each step.
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func aRelatedQueryReturnsTheLinkedRowsAndFollowsATargetChangeAndALinkChange() async throws {
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: MockTransport())
    let ann = try await User.create(name: "Ann").optimisticValue!
    let bob = try await User.create(name: "Bob").optimisticValue!
    let seat = try await Presence.create(userId: ann.id, room: "r").optimisticValue!
    let related = Projected<User>(try DB.account().related(Presence.where(.equal("room", .string("r"))), Presence.Relations.user))
    try await related.loaded()

    await eventually("the linked row shows") { names(related) == ["Ann"] }

    _ = try await ann.update(name: "Anna")
    await eventually("a change of the target shows") { names(related) == ["Anna"] }

    _ = try await seat.update(userId: bob.id)
    await eventually("a change of the link moves the target") { names(related) == ["Bob"] }

    _ = try await Presence.create(userId: ann.id, room: "r")
    await eventually("a second link adds its target") { Set(names(related)) == ["Anna", "Bob"] && names(related).count == 2 }
  }
}
