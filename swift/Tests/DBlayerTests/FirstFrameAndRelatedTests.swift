import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

private final class FirstFrameSink: ValueSink, @unchecked Sendable {
  struct Frame { let onMain: Bool; let insideSubscribe: Bool; let ids: [String] }

  private let lock = NSLock()
  private var inside = false
  private var frame: Frame?

  var first: Frame? { lock.withLock { frame } }
  func subscribing(_ value: Bool) { lock.withLock { inside = value } }

  func delivered(delivery: StreamDelivery) {
    guard case let .snapshot(_, rows, _) = delivery.value else { return }
    lock.withLock { if frame == nil { frame = Frame(onMain: Thread.isMainThread, insideSubscribe: inside, ids: rows.map(\.id)) } }
  }
}

@MainActor private func names(_ projected: Projected<User>) -> [String] { projected.rows.map(\.name) }

/// Spec B, first frame: the cached rows come with the first delivery, read off the calling thread. Related queries: the target rows of a link follow a change of the target and of the link.
extension BridgeProbeTests {
  /// Requirement: a stream opened from the main thread over cached rows delivers them in its first frame, not inside the opening call and not on the main thread. Observable: the thread flag, the flag of the opening call and the ids of the first snapshot.
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func theFirstFrameOfAWarmStreamArrivesOffTheCallingThreadWithTheCachedRows() async throws {
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: MockTransport())
    let ann = try await User.create(name: "Ann")
    let seats = [try await Presence.create(userId: ann.id, room: "r"), try await Presence.create(userId: ann.id, room: "r")]
    let scope = try DB.account()
    let sink = FirstFrameSink()

    sink.subscribing(true)
    let stream = try DB.database.subscribe(epoch: scope.epoch, request: .query(queryJson: try Presence.where(.equal("room", .string("r"))).json), sink: sink)
    sink.subscribing(false)
    defer { try? DB.database.cancelStream(stream: stream) }

    await eventually("the first frame arrives") { sink.first != nil }
    let frame = try #require(sink.first)
    #expect(!frame.onMain, "the first frame was delivered on the main thread")
    #expect(!frame.insideSubscribe, "the first frame was delivered inside the opening call")
    #expect(Set(frame.ids) == Set(seats.map(\.id)), "the first frame lacked the cached rows: \(frame.ids)")
  }

  /// Requirement: a related query returns the target rows of the link, and follows a change of a target row and a change of the link. Observable: the names the projection shows after each step.
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func aRelatedQueryReturnsTheLinkedRowsAndFollowsATargetChangeAndALinkChange() async throws {
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: MockTransport())
    let ann = try await User.create(name: "Ann")
    let bob = try await User.create(name: "Bob")
    let seat = try await Presence.create(userId: ann.id, room: "r")
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
