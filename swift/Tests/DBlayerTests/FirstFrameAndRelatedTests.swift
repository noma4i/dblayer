import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

/// Spec B, first frame: the cached rows come with the first value, read off the calling thread. Related queries: a stream over a server relation follows a change of its rows and of the link.
extension BridgeProbeTests {
  /// Requirement: a stream opened over cached rows delivers them in its very first value, which the reader takes off the main thread. Observable: the kind, the rows and the revision of the first value, and the thread that took it.
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func theFirstFrameOfAWarmStreamArrivesOffTheCallingThreadWithTheCachedRows() async throws {
    try await openSigned(config: Config(path: nil, dataVersion: nil), modelsJson: modelsJSON, transport: MockTransport())
    let ann = try await User.create(name: "Ann").optimisticValue!
    let seats = [try await Presence.create(userId: ann.id, room: "r").optimisticValue!, try await Presence.create(userId: ann.id, room: "r").optimisticValue!]
    let stream = try DB.account().stream(Presence.where(.equal("room", .string("r"))))

    let first = await Task.detached { () -> (onMain: Bool, ids: [String]?, revision: Int64?) in
      guard case let .snapshot(revision, rows, _, _)? = await Cursor(stream).next("the first value of the stream") else { return ((pthread_main_np() == 1), nil, nil) }
      return ((pthread_main_np() == 1), rows.map(\.id), revision)
    }.value
    await stream.cancel()

    #expect(!first.onMain, "the first value was taken on the calling thread")
    #expect(first.revision != nil, "the first value was not a snapshot with a revision")
    #expect(Set(first.ids ?? []) == Set(seats.map(\.id)), "the first value lacked the cached rows: \(String(describing: first.ids))")
  }
}
