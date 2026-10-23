import Testing
@testable import DBlayer

@MainActor
struct LiveCancellationTests {
  @Test
  func currentRethrowsCancellation() async {
    let source = LiveSource<Int>(initial: 7) { _ in throw CancellationError() }

    await #expect(throws: CancellationError.self) { try await source.current() }
  }

  struct Refused: Error {}

  @Test
  func aCancelledStreamIsNotAFailureOfTheBox() async {
    let cancelled = LiveBox(LiveSource<Int>(initial: 7) { _ in throw CancellationError() })
    let refused = LiveBox(LiveSource<Int>(initial: 7) { _ in throw Refused() })

    await eventually("the refused box failure") { refused.failure != nil }
    #expect(cancelled.failure == nil)
  }

  @Test
  func aCancelledSubscriptionIsNotAFailureOfTheList() async {
    let cancelled = LiveList(Query<Moment>(), snapshots: LiveSource<Snapshot<Moment>>(initial: Snapshot(rows: [], replaced: [:])) { _ in throw CancellationError() })
    let refused = LiveList(Query<Moment>(), snapshots: LiveSource<Snapshot<Moment>>(initial: Snapshot(rows: [], replaced: [:])) { _ in throw Refused() })

    await eventually("the refused list error") { refused.snapshot.error != nil }
    #expect(cancelled.snapshot.error == nil)
  }
}
