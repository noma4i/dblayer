import Testing
@testable import DBlayer

@MainActor
struct LiveCancellationTests {
  @Test
  func currentRethrowsCancellation() async {
    let source = LiveSource<Int>(initial: 7) { _ in throw CancellationError() }

    await #expect(throws: CancellationError.self) { try await source.current() }
  }

  @Test
  func aCancelledStreamIsNotAFailureOfTheBox() async {
    let box = LiveBox(LiveSource<Int>(initial: 7) { _ in throw CancellationError() })
    try? await Task.sleep(for: .milliseconds(200))

    #expect(box.failure == nil)
  }

  @Test
  func aCancelledSubscriptionIsNotAFailureOfTheList() async {
    let snapshots = LiveSource<Snapshot<Moment>>(initial: Snapshot(rows: [], replaced: [:])) { _ in throw CancellationError() }
    let list = LiveList(Query<Moment>(), snapshots: snapshots)
    try? await Task.sleep(for: .milliseconds(200))

    #expect(list.snapshot.error == nil)
  }
}
