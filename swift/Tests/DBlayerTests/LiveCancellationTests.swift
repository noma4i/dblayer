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

    await box.task?.value
    #expect(box.failure == nil)
  }

  @Test
  func aCancelledSubscriptionIsNotAFailureOfTheList() async {
    let list = LiveList(Query<Moment>(), snapshots: LiveSource<Snapshot<Moment>>(initial: Snapshot(rows: [], replaced: [:])) { _ in throw CancellationError() })

    for task in list.tasks { await task.value }
    #expect(list.snapshot.error == nil)
  }
}
