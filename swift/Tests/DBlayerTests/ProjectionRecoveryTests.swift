import Testing
@testable import DBlayer

/// C09: a reader learns of a gap from the base of a delta, not from the positions of the deliveries. A position counts entries of the core journal, and the core replaces dropped list values with one snapshot.
struct ProjectionRecoveryTests {
  private let state = StreamState(phase: .loaded, settled: true, revision: 0, initial: false, refreshing: false, fetchingMore: false, hasMore: false, complete: true, total: nil, loadedAt: nil, error: nil, errorKind: nil)

  private func snapshot(_ seq: UInt64, revision: Int64) -> StreamDelivery {
    StreamDelivery(stream: 1, epoch: 1, seq: seq, value: .snapshot(revision: revision, rows: [], state: state))
  }

  private func delta(_ seq: UInt64, base: Int64, revision: Int64) -> StreamDelivery {
    StreamDelivery(stream: 1, epoch: 1, seq: seq, value: .delta(base: base, revision: revision, size: 0, rows: [], rowIndexes: [], removed: [], placed: [], replaced: [], renamed: [], state: state))
  }

  @Test
  func aDeltaOffTheShownRevisionAsksOnceForASnapshotAndTheSnapshotClosesTheGap() {
    var projection = ListProjection<Moment>()
    var applied = projection.apply(snapshot(1, revision: 1))
    #expect(applied == .changed)
    applied = projection.apply(delta(2, base: 1, revision: 2))
    #expect(applied == .changed)
    applied = projection.apply(delta(3, base: 3, revision: 4))
    #expect(applied == .resync, "a delta off the shown revision must ask for a snapshot")
    applied = projection.apply(delta(4, base: 4, revision: 5))
    #expect(applied == .ignored, "one request per gap: the next delta waits for the snapshot")
    applied = projection.apply(snapshot(5, revision: 5))
    #expect(applied == .changed)
    applied = projection.apply(delta(6, base: 5, revision: 6))
    #expect(applied == .changed)
  }

  @Test
  func aJumpOfThePositionWithTheShownBaseAppliesTheDelta() {
    var projection = ListProjection<Moment>()
    var applied = projection.apply(snapshot(1, revision: 1))
    #expect(applied == .changed)
    applied = projection.apply(delta(300, base: 1, revision: 2))
    #expect(applied == .changed, "a position counts journal entries: a jump with the shown base is no gap")
  }
}
