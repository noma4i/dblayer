import Testing
@testable import DBlayer

/// C09: a reader that fell behind reads a jump in the sequence of the deliveries. A jump means that deliveries were dropped, also the snapshot that was asked for.
struct ProjectionRecoveryTests {
  private let state = StreamState(phase: .loaded, initial: false, refreshing: false, fetchingMore: false, hasMore: false, complete: true, total: nil, loadedAt: nil, error: nil, errorKind: nil)

  private func snapshot(_ seq: UInt64, revision: Int64) -> StreamDelivery {
    StreamDelivery(stream: 1, epoch: 1, seq: seq, value: .snapshot(revision: revision, rows: [], state: state))
  }

  private func delta(_ seq: UInt64, base: Int64, revision: Int64) -> StreamDelivery {
    StreamDelivery(stream: 1, epoch: 1, seq: seq, value: .delta(base: base, revision: revision, size: 0, rows: [], rowIndexes: [], removed: [], placed: [], replaced: [], renamed: [], state: state))
  }

  @Test
  func aLostRecoverySnapshotAsksAgainAndTheNextSnapshotClosesTheGap() {
    var projection = ListProjection<Moment>()
    var applied = projection.apply(snapshot(1, revision: 1))
    #expect(applied == .changed)
    applied = projection.apply(delta(2, base: 1, revision: 2))
    #expect(applied == .changed)
    applied = projection.apply(delta(4, base: 3, revision: 4))
    #expect(applied == .resync, "a delta behind a gap must ask for a snapshot")
    applied = projection.apply(delta(7, base: 6, revision: 7))
    #expect(applied == .resync, "the recovery snapshot was dropped: the projection must ask again")
    applied = projection.apply(delta(8, base: 7, revision: 8))
    #expect(applied == .ignored, "one request per gap: a delta without a jump waits for the snapshot")
    applied = projection.apply(snapshot(9, revision: 9))
    #expect(applied == .changed)
    applied = projection.apply(delta(10, base: 9, revision: 10))
    #expect(applied == .changed)
  }

  @Test
  func aJumpAtTheFirstDeliveryReadAsksForASnapshotAlthoughTheBaseMatches() {
    var projection = ListProjection<Moment>()
    var applied = projection.apply(snapshot(1, revision: 1))
    #expect(applied == .changed)
    applied = projection.apply(delta(300, base: 1, revision: 2))
    #expect(applied == .resync, "deliveries before this one were dropped: the rows may be stale")
  }
}
