import Testing
@testable import DBlayer

/// C09: a reader learns of a gap from the base of a delta, not from the positions of the deliveries. A position counts entries of the core journal, and the core replaces dropped list values with one snapshot.
struct ProjectionRecoveryTests {
  private let state = ReadState(phase: .loaded, error: nil, loaded: nil, hasMore: false, settled: true, total: nil)

  private func snapshot(revision: Int64) -> ReadValue {
    .snapshot(revision: revision, rows: [], state: state)
  }

  private func delta(base: Int64, revision: Int64) -> ReadValue {
    .delta(base: base, revision: revision, size: 0, rows: [], indexes: [], removed: [], placed: [], replaced: [], renamed: [], state: state)
  }

  @Test
  func aDeltaOffTheShownRevisionAsksOnceForASnapshotAndTheSnapshotClosesTheGap() {
    var projection = ListProjection<Moment>()
    var applied = projection.apply(snapshot(revision: 1))
    #expect(applied == .changed)
    applied = projection.apply(delta(base: 1, revision: 2))
    #expect(applied == .changed)
    applied = projection.apply(delta(base: 3, revision: 4))
    #expect(applied == .resync, "a delta off the shown revision must ask for a snapshot")
    applied = projection.apply(delta(base: 4, revision: 5))
    #expect(applied == .ignored, "one request per gap: the next delta waits for the snapshot")
    applied = projection.apply(snapshot(revision: 5))
    #expect(applied == .changed)
    applied = projection.apply(delta(base: 5, revision: 6))
    #expect(applied == .changed)
  }

  @Test
  func aJumpOfThePositionWithTheShownBaseAppliesTheDelta() {
    var projection = ListProjection<Moment>()
    var applied = projection.apply(snapshot(revision: 1))
    #expect(applied == .changed)
    applied = projection.apply(delta(base: 1, revision: 300))
    #expect(applied == .changed, "a jump of the revision with the shown base is no gap")
  }
}
