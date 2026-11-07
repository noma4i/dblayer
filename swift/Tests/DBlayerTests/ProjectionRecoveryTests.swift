import Testing
@testable import DBlayer

/// A projection mirrors the deliveries of the core: it applies each one as it comes, and a row that cannot be decoded throws so that the reader asks the core for a snapshot.
struct ProjectionRecoveryTests {
  private let state = ReadState(phase: .loaded, error: nil, loaded: nil, hasMore: false, settled: true, total: nil)

  @Test
  func aDeltaIsAppliedAsItComesAndKeepsOnlyItsOwnEdits() throws {
    var projection = ListProjection<Moment>()
    #expect(try projection.apply(.snapshot(revision: 1, rows: [], edits: [], state: state)))
    #expect(projection.lastEdits.isEmpty)
    #expect(try projection.apply(.delta(base: 1, revision: 300, edits: [], state: state)))
    #expect(projection.revision == 300 && projection.lastEdits.isEmpty)
  }

  @Test
  func theSnapshotEditsComeFromTheCore() throws {
    var projection = ListProjection<Moment>()
    projection.apply(ListDelivery<Moment>.snapshot(revision: 1, rows: [], edits: [.remove(0, "m1"), .move(1, 0)], state: state))
    #expect(projection.lastEdits.count == 2)
    guard case let .remove(index, id) = projection.lastEdits[0], case let .move(from, to) = projection.lastEdits[1] else {
      Issue.record("the edits of the snapshot are not the edits of the core: \(projection.lastEdits)")
      return
    }
    #expect(index == 0 && id == "m1" && from == 1 && to == 0)
  }
}
