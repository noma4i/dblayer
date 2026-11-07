import Testing
@testable import DBlayer

/// A projection mirrors the deliveries of the core: it applies each one as it comes, and a row that cannot be decoded throws so that the reader asks the core for a snapshot.
struct ProjectionRecoveryTests {
  private let state = ReadState(phase: .loaded, error: nil, loaded: nil, hasMore: false, settled: true, total: nil)

  @Test
  func aDeltaIsAppliedAsItComesAndKeepsOnlyItsOwnEdits() throws {
    var projection = ListProjection<Moment>()
    #expect(try projection.apply(.snapshot(revision: 1, rows: [], state: state)))
    #expect(projection.lastEdits.isEmpty)
    #expect(try projection.apply(.delta(base: 1, revision: 300, edits: [], state: state)))
    #expect(projection.revision == 300 && projection.lastEdits.isEmpty)
  }
}
