import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

extension BridgeProbeTests {
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func theValueOfAHeldStreamIsThereWhenLoadedReturnsAndARefreshStillAsksTheServer() async throws {
    try await DB.close()
    defer { Task { @MainActor in try? await DB.close() } }
    let transport = MockTransport()
    for _ in 0..<2 { transport.enqueue("CurrentUserQuery", reply: #"{"data":{"user":{"id":"u1","name":"u1"}}}"#) }
    try await DB.openTesting(transport: transport, modelsJSON: modelsJSON)
    try DB.database.registerOperations(defsJson: #"[{"name":"CurrentUserQuery","document":"query CurrentUserQuery { user { id name } }"}]"#)
    let held = Projected<CurrentUser>(try DB.account().stream(CurrentUser.current))

    try await held.loaded()
    #expect(held.value?.id == "u1")
    try await held.loaded()
    #expect(held.value?.id == "u1")
    #expect(transport.calls.filter { $0.operationName == "CurrentUserQuery" }.count == 1)
    try await CurrentUser.current.refresh()
    #expect(transport.calls.filter { $0.operationName == "CurrentUserQuery" }.count == 2)
  }

  /// Requirement: a delivery that applied no rows (a count of missed events, a delta that waits for the snapshot, a state after a gap) does not make the rows of its number visible: after a gap only the recovery snapshot does.
  @Test
  func aProjectionShowsTheRowsOfADeliveryOnlyWhenItAppliedThem() {
    let state = StreamState(phase: .loaded, initial: false, refreshing: false, fetchingMore: false, hasMore: false, complete: true, total: nil, loadedAt: nil, error: nil, errorKind: nil)
    func delta(_ base: Int64, _ revision: Int64) -> StreamValue {
      .delta(base: base, revision: revision, size: 0, rows: [], rowIndexes: [], removed: [], placed: [], replaced: [], renamed: [], state: state)
    }
    let steps: [(seq: UInt64, value: StreamValue, shown: UInt64)] = [
      (1, .snapshot(revision: 1, rows: [], state: state), 1),
      (2, delta(1, 2), 2),
      (9, .missed(count: 6), 2),
      (9, delta(2, 3), 2),
      (10, delta(3, 4), 2),
      (11, .state(state: state), 2),
      (12, .snapshot(revision: 5, rows: [], state: state), 12),
      (13, .ended(reason: .closed), 12),
    ]
    var projection = ListProjection<Moment>()
    for step in steps {
      _ = projection.apply(StreamDelivery(stream: 1, epoch: 1, seq: step.seq, value: step.value))
      #expect(projection.shownSeq == step.shown, "after \(step.value) at \(step.seq)")
    }
  }

  /// Requirement: when the first deliveries are a count of missed events and a state (the snapshot was dropped), nothing is shown and the recovery snapshot is asked once; the snapshot then shows its rows.
  @Test
  func aMissedThenAStateBeforeAnySnapshotShowsNothingAndAsksTheRecoverySnapshot() {
    let state = StreamState(phase: .loaded, initial: false, refreshing: false, fetchingMore: false, hasMore: false, complete: true, total: nil, loadedAt: nil, error: nil, errorKind: nil)
    let steps: [(seq: UInt64, value: StreamValue, applied: ListProjection<Moment>.Applied, shown: UInt64?)] = [
      (0, .missed(count: 5), .ignored, nil),
      (6, .state(state: state), .resync, nil),
      (7, .state(state: state), .changed, nil),
      (8, .snapshot(revision: 2, rows: [], state: state), .changed, 8),
    ]
    var projection = ListProjection<Moment>()
    for step in steps {
      let applied = projection.apply(StreamDelivery(stream: 1, epoch: 1, seq: step.seq, value: step.value))
      #expect(applied == step.applied, "applied after \(step.value)")
      #expect(projection.shownSeq == step.shown, "shown after \(step.value)")
    }
  }

  /// Requirement: a first delivery that carries a failed load is shown, so `loaded()` ends and the error is visible; no recovery snapshot is asked.
  @Test
  func aFirstStateWithAFailedLoadIsShownAndAsksNoResync() {
    let failed = StreamState(phase: .failed, initial: false, refreshing: false, fetchingMore: false, hasMore: false, complete: true, total: nil, loadedAt: nil, error: DbError.Invalid(detail: "read"), errorKind: nil)
    var projection = ListProjection<Moment>()
    let applied = projection.apply(StreamDelivery(stream: 1, epoch: 1, seq: 1, value: .state(state: failed)))
    #expect(applied == .changed)
    #expect(projection.shownSeq == 1)
    #expect(projection.state?.error != nil)
  }

  /// Requirement: `loaded()` returns normally only when the projection shows the rows of the loaded revision, and throws when the reading ends first.
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func loadedNeverReturnsWithoutRowsWhenTheReaderIsCancelledBeforeThem() async throws {
    try await DB.close()
    defer { Task { @MainActor in try? await DB.close() } }
    let transport = MockTransport()
    transport.enqueue("CurrentUserQuery", reply: #"{"data":{"user":{"id":"u1","name":"u1"}}}"#)
    try await DB.openTesting(transport: transport, modelsJSON: modelsJSON)
    try DB.database.registerOperations(defsJson: #"[{"name":"CurrentUserQuery","document":"query CurrentUserQuery { user { id name } }"}]"#)
    let stream = try DB.account().stream(CurrentUser.current)
    try await stream.loaded()
    let held = Projected<CurrentUser>(stream)
    let waiting = Task { @MainActor in try await held.loaded() }
    held.reading?.cancel()

    do {
      try await waiting.value
      #expect(held.value?.id == "u1", "loaded() returned with no rows")
    } catch {
      #expect(held.value == nil, "loaded() threw after the rows were shown")
    }
  }

  /// Requirement: for a stream of one entity that the library loads from the server, the row is visible when `loaded()` returns.
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func theRowOfAnEntityStreamThatLoadsFromTheServerIsThereWhenLoadedReturns() async throws {
    try await DB.close()
    defer { Task { @MainActor in try? await DB.close() } }
    let transport = MockTransport()
    transport.enqueue("ContainerNode", reply: #"{"data":{"node":{"id":"c1","title":"one","shareUrl":"u","isPublic":true}}}"#)
    try await DB.openTesting(transport: transport, modelsJSON: modelsJSON)
    try DB.database.registerOperations(defsJson: #"[{"name":"ContainerNode","document":"query ContainerNode($id: ID!) { node(id: $id) { ... on Container { id title shareUrl isPublic } } }"}]"#)
    let held = Projected<Container>(try DB.account().entity(Container.self, "c1", fetch: .force))

    try await held.loaded()
    #expect(held.value?.title == "one")
  }
}
