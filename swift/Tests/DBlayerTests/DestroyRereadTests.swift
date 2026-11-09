import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

extension BridgeProbeTests {
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func aMissingDestroyedRowCausesOneRereadOfTheOpenList() async throws {
    await DB.closeForCleanup()
    let transport = MockTransport()
    for (id, uuid) in [("1", "a"), ("2", "b")] {
      transport.enqueue("MomentsFeed", reply: #"{"data":{"feed":{"nodes":[{"__typename":"Moment","id":"\#(id)","uuid":"\#(uuid)","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}"#)
    }
    transport.enqueue("DeleteMoment", reply: #"{"errors":[{"message":"gone","extensions":{"code":"NOT_FOUND"}}]}"#)
    try await openSigned(config: Config(path: nil, dataVersion: nil), modelsJson: modelsJSON, transport: transport)
    try await registerStandOperations(DB.database, defsJson: #"[{"name":"MomentsFeed","document":"query MomentsFeed($first: Int, $after: String) { feed(first: $first, after: $after) { nodes { id uuid likedByMe media createdAt } pageInfo { hasNextPage endCursor } } }"},{"name":"DeleteMoment","document":"mutation DeleteMoment($id: ID!) { deleteMoment(id: $id) { id } }"}]"#)
    let held = Projected<Moment>(try DB.account().stream(Moment.feed))
    let (events, signal) = AsyncStream<Int>.makeStream()
    let cursor = Cursor(events)
    defer { signal.finish() }
    let initial = Task { @MainActor in
      do { try await held.loaded(); signal.yield(1) }
      catch { signal.yield(9) }
    }
    defer { initial.cancel() }
    guard await cursor.next("the first list is shown") == 1 else { Issue.record("the first load failed"); return }
    let row = try #require(held.value)
    try #require(row.id == "1")
    let outcome = Task {
      do {
        let receipt = try await row.destroy()
        if case .confirmed = try await receipt.outcome() { signal.yield(2) }
        else { signal.yield(9) }
      } catch { signal.yield(9) }
    }
    defer { outcome.cancel() }
    guard await cursor.next("the missing row is confirmed") == 2 else { Issue.record("the missing row was not confirmed"); return }
    guard await observe("the reread is shown", { held.rows.map(\.id) == ["2"] }) else { return }
    #expect(transport.calls.filter { $0.operationName == "MomentsFeed" }.count == 2)
  }
}
