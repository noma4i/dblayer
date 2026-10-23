import Foundation
import Testing
@testable import DBlayer

private struct PeekFailed: Error {}

private final class ThrowingSource: EntitySource, @unchecked Sendable {
  func cachedRow(_ id: String) throws -> WireRow? { throw PeekFailed() }
  func retain(_ ids: [String]) {}
  func release(_ ids: [String]) {}
}

private final class BrokenRowSource: EntitySource, @unchecked Sendable {
  func cachedRow(_ id: String) throws -> WireRow? { var row = SilentErrorTests.wire(id); row.layout = "other"; return row }
  func retain(_ ids: [String]) {}
  func release(_ ids: [String]) {}
}

@MainActor
struct SilentErrorTests {
  nonisolated static func wire(_ id: String) -> WireRow {
    WireRow(id: id, revision: 1, estimatedBytes: 1, layout: Moment.wireLayout,
      cells: [.text(value: id), .null, .text(value: id), .bool(value: false), .keyed(entries: []), .null, .text(value: "x")],
      sync: WireSync(state: .synced, actions: [], dirtyFields: [], error: nil, attempts: 0, since: 0), upload: nil)
  }

  nonisolated static func full(_ rows: [WireRow]) -> LiveChange {
    LiveChange(revision: 1, full: true, size: UInt32(rows.count), rows: rows, rowIndexes: [], removed: [], placed: [], replaced: [], renamed: [], count: nil, error: nil, inserted: rows.indices.map { UInt32($0) })
  }

  @Test
  func aRowThatDoesNotDecodeReachesTheStateAsAFailure() {
    var broken = Self.wire("2")
    broken.layout = "other"
    let applied = ListFeed<Moment>().apply(Self.full([Self.wire("1"), broken]))
    #expect(applied.undecodable == ["2"])
    #expect(applied.failure != nil)
    #expect(ListFeed<Moment>().apply(Self.full([Self.wire("1")])).failure == nil)
  }

  @Test
  func aSeedThatDoesNotDecodeFailsTheEntityPhase() {
    let entity = Entities<Moment>(source: BrokenRowSource()).entity("1")
    guard case .failed = entity.phase else { Issue.record("phase \(entity.phase)"); return }
    #expect(entity.value == nil)
  }

  @Test
  func aSeedReadThatThrowsFailsTheEntityPhase() {
    let entity = Entities<Moment>(source: ThrowingSource()).entity("1")
    guard case .failed = entity.phase else { Issue.record("phase \(entity.phase)"); return }
  }

  @Test
  func readThrowsOnTextThatIsNotJSON() {
    #expect(throws: (any Error).self) { try JSONValue.read("{broken") }
  }
}

private final class IdleSink: SubscriptionSink, @unchecked Sendable {
  override func ready() {}
  override func failed(error: DbError) {}
  override func closed() {}
}

struct SubscribeRequestTests {
  @Test
  func variablesThatAreNotJSONLeaveNoSocket() async throws {
    let session = URLSession(configuration: .ephemeral)
    defer { session.invalidateAndCancel() }
    let transport = UrlSessionTransport(endpoint: URL(string: "http://127.0.0.1:9")!, headers: { [:] }, session: session)
    let request = TransportRequest(document: "subscription S { id }", operationName: "S", variablesJson: "{broken")
    await #expect(throws: (any Error).self) { try await transport.subscribe(request: request, sink: IdleSink(noHandle: .init())) }
    #expect(transport.openSocketCount == 0)
  }
}
