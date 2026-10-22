import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

private final class IgnoredChanges: QueryListener, @unchecked Sendable {
  func changed(change: LiveChange) {}
}

struct ScopedSubscriptionTests {
  @Test func readingARelationWithParamsSubscribesWithThoseVariables() async throws {
    let transport = MockTransport()
    for _ in 0 ..< 4 { transport.enqueue("documentBySlug", reply: #"{"data":{"documentBySlug":{"id":"d1","slug":"a"}}}"#) }
    let database = try await openDatabase(config: Config(path: nil, dataVersion: nil, cacheRows: 8), modelsJson: modelsJSON, transport: transport)
    try database.registerOperations(defsJson: #"[{"name":"documentBySlug","document":"query documentBySlug($slug: String!, $source: String!) { documentBySlug(slug: $slug, source: $source) { id slug } }","rootPath":"documentBySlug"},{"name":"documentUpdated","document":"subscription documentUpdated($slug: String!, $source: String!) { documentUpdated(slug: $slug, source: $source) { id slug } }","rootPath":"documentUpdated"}]"#)
    let listener = IgnoredChanges()
    let first = try database.liveQuery(queryJson: #"{"model":"Document","relation":"bySlug","params":{"slug":"a","source":"web"}}"#, listener: listener)
    let second = try database.liveQuery(queryJson: #"{"model":"Document","relation":"bySlug","params":{"slug":"b","source":"web"}}"#, listener: listener)
    defer { first.cancel(); second.cancel() }
    await eventually("both subscriptions", timeout: .seconds(2)) { transport.calls.filter { $0.operationName == "documentUpdated" }.count == 2 }
    let sent = transport.calls.filter { $0.operationName == "documentUpdated" }.map(\.variablesJson)
    #expect(sent.contains { $0.contains(#""slug":"a""#) && $0.contains(#""source":"web""#) })
    #expect(sent.contains { $0.contains(#""slug":"b""#) && $0.contains(#""source":"web""#) })
  }
}
