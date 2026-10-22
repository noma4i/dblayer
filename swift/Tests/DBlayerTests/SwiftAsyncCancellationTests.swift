import Foundation
import Testing
@testable import DBlayer

private final class SilentQuery: QueryListener, @unchecked Sendable {
  func changed(change: LiveChange) {}
}

struct SwiftAsyncCancellationTests {
  @Test
  func aNonThrowingCurrentKeepsItsResultInACancelledTask() async throws {
    let models = #"[{"key":"Moment","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"}},{"name":"text","kind":{"type":"scalar","kind":"str"}}]}]"#
    let database = try await openDatabase(config: Config(path: nil, dataVersion: nil, cacheRows: 8), modelsJson: models, transport: nil, encrypted: nil, credentials: nil, http: nil)
    let row = try await database.create(model: "Moment", valuesJson: #"{"text":"hello"}"#, argsJson: "{}")
    let subscription = try database.liveQuery(queryJson: #"{"model":"Moment"}"#, listener: SilentQuery())
    let call = Task {
      withUnsafeCurrentTask { $0?.cancel() }
      return await subscription.current()
    }
    let value = await call.value
    #expect(value.rows.map(\.id) == [row.id])
    subscription.cancel()
  }
}
