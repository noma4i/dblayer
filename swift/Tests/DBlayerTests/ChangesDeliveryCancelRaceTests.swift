import Foundation
import Testing
@testable import DBlayer

private actor Finished {
  private(set) var done = false
  func mark() { done = true }
}

struct ChangesDeliveryCancelRaceTests {
  @Test
  func swiftChangesDeliveryCancelRace() async throws {
    let models = #"[{"key":"Moment","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"}},{"name":"text","kind":{"type":"scalar","kind":"str"}}]}]"#
    let database = try await openDatabase(config: Config(path: nil, dataVersion: nil, cacheRows: 8), modelsJson: models, transport: nil, encrypted: nil, credentials: nil, http: nil)
    let base = try database.subscriptionCensus().subscriptions
    let queue = try database.changes(queryJson: #"{"model":"Moment"}"#)
    let cursor = Cursor(AsyncStream { await queue.next() })
    let readerEnded = Finished()
    let producerEnded = Finished()
    let cancellerEnded = Finished()
    Task {
      await eventually("the pending next ends after the cancel") { await cursor.next("the next change") == nil }
      await readerEnded.mark()
    }
    Task {
      for number in 0..<20 { _ = try? await database.create(model: "Moment", valuesJson: #"{"text":"\#(number)"}"#, argsJson: "{}") }
      await producerEnded.mark()
    }
    Task {
      await Task.yield()
      queue.cancel()
      await cancellerEnded.mark()
    }
    await eventually("the reader ends") { await readerEnded.done }
    await eventually("the producer ends") { await producerEnded.done }
    await eventually("the canceller ends") { await cancellerEnded.done }
    #expect(await cursor.next("a cancelled stream stays ended") == nil)
    await eventually("no subscription remains") { (try? database.subscriptionCensus().subscriptions) == base }
  }
}
