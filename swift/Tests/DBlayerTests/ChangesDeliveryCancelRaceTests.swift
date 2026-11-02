import Foundation
import Testing
@testable import DBlayer

private final class Once<Value: Sendable>: @unchecked Sendable {
  private let lock = NSLock()
  private var continuation: CheckedContinuation<Value?, Never>?
  private var value: Value??
  func wait() async -> Value? {
    await withCheckedContinuation { (waiting: CheckedContinuation<Value?, Never>) in
      let ready = lock.withLock { () -> Value?? in
        if let value { return value }
        continuation = waiting
        return nil
      }
      if let ready { waiting.resume(returning: ready) }
    }
  }
  func finish(_ result: Value?) {
    let waiting = lock.withLock { () -> CheckedContinuation<Value?, Never>? in
      guard value == nil else { return nil }
      value = .some(result)
      defer { continuation = nil }
      return continuation
    }
    waiting?.resume(returning: result)
  }
}

private func finishesWithinTheBudget<Value: Sendable>(_ work: @escaping @Sendable () async -> Value) async -> Value? {
  let once = Once<Value>()
  Task { once.finish(await work()) }
  Task { try? await Task.sleep(for: hangGuard); once.finish(nil) }
  return await once.wait()
}

struct ChangesDeliveryCancelRaceTests {
  @Test
  func swiftChangesDeliveryCancelRace() async throws {
    let models = #"[{"key":"Moment","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"}},{"name":"text","kind":{"type":"scalar","kind":"str"}}]}]"#
    let database = try await openDatabase(config: Config(path: nil, dataVersion: nil, cacheRows: 8), modelsJson: models, transport: nil, encrypted: nil, credentials: nil, http: nil)
    let base = try database.subscriptionCensus().subscriptions
    let queue = try database.changes(queryJson: #"{"model":"Moment"}"#)
    let reader = Task { () -> Int in
      var delivered = 0
      while await queue.next() != nil { delivered += 1 }
      return delivered
    }
    let producer = Task { () -> Bool in
      for number in 0..<20 { _ = try? await database.create(model: "Moment", valuesJson: #"{"text":"\#(number)"}"#, argsJson: "{}") }
      return true
    }
    let canceller = Task { () -> Bool in
      await Task.yield()
      queue.cancel()
      return true
    }
    let ended = await finishesWithinTheBudget { await reader.value }
    #expect(ended != nil, "the pending next never ended after the cancel")
    #expect(await finishesWithinTheBudget { await producer.value } == true)
    #expect(await finishesWithinTheBudget { await canceller.value } == true)
    let after = await finishesWithinTheBudget { await queue.next() }
    #expect(after != nil && after! == nil, "a cancelled stream stays ended")
    await eventually("no subscription remains") { (try? database.subscriptionCensus().subscriptions) == base }
  }
}
