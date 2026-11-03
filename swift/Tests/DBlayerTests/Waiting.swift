import Foundation
import Testing
let hangGuard: Duration = .seconds(60)
func eventually(
  _ what: String = "the condition",
  timeout: Duration = hangGuard,
  isolation: isolated (any Actor)? = #isolation,
  sourceLocation: SourceLocation = #_sourceLocation,
  _ condition: () async -> Bool
) async {
  let deadline = ContinuousClock.now + timeout
  while ContinuousClock.now < deadline {
    if await condition() {
      return
    }
    try? await Task.sleep(for: .milliseconds(5))
  }
  if await condition() {
    return
  }
  Issue.record("timed out after \(timeout) waiting for \(what)", sourceLocation: sourceLocation)
}
final class Done: @unchecked Sendable {
  private let lock = NSLock()
  private var flag = false
  var value: Bool { lock.withLock { flag } }
  func set() { lock.withLock { flag = true } }
}
/// Runs `body` on its own task and records an issue when it does not return within the guard, so a call that hangs fails the test instead of stopping it.
@discardableResult
func finishes(
  _ what: String,
  timeout: Duration = hangGuard,
  isolation: isolated (any Actor)? = #isolation,
  sourceLocation: SourceLocation = #_sourceLocation,
  _ body: @escaping @Sendable () async -> Void
) async -> Bool {
  let done = Done()
  Task {
    await body()
    done.set()
  }
  await eventually("\(what) to return", timeout: timeout, isolation: isolation, sourceLocation: sourceLocation) { done.value }
  return done.value
}
func parkUntilCancelled() async throws {
  try await Task.sleep(for: .seconds(3600))
}
final class Cursor<Sequence: AsyncSequence & SendableMetatype>: @unchecked Sendable where Sequence.Element: Sendable, Sequence.AsyncIterator: SendableMetatype {
  private var iterator: Sequence.AsyncIterator

  init(_ sequence: Sequence) {
    iterator = sequence.makeAsyncIterator()
  }

  func next(
    _ what: String = "the next element",
    timeout: Duration = hangGuard,
    sourceLocation: SourceLocation = #_sourceLocation
  ) async -> Sequence.Element? {
    let element: Sequence.Element?? = await withTaskGroup(of: Sequence.Element??.self) { group in
      group.addTask { [self] in .some(try? await iterator.next()) }
      group.addTask {
        try? await Task.sleep(for: timeout)
        return nil
      }
      let first = await group.next() ?? nil
      group.cancelAll()
      return first
    }
    guard let element else {
      Issue.record("timed out after \(timeout) waiting for \(what)", sourceLocation: sourceLocation)
      return nil
    }
    return element
  }
  func first(
    _ what: String,
    timeout: Duration = hangGuard,
    sourceLocation: SourceLocation = #_sourceLocation,
    where predicate: @Sendable (Sequence.Element) -> Bool
  ) async -> Sequence.Element? {
    let deadline = ContinuousClock.now + timeout
    while ContinuousClock.now < deadline {
      let remaining = deadline - ContinuousClock.now
      guard let element = await next(what, timeout: remaining, sourceLocation: sourceLocation) else {
        return nil
      }
      if predicate(element) {
        return element
      }
    }
    Issue.record("timed out after \(timeout) waiting for \(what)", sourceLocation: sourceLocation)
    return nil
  }
  func collect(_ count: Int, _ what: String, timeout: Duration = hangGuard, sourceLocation: SourceLocation = #_sourceLocation) async -> [Sequence.Element] {
    let deadline = ContinuousClock.now + timeout
    var elements: [Sequence.Element] = []
    while elements.count < count, ContinuousClock.now < deadline {
      guard let element = await next(what, timeout: deadline - ContinuousClock.now, sourceLocation: sourceLocation) else {
        break
      }
      elements.append(element)
    }
    return elements
  }
}
