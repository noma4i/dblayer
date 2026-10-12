import Foundation
import Testing

/// The one bound of every wait: it only stops a hang, no wait is expected to come near it.
let hangGuard: Duration = .seconds(60)

/// The one way a test waits for a condition. It is bounded, and the failure names the condition and the line.
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

/// Reads a stream one element at a time. Every read is bounded and names what it waits for.
final class Cursor<Sequence: AsyncSequence>: @unchecked Sendable where Sequence.Element: Sendable {
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

  /// Reads until an element satisfies the predicate, within one bound for the whole wait.
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

  /// Reads `count` elements, within one bound for the whole wait.
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
