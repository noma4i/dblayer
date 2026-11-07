import Foundation
import Testing
@testable import DBlayer

/// A path the test moves by hand: every observer gets the current state at once and each change after.
final class FakePath: PathSource, @unchecked Sendable {
  private let lock = NSLock()
  private var online = false
  private var observers: [Int: @Sendable (Bool) -> Void] = [:]
  private var next = 0

  func observe(_ changed: @escaping @Sendable (Bool) -> Void) -> @Sendable () -> Void {
    let (id, current) = lock.withLock { () -> (Int, Bool) in
      next += 1
      observers[next] = changed
      return (next, online)
    }
    changed(current)
    return { [self] in lock.withLock { _ = observers.removeValue(forKey: id) } }
  }

  func set(_ value: Bool) {
    let targets = lock.withLock { () -> [@Sendable (Bool) -> Void] in
      online = value
      return Array(observers.values)
    }
    targets.forEach { $0(value) }
  }

  var observerCount: Int { lock.withLock { observers.count } }
}

extension DB {
  /// Test cleanup: a database a test already closed is closed.
  @MainActor static func closeForCleanup() async {
    do { try await close() } catch DbError.Closed(_) {} catch DbError.NotOpen(_) {} catch { Issue.record("close failed: \(error)") }
  }
}
