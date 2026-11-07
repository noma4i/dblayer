import Foundation

/// The common source of change events of the tests: the doubles and the collectors call `pulse()` at each change a test can observe, a waiter resumes at the next pulse.
public final class TestPulse: @unchecked Sendable {
  public static let shared = TestPulse()

  private let lock = NSLock()
  private var generation = 0
  private var nextWaiter = 0
  private var waiters: [Int: CheckedContinuation<Void, Never>] = [:]

  public var current: Int { lock.withLock { generation } }

  public func pulse() {
    let resumed = lock.withLock { () -> [CheckedContinuation<Void, Never>] in
      generation += 1
      defer { waiters = [:] }
      return Array(waiters.values)
    }
    resumed.forEach { $0.resume() }
  }

  /// Returns at the first pulse after `seen`, or when the task is cancelled.
  public func next(after seen: Int) async {
    let id = lock.withLock { () -> Int in nextWaiter += 1; return nextWaiter }
    await withTaskCancellationHandler {
      await withCheckedContinuation { (continuation: CheckedContinuation<Void, Never>) in
        let resumeNow = lock.withLock { () -> Bool in
          if generation != seen || Task.isCancelled { return true }
          waiters[id] = continuation
          return false
        }
        if resumeNow { continuation.resume() }
      }
    } onCancel: {
      let waiter = lock.withLock { waiters.removeValue(forKey: id) }
      waiter?.resume()
    }
  }
}
