import Foundation

private final class CancellationState: @unchecked Sendable {
  private let lock = NSLock()
  private let onCancel: @Sendable () -> Void
  private var isCancelled = false

  init(onCancel: @escaping @Sendable () -> Void) {
    self.onCancel = onCancel
  }

  func cancel() {
    lock.lock()
    guard !isCancelled else {
      lock.unlock()
      return
    }
    isCancelled = true
    lock.unlock()
    onCancel()
  }
}

public struct SubscriptionHandle: Sendable {
  private let state: CancellationState

  public init(onCancel: @escaping @Sendable () -> Void) {
    state = CancellationState(onCancel: onCancel)
  }

  public func cancel() {
    state.cancel()
  }
}
