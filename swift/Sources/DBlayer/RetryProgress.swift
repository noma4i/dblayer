import Foundation

/// The snapshots of the encrypted HTTP calls that wait for a retry, pulled one by one from the core. The first is the current snapshot. A slow reader gets the newest one. Cancelling the reading task ends only that reader.
public struct RetryChanges: AsyncSequence, Sendable {
  public typealias Element = RetrySnapshot
  let progress: RetryProgress

  public func makeAsyncIterator() -> AsyncIterator { AsyncIterator(subscription: progress.subscribe()) }

  public struct AsyncIterator: AsyncIteratorProtocol {
    let subscription: RetrySubscription

    public mutating func next() async -> RetrySnapshot? {
      let subscription = subscription
      return await withTaskCancellationHandler { await subscription.next() } onCancel: { subscription.cancel() }
    }
  }
}

extension RetryProgress {
  public var changes: RetryChanges { RetryChanges(progress: self) }
}
