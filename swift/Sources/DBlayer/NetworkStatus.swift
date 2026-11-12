import Foundation

/// The values of the network of the phone as the platform reports them, pulled one by one from the core. The first is the current value, and there is none before the platform gave the first. A slow reader gets the newest one. Cancelling the reading task ends only that reader.
public struct NetworkChanges: AsyncSequence, Sendable {
  public typealias Element = Bool
  let status: NetworkStatus

  public func makeAsyncIterator() -> AsyncIterator { AsyncIterator(subscription: status.subscribe()) }

  public struct AsyncIterator: AsyncIteratorProtocol {
    let subscription: NetworkSubscription

    public mutating func next() async -> Bool? {
      let subscription = subscription
      return await withTaskCancellationHandler { await subscription.next() } onCancel: { subscription.cancel() }
    }
  }
}

extension NetworkStatus {
  /// The last network value of the platform, `nil` before the first.
  public var isOnline: Bool? { current() }
  public var changes: NetworkChanges { NetworkChanges(status: self) }
}
