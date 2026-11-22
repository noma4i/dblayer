import Foundation

public struct CloudChanges: AsyncSequence, Sendable {
  public typealias Element = CloudStatus
  let database: Database

  public func makeAsyncIterator() -> AsyncIterator {
    let subscription = database.cloudChanges()
    return AsyncIterator(subscription: subscription, initial: subscription == nil ? database.cloudStatus() : nil)
  }

  public struct AsyncIterator: AsyncIteratorProtocol {
    let subscription: CloudSubscription?
    var initial: CloudStatus?

    public mutating func next() async -> CloudStatus? {
      guard let subscription else {
        defer { initial = nil }
        return Task.isCancelled ? nil : initial
      }
      return await withTaskCancellationHandler { await subscription.next() } onCancel: { subscription.cancel() }
    }
  }
}
