import Foundation

final class RetrySnapshotListener: RetryProgressListener, @unchecked Sendable {
  let body: @Sendable (RetrySnapshot) -> Void
  init(_ body: @escaping @Sendable (RetrySnapshot) -> Void) { self.body = body }
  func changed(snapshot: RetrySnapshot) { body(snapshot) }
}

extension RetryProgress {
  /// The encrypted HTTP calls that wait for a retry. A reader gets the current snapshot first, then the newest one. Cancelling the reading task ends only that reader.
  public var changes: AsyncStream<RetrySnapshot> {
    AsyncStream(bufferingPolicy: .bufferingNewest(1)) { continuation in
      continuation.yield(current())
      do {
        let subscription = try subscribe(listener: RetrySnapshotListener { continuation.yield($0) })
        continuation.onTermination = { _ in subscription.cancel() }
      } catch {
        continuation.finish()
      }
    }
  }
}
