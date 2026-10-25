import Foundation

final class RetrySnapshotListener: RetryProgressListener, @unchecked Sendable {
  let body: @Sendable (RetrySnapshot) -> Void
  init(_ body: @escaping @Sendable (RetrySnapshot) -> Void) { self.body = body }
  func changed(snapshot: RetrySnapshot) { body(snapshot) }
}

extension RetryProgress {
  /// The encrypted HTTP calls that wait for a retry. A reader gets the current snapshot first, then the newest one. Cancelling the reading task ends only that reader.
  public var changes: LiveSource<RetrySnapshot> {
    LiveSource(initial: current()) { emit in
      let subscription = try self.subscribe(listener: RetrySnapshotListener(emit))
      return { subscription.cancel() }
    }
  }
}
