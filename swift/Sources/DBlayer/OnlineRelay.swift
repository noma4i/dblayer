import Foundation

final class OnlineRelay: @unchecked Sendable {
  private let lock = NSLock()
  private let connectivity: @Sendable (Bool) -> Void
  private var online = false
  private var sink: (@Sendable (Bool) -> Void)?
  private var connectivityDue = false
  private var sinkDue = false
  private var delivering = false

  init(connectivity: @escaping @Sendable (Bool) -> Void) {
    self.connectivity = connectivity
  }

  func attach(_ sink: @escaping @Sendable (Bool) -> Void) {
    let previous = lock.withLock { () -> (@Sendable (Bool) -> Void)? in
      defer { self.sink = sink; sinkDue = true }
      return self.sink
    }
    withExtendedLifetime(previous) {}
    deliver()
  }

  func set(_ online: Bool) {
    lock.withLock {
      self.online = online
      connectivityDue = true
      sinkDue = sink != nil
    }
    deliver()
  }

  private func deliver() {
    let claimed = lock.withLock { () -> Bool in
      if delivering { return false }
      delivering = true
      return true
    }
    guard claimed else { return }
    while true {
      let work = lock.withLock { () -> (online: Bool, connectivity: Bool, sink: (@Sendable (Bool) -> Void)?)? in
        guard connectivityDue || sinkDue else { delivering = false; return nil }
        defer { connectivityDue = false; sinkDue = false }
        return (online, connectivityDue, sinkDue ? sink : nil)
      }
      guard let work else { return }
      if work.connectivity { connectivity(work.online) }
      work.sink?(work.online)
    }
  }
}
