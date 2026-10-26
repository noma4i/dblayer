import Foundation

final class PowerSaving: @unchecked Sendable {
  static let shared = PowerSaving()
  private let lock = NSLock()
  private var token: NSObjectProtocol?

  func start(
    name: Notification.Name = .NSProcessInfoPowerStateDidChange,
    read: @escaping @Sendable () -> Bool = { ProcessInfo.processInfo.isLowPowerModeEnabled },
    report: @escaping @Sendable (Bool) -> Void
  ) {
    stop()
    let observer = NotificationCenter.default.addObserver(forName: name, object: nil, queue: nil) { _ in report(read()) }
    lock.withLock { token = observer }
    report(read())
  }

  func stop() {
    let removed = lock.withLock { () -> NSObjectProtocol? in
      defer { token = nil }
      return token
    }
    if let removed { NotificationCenter.default.removeObserver(removed) }
  }
}
