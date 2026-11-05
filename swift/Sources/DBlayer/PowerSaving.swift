import Foundation

enum PowerSaving {
  static var lowRam: Bool { ProcessInfo.processInfo.physicalMemory <= 2 << 30 }

  /// Reports the OS value at once and on every change until the returned stop runs.
  static func observe(
    name: Notification.Name = .NSProcessInfoPowerStateDidChange,
    read: @escaping @Sendable () -> Bool = { ProcessInfo.processInfo.isLowPowerModeEnabled },
    _ report: @escaping @Sendable (Bool) -> Void
  ) -> @Sendable () -> Void {
    let token = NotificationCenter.default.addObserver(forName: name, object: nil, queue: nil) { _ in report(read()) }
    report(read())
    return { NotificationCenter.default.removeObserver(token) }
  }
}
