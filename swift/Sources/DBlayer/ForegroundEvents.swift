import Foundation
#if canImport(UIKit)
import UIKit
#elseif canImport(AppKit)
import AppKit
#endif

enum ForegroundEvents {
  static var systemName: Notification.Name? {
    #if canImport(UIKit)
    UIApplication.willEnterForegroundNotification
    #elseif canImport(AppKit)
    NSApplication.didBecomeActiveNotification
    #else
    nil
    #endif
  }

  static var systemActive: Bool {
    #if canImport(UIKit)
    UIApplication.shared.applicationState == .active
    #elseif canImport(AppKit)
    NSApplication.shared.isActive
    #else
    false
    #endif
  }

  /// Calls `deliver` once at registration when the app is active, then on every notification until the returned stop runs.
  static func observe(name: Notification.Name? = ForegroundEvents.systemName, active: @escaping @Sendable () -> Bool = { ForegroundEvents.systemActive }, _ deliver: @escaping @Sendable () -> Void) -> @Sendable () -> Void {
    guard let name else { return {} }
    DispatchQueue.main.async { if active() { deliver() } }
    let token = NotificationCenter.default.addObserver(forName: name, object: nil, queue: nil) { _ in deliver() }
    return { NotificationCenter.default.removeObserver(token) }
  }
}
