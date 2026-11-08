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

  /// Calls `deliver` once at registration when the app is active, then on every notification. The returned stop returns after the queued first call ran or was dropped: no `deliver` runs after it.
  static func observe(name: Notification.Name? = ForegroundEvents.systemName, active: @escaping @Sendable () -> Bool = { ForegroundEvents.systemActive }, _ deliver: @escaping @Sendable () -> Void) -> @Sendable () -> Void {
    guard let name else { return {} }
    let initial = DispatchWorkItem { if active() { deliver() } }
    DispatchQueue.main.async(execute: initial)
    let token = NotificationCenter.default.addObserver(forName: name, object: nil, queue: nil) { _ in deliver() }
    return {
      initial.cancel()
      NotificationCenter.default.removeObserver(token)
      if !Thread.isMainThread { initial.wait() }
    }
  }
}
