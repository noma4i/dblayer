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

  static var backgroundName: Notification.Name? {
    #if canImport(UIKit)
    UIApplication.didEnterBackgroundNotification
    #elseif canImport(AppKit)
    NSApplication.didResignActiveNotification
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

  /// Calls `deliver` once at registration when the app is active, then on every notification, and `background` on every move to the background. The core drops a delivery of a registration that ended, so the stop needs no wait.
  static func observe(name: Notification.Name? = ForegroundEvents.systemName, active: @escaping @Sendable () -> Bool = { ForegroundEvents.systemActive }, backgroundName: Notification.Name? = ForegroundEvents.backgroundName, _ deliver: @escaping @Sendable () -> Void, background: @escaping @Sendable () -> Void = {}) -> @Sendable () -> Void {
    guard let name else { return {} }
    let initial = DispatchWorkItem { if active() { deliver() } }
    DispatchQueue.main.async(execute: initial)
    let token = NotificationCenter.default.addObserver(forName: name, object: nil, queue: nil) { _ in deliver() }
    let backgroundToken = backgroundName.map { NotificationCenter.default.addObserver(forName: $0, object: nil, queue: nil) { _ in background() } }
    return {
      initial.cancel()
      NotificationCenter.default.removeObserver(token)
      if let backgroundToken { NotificationCenter.default.removeObserver(backgroundToken) }
    }
  }
}
