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

  static var systemInBackground: Bool {
    #if canImport(UIKit)
    UIApplication.shared.applicationState == .background
    #elseif canImport(AppKit)
    !NSApplication.shared.isActive
    #else
    false
    #endif
  }

  /// Calls `deliver` once at registration unless the app is in the background, then `background` once instead (an inactive app counts as foreground), then on every notification, and `background` on every move to the background. The core drops a delivery of a registration that ended, so the stop needs no wait.
  static func observe(name: Notification.Name? = ForegroundEvents.systemName, inBackground: @escaping @Sendable () -> Bool = { ForegroundEvents.systemInBackground }, backgroundName: Notification.Name? = ForegroundEvents.backgroundName, _ deliver: @escaping @Sendable () -> Void, background: @escaping @Sendable () -> Void = {}) -> @Sendable () -> Void {
    guard let name else { return {} }
    let initial = DispatchWorkItem { if inBackground() { background() } else { deliver() } }
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
