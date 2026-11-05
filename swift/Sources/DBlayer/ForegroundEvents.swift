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

  /// Calls `deliver` on every notification until the returned stop runs.
  static func observe(name: Notification.Name? = ForegroundEvents.systemName, _ deliver: @escaping @Sendable () -> Void) -> @Sendable () -> Void {
    guard let name else { return {} }
    let token = NotificationCenter.default.addObserver(forName: name, object: nil, queue: nil) { _ in deliver() }
    return { NotificationCenter.default.removeObserver(token) }
  }
}
