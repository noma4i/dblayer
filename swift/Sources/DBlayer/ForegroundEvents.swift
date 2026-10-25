import Foundation
#if canImport(UIKit)
import UIKit
#elseif canImport(AppKit)
import AppKit
#endif

final class ForegroundEvents: @unchecked Sendable {
  static let shared = ForegroundEvents()
  private let lock = NSLock()
  private var token: NSObjectProtocol?

  static var systemName: Notification.Name? {
    #if canImport(UIKit)
    UIApplication.willEnterForegroundNotification
    #elseif canImport(AppKit)
    NSApplication.didBecomeActiveNotification
    #else
    nil
    #endif
  }

  func start(name: Notification.Name? = ForegroundEvents.systemName, deliver: @escaping @Sendable () -> Void) {
    stop()
    guard let name else { return }
    let observer = NotificationCenter.default.addObserver(forName: name, object: nil, queue: nil) { _ in deliver() }
    lock.withLock { token = observer }
  }

  func stop() {
    let removed = lock.withLock { () -> NSObjectProtocol? in
      defer { token = nil }
      return token
    }
    if let removed { NotificationCenter.default.removeObserver(removed) }
  }
}
