import CloudKit
import Foundation
#if canImport(UIKit)
import UIKit
#elseif canImport(AppKit)
import AppKit
#endif

private final class LinkRegistry: @unchecked Sendable {
  private let lock = NSLock()
  private var links: [CloudKitLink] = []

  func add(_ link: CloudKitLink) {
    lock.withLock { links.append(link) }
  }

  func remove(_ link: CloudKitLink) {
    lock.withLock { links.removeAll { $0 === link } }
  }

  func live() -> [CloudKitLink] {
    lock.withLock { links }
  }
}

public final class CloudKitLink: @unchecked Sendable {
  private static let registry = LinkRegistry()

  public let link: CloudLink
  private var foreground: NSObjectProtocol?

  init(link: CloudLink) {
    self.link = link
    foreground = NotificationCenter.default.addObserver(forName: Self.foregroundName, object: nil, queue: nil) { _ in
      link.remoteChanged()
    }
    Self.registry.add(self)
  }

  public func stop() {
    Self.registry.remove(self)
    if let foreground { NotificationCenter.default.removeObserver(foreground) }
    foreground = nil
    link.stop()
  }

  @discardableResult
  public static func receive<Key: Hashable>(_ userInfo: [Key: Any]) -> Bool {
    guard CKNotification(fromRemoteNotificationDictionary: userInfo)?.subscriptionID == CloudKitRecords.subscriptionID else { return false }
    registry.live().forEach { $0.link.remoteChanged() }
    return true
  }

  private static var foregroundName: Notification.Name {
    #if canImport(UIKit)
    UIApplication.willEnterForegroundNotification
    #elseif canImport(AppKit)
    NSApplication.didBecomeActiveNotification
    #endif
  }
}

extension Database {
  @discardableResult
  public func connectCloudKit(container: CKContainer) throws -> CloudKitLink {
    let link = try connectCloud(io: CloudKitAdapter(container: container))
    #if canImport(UIKit)
    DispatchQueue.main.async { UIApplication.shared.registerForRemoteNotifications() }
    #endif
    return CloudKitLink(link: link)
  }
}
