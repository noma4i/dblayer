import Foundation
@testable import DBlayer

final class RecordingLease: CableLease, @unchecked Sendable {
  private let lock = NSLock()
  private var opened: String?
  var token: String? { lock.withLock { opened } }
  override func opened(token: String) throws { lock.withLock { opened = token } }
}
