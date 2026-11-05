import Foundation
@testable import DBlayer

final class RecordingLease: CableLease, @unchecked Sendable {
  private let lock = NSLock()
  private var opened: String?
  var token: String? { lock.withLock { opened } }
  override func opened(token: String) throws { lock.withLock { opened = token } }
}

func cableURL(_ origin: URL) -> String {
  var components = URLComponents(url: origin, resolvingAgainstBaseURL: false)!
  components.scheme = origin.scheme == "https" ? "wss" : "ws"
  components.percentEncodedPath = "/api/cable"
  return components.string!
}
