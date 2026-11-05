import Foundation
import Network

protocol PathSource: Sendable {
  /// Reports the path state at once and on every change until the returned stop runs.
  func observe(_ changed: @escaping @Sendable (Bool) -> Void) -> @Sendable () -> Void
}

struct SystemPath: PathSource {
  func observe(_ changed: @escaping @Sendable (Bool) -> Void) -> @Sendable () -> Void {
    let monitor = NWPathMonitor()
    monitor.pathUpdateHandler = { changed($0.status == .satisfied) }
    monitor.start(queue: DispatchQueue(label: "com.noma4i.dblayer.reachability"))
    return { monitor.cancel() }
  }
}

enum Reachability {
  @TaskLocal static var source: any PathSource = SystemPath()

  static func observe(_ changed: @escaping @Sendable (Bool) -> Void) -> @Sendable () -> Void { source.observe(changed) }
}
