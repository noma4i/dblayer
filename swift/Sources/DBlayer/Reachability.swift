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
}

/// The OS observers the core starts after `open` and stops in `Closing`: each OS value goes to the sink as it comes.
final class SystemObservers: PlatformObserver, @unchecked Sendable {
  private let path: any PathSource
  private var stops: [@Sendable () -> Void] = []

  init(path: any PathSource = Reachability.source) { self.path = path }

  func start(sink: OnlineSink) {
    stops = [
      path.observe { sink.setOnline(online: $0) },
      ForegroundEvents.observe { sink.enterForeground() },
      PowerSaving.observe { sink.setPowerSaving(saving: $0) }
    ]
  }

  func stop() {
    stops.forEach { $0() }
    stops = []
  }
}
