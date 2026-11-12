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

/// The OS observers the core starts after `open` and stops in `Closing`: each OS value goes to the sink as it comes.
final class SystemObservers: PlatformObserver, @unchecked Sendable {
  private let path: any PathSource
  private var stops: [@Sendable () -> Void] = []

  init(path: any PathSource = SystemPath()) { self.path = path }

  func start(sink: OnlineSink) {
    let registration = sink.registerForeground()
    stops = [
      path.observe { sink.setOnline(online: $0) },
      ForegroundEvents.observe { sink.enterForeground(token: registration) },
      PowerSaving.observe { sink.setPowerSaving(saving: $0) }
    ]
  }

  func stop() {
    stops.forEach { $0() }
    stops = []
  }
}

/// The observer of `NetworkStatus`: the core starts it once per process and never stops it.
final class StatusObserver: PlatformObserver, @unchecked Sendable {
  func start(sink: OnlineSink) { _ = SystemPath().observe { sink.setOnline(online: $0) } }

  func stop() {}
}
