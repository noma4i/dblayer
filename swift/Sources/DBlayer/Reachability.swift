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
  private var stops: [@Sendable () -> Void] = []

  func start(sink: OnlineSink) {
    let registration = sink.registerForeground()
    stops = [
      ForegroundEvents.observe { sink.enterForeground(token: registration) },
      PowerSaving.observe { sink.setPowerSaving(saving: $0) }
    ]
  }

  func stop() {
    stops.forEach { $0() }
    stops = []
  }
}

/// The observer of `NetworkStatus`: the core starts it once per process and never stops it. The database and `serverConfig` read its value. `makePath` is the one creation point of the network observer of the process.
final class StatusObserver: PlatformObserver, @unchecked Sendable {
  private let makePath: @Sendable () -> any PathSource

  init(makePath: @escaping @Sendable () -> any PathSource = { SystemPath() }) { self.makePath = makePath }

  func start(sink: OnlineSink) { _ = makePath().observe { sink.setOnline(online: $0) } }

  func stop() {}
}
