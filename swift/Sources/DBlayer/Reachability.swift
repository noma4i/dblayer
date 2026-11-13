import Foundation
import Network

struct SystemPath {
  /// Reports the path at once and on every change until the returned stop runs. The network number grows when the main interface changes.
  func observe(_ changed: @escaping @Sendable (NetworkPath) -> Void) -> @Sendable () -> Void {
    let monitor = NWPathMonitor()
    let network = NetworkCounter()
    monitor.pathUpdateHandler = { path in
      let online = path.status == .satisfied
      let kind: NetworkKind = !online ? .none : path.usesInterfaceType(.wifi) ? .wifi : path.usesInterfaceType(.cellular) ? .cellular : .other
      changed(NetworkPath(online: online, kind: kind, network: network.number(of: path.availableInterfaces.first?.name)))
    }
    monitor.start(queue: DispatchQueue(label: "com.noma4i.dblayer.reachability"))
    return { monitor.cancel() }
  }
}

/// The number of the default network: it grows each time the name of the main interface changes. Only the queue of the monitor calls it.
private final class NetworkCounter: @unchecked Sendable {
  private var interface: String?
  private var count: UInt64 = 0

  func number(of name: String?) -> UInt64 {
    if name != nil, name != interface { count += 1; interface = name }
    return count
  }
}

/// The OS observers the core starts after `open` and stops in `Closing`: each OS value goes to the sink as it comes.
final class SystemObservers: PlatformObserver, @unchecked Sendable {
  private var stops: [@Sendable () -> Void] = []

  func start(sink: OnlineSink) {
    let registration = sink.registerForeground()
    stops = [
      ForegroundEvents.observe({ sink.enterForeground(token: registration) }, background: { sink.enterBackground(token: registration) }),
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
  func start(sink: OnlineSink) { _ = SystemPath().observe { sink.setPath(path: $0) } }

  func stop() {}
}
