import Foundation
import Network

final class Reachability: @unchecked Sendable {
  static let shared = Reachability()
  private let monitor: NWPathMonitor?
  private let queue = DispatchQueue(label: "com.noma4i.dblayer.reachability")
  private let lock = NSLock()
  private let connectivity: Connectivity
  private let relay: OnlineRelay
  private var started = false

  init(monitor: NWPathMonitor? = NWPathMonitor()) {
    self.monitor = monitor
    let connectivity = Connectivity(online: false)
    self.connectivity = connectivity
    relay = OnlineRelay { connectivity.setOnline(online: $0) }
  }

  @discardableResult
  func start(_ sink: OnlineSink? = nil) -> Connectivity {
    if let sink { relay.attach { sink.setOnline(online: $0) } }
    let first = lock.withLock { () -> Bool in
      defer { started = true }
      return !started
    }
    if first {
      monitor?.pathUpdateHandler = { [weak self] path in self?.changed(path.status == .satisfied) }
      monitor?.start(queue: queue)
    }
    return connectivity
  }

  func detach() {
    relay.detach()
  }

  func changed(_ online: Bool) {
    relay.set(online)
  }
}
