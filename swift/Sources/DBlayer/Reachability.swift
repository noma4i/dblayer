import Foundation
import Network

final class Reachability: @unchecked Sendable {
  static let shared = Reachability()
  private let monitor = NWPathMonitor()
  private let queue = DispatchQueue(label: "com.noma4i.dblayer.reachability")
  private let lock = NSLock()
  private let connectivity = Connectivity(online: false)
  private var started = false
  private var online = false
  private var sink: OnlineSink?

  private init() {}

  @discardableResult
  func start(_ sink: OnlineSink? = nil) -> Connectivity {
    lock.withLock {
      if let sink { self.sink = sink; sink.setOnline(online: online) }
      if !started {
        started = true
        monitor.pathUpdateHandler = { [weak self] path in self?.changed(path.status == .satisfied) }
        monitor.start(queue: queue)
      }
      return connectivity
    }
  }

  private func changed(_ online: Bool) {
    lock.withLock {
      self.online = online
      connectivity.setOnline(online: online)
      sink?.setOnline(online: online)
    }
  }
}
