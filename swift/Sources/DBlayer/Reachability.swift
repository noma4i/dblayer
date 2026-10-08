import Network

public final class Reachability: @unchecked Sendable {
    private let monitor = NWPathMonitor()
    private let queue = DispatchQueue(label: "com.noma4i.dblayer.reachability")

    public init() {}

    public func start(_ sink: OnlineSink) {
        monitor.pathUpdateHandler = { path in sink.setOnline(online: path.status == .satisfied) }
        monitor.start(queue: queue)
    }

    public func stop() {
        monitor.cancel()
    }
}
