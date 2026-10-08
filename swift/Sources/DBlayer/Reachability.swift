import Network

public protocol OnlineSink: AnyObject {
    func setOnline(_ online: Bool)
}

public final class Reachability {
    private let monitor = NWPathMonitor()
    private let queue = DispatchQueue(label: "com.noma4i.dblayer.reachability")

    public init() {}

    public func start(sink: OnlineSink) {
        monitor.pathUpdateHandler = { [weak sink] path in
            sink?.setOnline(path.status == .satisfied)
        }
        monitor.start(queue: queue)
    }

    public func stop() {
        monitor.cancel()
    }
}
