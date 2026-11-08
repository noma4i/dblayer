import Foundation
import DBlayerTesting
import Testing
@testable import DBlayer

private final class Delivered: @unchecked Sendable {
  private let lock = NSLock()
  private var value = 0
  var count: Int { lock.withLock { value } }
  func add() { lock.withLock { value += 1 } }
}

private final class FakeOs: PlatformObserver, @unchecked Sendable {
  let name: Notification.Name
  private let lock = NSLock()
  private var held: (sink: OnlineSink, registration: UInt64)?
  private var stopObserving: @Sendable () -> Void = {}
  init(name: Notification.Name) { self.name = name }

  var registration: (sink: OnlineSink, registration: UInt64)? { lock.withLock { held } }

  func start(sink: OnlineSink) {
    sink.setOnline(online: true)
    let registration = sink.registerForeground()
    let stop = ForegroundEvents.observe(name: name, active: { false }) { sink.enterForeground(token: registration) }
    lock.withLock { held = (sink, registration); stopObserving = stop }
  }

  func stop() {
    let stop = lock.withLock { stopObserving }
    stop()
  }
}

extension BridgeProbeTests {
  static let reloadingOnForeground = modelsJSON.replacingOccurrences(of: #""name":"feed","updates""#, with: #""name":"feed","refresh_on_foreground":true,"updates""#)
  static let feedOperations = #"[{"name":"MomentsFeed","document":"query MomentsFeed($first: Int, $after: String) { feed(first: $first, after: $after) { nodes { id uuid likedByMe media createdAt } pageInfo { hasNextPage endCursor } } }"}]"#

  @MainActor
  private func openFailedFeed(_ transport: MockTransport, _ os: FakeOs) async throws -> ListReader<Moment> {
    for _ in 0..<16 { transport.enqueue("MomentsFeed", failure: .Http(status: 400, detail: "bad request")) }
    try await openSignedClocked(modelsJson: Self.reloadingOnForeground, transport: transport, operations: Self.feedOperations, observers: os)
    let list = ListReader(Moment.feed)
    await eventually("the relation fails") { list.snapshot.phase == .failed }
    return list
  }

  /// boundary_observer_stop_orders (P-BND-09, D-177), delivery before stop: a notification reaches the core with the token the core issued.
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func aDeliveryOfTheLiveRegistrationReloadsTheFailedWindow() async throws {
    let transport = MockTransport()
    let os = FakeOs(name: Notification.Name("DBlayerTestForeground"))
    _ = try await openFailedFeed(transport, os)
    let before = transport.calls.count

    NotificationCenter.default.post(name: os.name, object: nil)
    await eventually("the delivery reloads the failed window") { transport.calls.count > before }
  }

  /// boundary_observer_stop_orders, stop before delivery: after `close` acknowledged the stop, a notification and a delivery with the old token reach nothing.
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func noDeliveryReachesTheCoreAfterTheStopOfTheObserversIsAcknowledged() async throws {
    let transport = MockTransport()
    let os = FakeOs(name: Notification.Name("DBlayerTestForegroundStop"))
    _ = try await openFailedFeed(transport, os)
    let held = try #require(os.registration)
    let before = transport.calls.count

    try await DB.close()
    NotificationCenter.default.post(name: os.name, object: nil)
    held.sink.enterForeground(token: held.registration)
    #expect(transport.calls.count == before)
  }

  @Test(.timeLimit(.minutes(1)))
  func anActiveAppDeliversForegroundAtRegistration() async {
    let (events, continuation) = AsyncStream<Void>.makeStream()
    let stop = ForegroundEvents.observe(name: Notification.Name("DBlayerTestRegistration"), active: { true }) { continuation.yield() }
    #expect(await Cursor(events).next("foreground at registration") != nil)
    stop()
  }

  /// boundary_observer_stop_orders (P-BND-09): a first call queued on the main queue and stopped before it ran never delivers.
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func aStopBeforeTheQueuedFirstCallDropsIt() async {
    let delivered = Delivered()
    let stop = ForegroundEvents.observe(name: Notification.Name("DBlayerTestStop"), active: { true }) { delivered.add() }
    stop()
    await withCheckedContinuation { continuation in DispatchQueue.main.async { continuation.resume() } }
    #expect(delivered.count == 0)
  }
}
