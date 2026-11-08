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

extension BridgeProbeTests {
  static let reloadingOnForeground = modelsJSON.replacingOccurrences(of: #""name":"feed","updates""#, with: #""name":"feed","refresh_on_foreground":true,"updates""#)

  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func aForegroundNotificationReachesTheCoreUntilTheRegistrationEnds() async throws {
    let name = Notification.Name("DBlayerTestForeground")
    let transport = MockTransport()
    for _ in 0..<8 { transport.enqueue("MomentsFeed", failure: .Http(status: 400, detail: "bad request")) }
    try await openSigned(config: Config(path: nil, dataVersion: nil), modelsJson: Self.reloadingOnForeground, transport: transport)
    try await DB.database.registerOperations(defsJson: #"[{"name":"MomentsFeed","document":"query MomentsFeed($first: Int, $after: String) { feed(first: $first, after: $after) { nodes { id uuid likedByMe media createdAt } pageInfo { hasNextPage endCursor } } }"}]"#)
    let list = ListReader(Moment.feed)
    await eventually("the relation fails") { list.snapshot.phase == .failed }
    let sink = try DB.database.onlineSink()
    let delivered = Delivered()
    let stop = ForegroundEvents.observe(name: name, active: { false }) {
      delivered.add()
      sink.enterForeground()
    }
    let before = transport.calls.count

    NotificationCenter.default.post(name: name, object: nil)
    #expect(delivered.count == 1)
    await eventually("the delivered notification reloads the failed window") { transport.calls.count > before }

    stop()
    NotificationCenter.default.post(name: name, object: nil)
    #expect(delivered.count == 1)
  }

  @Test(.timeLimit(.minutes(1)))
  func anActiveAppDeliversForegroundAtRegistration() async {
    let (events, continuation) = AsyncStream<Void>.makeStream()
    let stop = ForegroundEvents.observe(name: Notification.Name("DBlayerTestRegistration"), active: { true }) { continuation.yield() }
    #expect(await Cursor(events).next("foreground at registration") != nil)
    stop()
  }
}
