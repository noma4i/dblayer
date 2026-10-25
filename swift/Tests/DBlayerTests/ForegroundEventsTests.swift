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
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func aForegroundNotificationReachesTheCoreUntilTheRegistrationEnds() async throws {
    let name = Notification.Name("DBlayerTestForeground")
    let transport = MockTransport()
    for _ in 0..<8 { transport.enqueue("MomentsFeed", failure: .Http(status: 400, detail: "bad request")) }
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: transport)
    try DB.database.registerOperations(defsJson: #"[{"name":"MomentsFeed","document":"query MomentsFeed($first: Int, $after: String) { feed(first: $first, after: $after) { nodes { id uuid likedByMe media createdAt } pageInfo { hasNextPage endCursor } } }"}]"#)
    let list = LiveList(Moment.feed)
    await eventually("the relation fails") { list.snapshot.phase == .failed }
    let sink = try DB.database.onlineSink()
    let delivered = Delivered()
    ForegroundEvents.shared.start(name: name) {
      delivered.add()
      sink.enterForeground()
    }
    let before = transport.calls.count

    NotificationCenter.default.post(name: name, object: nil)
    #expect(delivered.count == 1)
    await eventually("the delivered notification reloads the failed window") { transport.calls.count > before }

    ForegroundEvents.shared.stop()
    NotificationCenter.default.post(name: name, object: nil)
    #expect(delivered.count == 1)
  }
}
