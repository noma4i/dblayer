import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

extension BridgeProbeTests {
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func aListThatCannotAttachShowsTheFailureInItsSnapshot() async throws {
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 8), modelsJson: modelsJSON, transport: nil)
    DB.close()

    let list = LiveList(Moment.recent)
    await eventually("the attach failure reaches the snapshot") { list.snapshot.phase == .failed }

    #expect(list.snapshot.errorKind == "subscription")
    #expect(list.snapshot.error is DatabaseNotOpen)
  }
}
