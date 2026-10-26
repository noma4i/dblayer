import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

private final class Landed: @unchecked Sendable {
  private let lock = NSLock()
  private var flag = false
  var value: Bool {
    get { lock.lock(); defer { lock.unlock() }; return flag }
    set { lock.lock(); defer { lock.unlock() }; flag = newValue }
  }
}

extension BridgeProbeTests {
  @Test(.timeLimit(.minutes(1)))
  func aLiveReadWhileTheCreateReplyLandsNeverSeesAnEmptySet() async throws {
    let transport = MockTransport()
    transport.enqueue("CreateRecord", reply: #"{"data":{"createRecord":{"__typename":"Record","id":"x1","containerId":"c","authorId":"u1","text":"hi","createdAt":"2026-10-08T00:00:00.000Z"}}}"#)
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: transport)
    try DB.database.registerOperations(defsJson: #"[{"name":"CreateRecord","document":"mutation CreateRecord($containerId: ID!, $text: String!, $signedId: String, $attachedUserId: ID) { createRecord(containerId: $containerId, text: $text, signedId: $signedId, attachedUserId: $attachedUserId) { id containerId authorId text createdAt } }"}]"#)
    let json = #"{"model":"Record"}"#
    let held = try DB.database.liveQuery(queryJson: json, listener: ChangeListener { _ in })

    let local = try await Record.create(containerId: "c", authorId: "u1", text: "hi", attachedUserId: nil, signedId: nil)
    let landed = Landed()
    let landing = Task {
      try await local.settled(action: nil)
      landed.value = true
    }
    var counts: [Int] = []
    repeat {
      let live = try DB.database.liveQuery(queryJson: json, listener: ChangeListener { _ in })
      let seen = await live.current().rows.count
      live.cancel()
      if seen == 1 || !counts.isEmpty { counts.append(seen) }
    } while !landed.value
    try await landing.value
    held.cancel()

    #expect(!counts.isEmpty && counts.allSatisfy { $0 == 1 }, "rows seen by fresh live reads from the temporary row until the reply landed: \(counts)")
  }
}
