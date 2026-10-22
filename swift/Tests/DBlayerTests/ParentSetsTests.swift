import Foundation
import Testing
@testable import DBlayer

extension BridgeProbeTests {
struct ParentSetsTests {
  @Test
  func aLocalRecordMovesTheContainerPointerAndDiscardingItGivesThePointerBack() async throws {
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: nil)
    let container = try await Container.create(title: "t")
    let record = try await Record.create(containerId: container.id, authorId: "u1", text: "hi")

    let pointed = await Container.find(container.id).current()
    #expect(pointed?.lastRecordId == record.id)
    #expect(pointed?.lastRecordAt != nil)

    try await record.discard()
    let restored = await Container.find(container.id).current()
    #expect(restored?.lastRecordId == nil)
    #expect(restored?.lastRecordAt == nil)
  }
}
}
