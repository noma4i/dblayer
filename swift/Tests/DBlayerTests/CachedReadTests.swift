import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

extension BridgeProbeTests {
  @Suite struct CachedReadTests {
    struct UnreadableMoment: DBModel {
      struct Unreadable: Error {}
      static let modelName = Moment.modelName
      static let wireLayout = Moment.wireLayout
      var id: String { "" }
      var sync: SyncInfo { SyncInfo() }
      var upload: UploadInfo? { nil }
      init(wire: WireRow) throws { throw Unreadable() }
    }

    @Test
    func cachedReadsThrowWhenARowDoesNotDecode() async throws {
      try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: nil)
      let moment = try await Moment.create(text: "m")
      let query = Query<UnreadableMoment>()
      try await query.warm()
      #expect(throws: UnreadableMoment.Unreadable.self) { try query.cached() }
      #expect(throws: UnreadableMoment.Unreadable.self) { try query.cachedRow(moment.id) }
    }
  }
}
