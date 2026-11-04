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
      var uploads: [String: UploadInfo] { [:] }
      init(wire: WireRow) throws { throw Unreadable() }
    }

    @Test
    func cachedReadsThrowWhenARowDoesNotDecode() async throws {
      try await reopenSigned(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON)
      let moment = try await Moment.send(in: DB.account(), text: "m").optimisticValue!
      let query = Query<UnreadableMoment>()
      try await query.warm()
      await #expect(throws: UnreadableMoment.Unreadable.self) { try await query.cached() }
      await #expect(throws: UnreadableMoment.Unreadable.self) { try await query.cachedRow(moment.id) }
    }
  }
}
