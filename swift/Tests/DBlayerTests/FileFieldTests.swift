import Foundation
import Testing
@testable import DBlayer

extension BridgeProbeTests {
  @Test(.timeLimit(.minutes(1)))
  func inv_files_typed_aFileIsAttachedReadLocatedAndDetachedThroughTheRecord() async throws {
    try await reopen(config: Config(path: nil, dataVersion: nil), modelsJson: modelsJSON, transport: nil)
    let user = try await User.create(name: "ann").optimisticValue!
    #expect(try await user.file(.avatar) == nil)

    let bytes = Data([1, 2, 3, 4])
    let info = try await user.attach(.avatar, data: bytes, name: "a.png", mime: "image/png")
    #expect((info.name, info.mime, info.size) == ("a.png", "image/png", 4))
    #expect(try await user.file(.avatar) == info)
    #expect(try await user.read(.avatar) == bytes)
    let url = try #require(try await user.url(.avatar))
    #expect(try Data(contentsOf: url) == bytes)
    let row = try #require(try await User.find(user.id).current())
    #expect(row.avatar == info)

    let other = try await user.attach(.avatar, data: Data([9]), name: "b.png", mime: "image/png")
    #expect(try await user.read(.avatar) == Data([9]) && other.id != info.id)

    #expect(try await user.detach(.avatar))
    #expect(try await user.file(.avatar) == nil)
    #expect(try await user.read(.avatar) == nil)
  }
}
