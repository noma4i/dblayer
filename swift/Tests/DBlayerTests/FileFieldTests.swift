import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

extension BridgeProbeTests {
  @Test(.timeLimit(.minutes(1)))
  func inv_files_typed_aFileIsAttachedReadLocatedAndDetachedThroughTheRecord() async throws {
    try await openSigned(config: Config(path: nil, dataVersion: nil), modelsJson: modelsJSON, transport: nil)
    let user = try await User.create(name: "ann").optimisticValue!
    #expect(try await user.file(.avatar) == nil)

    let bytes = Data([1, 2, 3, 4])
    let source = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
    defer { try? FileManager.default.removeItem(at: source) }
    try bytes.write(to: source)
    let info = try await user.attach(.avatar, source: source, name: "a.png", mime: "image/png")
    #expect((info.name, info.mime, info.size) == ("a.png", "image/png", 4))
    #expect(try await user.file(.avatar) == info)
    let url = try #require(try await user.url(.avatar))
    #expect(try Data(contentsOf: url) == bytes)
    let row = try #require(try await User.find(user.id).current())
    #expect(row.avatar == info)
    #expect(row.rowRevision > 0 && row.estimatedBytes > 0, "bridge_prop_projection_revision: the row keeps the metadata of its wire row")

    try Data([9]).write(to: source)
    let other = try await user.attach(.avatar, source: source, name: "b.png", mime: "image/png")
    let otherURL = try #require(try await user.url(.avatar))
    #expect(try Data(contentsOf: otherURL) == Data([9]) && other.id != info.id)

    #expect(try await user.detach(.avatar))
    #expect(try await user.file(.avatar) == nil)
    #expect(try await user.url(.avatar) == nil)
  }
}
