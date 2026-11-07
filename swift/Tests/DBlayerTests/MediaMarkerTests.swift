import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

extension BridgeProbeTests {
  @Test @MainActor
  func mediaMarkerPlatforms() async throws {
    let root = URL(fileURLWithPath: #filePath).deletingLastPathComponent().deletingLastPathComponent().deletingLastPathComponent().deletingLastPathComponent()
    let spec = try String(contentsOf: root.appendingPathComponent("fixtures/media-marker.json"), encoding: .utf8)
    let document = try String(contentsOf: root.appendingPathComponent("fixtures/operations/queries/media.graphql"), encoding: .utf8)
    let transport = MockTransport()
    try await reopen(config: Config(path: nil, dataVersion: nil), modelsJson: spec, transport: transport)
    try await DB.database.registerOperations(defsJson: JSONValue.array(["chatMedia", "chatMediaState"].map { .object(["name": .string($0), "document": .string(document)]) }).text)
    func marker(_ count: Int, _ sequence: Int) {
      transport.enqueue("chatMediaState", reply: #"{"data":{"node":{"mediaLibrary":{"revision":"1","visualCount":\#(count),"audioCount":0,"giftCount":0,"latestSequence":\#(sequence)}}}}"#)
    }
    func page(_ ids: [Int], _ hasNext: Bool) {
      let nodes = ids.map { #"{"id":"\#($0)","text":"media","sequenceNumber":\#($0)}"# }.joined(separator: ",")
      transport.enqueue("chatMedia", reply: #"{"data":{"node":{"messages":{"nodes":[\#(nodes)],"pageInfo":{"hasNextPage":\#(hasNext),"endCursor":"\#(ids.last ?? 0)"}}}}}"#)
    }
    marker(0, 2)
    page([1], true)
    page([2], false)
    marker(0, 2)
    marker(2, 2)
    let query = try MediaEntry.media("c1")
    let media = ListReader(query)
    try await media.waitForInitialLoad()
    await eventually("media initial snapshot") { media.snapshot.complete }
    #expect(media.rows.map(\.id) == ["1", "2"])
    marker(2, 5)
    page([3, 4], true)
    page([5], false)
    marker(2, 5)
    marker(5, 5)
    try await media.refresh()
    await eventually("media tail snapshot") { media.snapshot.complete && media.rows.count == 5 }
    #expect(media.rows.map(\.id) == ["1", "2", "3", "4", "5"])
    let calls = transport.calls.filter { $0.operationName == "chatMedia" }
    let variables = try calls.map { try JSONDecoder().decode(JSONValue.self, from: Data($0.variablesJson.utf8)) }
    #expect(variables == [
      .object(["chatId": .string("c1"), "first": .integer(2)]),
      .object(["chatId": .string("c1"), "first": .integer(2), "after": .string("1")]),
      .object(["chatId": .string("c1"), "first": .integer(2), "afterSequence": .integer(2)]),
      .object(["chatId": .string("c1"), "first": .integer(2), "afterSequence": .integer(2), "after": .string("4")]),
    ])
    #expect(media.snapshot.error == nil)
  }
}
