import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

/// The tests share `DB.database` with the rest of `BridgeProbeTests`, so they run in its serialized suite.
extension BridgeProbeTests {
  private static let nodeDocuments: [(String, String)] = [
    ("ContainerNode", "query ContainerNode($id: ID!) { node(id: $id) { ... on Container { id title shareUrl isPublic } } }"),
    ("ContainersByIds", "query ContainersByIds($ids: [ID!]!) { nodes(ids: $ids) { ... on Container { id title } } }"),
    ("ContainerMedia", "query ContainerMedia($containerId: ID!, $first: Int, $after: String) { node(id: $containerId) { ... on Container { records(first: $first, after: $after) { edges { node { id containerId authorId text createdAt media { kind transcoded fileUrl } } } pageInfo { hasNextPage endCursor } } } } }"),
    ("RecordTranscodeStatus", "query RecordTranscodeStatus($id: ID!) { node(id: $id) { ... on Record { id containerId media { kind transcoded fileUrl } } } }"),
    ("MomentTranscodeStatus", "query MomentTranscodeStatus($momentId: ID!) { node(id: $momentId) { ... on Moment { id uuid createdAt media } } }"),
  ]

  private func openNodeRig(models: String = modelsJSON) async throws -> MockTransport {
    let transport = MockTransport()
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 64), modelsJson: models, transport: transport)
    let defs = Self.nodeDocuments.map { JSONValue.object(["name": .string($0.0), "document": .string($0.1)]) }
    try DB.database.registerOperations(defsJson: JSONValue.array(defs).text)
    return transport
  }

  private func nodeCalls(_ transport: MockTransport, _ operation: String) -> [TransportRequest] {
    transport.calls.filter { $0.operationName == operation }
  }

  @Test(.timeLimit(.minutes(1)))
  func aNodeReadReturnsTheTypedRowAndOnlyAnEventForcesAnotherRequest() async throws {
    let transport = try await openNodeRig()
    transport.enqueue("ContainerNode", reply: #"{"data":{"node":{"id":"c1","title":"one","shareUrl":"u","isPublic":true}}}"#)
    transport.enqueue("ContainerNode", reply: #"{"data":{"node":{"id":"c1","title":"two"}}}"#)

    let first = try await Container.node("c1")
    #expect(first?.title == "one" && first?.shareUrl == "u" && first?.isPublic == true)
    #expect(nodeCalls(transport, "ContainerNode").first?.variablesJson == #"{"id":"c1"}"#)
    #expect(try await Container.node("c1")?.title == "one")
    #expect(nodeCalls(transport, "ContainerNode").count == 1)
    let rereads = try await Container.node("c1", force: true)
    #expect(rereads?.title == "two" && rereads?.shareUrl == "u")
    #expect(nodeCalls(transport, "ContainerNode").count == 2)
  }

  @Test(.timeLimit(.minutes(1)))
  func aRelationUnderNodeLandsAndASubsetReadChangesOnlyTheSelectedFields() async throws {
    let transport = try await openNodeRig()
    let record = #"{"id":"m1","containerId":"c1","authorId":"a","text":"keep","createdAt":"2026-10-01T00:00:00.000Z","media":{"kind":"VIDEO","transcoded":false,"fileUrl":"http://f"}}"#
    transport.enqueue("ContainerMedia", reply: #"{"data":{"node":{"records":{"edges":[{"node":\#(record)}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}}"#)
    try await Record.containerMedia("c1").load()
    #expect(try await Record.find("m1").current()?.text == "keep")

    transport.enqueue("RecordTranscodeStatus", reply: #"{"data":{"node":{"id":"m1","containerId":"c1","media":{"kind":"VIDEO","transcoded":true,"fileUrl":""}}}}"#)
    let status = try await Record.transcodeStatus("m1")
    #expect(status?.text == "keep" && status?.media?.transcoded == true && status?.media?.fileUrl == "http://f")
    #expect(nodeCalls(transport, "RecordTranscodeStatus").first?.variablesJson == #"{"id":"m1"}"#)
  }

  @Test(.timeLimit(.minutes(1)))
  func theVariableNameOfANodeReadFollowsTheDeclaration() async throws {
    let transport = try await openNodeRig()
    transport.enqueue("MomentTranscodeStatus", reply: #"{"data":{"node":{"id":"mo1","uuid":"u","createdAt":"2026-10-01T00:00:00.000Z","media":{"kind":"VIDEO"}}}}"#)
    #expect(try await Moment.transcodeStatus("mo1")?.id == "mo1")
    #expect(nodeCalls(transport, "MomentTranscodeStatus").first?.variablesJson == #"{"momentId":"mo1"}"#)
  }

  @Test(.timeLimit(.minutes(1)))
  func nodesReadsManyInOneRequestInTheAnsweredOrder() async throws {
    let transport = try await openNodeRig()
    transport.enqueue("ContainersByIds", reply: #"{"data":{"nodes":[{"id":"c2","title":"b"},null,{},{"id":"c1","title":"a"}]}}"#)
    let rows = try await Container.nodes(["c2", "x", "y", "c1"])
    #expect(rows.map(\.title) == ["b", "a"])
    #expect(nodeCalls(transport, "ContainersByIds").count == 1)
    #expect(nodeCalls(transport, "ContainersByIds").first?.variablesJson == #"{"ids":["c2","x","y","c1"]}"#)
  }

  @Test(.timeLimit(.minutes(1)))
  func anAnswerWithoutAMatchingTypeIsNilAndWritesNothing() async throws {
    let transport = try await openNodeRig()
    transport.enqueue("ContainerNode", reply: #"{"data":{"node":{"id":"c1","title":"old"}}}"#)
    _ = try await Container.node("c1")
    for answer in [#"{"data":{"node":null}}"#, #"{"data":{"node":{}}}"#, #"{"data":{"node":{"__typename":"Gadget"}}}"#] {
      transport.enqueue("ContainerNode", reply: answer)
      #expect(try await Container.node("c1", force: true) == nil)
    }
    #expect(try await Container.find("c1").current()?.title == "old")
  }

  @Test(.timeLimit(.minutes(1)))
  func nodesKeepsDuplicatesInResponseOrder() async throws {
    let transport = try await openNodeRig()
    transport.enqueue("ContainersByIds", reply: #"{"data":{"nodes":[{"id":"c2","title":"old","shareUrl":"kept"},null,{"id":"c1","title":"middle"},{"id":"c2","title":"last"}]}}"#)
    let rows = try await Container.nodes(["c2", "missing", "c1", "c2"])
    #expect(rows.map(\.id) == ["c2", "c1", "c2"])
    #expect(rows.map(\.title) == ["last", "middle", "last"])
    #expect(try await Container.find("c2").current()?.shareUrl == "kept")
  }

  @Test(.timeLimit(.minutes(1)))
  func nullPathsClearAndMissingPathsPreserve() async throws {
    var models = try #require(JSONSerialization.jsonObject(with: Data(modelsJSON.utf8)) as? [[String: Any]])
    let index = try #require(models.firstIndex { $0["key"] as? String == "Container" })
    var fields = try #require(models[index]["fields"] as? [[String: Any]])
    let field = try #require(fields.firstIndex { $0["name"] as? String == "shareUrl" })
    fields[field]["from"] = "profile.url"
    models[index]["fields"] = fields
    let transport = try await openNodeRig(models: String(decoding: JSONSerialization.data(withJSONObject: models), as: UTF8.self))
    for (patch, expected) in [(#""profile":{}"#, "kept" as String?), (#""profile":null"#, nil), (#""profile":{"url":null}"#, nil)] {
      transport.enqueue("ContainerNode", reply: #"{"data":{"node":{"id":"c1","title":"container","profile":{"url":"kept"}}}}"#)
      #expect(try await Container.node("c1", force: true)?.shareUrl == "kept")
      transport.enqueue("ContainerNode", reply: #"{"data":{"node":{"id":"c1",\#(patch)}}}"#)
      #expect(try await Container.node("c1", force: true)?.shareUrl == expected)
    }
  }
}
