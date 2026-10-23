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
}
