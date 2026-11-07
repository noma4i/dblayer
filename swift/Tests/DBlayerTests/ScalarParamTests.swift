import Foundation
import DBlayerTesting
import Testing
@testable import DBlayer

extension BridgeProbeTests {
  private static let tailDocument = "query tailSince($containerId: ID!, $afterSequence: Int, $pinned: Boolean, $first: Int, $after: String) { tailSince(containerId: $containerId, afterSequence: $afterSequence, pinned: $pinned, first: $first, after: $after) { edges { cursor node { id text } } pageInfo { hasNextPage endCursor } } }"

  struct ParamCase {
    let name: String
    let relation: String?
    let params: [String: JSONValue]
    let expected: [String: JSONValue]
    init<Model>(_ name: String, _ query: Query<Model>, _ expected: [String: JSONValue]) {
      self.name = name
      relation = query.relation
      params = query.params
      self.expected = expected
    }
  }

  @Test
  func aScopeParamIsSentAsItsJsonValue() throws {
    let paramCases: [ParamCase] = try [
    ParamCase("an int and a bool", Tail.since("c1", afterSequence: 120, pinned: true), ["containerId": .string("c1"), "afterSequence": .integer(120), "pinned": .bool(true)]),
    ParamCase("another int", Tail.since("c1", afterSequence: 121, pinned: true), ["containerId": .string("c1"), "afterSequence": .integer(121), "pinned": .bool(true)]),
    ParamCase("a schema enum", Inbox.shelf(list: .primary), ["list": .string("PRIMARY")]),
    ParamCase("another schema enum value", Inbox.shelf(list: .secondary), ["list": .string("SECONDARY")]),
    ParamCase("a schema enum value the build lacks", Inbox.shelf(list: .unknown("LATER")), ["list": .string("LATER")]),
    ParamCase("the default order", Roster.containerMembers("c1"), ["containerId": .string("c1"), "order": .string("JOINED")]),
    ParamCase("an order of the schema", Roster.containerMembers("c1", order: .ownerFirst), ["containerId": .string("c1"), "order": .string("OWNER_FIRST")]),
    ParamCase("an order the build lacks", Roster.containerMembers("c1", order: .unknown("LATER")), ["containerId": .string("c1"), "order": .string("LATER")]),
  ]

    for item in paramCases {
      #expect(item.relation != nil, Comment(rawValue: item.name))
      #expect(item.params == item.expected, Comment(rawValue: item.name))
    }
  }

  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func aTypedScopeParamReachesTheServerAsANumberAndAKeyOfItsOwnWindow() async throws {
    let transport = MockTransport()
    try await reopen(config: Config(path: nil, dataVersion: nil), modelsJson: modelsJSON, transport: transport)
    try await DB.database.registerOperations(defsJson: JSONValue.array([.object(["name": .string("tailSince"), "document": .string(Self.tailDocument)])]).text)
    transport.enqueue("tailSince", reply: #"{"data":{"tailSince":{"edges":[{"cursor":"c1","node":{"id":"t1","text":"a"}}],"pageInfo":{"hasNextPage":false,"endCursor":"c1"}}}}"#)

    try await Tail.since("c1", afterSequence: 120, pinned: false).refresh()

    let sent = transport.calls.first { $0.operationName == "tailSince" }?.variablesJson ?? ""
    #expect(try JSONValue.read(sent) == JSONValue.read(#"{"containerId":"c1","afterSequence":120,"pinned":false,"first":50}"#), "variables sent: \(sent)")
  }
}
