import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

extension BridgeProbeTests {
  private func withoutNullKeys(_ value: JSONValue) -> JSONValue {
    switch value {
    case let .object(fields): .object(fields.filter { $0.value != .null }.mapValues(withoutNullKeys))
    case let .array(items): .array(items.map(withoutNullKeys))
    default: value
    }
  }

  private func vectors() throws -> [[String: JSONValue]] {
    let root = URL(fileURLWithPath: #filePath).deletingLastPathComponent().deletingLastPathComponent().deletingLastPathComponent().deletingLastPathComponent()
    return try JSONDecoder().decode([JSONValue].self, from: Data(contentsOf: root.appendingPathComponent("fixtures/union.json"))).map { try $0.objectValue }
  }

  private func open(_ transport: MockTransport? = nil) async throws -> Database {
    try await openSigned(config: Config(path: nil, dataVersion: nil), modelsJson: modelsJSON, transport: transport)
    return try DB.database
  }

  @Test
  func aStoredUnionReadsFromCellsAndEncodesLikeTheSharedCases() async throws {
    let database = try await open()
    for fields in try vectors() {
      let label = fields["name"]!.stringValue!
      let values = fields["attachment"].map { JSONValue.object(["attachment": $0]) } ?? .object([:])
      let wire = try await database.create(epoch: database.sessionEpoch(), model: "UnionSample", valuesJson: values.text, argsJson: "{}").optimisticValue!
      let sample = try UnionSample.decode(wire)
      let encoded = try sample.attachment.map { try JSONValue.encode($0) } ?? .null
      #expect(withoutNullKeys(encoded) == withoutNullKeys(fields["output"]!), Comment(rawValue: label))
      if let input = fields["attachment"], input != .null {
        let decoded = try JSONDecoder().decode(RecordAttachment.self, from: JSONEncoder().encode(fields["output"]!))
        #expect(decoded == sample.attachment, Comment(rawValue: label))
      }
    }
  }

  @Test
  func typedVariantsAreWrittenReadSwitchedAndKeptWhenUnknown() async throws {
    _ = try await open()
    let reward = RecordAttachment.reward(RewardAttachment(id: "g2", tier: "silver", status: nil, tags: ["x"]))
    let created = try await UnionSample.create(attachment: reward, props: .emoji(MomentEmojiProps(emoji: "fire"))).optimisticValue!
    #expect(created.attachment == reward && created.props == .emoji(MomentEmojiProps(emoji: "fire")))

    let reaction = RecordAttachment.reaction(ReactionAttachment(body: "+1"))
    let switched = try await created.update(attachment: reaction).optimisticValue!
    #expect(switched.attachment == reaction && switched.props == created.props)

    let future = RecordAttachment.unknown(typename: "FutureAttachment")
    let kept = try await switched.update(attachment: future).optimisticValue!
    #expect(kept.attachment == future)
    let again = try await kept.update(label: "renamed").optimisticValue!
    #expect(again.attachment == future && again.label == "renamed")

    let none = try await UnionSample.create().optimisticValue!
    #expect(none.attachment == nil && none.props == nil)
  }

  @Test
  func rowsFilterByTheVariantKind() async throws {
    _ = try await open()
    _ = try await UnionSample.create(attachment: .moment(MomentAttachment(momentId: "m1")))
    _ = try await UnionSample.create(attachment: .moment(MomentAttachment(momentId: "m2")))
    _ = try await UnionSample.create(attachment: .reaction(ReactionAttachment(body: "x")))
    _ = try await UnionSample.create(attachment: .unknown(typename: "FutureAttachment"))
    _ = try await UnionSample.create()
    #expect(try await UnionSample.where(UnionSample.attachment == .moment).count.current() == 2)
    #expect(try await UnionSample.where(UnionSample.attachment == .reward).count.current() == 0)
    #expect(try await UnionSample.where(UnionSample.attachment.in([.moment, .reaction])).count.current() == 3)
    #expect(try await UnionSample.where(UnionSample.attachment.isNull).count.current() == 1)
  }

  private static let nodes: [String: String] = [
    "m1": #"{"__typename":"RewardAttachment","id":"g1","rewardTier":"gold","status":"sent","tags":["a"]}"#,
    "m2": #"{"__typename":"MomentAttachment","momentId":7}"#,
    "m3": #"{"__typename":"ProfileAttachment","user":{"id":"u1","name":"Ada"}}"#,
    "m4": #"{"__typename":"ReactionAttachment","body":"+1"}"#,
    "m5": #"{"__typename":"FutureAttachment","size":3}"#,
    "m6": "null",
  ]

  private static func threadReply(_ attachments: [String: String]) -> String {
    let rows = attachments.keys.sorted().map { id in
      #"{"__typename":"Record","id":"\#(id)","containerId":"c1","authorId":"a","text":"t","createdAt":"2026-10-0\#(id.dropFirst())T00:00:00.000Z","sequenceNumber":\#(id.dropFirst()),"attachment":\#(attachments[id]!)}"#
    }
    return #"{"data":{"container":{"records":{"nodes":[\#(rows.joined(separator: ","))],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}}"#
  }

  private static let threadDocument = "query ContainerThread($containerId: ID!, $first: Int, $after: String) { container(id: $containerId) { records(first: $first, after: $after) { nodes { id containerId authorId text createdAt sequenceNumber attachment { __typename ... on RewardAttachment { id rewardTier: kind status tags } ... on MomentAttachment { momentId caption } ... on ProfileAttachment { user { id name } } ... on ReactionAttachment { body } } } pageInfo { hasNextPage endCursor } } } }"

  @Test(.timeLimit(.minutes(1)))
  func aServerAnswerCarriesEveryVariantAndLaterAnswersReplaceTheVariant() async throws {
    let transport = MockTransport()
    transport.enqueue("ContainerThread", reply: Self.threadReply(Self.nodes))
    _ = try await open(transport)
    try await DB.database.registerOperations(defsJson: JSONValue.array([.object(["name": .string("ContainerThread"), "document": .string(Self.threadDocument)])]).text)
    let thread = try Record.thread("c1")
    try await thread.load()

    #expect(try await Record.find("m1").current()?.attachment == .reward(RewardAttachment(id: "g1", tier: "gold", status: "sent", tags: ["a"])))
    #expect(try await Record.find("m2").current()?.attachment == .moment(MomentAttachment(momentId: "7", caption: nil)))
    #expect(try await Record.find("m3").current()?.attachment == .profile(ProfileAttachment(user: AttachmentUser(id: "u1", name: "Ada"))))
    #expect(try await Record.find("m4").current()?.attachment == .reaction(ReactionAttachment(body: "+1")))
    #expect(try await Record.find("m5").current()?.attachment == .unknown(typename: "FutureAttachment"))
    #expect(try await Record.find("m6").current()?.attachment == nil)

    var changed = Self.nodes
    changed["m1"] = #"{"__typename":"ReactionAttachment","body":"now"}"#
    changed["m2"] = #"{"__typename":"MomentAttachment","momentId":8,"caption":"edited"}"#
    changed["m5"] = #"{"__typename":"RewardAttachment","id":"g9","rewardTier":"bronze","status":null,"tags":[]}"#
    changed["m6"] = #"{"__typename":"ReactionAttachment","body":"late"}"#
    transport.enqueue("ContainerThread", reply: Self.threadReply(changed))
    try await thread.refresh()

    #expect(try await Record.find("m1").current()?.attachment == .reaction(ReactionAttachment(body: "now")))
    #expect(try await Record.find("m2").current()?.attachment == .moment(MomentAttachment(momentId: "8", caption: "edited")))
    #expect(try await Record.find("m3").current()?.attachment == .profile(ProfileAttachment(user: AttachmentUser(id: "u1", name: "Ada"))))
    #expect(try await Record.find("m5").current()?.attachment == .reward(RewardAttachment(id: "g9", tier: "bronze", status: nil, tags: [])))
    #expect(try await Record.find("m6").current()?.attachment == .reaction(ReactionAttachment(body: "late")))
  }

  @Test(.timeLimit(.minutes(1)))
  func aCreatedRecordTakesItsVariantFromTheServerAnswer() async throws {
    let transport = MockTransport()
    transport.enqueue("CreateRecord", reply: #"{"data":{"createRecord":{"__typename":"Record","id":"x1","containerId":"c","authorId":"u1","text":"hi","createdAt":"2026-10-08T00:00:00.000Z","attachment":{"__typename":"MomentAttachment","momentId":"m9","caption":"look"}}}}"#)
    _ = try await open(transport)
    try await DB.database.registerOperations(defsJson: #"[{"name":"CreateRecord","document":"mutation CreateRecord($containerId: ID!, $text: String!, $signedId: String, $attachedUserId: ID) { createRecord(containerId: $containerId, text: $text, signedId: $signedId, attachedUserId: $attachedUserId) { id containerId authorId text createdAt attachment { __typename ... on MomentAttachment { momentId caption } } } }"}]"#)

    let sent = try await Record.create(containerId: "c", authorId: "u1", text: "hi", in: DB.account())
    #expect(sent.optimisticValue?.attachment == nil)
    let confirmed = try await sent.confirmed()
    #expect(try await Record.find(confirmed.id).current()?.attachment == .moment(MomentAttachment(momentId: "m9", caption: "look")))
  }
}
