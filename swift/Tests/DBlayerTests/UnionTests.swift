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
    return try JSONDecoder().decode([JSONValue].self, from: Data(contentsOf: root.appendingPathComponent("fixtures/union.json"))).map(\.objectValue)
  }

  private func open(_ transport: MockTransport? = nil) async throws -> Database {
    DB.database = try await Database.open(config: Config(path: nil, dataVersion: nil, cacheRows: 64), modelsJson: modelsJSON, transport: transport)
    return DB.database
  }

  @Test
  func aStoredUnionReadsFromCellsAndEncodesLikeTheSharedCases() async throws {
    let database = try await open()
    for fields in try vectors() {
      let label = fields["name"]!.stringValue!
      let values = fields["attachment"].map { JSONValue.object(["attachment": $0]) } ?? .object([:])
      let wire = try await database.create(model: "UnionSample", valuesJson: values.text, argsJson: "{}")
      let sample = try UnionSample.decode(wire)
      let encoded = sample.attachment.map { JSONValue.encode($0) } ?? .null
      #expect(withoutNullKeys(encoded) == withoutNullKeys(fields["output"]!), Comment(rawValue: label))
      if let input = fields["attachment"], input != .null {
        let decoded = try JSONDecoder().decode(MessageAttachment.self, from: JSONEncoder().encode(input))
        #expect(decoded == sample.attachment, Comment(rawValue: label))
      }
    }
  }

  @Test
  func typedVariantsAreWrittenReadSwitchedAndKeptWhenUnknown() async throws {
    _ = try await open()
    let gift = MessageAttachment.gift(GiftAttachment(id: "g2", tier: "silver", status: nil, tags: ["x"]))
    let created = try await UnionSample.create(attachment: gift, props: .emoji(MomentEmojiProps(emoji: "fire")))
    #expect(created.attachment == gift && created.props == .emoji(MomentEmojiProps(emoji: "fire")))

    let reaction = MessageAttachment.reaction(ReactionAttachment(body: "+1"))
    let switched = try await created.update(attachment: reaction)
    #expect(switched.attachment == reaction && switched.props == created.props)

    let future = MessageAttachment.unknown(typename: "FutureAttachment")
    let kept = try await switched.update(attachment: future)
    #expect(kept.attachment == future)
    let again = try await kept.update(label: "renamed")
    #expect(again.attachment == future && again.label == "renamed")

    let none = try await UnionSample.create()
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
    #expect(await UnionSample.where(UnionSample.attachment == .moment).count.current() == 2)
    #expect(await UnionSample.where(UnionSample.attachment == .gift).count.current() == 0)
    #expect(await UnionSample.where(UnionSample.attachment.in([.moment, .reaction])).count.current() == 3)
    #expect(await UnionSample.where(UnionSample.attachment.isNull).count.current() == 1)
  }

  private static let nodes: [String: String] = [
    "m1": #"{"__typename":"GiftAttachment","id":"g1","giftTier":"gold","status":"sent","tags":["a"]}"#,
    "m2": #"{"__typename":"MomentAttachment","momentId":7}"#,
    "m3": #"{"__typename":"ProfileAttachment","user":{"id":"u1","name":"Ada"}}"#,
    "m4": #"{"__typename":"ReactionAttachment","body":"+1"}"#,
    "m5": #"{"__typename":"FutureAttachment","size":3}"#,
    "m6": "null",
  ]

  private static func threadReply(_ attachments: [String: String]) -> String {
    let rows = attachments.keys.sorted().map { id in
      #"{"__typename":"Message","id":"\#(id)","chatId":"c1","authorId":"a","text":"t","createdAt":"2026-10-0\#(id.dropFirst())T00:00:00.000Z","sequenceNumber":\#(id.dropFirst()),"attachment":\#(attachments[id]!)}"#
    }
    return #"{"data":{"chat":{"messages":{"nodes":[\#(rows.joined(separator: ","))],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}}"#
  }

  private static let threadDocument = "query ChatThread($chatId: ID!, $first: Int, $after: String) { chat(id: $chatId) { messages(first: $first, after: $after) { nodes { id chatId authorId text createdAt sequenceNumber attachment { __typename ... on GiftAttachment { id giftTier: kind status tags } ... on MomentAttachment { momentId caption } ... on ProfileAttachment { user { id name } } ... on ReactionAttachment { body } } } pageInfo { hasNextPage endCursor } } } }"

  @Test(.timeLimit(.minutes(1)))
  func aServerAnswerCarriesEveryVariantAndLaterAnswersReplaceTheVariant() async throws {
    let transport = MockTransport()
    transport.enqueue("ChatThread", reply: Self.threadReply(Self.nodes))
    _ = try await open(transport)
    try DB.database.registerOperations(defsJson: JSONValue.array([.object(["name": .string("ChatThread"), "document": .string(Self.threadDocument)])]).text)
    let thread = Message.thread("c1")
    try await thread.load()

    #expect(await Message.find("m1").current()?.attachment == .gift(GiftAttachment(id: "g1", tier: "gold", status: "sent", tags: ["a"])))
    #expect(await Message.find("m2").current()?.attachment == .moment(MomentAttachment(momentId: "7", caption: nil)))
    #expect(await Message.find("m3").current()?.attachment == .profile(ProfileAttachment(user: AttachmentUser(id: "u1", name: "Ada"))))
    #expect(await Message.find("m4").current()?.attachment == .reaction(ReactionAttachment(body: "+1")))
    #expect(await Message.find("m5").current()?.attachment == .unknown(typename: "FutureAttachment"))
    #expect(await Message.find("m6").current()?.attachment == nil)
    #expect(await Message.where(Message.attachment == .moment).count.current() == 1)
    #expect(await Message.where(Message.attachment.isNull).count.current() == 1)

    var changed = Self.nodes
    changed["m1"] = #"{"__typename":"ReactionAttachment","body":"now"}"#
    changed["m2"] = #"{"__typename":"MomentAttachment","momentId":8,"caption":"edited"}"#
    changed["m5"] = #"{"__typename":"GiftAttachment","id":"g9","giftTier":"bronze","status":null,"tags":[]}"#
    changed["m6"] = #"{"__typename":"ReactionAttachment","body":"late"}"#
    transport.enqueue("ChatThread", reply: Self.threadReply(changed))
    try await thread.refresh()

    #expect(await Message.find("m1").current()?.attachment == .reaction(ReactionAttachment(body: "now")))
    #expect(await Message.find("m2").current()?.attachment == .moment(MomentAttachment(momentId: "8", caption: "edited")))
    #expect(await Message.find("m3").current()?.attachment == .profile(ProfileAttachment(user: AttachmentUser(id: "u1", name: "Ada"))))
    #expect(await Message.find("m5").current()?.attachment == .gift(GiftAttachment(id: "g9", tier: "bronze", status: nil, tags: [])))
    #expect(await Message.find("m6").current()?.attachment == .reaction(ReactionAttachment(body: "late")))
    #expect(await Message.where(Message.attachment == .gift).count.current() == 1)
    #expect(await Message.where(Message.attachment == .reaction).count.current() == 3)
  }

  @Test(.timeLimit(.minutes(1)))
  func aCreatedMessageTakesItsVariantFromTheServerAnswer() async throws {
    let transport = MockTransport()
    transport.enqueue("CreateMessage", reply: #"{"data":{"createMessage":{"__typename":"Message","id":"x1","chatId":"c","authorId":"u1","text":"hi","createdAt":"2026-10-08T00:00:00.000Z","attachment":{"__typename":"MomentAttachment","momentId":"m9","caption":"look"}}}}"#)
    _ = try await open(transport)
    try DB.database.registerOperations(defsJson: #"[{"name":"CreateMessage","document":"mutation CreateMessage($chatId: ID!, $text: String!, $signedId: String, $attachedUserId: ID) { createMessage(chatId: $chatId, text: $text, signedId: $signedId, attachedUserId: $attachedUserId) { id chatId authorId text createdAt attachment { __typename ... on MomentAttachment { momentId caption } } } }"}]"#)

    let local = try await Message.create(chatId: "c", authorId: "u1", text: "hi")
    #expect(local.attachment == nil)
    try await local.settled()
    #expect(try await Message.find(local.confirmedId()).current()?.attachment == .moment(MomentAttachment(momentId: "m9", caption: "look")))
  }
}
