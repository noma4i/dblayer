import Foundation
import DBlayerTesting
import Testing
@testable import DBlayer

extension BridgeProbeTests {
  @MainActor
  static func openOperationRig(_ name: String, document: String, reply: String) async throws -> MockTransport {
    let transport = MockTransport()
    transport.enqueue(name, reply: reply)
    try await openSigned(config: Config(path: nil), modelsJson: modelsJSON, transport: transport)
    try await registerStandOperations(DB.database, defsJson: #"[{"name":"\#(name)","document":"\#(document)"}]"#)
    return transport
  }

  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func anOperationWithoutAModelSendsItsVariablesAndReadsTheDeclaredResult() async throws {
    let transport = try await Self.openOperationRig("QrCodeCreate", document: "mutation QrCodeCreate($input: CreateQrCodeInput!) { qrCodeCreate(input: $input) { url } }", reply: #"{"data":{"qrCodeCreate":{"url":"https://q"}}}"#)

    let result = try await Operations.qrCodeCreate(kind: "container", size: 3)

    #expect(result.url == "https://q")
    let sent = transport.calls.first { $0.operationName == "QrCodeCreate" }?.variablesJson ?? ""
    #expect(try JSONValue.read(sent) == JSONValue.read(#"{"input":{"kind":"container","size":3}}"#), "variables sent: \(sent)")
  }

  @Test
  func aLooseResultIsRefusedByTheWrapper() throws {
    func decode(_ text: String) throws -> PricingLoadResultRewards { try JSONDecoder().decode(PricingLoadResultRewards.self, from: Data(text.utf8)) }
    for loose in [#"{"id":7}"#, #"{"id":"7","price":"3"}"#, #"{"id":"7","name":5}"#] {
      #expect(throws: (any Error).self, Comment(rawValue: loose)) { try decode(loose) }
    }
    #expect(try decode(#"{"id":"7","name":null,"price":3}"#) == PricingLoadResultRewards(id: "7", name: nil, price: 3))
  }

  @MainActor
  @Test(.timeLimit(.minutes(1)), arguments: [
    ("a status", DbError.Http(status: 401, detail: "unauthorized"), "Unauthorized"),
    ("graphql messages", DbError.Graphql(messages: ["nope", "bad"], extensions: nil), "Graphql nope,bad")
  ])
  func aRefusalOfTheServerArrivesTypedThroughAnOperationCall(_ name: String, _ failure: DbError, _ expected: String) async throws {
    let transport = MockTransport()
    transport.enqueue("QrCodeCreate", failure: failure)
    try await openSigned(config: Config(path: nil), modelsJson: modelsJSON, transport: transport)
    try await registerStandOperations(DB.database, defsJson: #"[{"name":"QrCodeCreate","document":"mutation QrCodeCreate($input: CreateQrCodeInput!) { qrCodeCreate(input: $input) { url } }"}]"#)

    do {
      _ = try await Operations.qrCodeCreate(kind: "container", size: 3)
      Issue.record("\(name): the refused call did not throw")
    } catch let error as DbError {
      #expect(TransportFailureTests.shape(error) == expected, "\(name): \(error)")
    } catch {
      Issue.record("\(name): the refusal surfaced as \(error), not as a database error")
    }
  }

  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func inputObjectsDistinguishOmittedNullAndValues() async throws {
    let transport = try await Self.openOperationRig("StickerSend", document: "mutation StickerSend($input: StickerInput!) { stickerSend(input: $input) { id } }", reply: #"{"data":{"stickerSend":{"id":"s1"}}}"#)
    let input = StickerInput(name: "wave", mood: .null, tags: ["a", "b"], notes: .value(["n", nil]), pinned: true, owner: OwnerInput(id: "7"), helpers: .value([OwnerInput(id: "8", nickname: .value("ann"))]))

    let result = try await Operations.stickerSend(input: input)

    #expect(result.id == "s1")
    let sent = transport.calls.first { $0.operationName == "StickerSend" }?.variablesJson ?? ""
    let expected = #"{"input":{"name":"wave","mood":null,"tags":["a","b"],"notes":["n",null],"pinned":true,"owner":{"id":"7"},"helpers":[{"id":"8","nickname":"ann"}]}}"#
    #expect(try JSONValue.read(sent) == JSONValue.read(expected), "variables sent: \(sent)")
  }

  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func recursiveInputsPreserveNestedValues() async throws {
    let transport = try await Self.openOperationRig("StickerSend", document: "mutation StickerSend($input: StickerInput!) { stickerSend(input: $input) { id } }", reply: #"{"data":{"stickerSend":{"id":"s1"}}}"#)
    let recursive = RecursiveInput(next: .value(RecursiveInput(next: .null)), choice: .value(.next(.value("leaf"))))
    let input = StickerInput(name: "tree", tags: [], pinned: false, owner: OwnerInput(id: "7"), recursive: .value(recursive))
    _ = try await Operations.stickerSend(input: input)
    let sent = try #require(transport.calls.first { $0.operationName == "StickerSend" }).variablesJson
    let expected = #"{"input":{"name":"tree","tags":[],"pinned":false,"owner":{"id":"7"},"recursive":{"next":{"next":null},"choice":{"next":{"value":"leaf"}}}}}"#
    #expect(try JSONValue.read(sent) == JSONValue.read(expected))
  }

  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func inputNamesPreserveGraphQLKeys() async throws {
    let transport = try await Self.openOperationRig("StickerSend", document: "mutation StickerSend($input: StickerInput!) { stickerSend(input: $input) { id } }", reply: #"{"data":{"stickerSend":{"id":"s1"}}}"#)
    let keywords = KeywordInput(default: "d", when: "w", class: "c", container: "box", encoder: "wire", choices: [.default("a"), .when("b")])
    let input = StickerInput(name: "keys", tags: [], pinned: false, owner: OwnerInput(id: "7"), keywords: .value(keywords))
    _ = try await Operations.stickerSend(input: input)
    let sent = try #require(transport.calls.first { $0.operationName == "StickerSend" }).variablesJson
    let expected = #"{"input":{"name":"keys","tags":[],"pinned":false,"owner":{"id":"7"},"keywords":{"default":"d","when":"w","class":"c","container":"box","encoder":"wire","choices":[{"default":"a"},{"when":"b"}]}}}"#
    #expect(try JSONValue.read(sent) == JSONValue.read(expected))
  }

  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func aResultWithListsObjectsAndRawJsonIsDecodedIntoGeneratedTypes() async throws {
    _ = try await Self.openOperationRig("Pricing", document: "query Pricing { pricing { rewards { id name price } videoIntro { enabled } tags spec } }", reply: #"{"data":{"pricing":{"rewards":[{"id":7,"name":"rose","price":5},{"id":"8","name":"ring"}],"videoIntro":{"enabled":true},"tags":["a","b"],"spec":{"k":[1,2]}}}}"#)

    let result = try await Operations.pricingLoad()

    #expect(result.rewards?.compactMap(\.id) == ["7", "8"])
    #expect(result.rewards?.first?.price == 5)
    #expect(result.rewards?.last?.price == nil)
    #expect(result.videoIntro?.enabled == true)
    #expect(result.tags == ["a", "b"])
    #expect(try result.spec == JSONValue.read(#"{"k":[1,2]}"#))
  }

  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func aUnionAnswerIsDecodedByItsTypenameIntoTheVariantOfTheOperation() async throws {
    _ = try await Self.openOperationRig("PeerLoad", document: "query PeerLoad { peerLoad { peer { __typename ... on Person { name } ... on Robot { serial } } } }", reply: #"{"data":{"peerLoad":{"peer":{"__typename":"Robot","serial":"r2"}}}}"#)

    let result = try await Operations.peerLoad()

    guard case let .robot(robot)? = result.peer else {
      Issue.record("the peer is not a robot: \(String(describing: result.peer))")
      return
    }
    #expect(robot.serial == "r2")
    #expect(result.peer?.kind == .robot)
  }

  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func aListOperationReturnsOneGeneratedResultPerElementOfTheRootList() async throws {
    let transport = try await Self.openOperationRig("Giphy", document: "query Giphy($type: String!, $limit: Int) { giphy(type: $type, limit: $limit) { id url isSticker } }", reply: #"{"data":{"giphy":[{"id":1,"url":"u1","isSticker":true},{"id":"2"}]}}"#)

    let result = try await Operations.giphyLoad(type: "gif")

    #expect(result.map(\.id) == ["1", "2"])
    #expect(result.map(\.url) == ["u1", nil])
    #expect(result.map(\.isSticker) == [true, nil])
    let sent = transport.calls.filter { $0.operationName == "Giphy" }.map(\.variablesJson)
    #expect(try JSONValue.read(sent[0]) == JSONValue.read(#"{"type":"gif"}"#), "an unset optional parameter is not sent (M85): \(sent[0])")
  }

  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func aJsonScalarOfAnInputObjectIsSentAsAnObjectAndAnOmittedListIsNotSent() async throws {
    let transport = try await Self.openOperationRig("MomentCreate", document: "mutation MomentCreate($input: MomentInput!) { momentCreate(input: $input) { moment { id } } }", reply: #"{"data":{"momentCreate":{"moment":{"id":9}}}}"#)
    let widget = WidgetInput(kind: .emoji, settings: SettingsInput(itemProps: .object(["emoji": "fire", "size": 3, "tags": .array(["a"])]), rotate: .value(1.5)))

    let sent = try await Operations.momentCreate(input: MomentInput(file: "s1", widgets: .value([widget])))
    #expect(sent.moment?.id == "9")
    transport.enqueue("MomentCreate", reply: #"{"data":{"momentCreate":{"moment":{"id":9}}}}"#)
    let without = try await Operations.momentCreate(input: MomentInput(file: "s2"))
    #expect(without.moment?.id == "9")

    let calls = transport.calls.filter { $0.operationName == "MomentCreate" }.map(\.variablesJson)
    #expect(try JSONValue.read(calls[0]) == JSONValue.read(#"{"input":{"file":"s1","widgets":[{"kind":"EMOJI","settings":{"itemProps":{"emoji":"fire","size":3,"tags":["a"]},"rotate":1.5}}]}}"#), "variables sent: \(calls[0])")
    #expect(try JSONValue.read(calls[1]) == JSONValue.read(#"{"input":{"file":"s2"}}"#), "variables sent: \(calls[1])")
  }
}
