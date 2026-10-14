import Foundation
import DBlayerTesting
import Testing
@testable import DBlayer

extension BridgeProbeTests {
  @MainActor
  static func openOperationRig(_ name: String, document: String, reply: String) throws -> MockTransport {
    let transport = MockTransport()
    transport.enqueue(name, reply: reply)
    DB.database = try Database.open(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: transport)
    try DB.database.registerOperations(defsJson: #"[{"name":"\#(name)","document":"\#(document)"}]"#)
    return transport
  }

  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func anOperationWithoutAModelSendsItsVariablesAndReadsTheDeclaredResult() async throws {
    let transport = try Self.openOperationRig("QrCodeCreate", document: "mutation QrCodeCreate($input: CreateQrCodeInput!) { qrCodeCreate(input: $input) { url } }", reply: #"{"data":{"qrCodeCreate":{"url":"https://q"}}}"#)

    let result = try await Operations.qrCodeCreate(kind: "chat", size: 3)

    #expect(result.url == "https://q")
    let sent = transport.calls.first { $0.operationName == "QrCodeCreate" }?.variablesJson ?? ""
    #expect(JSONValue.parse(sent) == JSONValue.parse(#"{"input":{"kind":"chat","size":3}}"#), "variables sent: \(sent)")
  }

  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func anInputObjectOfTheSchemaIsSentNestedAndWithoutItsNilFields() async throws {
    let transport = try Self.openOperationRig("StickerSend", document: "mutation StickerSend($input: StickerInput!) { stickerSend(input: $input) { id } }", reply: #"{"data":{"stickerSend":{"id":"s1"}}}"#)
    let input = StickerInput(name: "wave", tags: ["a", "b"], notes: ["n", nil], pinned: true, owner: OwnerInput(id: "7"), helpers: [OwnerInput(id: "8", nickname: "ann")])

    let result = try await Operations.stickerSend(input: input)

    #expect(result.id == "s1")
    let sent = transport.calls.first { $0.operationName == "StickerSend" }?.variablesJson ?? ""
    let expected = #"{"input":{"name":"wave","tags":["a","b"],"notes":["n",null],"pinned":true,"owner":{"id":"7"},"helpers":[{"id":"8","nickname":"ann"}]}}"#
    #expect(JSONValue.parse(sent) == JSONValue.parse(expected), "variables sent: \(sent)")
  }

  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func aResultWithListsObjectsAndRawJsonIsDecodedIntoGeneratedTypes() async throws {
    _ = try Self.openOperationRig("Pricing", document: "query Pricing { pricing { gifts { id name price } videoIntro { enabled } tags spec } }", reply: #"{"data":{"pricing":{"gifts":[{"id":7,"name":"rose","price":5},{"id":"8","name":"ring"}],"videoIntro":{"enabled":true},"tags":["a","b"],"spec":{"k":[1,2]}}}}"#)

    let result = try await Operations.pricingLoad()

    #expect(result.gifts?.compactMap(\.id) == ["7", "8"])
    #expect(result.gifts?.first?.price == 5)
    #expect(result.gifts?.last?.price == nil)
    #expect(result.videoIntro?.enabled == true)
    #expect(result.tags == ["a", "b"])
    #expect(result.spec == JSONValue.parse(#"{"k":[1,2]}"#))
  }

  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func aJsonScalarOfAnInputObjectIsSentAsAnObjectAndAnOmittedListIsNotSent() async throws {
    let transport = try Self.openOperationRig("MomentCreate", document: "mutation MomentCreate($input: MomentInput!) { momentCreate(input: $input) { moment { id } } }", reply: #"{"data":{"momentCreate":{"moment":{"id":9}}}}"#)
    let widget = WidgetInput(kind: "EMOJI", settings: SettingsInput(itemProps: .object(["emoji": "fire", "size": 3, "tags": .array(["a"])]), rotate: 1.5))

    let sent = try await Operations.momentCreate(input: MomentInput(file: "s1", widgets: [widget]))
    #expect(sent.moment?.id == "9")
    transport.enqueue("MomentCreate", reply: #"{"data":{"momentCreate":{"moment":{"id":9}}}}"#)
    let without = try await Operations.momentCreate(input: MomentInput(file: "s2"))
    #expect(without.moment?.id == "9")

    let calls = transport.calls.filter { $0.operationName == "MomentCreate" }.map(\.variablesJson)
    #expect(JSONValue.parse(calls[0]) == JSONValue.parse(#"{"input":{"file":"s1","widgets":[{"kind":"EMOJI","settings":{"itemProps":{"emoji":"fire","size":3,"tags":["a"]},"rotate":1.5}}]}}"#), "variables sent: \(calls[0])")
    #expect(JSONValue.parse(calls[1]) == JSONValue.parse(#"{"input":{"file":"s2"}}"#), "variables sent: \(calls[1])")
  }
}
