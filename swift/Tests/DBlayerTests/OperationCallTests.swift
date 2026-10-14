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
}
