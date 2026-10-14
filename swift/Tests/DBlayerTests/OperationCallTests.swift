import Foundation
import DBlayerTesting
import Testing
@testable import DBlayer

extension BridgeProbeTests {
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func anOperationWithoutAModelSendsItsVariablesAndReadsTheDeclaredResult() async throws {
    let transport = MockTransport()
    transport.enqueue("QrCodeCreate", reply: #"{"data":{"qrCodeCreate":{"url":"https://q"}}}"#)
    DB.database = try Database.open(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: transport)
    try DB.database.registerOperations(defsJson: #"[{"name":"QrCodeCreate","document":"mutation QrCodeCreate($input: CreateQrCodeInput!) { qrCodeCreate(input: $input) { url } }"}]"#)

    let result = try await Operations.qrCodeCreate(kind: "chat", size: 3)

    #expect(result.url == "https://q")
    let sent = transport.calls.first { $0.operationName == "QrCodeCreate" }?.variablesJson ?? ""
    #expect(sent == #"{"input":{"kind":"chat","size":3}}"#, "variables sent: \(sent)")
  }
}
