import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

extension BridgeProbeTests {
  @MainActor
  @Test(.timeLimit(.minutes(1)), arguments: [
    ("UNKNOWN_PRODUCT", "FINISH", TransactionDisposition.finish),
    ("REACTIVATION_GRANT_MISSING", "KEEP", TransactionDisposition.keep),
    ("FUTURE_CODE", "LATER", TransactionDisposition.unknown(value: "LATER")),
    ("DUPLICATE_CLIENT_KEY", nil as String?, nil as TransactionDisposition?)
  ])
  func graphqlExtensionsReachDirectAndQueuedCalls(_ code: String, _ text: String?, _ disposition: TransactionDisposition?) async throws {
    let transport = MockTransport()
    let operation = code == "DUPLICATE_CLIENT_KEY" ? "RecordAlbumSend" : "PurchaseCreate"
    let root = code == "DUPLICATE_CLIENT_KEY" ? "recordAlbumSend" : "purchaseCreate"
    let database = try await openDatabase(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON.replacingOccurrences(of: "CreateMoment", with: operation), transport: transport)
    try database.registerOperations(defsJson: #"[{"name":"\#(operation)","document":"mutation \#(operation) { \#(root) { id } }"}]"#)
    let extra = text.map { #", "transactionDisposition":"\#($0)""# } ?? ""
    let response = #"{"errors":[{"message":"refused","extensions":{"code":"\#(code)"\#(extra),"detail":"refused","limit":3,"context":{"values":[true,null,2]}}},{"message":"secondary","extensions":{"code":"SECONDARY","transactionDisposition":"KEEP"}}]}"#
    for queued in [false, true] {
      transport.enqueue(operation, reply: response)
      do {
        if queued {
          let receipt = try await database.send(request: CommandRequest(epoch: database.sessionEpoch(), model: "Moment", valuesJson: #"{"text":"hello","media":null}"#, argsJson: "{}", upload: nil))
          try await database.settled(epoch: database.sessionEpoch(), model: "Moment", rowId: receipt.row.id, action: nil)
        } else {
          _ = try await database.run(operation: operation, variablesJson: "{}")
        }
        Issue.record("the refused purchase did not throw")
      } catch let DbError.Graphql(_, extensions) {
        #expect(extensions?.code == code)
        #expect(extensions?.transactionDisposition == disposition)
        #expect(extensions?.fields == ["detail": "\"refused\"", "limit": "3", "context": #"{"values":[true,null,2]}"#])
        let failure = OperationFailure(kind: "rejected", message: "refused", extensions: extensions)
        let restored = try JSONDecoder().decode(OperationFailure.self, from: JSONEncoder().encode(failure))
        #expect(restored.extensions?.code == code)
        #expect(restored.extensions?.transactionDisposition == disposition)
        let encoded = try JSONValue.decode(JSONEncoder().encode(restored.extensions))
        #expect(encoded.objectValue["fields"] == .object(["detail": .string("\"refused\""), "limit": .string("3"), "context": .string(#"{"values":[true,null,2]}"#)]))
      }
    }
  }
}
