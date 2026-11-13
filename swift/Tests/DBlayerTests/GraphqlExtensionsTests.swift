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
    let database = try await openSigned(config: Config(path: nil), modelsJson: modelsJSON.replacingOccurrences(of: "CreateMoment", with: operation), transport: transport)
    try await registerStandOperations(database, defsJson: #"[{"name":"\#(operation)","document":"mutation \#(operation) { \#(root) { id } }"}]"#)
    let extra = text.map { #", "transactionDisposition":"\#($0)""# } ?? ""
    let response = #"{"errors":[{"message":"refused","extensions":{"code":"\#(code)"\#(extra),"detail":"refused","limit":3,"context":{"values":[true,null,2]}}},{"message":"secondary","extensions":{"code":"SECONDARY","transactionDisposition":"KEEP"}}]}"#
    for queued in [false, true] {
      transport.enqueue(operation, reply: response)
      let extensions: GraphqlExtensions?
      if queued {
        let receipt = try await database.send(request: CommandRequest(epoch: database.sessionEpoch(), model: "Moment", valuesJson: #"{"text":"hello","media":null}"#, argsJson: "{}", upload: nil))
        do {
          _ = try await database.confirmed(receipt: receipt, discardOnFailure: false)
          Issue.record("the refused purchase was not rejected")
          continue
        } catch let DbError.Rejected(failure, _) {
          extensions = failure.extensions
        }
      } else {
        do {
          _ = try await database.call(epoch: database.sessionEpoch(), operation: operation, variablesJson: "{}")
          Issue.record("the refused purchase did not throw")
          continue
        } catch let DbError.Graphql(_, thrown) {
          extensions = thrown
        }
      }
      #expect(extensions?.code == code)
      #expect(extensions?.transactionDisposition == disposition)
      #expect(extensions?.fields == ["detail": "\"refused\"", "limit": "3", "context": #"{"values":[true,null,2]}"#])
      let failure = OperationFailure(kind: "rejected", sessionCode: nil, requestSize: nil, requestLimit: nil, message: "refused", extensions: extensions)
      let restored = try JSONDecoder().decode(OperationFailure.self, from: JSONEncoder().encode(failure))
      #expect(restored.extensions?.code == code)
      #expect(restored.extensions?.transactionDisposition == disposition)
      let encoded = try JSONValue.decode(JSONEncoder().encode(restored.extensions))
      #expect((try encoded.objectValue)["fields"] == .object(["detail": .string("\"refused\""), "limit": .string("3"), "context": .string(#"{"values":[true,null,2]}"#)]))
    }
  }
}

@Test func operationFailureCarriesEveryWireFailureField() throws {
  let wire = WireFailure(kind: "request_too_large", sessionCode: .rateLimited, requestSize: 9_000_000_000, requestLimit: 8_000_000_000, message: "too large", extensions: nil)
  let failure = OperationFailure(wire: wire)
  #expect(failure.kind == "request_too_large")
  #expect(failure.sessionCode == .rateLimited)
  #expect(failure.requestSize == 9_000_000_000)
  #expect(failure.requestLimit == 8_000_000_000)
  #expect(failure.message == "too large")
  #expect(try JSONDecoder().decode(OperationFailure.self, from: JSONEncoder().encode(failure)) == failure)
}
