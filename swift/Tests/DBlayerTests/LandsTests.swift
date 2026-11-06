import DBlayerTesting
import Testing
@testable import DBlayer

extension BridgeProbeTests {
  @Test
  func landsDeclarationAndRowCrossTheBridge() async throws {
    let schema = #"[{"key":"Record","fields":[{"name":"id","kind":{"type":"scalar","kind":"id"}}],"actions":[{"name":"create","root_shape":"upsert","durable":true,"mutation":"CreateRecord","root":"createRecord.record","has_optimistic":true,"has_temp_row_retention":true,"lands":[{"model":"Record","root":"createRecord.extra"}]}]}]"#
    let transport = MockTransport()
    transport.enqueue("CreateRecord", reply: #"{"data":{"createRecord":{"record":{"id":"r1"},"extra":{"id":"r2"}}}}"#)
    let database = try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 64), modelsJson: schema, transport: transport)
    try database.registerOperations(defsJson: #"[{"name":"CreateRecord","document":"mutation CreateRecord { createRecord { record { id } extra { id } } }"}]"#)
    let receipt = try await database.send(request: CommandRequest(epoch: database.sessionEpoch(), model: "Record", valuesJson: "{}", argsJson: "{}", upload: nil))
    guard case .confirmed = try await database.outcome(receipt: receipt) else { Issue.record("the create was not confirmed"); return }
    #expect(try await database.readRow(epoch: database.sessionEpoch(), model: "Record", rowId: "r2")?.id == "r2")
  }

}
