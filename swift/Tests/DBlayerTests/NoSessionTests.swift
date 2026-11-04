import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

extension BridgeProbeTests {
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func serverCallsWithoutATransportReportNoSession() async throws {
    try await DB.close()
    defer { Task { @MainActor in try? await DB.close() } }
    try await DB.openTesting(transport: nil, modelsJSON: modelsJSON)
    let database = try DB.database
    let epoch = database.sessionEpoch()
    do {
      _ = try await database.create(epoch: epoch, model: "Club", valuesJson: #"{"name":"friends"}"#, argsJson: "{}")
      Issue.record("create accepted without a session")
    } catch DbError.NoSession {}
    do {
      try await database.load(epoch: epoch, model: "Club", relation: "all", paramsJson: "{}")
      Issue.record("load accepted without a session")
    } catch DbError.NoSession {}
    do {
      _ = try await database.send(request: CommandRequest(epoch: epoch, model: "Record", valuesJson: "{}", argsJson: "{}", upload: nil))
      Issue.record("send accepted without a session")
    } catch DbError.NoSession {}
    do {
      _ = try await database.act(epoch: epoch, model: "Club", rowId: "g1", action: "leave", patchJson: "{}", argsJson: "{}")
      Issue.record("act accepted without a session")
    } catch DbError.NoSession {}
  }
}
