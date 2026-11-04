import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

extension BridgeProbeTests {
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func theValueOfAHeldStreamIsThereWhenLoadedReturnsAndARefreshStillAsksTheServer() async throws {
    try await DB.close()
    defer { Task { @MainActor in try? await DB.close() } }
    let transport = MockTransport()
    for _ in 0..<2 { transport.enqueue("CurrentUserQuery", reply: #"{"data":{"user":{"id":"u1","name":"u1"}}}"#) }
    try await DB.openTesting(transport: transport, modelsJSON: modelsJSON)
    try DB.database.registerOperations(defsJson: #"[{"name":"CurrentUserQuery","document":"query CurrentUserQuery { user { id name } }"}]"#)
    let held = Projected<CurrentUser>(try DB.account().stream(CurrentUser.current))

    try await held.loaded()
    #expect(held.value?.id == "u1")
    try await held.loaded()
    #expect(held.value?.id == "u1")
    #expect(transport.calls.filter { $0.operationName == "CurrentUserQuery" }.count == 1)
    try await CurrentUser.current.refresh()
    #expect(transport.calls.filter { $0.operationName == "CurrentUserQuery" }.count == 2)
  }
}
