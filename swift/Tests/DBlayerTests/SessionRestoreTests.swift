import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

extension BridgeProbeTests {
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func theLoadThatRestoresAResetSessionReturnsItsOwnResult() async throws {
    try await DB.close()
    defer { Task { @MainActor in try? await DB.close() } }
    let transport = MockTransport()
    for (user, region) in [("u1", "r1"), ("u2", "r2")] {
      transport.enqueue("CurrentUserQuery", reply: #"{"data":{"user":{"id":"\#(user)","name":"\#(user)"}}}"#)
      transport.enqueue("CurrentRegionQuery", reply: #"{"data":{"region":{"id":"\#(region)","name":"\#(region)"}}}"#)
    }
    try await DB.openTesting(transport: transport, modelsJSON: modelsJSON)
    try DB.database.registerOperations(defsJson: #"[{"name":"CurrentUserQuery","document":"query CurrentUserQuery { user { id name } }"},{"name":"CurrentRegionQuery","document":"query CurrentRegionQuery { region { id name } }"}]"#)
    try await CurrentUser.current.load(in: DB.account())
    try await CurrentRegion.current.load(in: DB.account())
    #expect(try await CurrentUser.current.read(in: DB.account()).map(\.id) == ["u1"])

    try await DB.sessionReset()
    try await CurrentUser.current.load(in: DB.account())
    try await CurrentRegion.current.load(in: DB.account())

    #expect(transport.calls.count == 4)
    #expect(try await CurrentUser.current.read(in: DB.account()).map(\.id) == ["u2"])
    #expect(try await CurrentRegion.current.read(in: DB.account()).map(\.id) == ["r2"])
  }
}
