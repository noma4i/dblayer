import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

extension BridgeProbeTests {
  @MainActor @Test(.timeLimit(.minutes(1)))
  func aFailedProfileRefreshKeepsTheValueWithoutExtraRequests() async throws {
    let transport = MockTransport()
    let definitions = #"[{"name":"CurrentUserQuery","document":"query CurrentUserQuery { user { id name } }"}]"#
    let clock = try await openSignedClocked(modelsJson: modelsJSON, transport: transport, operations: definitions)
    transport.enqueue("CurrentUserQuery", reply: #"{"data":{"user":{"id":"u1","name":"Ann"}}}"#)
    transport.enqueue("CurrentUserQuery", failure: .Network(detail: "offline"))
    transport.enqueue("CurrentUserQuery", failure: .Http(status: 400, detail: "refused after retry"))
    let profile = Projected<CurrentUser>(try DB.account().singleton(CurrentUser.self))
    try await profile.refresh()
    guard await observe("the profile is shown", { profile.value?.name == "Ann" }) else { return }
    let armed = clock.armed()
    let refresh = Task { try await profile.refresh() }
    await clock.awaitArmed(after: armed)
    clock.advance(by: 120)
    do { try await refresh.value; Issue.record("the refused refresh succeeded") } catch {}
    guard await observe("the refresh error is shown", { profile.error != nil }) else { return }
    #expect(profile.value?.name == "Ann")
    let calls = transport.calls.filter { $0.operationName == "CurrentUserQuery" }
    #expect(calls.count == 3, "profile requests: \(calls.map(\.operationName))")
    await profile.stream.cancel()
    await DB.closeForCleanup()
  }
}
