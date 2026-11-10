import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

extension BridgeProbeTests {
  @MainActor @Test(.timeLimit(.minutes(1)))
  func aFailedProfileRefreshKeepsTheValueWithoutExtraRequests() async throws {
    let transport = MockTransport()
    let definitions = #"[{"name":"CurrentUserQuery","document":"query CurrentUserQuery { user { id name } }"},{"name":"DblayerViewerChanges","document":"subscription DblayerViewerChanges { viewerChanges { changes { area kind entityId } } }"}]"#
    var schema = try #require(JSONSerialization.jsonObject(with: Data(modelsJSON.utf8)) as? [String: Any])
    var models = try #require(schema["models"] as? [[String: Any]])
    let index = try #require(models.firstIndex { $0["key"] as? String == "CurrentUser" })
    models[index]["changes"] = [["area":"COUNTERS", "relation":"current"], ["area":"GIFT", "relation":"current"]]
    models[index]["change_feed"] = ["subscription":"DblayerViewerChanges", "root":"viewerChanges.changes", "area":"area", "kind":"kind", "id":"entityId"]
    schema["models"] = models
    let spec = String(decoding: try JSONSerialization.data(withJSONObject: schema), as: UTF8.self)
    let clock = try await openSignedClocked(modelsJson: spec, transport: transport, operations: definitions)
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
  @MainActor @Test(.timeLimit(.minutes(1)))
  func encryptedCableKeepsAnObservedProfileAcrossClockAdvances() async throws {
    let transport = MockTransport()
    let definitions = #"[{"name":"CurrentUserQuery","document":"query CurrentUserQuery { user { id name } }"},{"name":"DblayerViewerChanges","document":"subscription DblayerViewerChanges { viewerChanges { changes { area kind entityId } } }"}]"#
    var schema = try #require(JSONSerialization.jsonObject(with: Data(modelsJSON.utf8)) as? [String: Any])
    var models = try #require(schema["models"] as? [[String: Any]])
    for index in models.indices { models[index].removeValue(forKey: "change_feed") }
    let index = try #require(models.firstIndex { $0["key"] as? String == "CurrentUser" })
    models[index]["changes"] = [["area":"COUNTERS", "relation":"current"]]
    models[index]["change_feed"] = ["subscription":"DblayerViewerChanges", "root":"viewerChanges.changes", "area":"area", "kind":"kind", "id":"entityId"]
    schema["models"] = models
    let spec = String(decoding: try JSONSerialization.data(withJSONObject: schema), as: UTF8.self)
    let clock = try await openSignedClocked(modelsJson: spec, transport: transport, operations: definitions)
    transport.enqueue("CurrentUserQuery", reply: #"{"data":{"user":{"id":"u1","name":"Ann"}}}"#)
    transport.enqueue("CurrentUserQuery", reply: #"{"data":{"user":{"id":"u1","name":"Bea"}}}"#)
    let profile = Projected<CurrentUser>(try DB.account().singleton(CurrentUser.self))
    try await profile.refresh()
    guard await observe("the profile is shown", { profile.value?.name == "Ann" }) else { return }
    let peer = try #require(transport.cable as? EncryptedCablePeer)
    let token = try #require(await Cursor(peer.subscribed.stream).next("the profile subscription"))
    let pings = Cursor(peer.ping.stream)
    for _ in 0..<5 {
      clock.advance(by: 120)
      let ping = try #require(await pings.next("ping on the original connection, which may have reopened"))
      #expect(ping == token)
    }
    let delivered = try peer.event(token: token, result: ["data": ["viewerChanges": ["changes": [["area":"COUNTERS", "kind":"UPDATED", "entityId":"u1"]]]]])
    try #require(delivered)
    guard await observe("the change rereads the profile", { profile.value?.name == "Bea" }) else { return }
    #expect(peer.openedCount == 1)
    #expect(transport.calls.filter { $0.operationName == "CurrentUserQuery" }.count == 2)
    await profile.stream.cancel()
    await DB.closeForCleanup()
  }

}
