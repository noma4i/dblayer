import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

private let queueFeed = #"{"data":{"feed":{"nodes":[{"__typename":"Moment","id":"1","uuid":"a","text":"m","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}],"pageInfo":{"hasNextPage":false,"endCursor":null}}}}"#
private let queueUpdated = #"{"data":{"updateMoment":{"__typename":"Moment","id":"1","uuid":"a","text":"edited","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}"#
private let queueDefs = #"[{"name":"MomentsFeed","document":"query MomentsFeed($first: Int, $after: String) { feed(first: $first, after: $after) { nodes { id uuid text likedByMe media createdAt } pageInfo { hasNextPage endCursor } } }"},{"name":"UpdateMoment","document":"mutation UpdateMoment($id: ID!, $text: String) { updateMoment(id: $id, text: $text) { id uuid text likedByMe media createdAt } }"},{"name":"DeleteMoment","document":"mutation DeleteMoment($id: ID!) { deleteMoment(id: $id) { id } }"}]"#

private func momentsKeepingUpdates() throws -> String {
  var models = try JSONSerialization.jsonObject(with: Data(modelsArrayJSON.utf8)) as! [[String: Any]]
  let index = models.firstIndex { $0["key"] as? String == "Moment" }!
  var actions = models[index]["actions"] as! [[String: Any]]
  let update = actions.firstIndex { $0["name"] as? String == "update" }!
  actions[update]["on_failure"] = "keep"
  models[index]["actions"] = actions
  return specs(String(decoding: try JSONSerialization.data(withJSONObject: models), as: UTF8.self))
}

private extension OperationQueueStream {
  var queues: AsyncCompactMapSequence<OperationQueueStream, [OperationInfo]> {
    compactMap { if case let .queue(items) = $0 { items } else { nil } }
  }
}

/// The queue of operations through its public API: `operations()` shows each write with its status, and `retry` takes the fields of an `OperationInfo`.
extension BridgeProbeTests {
  @Test @MainActor
  func anOfflineUpdateShowsInTheQueueAndLeavesItWhenTheNetworkReturns() async throws {
    let transport = MockTransport()
    transport.enqueue("MomentsFeed", reply: queueFeed)
    let hold = transport.hold("UpdateMoment", reply: queueUpdated)
    try await openSigned(config: Config(path: nil, dataVersion: nil), modelsJson: modelsJSON, transport: transport)
    try await DB.database.registerOperations(defsJson: queueDefs)
    let changes = Cursor(try await Moment.recent.live.stream())
    try await Moment.feed.load()
    let moment = try #require(await changes.first("the loaded row") { $0.map(\.text) == ["m"] }?.first)
    let queue = Cursor(try DB.account().operations().queues)
    try DB.database.setOnline(online: false)
    let receipt = try await moment.update(text: "edited")
    let pending = try #require(await queue.first("the update is pending") { $0.count == 1 && $0[0].status == .pending })
    #expect(pending[0].model == "Moment" && pending[0].action == "update" && pending[0].rowId == moment.id)
    try DB.database.setOnline(online: true)
    let sending = try #require(await queue.first("the update is sending") { $0.first?.status == .sending })
    #expect(sending[0].attempts == 1)
    hold.release()
    if case .confirmed = try await receipt.outcome() {} else { Issue.record("the update was not confirmed") }
    #expect(await queue.first("the queue is empty") { $0.isEmpty } != nil)
  }

  @Test @MainActor
  func aRefusedUpdateStaysFailedWithItsErrorAndRetryByItsFieldsSendsItAgain() async throws {
    let transport = MockTransport()
    transport.enqueue("MomentsFeed", reply: queueFeed)
    transport.enqueue("UpdateMoment", failure: .Http(status: 422, detail: "refused"))
    transport.enqueue("UpdateMoment", reply: queueUpdated)
    try await openSigned(config: Config(path: nil, dataVersion: nil), modelsJson: try momentsKeepingUpdates(), transport: transport)
    try await DB.database.registerOperations(defsJson: queueDefs)
    let changes = Cursor(try await Moment.recent.live.stream())
    try await Moment.feed.load()
    let moment = try #require(await changes.first("the loaded row") { $0.map(\.text) == ["m"] }?.first)
    let queue = Cursor(try DB.account().operations().queues)
    _ = try await moment.update(text: "edited")
    let failed = try #require(await queue.first("the update failed") { $0.first?.status == .failed }?.first)
    #expect(failed.error != nil)
    try await DB.account().retry(model: failed.model, id: failed.rowId, action: failed.action)
    #expect(await queue.first("the queue is empty") { $0.isEmpty } != nil)
    #expect(transport.calls.filter { $0.operationName == "UpdateMoment" }.count == 2)
  }

  @Test @MainActor
  func theStatusAndQueueStreamsEndWithTheCauseOfTheCore() async throws {
    let transport = MockTransport()
    transport.enqueue("MomentsFeed", reply: queueFeed)
    try await openSigned(config: Config(path: nil, dataVersion: nil), modelsJson: modelsJSON, transport: transport)
    try await DB.database.registerOperations(defsJson: queueDefs)
    let changes = Cursor(try await Moment.recent.live.stream())
    try await Moment.feed.load()
    let moment = try #require(await changes.first("the loaded row") { $0.map(\.text) == ["m"] }?.first)
    let queue = Cursor(try DB.account().operations())
    let status = Cursor(try DB.account().uploadStatus(moment, action: "upload_clip"))
    #expect(await queue.next("the first queue") != nil)
    #expect(await status.next("the first status") != nil)

    await DB.closeForCleanup()

    guard case .ended(.closed)? = await queue.first("the end of the queue stream", where: { if case .ended = $0 { true } else { false } }) else { Issue.record("the queue stream did not end with Closed"); return }
    guard case .ended(.closed)? = await status.first("the end of the status stream", where: { if case .ended = $0 { true } else { false } }) else { Issue.record("the status stream did not end with Closed"); return }
  }
}
