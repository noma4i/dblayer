import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

private let feedPage = #"{"data":{"feed":{"nodes":[{"__typename":"Moment","id":"1","uuid":"a","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}],"pageInfo":{"hasNextPage":false}}}}"#
private let feedDefs = #"[{"name":"MomentsFeed","document":"query MomentsFeed { feed { nodes { id uuid likedByMe media createdAt } pageInfo { hasNextPage endCursor } } }"},{"name":"CreateMoment","document":"mutation CreateMoment($text: String) { createMoment(text: $text) { id uuid likedByMe media createdAt } }"}]"#

extension BridgeProbeTests {
  /// K09: a list stream opened with `refreshOnConnect` loads again once each time the network returns, and the screen has no refresh of its own to write.
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func aListThatRefreshesOnConnectLoadsAgainOnceWhenTheNetworkReturns() async throws {
    let transport = MockTransport()
    transport.enqueue("MomentsFeed", reply: feedPage)
    transport.enqueue("MomentsFeed", reply: feedPage)
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: transport)
    try await DB.database.registerOperations(defsJson: feedDefs)
    let list = Projected<Moment>(try AccountScope().stream(Moment.feed, refreshOnConnect: true))
    try await list.loaded()
    let loads = { transport.calls.filter { $0.operationName == "MomentsFeed" }.count }
    #expect(loads() == 1)

    try DB.database.setOnline(online: false)
    try DB.database.setOnline(online: true)
    await eventually("the list loaded again") { loads() == 2 }
    try await list.loaded()
    #expect(loads() == 2, "the list loaded more or less than once on reconnect: \(transport.calls.map(\.operationName))")
  }

  /// K11: while the network is away `refresh` and `loadMore` fail at once with the network error, and the stream shows the error. Nothing reaches the transport.
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func aRefreshAndALoadMoreFailAtOnceOfflineAndTheStreamShowsTheError() async throws {
    let transport = MockTransport()
    transport.enqueue("MomentsFeed", reply: feedPage)
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: transport)
    try await DB.database.registerOperations(defsJson: feedDefs)
    let list = Projected<Moment>(try AccountScope().stream(Moment.feed))
    try await list.loaded()
    let calls = transport.calls.count
    try DB.database.setOnline(online: false)

    do {
      try await list.refresh()
      Issue.record("a refresh offline did not fail")
    } catch DbError.Offline {
    }
    do {
      _ = try await list.loadMore()
      Issue.record("a load more offline did not fail")
    } catch DbError.Offline {
    }
    await eventually("the stream shows the network error") {
      list.error != nil
    }
    #expect(transport.calls.count == calls, "a call offline reached the transport")
  }

  /// K10: a send whose first attempt failed on the network goes out at once when the network returns. No clock step is taken.
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func aDurableSendThatFailedOnTheNetworkGoesOutAtOnceWhenTheNetworkReturns() async throws {
    let transport = MockTransport()
    transport.enqueue("CreateMoment", failure: .Network(detail: "down"))
    transport.enqueue("CreateMoment", reply: #"{"data":{"createMoment":{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}}}"#)
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: transport)
    try await DB.database.registerOperations(defsJson: feedDefs)
    let sent = try await AccountScope().send(Moment.self, values: ["text": .string("hello")])
    let attempts = { transport.calls.filter { $0.operationName == "CreateMoment" }.count }
    await eventually("the first attempt failed on the network") { attempts() == 1 }

    try DB.database.setOnline(online: false)
    try DB.database.setOnline(online: true)
    #expect(try await sent.confirmed().id == "m77")
    #expect(attempts() == 2)
  }
}
