import Foundation
import DBlayerTesting
import Observation
import Testing
@testable import DBlayer

/// The tests share `DB.database` with the rest of `BridgeProbeTests`, so they run in its serialized suite.
extension BridgeProbeTests {
  @MainActor
  @Test
  func singletonRefreshStoresBeforeAnyReaderExists() async throws {
    let transport = try await Self.openSingletonRig()
    #expect(try DB.subscriptionCensus().observedQueries == 0)
    try await Self.login(transport, "1", "Ann")
    try await User.profile.warm()
    let cached = try await User.profile.cached()
    let rows = try #require(cached).value
    #expect(rows.map(\.id) == ["1"])
    #expect(rows.first?.name == "Ann")
    #expect(try DB.subscriptionCensus().observedQueries == 0)
    let entity = User.entity()
    await eventually("the stored singleton reaches its first reader") { entity.value?.name == "Ann" }
  }

  @MainActor
  static func openSingletonRig() async throws -> MockTransport {
    let transport = MockTransport()
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: transport)
    try DB.database.registerOperations(defsJson: #"[{"name":"UserProfile","document":"query UserProfile { me { id name } }"}]"#)
    return transport
  }

  /// The server answers the profile request with this user.
  @MainActor
  static func login(_ transport: MockTransport, _ id: String, _ name: String) async throws {
    transport.enqueue("UserProfile", reply: #"{"data":{"me":null}}"#)
    try await User.profile.load()
    transport.enqueue("UserProfile", reply: #"{"data":{"me":{"__typename":"User","id":"\#(id)","name":"\#(name)"}}}"#)
    try await User.profile.refresh()
  }

  /// REL-01 for the singleton and REL-08: 10 readers share 1 object, 1 value and 1 core subscription.
  @MainActor
  @Test
  func tenReadersCostOneSubscriptionAndSeeOneValue() async throws {
    let transport = try await Self.openSingletonRig()
    let readers = (0 ..< 10).map { _ in User.entity() }
    let feed = Entities<User>.shared.singleFeed
    try await Self.login(transport, "1", "Ann")
    await eventually(#"readers[0].value != nil"#) { readers[0].value != nil }
    #expect(Set(readers.map(ObjectIdentifier.init)).count == 1)
    #expect(readers.allSatisfy { $0.value?.name == "Ann" && $0.id == readers[0].value?.id })
    #expect(feed != nil && feed === Entities<User>.shared.singleFeed)
    #expect(try DB.subscriptionCensus().observedQueries == 1)
  }

  /// The value is nil before the first load and the reader is told when it loads.
  @MainActor
  @Test
  func emptyUntilTheRowLoadsAndTheReaderIsTold() async throws {
    let transport = try await Self.openSingletonRig()
    let entity = User.entity()
    #expect(entity.value == nil && entity.id.isEmpty && !entity.isStale)
    let told = Counter()
    withObservationTracking { _ = entity.value } onChange: { told.add() }
    try await Self.login(transport, "1", "Ann")
    await eventually(#"entity.value != nil"#) { entity.value != nil }
    #expect(entity.value?.name == "Ann" && told.value == 1)
  }

  /// REL-24: logout empties the entity at once and stops its feed.
  @MainActor
  @Test
  func logoutEmptiesTheEntityAtOnceAndStopsItsFeed() async throws {
    let transport = try await Self.openSingletonRig()
    let entity = User.entity()
    try await Self.login(transport, "1", "Ann")
    await eventually(#"entity.value != nil"#) { entity.value != nil }
    try await DB.logout()
    #expect(entity.value == nil && entity.id.isEmpty)
    #expect(User.entity() === entity)
    #expect(try DB.subscriptionCensus().observedQueries == 0)
    #expect(Entities<User>.shared.singleFeed == nil)
  }

  /// REL-24: the next login after a logout loads the new user into the same entity, and a second resume keeps the feed.
  @MainActor
  @Test
  func aLaterLoginLoadsTheNewUserIntoTheSameEntity() async throws {
    let transport = try await Self.openSingletonRig()
    let entity = User.entity()
    try await Self.login(transport, "1", "Ann")
    await eventually(#"entity.value != nil"#) { entity.value != nil }
    try await DB.logout()
    try await Self.login(transport, "2", "Bob")
    try await DB.resume()
    await eventually(#"entity.value?.name == "Bob""#) { entity.value?.name == "Bob" }
    #expect(try DB.subscriptionCensus().observedQueries == 1)
    let resumed = Entities<User>.shared.singleFeed
    try await DB.resume()
    #expect(resumed === Entities<User>.shared.singleFeed)
  }

  /// A delivery of a feed that a reset replaced writes nothing.
  @MainActor
  @Test
  func aDeliveryOfAReplacedFeedNeverWrites() async throws {
    let transport = try await Self.openSingletonRig()
    let entity = User.entity()
    let registry = Entities<User>.shared
    let old = try #require(registry.singleFeed)
    try await Self.login(transport, "1", "Ann")
    await eventually(#"entity.value != nil"#) { entity.value != nil }
    try await DB.logout()
    try await DB.resume()
    #expect(User.entity() === entity && entity.value == nil)
    registry.receive(from: old, revision: Int64.max, row: entity.value ?? User(id: "9", name: "Late"))
    #expect(entity.value == nil)
    try await Self.login(transport, "2", "Bob")
    await eventually(#"entity.value != nil"#) { entity.value != nil }
    #expect(entity.value?.name == "Bob")
    #expect(try DB.subscriptionCensus().observedQueries == 1)
  }

  /// REL-27: 10 seconds of the core clock without an event add no delivery and no subscription.
  @MainActor
  @Test
  func idleTenSecondsCreatesNoWork() async throws {
    DB.close()
    let transport = MockTransport()
    let clock = try await DB.openTesting(transport: transport, modelsJSON: modelsJSON)
    try DB.database.registerOperations(defsJson: #"[{"name":"UserProfile","document":"query UserProfile { me { id name } }"}]"#)
    let entity = User.entity()
    try await Self.login(transport, "1", "Ann")
    await eventually(#"entity.value != nil"#) { entity.value != nil }
    let feed = try #require(Entities<User>.shared.singleFeed)
    let before = (feed.delivered, try DB.subscriptionCensus().observedQueries, transport.calls.count)
    clock.advance(by: 10)
    try await DB.database.flush()
    #expect(try feed.delivered == before.0 && DB.subscriptionCensus().observedQueries == before.1 && transport.calls.count == before.2 && entity.value?.name == "Ann")
  }
}
