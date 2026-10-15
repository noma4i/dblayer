import Foundation
import DBlayerTesting
import Observation
import Testing
@testable import DBlayer

/// The tests share `DB.database` with the rest of `BridgeProbeTests`, so they run in its serialized suite.
extension BridgeProbeTests {
  @MainActor
  static func openSingletonRig() throws -> MockTransport {
    let transport = MockTransport()
    DB.database = try Database.open(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: transport)
    try DB.database.registerOperations(defsJson: #"[{"name":"UserProfile","document":"query UserProfile { me { id name } }"}]"#)
    return transport
  }

  /// The server answers the profile request with this user.
  @MainActor
  static func login(_ transport: MockTransport, _ id: String, _ name: String) async throws {
    transport.enqueue("UserProfile", reply: #"{"data":{"me":{"__typename":"User","id":"\#(id)","name":"\#(name)"}}}"#)
    try await User.profile.refresh()
  }

  /// REL-01 for the singleton and REL-08: 10 readers share 1 object, 1 value and 1 core subscription.
  @MainActor
  @Test
  func tenReadersCostOneSubscriptionAndSeeOneValue() async throws {
    let transport = try Self.openSingletonRig()
    let readers = (0 ..< 10).map { _ in User.entity() }
    let feed = Entities<User>.shared.singleFeed
    try await Self.login(transport, "1", "Ann")
    await eventually(#"readers[0].value != nil"#) { readers[0].value != nil }
    #expect(Set(readers.map(ObjectIdentifier.init)).count == 1)
    #expect(readers.allSatisfy { $0.value?.name == "Ann" && $0.id == readers[0].value?.id })
    #expect(feed != nil && feed === Entities<User>.shared.singleFeed)
    #expect(DB.subscriptionCensus().observedQueries == 1)
  }

  /// The value is nil before the first load and the reader is told when it loads.
  @MainActor
  @Test
  func emptyUntilTheRowLoadsAndTheReaderIsTold() async throws {
    let transport = try Self.openSingletonRig()
    let entity = User.entity()
    #expect(entity.value == nil && entity.id.isEmpty && !entity.isStale)
    let told = Counter()
    withObservationTracking { _ = entity.value } onChange: { told.add() }
    try await Self.login(transport, "1", "Ann")
    await eventually(#"entity.value != nil"#) { entity.value != nil }
    #expect(entity.value?.name == "Ann" && told.value == 1)
  }

  /// REL-24: logout empties the entity at once, and the next login loads the new user without the old one showing.
  @MainActor
  @Test
  func logoutEmptiesItAndALaterLoginLoadsTheNewUser() async throws {
    let transport = try Self.openSingletonRig()
    let entity = User.entity()
    try await Self.login(transport, "1", "Ann")
    await eventually(#"entity.value != nil"#) { entity.value != nil }
    let seen = Counter()
    try await DB.logout()
    #expect(entity.value == nil && entity.id.isEmpty)
    withObservationTracking { _ = entity.value } onChange: { seen.add() }
    try await Task.sleep(for: .milliseconds(200))
    #expect(entity.value == nil)
    try await Self.login(transport, "2", "Bob")
    await eventually(#"entity.value != nil"#) { entity.value != nil }
    #expect(entity.value?.name == "Bob" && DB.subscriptionCensus().observedQueries == 1)
  }

  /// A delivery of a feed that a newly opened database replaced writes nothing.
  @MainActor
  @Test
  func aDeliveryOfAReplacedFeedNeverWrites() async throws {
    let transport = try Self.openSingletonRig()
    let entity = User.entity()
    let registry = Entities<User>.shared
    let old = try #require(registry.singleFeed)
    try await Self.login(transport, "1", "Ann")
    await eventually(#"entity.value != nil"#) { entity.value != nil }
    let second = try Self.openSingletonRig()
    #expect(User.entity() === entity && entity.value == nil)
    registry.receive(from: old, revision: Int64.max, row: entity.value ?? User(id: "9", name: "Late"))
    #expect(entity.value == nil)
    try await Self.login(second, "2", "Bob")
    await eventually(#"entity.value != nil"#) { entity.value != nil }
    #expect(entity.value?.name == "Bob")
  }

  /// REL-27: 10 seconds without an event add no delivery and no subscription.
  @MainActor
  @Test
  func idleTenSecondsCreatesNoWork() async throws {
    let transport = try Self.openSingletonRig()
    let entity = User.entity()
    try await Self.login(transport, "1", "Ann")
    await eventually(#"entity.value != nil"#) { entity.value != nil }
    let feed = try #require(Entities<User>.shared.singleFeed)
    let before = (feed.delivered, DB.subscriptionCensus().observedQueries)
    try await Task.sleep(for: .seconds(10))
    #expect(feed.delivered == before.0 && DB.subscriptionCensus().observedQueries == before.1 && entity.value?.name == "Ann")
  }
}
