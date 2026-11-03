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

  /// The wait reuses the load the singleton started: one request, then it returns at once.
  @MainActor
  @Test
  func theFirstLoadWaitOfASingletonSendsNoRequestOfItsOwn() async throws {
    let transport = try await Self.openSingletonRig()
    transport.enqueue("UserProfile", reply: #"{"data":{"me":{"__typename":"User","id":"1","name":"Ann"}}}"#)
    let entity = User.entity()
    try await entity.waitForInitialLoad()
    #expect(entity.value?.name == "Ann")
    await eventually("the read of the singleton ends") { entity.read == .loaded }
    try await entity.waitForInitialLoad()
    #expect(transport.calls.filter { $0.operationName == "UserProfile" }.count == 1)
  }

  /// A failed first load reaches the waiter from the snapshot.
  @MainActor
  @Test
  func aFailedFirstLoadOfASingletonReachesTheWaiter() async throws {
    let transport = try await Self.openSingletonRig()
    transport.enqueue("UserProfile", reply: #"{"errors":[{"message":"denied"}]}"#)
    let entity = User.entity()
    await #expect(throws: EntityRefreshError.self) { try await entity.waitForInitialLoad() }
    guard case .failed = entity.read else { Issue.record("read is \(entity.read)"); return }
  }

  /// REL-01 for the singleton and REL-08: 10 readers share 1 object, 1 value and 1 core subscription.
  @MainActor
  @Test
  func tenReadersCostOneSubscriptionAndSeeOneValue() async throws {
    let transport = try await Self.openSingletonRig()
    let readers = (0 ..< 10).map { _ in User.entity() }
    try await Self.login(transport, "1", "Ann")
    await eventually(#"readers[0].value != nil"#) { readers[0].value != nil }
    #expect(Set(readers.map(ObjectIdentifier.init)).count == 1)
    #expect(readers.allSatisfy { $0.value?.name == "Ann" && $0.id == readers[0].value?.id })
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
    try await DB.sessionReset()
    #expect(entity.value == nil && entity.id.isEmpty)
    #expect(User.entity() === entity)
    #expect(try DB.subscriptionCensus().observedQueries == 0)
  }

  /// REL-24: the next login after a logout loads the new user into the same entity, and a second resume keeps the feed.
  @MainActor
  @Test
  func aLaterLoginLoadsTheNewUserIntoTheSameEntity() async throws {
    let transport = try await Self.openSingletonRig()
    let entity = User.entity()
    try await Self.login(transport, "1", "Ann")
    await eventually(#"entity.value != nil"#) { entity.value != nil }
    try await DB.sessionReset()
    try await Self.login(transport, "2", "Bob")
    try await DB.resume()
    await eventually(#"entity.value?.name == "Bob""#) { entity.value?.name == "Bob" }
    #expect(try DB.subscriptionCensus().observedQueries == 1)
    try await DB.resume()
    #expect(try DB.subscriptionCensus().observedQueries == 1)
  }

  /// REL-27: 10 seconds of the core clock without an event add no delivery and no subscription.
  @MainActor
  @Test
  func idleTenSecondsCreatesNoWork() async throws {
    try await DB.close()
    let transport = MockTransport()
    let clock = try await DB.openTesting(transport: transport, modelsJSON: modelsJSON)
    try DB.database.registerOperations(defsJson: #"[{"name":"UserProfile","document":"query UserProfile { me { id name } }"}]"#)
    let entity = User.entity()
    try await Self.login(transport, "1", "Ann")
    await eventually(#"entity.value != nil"#) { entity.value != nil }
    let before = (entity.snapshot.revision, try DB.subscriptionCensus().observedQueries, transport.calls.count)
    clock.advance(by: 10)
    try await DB.database.flush()
    #expect(try entity.snapshot.revision == before.0 && DB.subscriptionCensus().observedQueries == before.1 && transport.calls.count == before.2 && entity.value?.name == "Ann")
  }
}
