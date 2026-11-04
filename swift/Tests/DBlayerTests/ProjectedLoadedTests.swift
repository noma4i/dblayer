import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

extension BridgeProbeTests {
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func cancellingOnlyTheLoadWaitLeavesTheReaderRunning() async throws {
    try await DB.close()
    defer { Task { @MainActor in try? await DB.close() } }
    let transport = MockTransport()
    let gate = transport.hold("ContainerNode", reply: #"{"data":{"node":{"id":"c1","title":"one","shareUrl":"u","isPublic":true}}}"#)
    try await DB.openTesting(transport: transport, modelsJSON: modelsJSON)
    try DB.database.registerOperations(defsJson: #"[{"name":"ContainerNode","document":"query ContainerNode($id: ID!) { node(id: $id) { ... on Container { id title shareUrl isPublic } } }"}]"#)
    let held = Projected<Container>(try DB.account().entity(Container.self, "c1", fetch: .force))
    let (events, signal) = AsyncStream<Int>.makeStream()
    let cursor = Cursor(events)
    defer { signal.finish(); gate.release() }
    let entered = Task { await gate.entered(); signal.yield(0) }
    defer { entered.cancel() }
    guard await cursor.next("the request reaches the transport") == 0 else { Issue.record("the request did not start"); return }
    let waiting = Task { @MainActor in
      signal.yield(1)
      do { try await held.loaded(); signal.yield(2) }
      catch is CancellationError { signal.yield(3) }
      catch { signal.yield(4) }
    }
    guard await cursor.next("the wait starts") == 1 else { Issue.record("the wait did not start"); return }
    waiting.cancel()
    guard await cursor.next("the wait ends after cancellation") == 3 else { Issue.record("the wait did not end with cancellation"); return }
    gate.release()
    let continued = Task { @MainActor in
      do { try await held.loaded(); signal.yield(5) }
      catch { signal.yield(6) }
    }
    defer { continued.cancel() }
    guard await cursor.next("the reader continues") == 5 else { Issue.record("the reader did not continue"); return }
    #expect(held.value?.title == "one")
  }

  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func aReceiptRevisionWaitsUntilTheUpdatedRowIsShown() async throws {
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: MockTransport())
    let created = try await Presence.create(userId: "u1", room: "old")
    let row = try #require(created.optimisticValue)
    let held = Projected<Presence>(try DB.account().stream(Presence.where(.equal("userId", .string("u1")))))
    try await held.loaded()
    let receipt = try await row.update(room: "new")

    try await held.shown(receipt.revision)
    #expect(held.rows.first(where: { $0.id == row.id })?.room == "new")
  }

  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func aColdProfileSettlesWhenEmptyOrFailed() async throws {
    defer { Task { @MainActor in try? await DB.close() } }
    for failed in [false, true] {
      try await DB.close()
      let transport = MockTransport()
      if failed { transport.enqueue("CurrentUserQuery", failure: .Network(detail: "down")) }
      else { transport.enqueue("CurrentUserQuery", reply: #"{"data":{"user":null}}"#) }
      try await DB.openTesting(transport: transport, modelsJSON: modelsJSON)
      try DB.database.registerOperations(defsJson: #"[{"name":"CurrentUserQuery","document":"query CurrentUserQuery { user { id name } }"}]"#)
      let held = Projected<CurrentUser>(try DB.account().stream(CurrentUser.current))
      do {
        try await held.loaded()
        #expect(!failed, "the failed load returned successfully")
      } catch DbError.Network {
        #expect(failed, "the empty load returned an error")
      } catch { Issue.record("unexpected error: \(error)") }
      #expect(held.value == nil)
      #expect(held.phase == (failed ? .failed : .empty))
      if failed { if case .Network? = held.error {} else { Issue.record("the network error is missing") } }
    }
  }

  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func cancellingAReaderBeforeItsFirstLoadInterruptsTheWait() async throws {
    try await DB.close()
    defer { Task { @MainActor in try? await DB.close() } }
    let transport = MockTransport()
    let gate = transport.hold("ContainerNode", reply: #"{"data":{"node":{"id":"c1","title":"one","shareUrl":"u","isPublic":true}}}"#)
    try await DB.openTesting(transport: transport, modelsJSON: modelsJSON)
    try DB.database.registerOperations(defsJson: #"[{"name":"ContainerNode","document":"query ContainerNode($id: ID!) { node(id: $id) { ... on Container { id title shareUrl isPublic } } }"}]"#)
    let held = Projected<Container>(try DB.account().entity(Container.self, "c1", fetch: .force))
    await gate.entered()
    held.stream.cancel()
    gate.release()
    do {
      try await held.loaded()
      Issue.record("the cancelled stream reported a load")
    } catch DbError.Interrupted {} catch { Issue.record("unexpected error: \(error)") }
    #expect(held.value == nil)
  }

  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func aHeldEntityEndsItsLoadWaitAfterSessionReset() async throws {
    try await DB.close()
    defer { Task { @MainActor in try? await DB.close() } }
    let transport = MockTransport()
    transport.enqueue("ContainerNode", reply: #"{"data":{"node":{"id":"c1","title":"one","shareUrl":"u","isPublic":true}}}"#)
    try await DB.openTesting(transport: transport, modelsJSON: modelsJSON)
    try DB.database.registerOperations(defsJson: #"[{"name":"ContainerNode","document":"query ContainerNode($id: ID!) { node(id: $id) { ... on Container { id title shareUrl isPublic } } }"}]"#)
    let held = Projected<Container>(try DB.account().entity(Container.self, "c1", fetch: .force))
    try await held.loaded()
    try await DB.sessionReset()
    guard await observe("the stream ends", { held.isEnded }) else { return }
    #expect(held.projection.ended == .sessionEnded)
    do {
      try await held.loaded()
      Issue.record("the ended stream reported a load")
    } catch DbError.Interrupted {} catch { Issue.record("unexpected error: \(error)") }
  }

  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func anUnknownAssociationReportsItsErrorAfterTheStreamEnds() async throws {
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: MockTransport())
    let association = Association<Presence, User>("missing")
    let held = Projected<User>(try DB.account().related(Presence.where(.equal("room", .string("r"))), association))
    guard await observe("the stream ends", { held.isEnded }) else { return }
    #expect(held.projection.ended == .invalid)
    do {
      try await held.loaded()
      Issue.record("the unknown association was accepted")
    } catch DbError.Invalid(let detail) {
      #expect(detail.contains("missing"))
    } catch {
      Issue.record("unexpected error: \(error)")
    }
  }

  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func aCachedEntityRemainsLoadedWhenItsInitialNetworkReadFails() async throws {
    try await DB.close()
    defer { Task { @MainActor in try? await DB.close() } }
    let transport = MockTransport()
    transport.enqueue("ContainerNode", reply: #"{"data":{"node":{"id":"c1","title":"one","shareUrl":"u","isPublic":true}}}"#)
    transport.enqueue("ContainerNode", failure: .Network(detail: "down"))
    try await DB.openTesting(transport: transport, modelsJSON: modelsJSON)
    try DB.database.registerOperations(defsJson: #"[{"name":"ContainerNode","document":"query ContainerNode($id: ID!) { node(id: $id) { ... on Container { id title shareUrl isPublic } } }"}]"#)
    let first = Projected<Container>(try DB.account().entity(Container.self, "c1", fetch: .force))
    try await first.loaded()
    first.stream.cancel()
    let held = Projected<Container>(try DB.account().entity(Container.self, "c1", fetch: .force))

    try await held.loaded()
    #expect(held.value?.title == "one")
    #expect(held.phase == .loaded)
    if case .Network? = held.error {} else { Issue.record("the network error is missing") }
  }

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

  /// Requirement: `loaded()` returns normally only when the projection shows the rows of the loaded revision, and throws when the reading ends first.
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func loadedNeverReturnsWithoutRowsWhenTheReaderIsCancelledBeforeThem() async throws {
    try await DB.close()
    defer { Task { @MainActor in try? await DB.close() } }
    let transport = MockTransport()
    transport.enqueue("CurrentUserQuery", reply: #"{"data":{"user":{"id":"u1","name":"u1"}}}"#)
    try await DB.openTesting(transport: transport, modelsJSON: modelsJSON)
    try DB.database.registerOperations(defsJson: #"[{"name":"CurrentUserQuery","document":"query CurrentUserQuery { user { id name } }"}]"#)
    let held = Projected<CurrentUser>(try DB.account().stream(CurrentUser.current))
    let waiting = Task { @MainActor in try await held.loaded() }
    held.reading?.cancel()

    do {
      try await waiting.value
      #expect(held.value?.id == "u1", "loaded() returned with no rows")
    } catch {
      guard case DbError.Interrupted = error else {
        Issue.record("loaded() ended with \(error)")
        return
      }
    }
  }

  /// Requirement: for a stream of one entity that the library loads from the server, the row is visible when `loaded()` returns.
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func theRowOfAnEntityStreamThatLoadsFromTheServerIsThereWhenLoadedReturns() async throws {
    try await DB.close()
    defer { Task { @MainActor in try? await DB.close() } }
    let transport = MockTransport()
    transport.enqueue("ContainerNode", reply: #"{"data":{"node":{"id":"c1","title":"one","shareUrl":"u","isPublic":true}}}"#)
    try await DB.openTesting(transport: transport, modelsJSON: modelsJSON)
    try DB.database.registerOperations(defsJson: #"[{"name":"ContainerNode","document":"query ContainerNode($id: ID!) { node(id: $id) { ... on Container { id title shareUrl isPublic } } }"}]"#)
    let held = Projected<Container>(try DB.account().entity(Container.self, "c1", fetch: .force))

    try await held.loaded()
    #expect(held.value?.title == "one")
  }
}
