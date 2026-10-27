import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

private enum RecordEvents: GraphQLOperation {
  typealias Variables = [String: String]
  static let operationName = "RecordEvents"
  static let document = "subscription RecordEvents { recordEvents { entries { topic action recordId } } }"
}

private struct StaleContainer: DBModel {
  static let modelName = Container.modelName
  static let wireLayout = "stale"
  let row: Container
  var id: String { row.id }
  var sync: SyncInfo { row.sync }
  var uploads: [String: UploadInfo] { row.uploads }
  init(wire: WireRow) throws { row = try Container(wire: wire) }
}

extension BridgeProbeTests {
  @MainActor
  @Test(.timeLimit(.minutes(1)))
  func openTestingHoldsSubscriptionsUntilRegistrationCompletes() async throws {
    DB.close()
    defer { DB.close() }
    let transport = MockTransport()
    try await DB.openTesting(transport: transport, modelsJSON: modelsJSON, operations: [RecordEvents.self], listening: false)
    try await DB.resume()
    #expect(transport.calls.isEmpty)
    try DB.setListening(true)
    await eventually("registration complete starts the subscription") { transport.calls.count == 1 }
    try DB.setListening(true)
    try await DB.resume()
    #expect(transport.calls.map(\.operationName) == ["RecordEvents"])
  }

  @Test(.timeLimit(.minutes(1)))
  func declaredChangeEventsReachTheTypedStreamAndTheStreamEndsOnCancel() async throws {
    let transport = MockTransport()
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: transport)
    try DB.database.registerOperations(defsJson: #"[{"name":"RecordEvents","document":"subscription RecordEvents { recordEvents { entries { topic action recordId } } }","rootPath":"recordEvents"}]"#)
    let events = try await Container.events()
    let cursor = Cursor(events)
    try transport.emit("RecordEvents", json: #"{"data":{"recordEvents":{"entries":[{"topic":"CONTAINER","action":"DELETED","recordId":"c1"}]}}}"#)
    guard case .event(let seen)? = await cursor.next("the removal event") else {
      Issue.record("the removal event did not arrive")
      return
    }
    #expect(seen.area == "CONTAINER" && seen.kind == "DELETED" && seen.id == "c1")
    #expect(seen.before == nil && seen.after == nil)
    #expect(transport.calls.map(\.operationName) == ["RecordEvents"])

    events.cancel()
    let ended = await cursor.next("the end of the cancelled stream")
    #expect(ended == nil)
  }

  @Test(.timeLimit(.minutes(1)))
  func aModelWithoutItsOwnChangesSourceReceivesTheEventsOfTheSourceAnotherModelDeclares() async throws {
    let transport = MockTransport()
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: transport)
    try DB.database.registerOperations(defsJson: #"[{"name":"RecordEvents","document":"subscription RecordEvents { recordEvents { entries { topic action recordId } } }","rootPath":"recordEvents"}]"#)
    let events = try await Record.events()
    let cursor = Cursor(events)
    try transport.emit("RecordEvents", json: #"{"data":{"recordEvents":{"entries":[{"topic":"RECORD","action":"CREATED","recordId":"r1"}]}}}"#)
    guard case .event(let seen)? = await cursor.next("the record event") else {
      Issue.record("the record event did not arrive")
      return
    }
    #expect(seen.area == "RECORD" && seen.kind == "CREATED" && seen.id == "r1")
    #expect(transport.calls.map(\.operationName) == ["RecordEvents"])
    events.cancel()
  }

  @Test(.timeLimit(.minutes(1)))
  func aStaleModelEndsTheEventsWithTheDecodeFailure() async throws {
    let transport = MockTransport()
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: transport)
    try DB.database.registerOperations(defsJson: #"[{"name":"RecordEvents","document":"subscription RecordEvents { recordEvents { entries { topic action recordId } } }","rootPath":"recordEvents"},{"name":"ContainerNode","document":"query ContainerNode($id: ID!) { node(id: $id) { id title } }","rootPath":"node"}]"#)
    let events = try await StaleContainer.openEvents()
    transport.enqueue("ContainerNode", reply: #"{"data":{"node":{"id":"c1","title":"received"}}}"#)
    try transport.emit("RecordEvents", json: #"{"data":{"recordEvents":{"entries":[{"topic":"CONTAINER","action":"UPDATED","recordId":"c1"}]}}}"#)
    let cursor = Cursor(events)
    guard case .failed(let error)? = await cursor.next("the decode failure") else {
      Issue.record("the decode failure did not arrive")
      return
    }
    #expect(error is DbError)
    #expect(await cursor.next("the end after the failure") == nil)
  }

  @Test(.timeLimit(.minutes(1)))
  func aDeniedListeningOpensNoSubscriptionFromAStoredCredentialUntilTheAppAllowsIt() async throws {
    let transport = MockTransport()
    let operations = #"[{"name":"RecordEvents","document":"subscription RecordEvents { recordEvents { entries { topic action recordId } } }","rootPath":"recordEvents"}]"#
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: transport, operations: operations, listening: false)
    try await DB.resume()
    #expect(transport.calls.isEmpty)
    try DB.setListening(true)
    await eventually("the subscriptions start") { transport.calls.count == 1 }
    try DB.setListening(true)
    try await DB.resume()
    #expect(transport.calls.map(\.operationName) == ["RecordEvents"])
    let events = try await Container.events()
    let cursor = Cursor(events)
    try DB.setListening(false)
    try transport.emit("RecordEvents", json: #"{"data":{"recordEvents":{"entries":[{"topic":"CONTAINER","action":"DELETED","recordId":"c1"}]}}}"#)
    guard case .event(let seen)? = await cursor.next("the event after the listening is denied") else {
      Issue.record("the collector stopped with the listening")
      return
    }
    #expect(seen.id == "c1")
    #expect(transport.calls.map(\.operationName) == ["RecordEvents"])
    events.cancel()
  }
}
