import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

private final class EntityDecodeGate: @unchecked Sendable {
  let entered: AsyncStream<Void>
  let sink: AsyncStream<Void>.Continuation
  let allow = DispatchSemaphore(value: 0)
  init() { (entered, sink) = AsyncStream.makeStream() }
}

private struct DeferredEntity: DBModel {
  static let modelName = Moment.modelName
  static let wireLayout = Moment.wireLayout
  nonisolated(unsafe) static var gate: EntityDecodeGate?
  let moment: Moment
  var id: String { moment.id }
  var sync: SyncInfo { moment.sync }
  var uploads: [String: UploadInfo] { moment.uploads }
  init(wire: WireRow) throws {
    if let gate = Self.gate {
      gate.sink.yield(())
      guard gate.allow.wait(timeout: .now() + 5) == .success else { throw DbError.Invalid(detail: "entity decode gate timed out") }
    }
    moment = try Moment(wire: wire)
  }
}

extension BridgeProbeTests {
  @Test @MainActor
  func entityMainNativeCalls() async throws {
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: nil)
    let row = try await Moment.create(text: "warm")
    var held: Entity<Moment>? = Moment.entity(row.id)
    try await held?.waitForInitialLoad()
    weak var released = held
    held = nil
    await eventually("the warm entity releases") { released == nil }
    let probe = EntityCallProbe()
    let warm = Moment.entity(row.id)
    let cold = Moment.entity("absent")
    let live = Moment.entity(row.id)
    #expect(warm === live)
    #expect(warm.snapshot.phase == .opening && warm.value == nil)
    #expect(probe.calls() == 0)
    try await warm.waitForInitialLoad()
    try await cold.waitForInitialLoad()
    #expect(warm.value?.text == "warm" && cold.phase == .absent)
    #expect(probe.calls() == 0)
  }

  @Test @MainActor
  func entityInitialLocal() async throws {
    let transport = MockTransport()
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: transport)
    let row = try await Moment.create(text: "local")
    let gate = EntityDecodeGate()
    DeferredEntity.gate = gate
    defer { gate.allow.signal(); DeferredEntity.gate = nil }
    let present = DeferredEntity.entity(row.id)
    let absent = Moment.entity("missing")
    #expect(!present.snapshot.settled && present.phase == .opening)
    var returned = false
    let wait = Task { @MainActor in try await present.waitForInitialLoad(); returned = true }
    #expect(await Cursor(gate.entered).next("the core row reaches the decoder") != nil)
    await Task.yield()
    #expect(!returned)
    gate.allow.signal()
    try await wait.value
    try await absent.waitForInitialLoad()
    #expect(present.snapshot.value?.moment.text == "local" && present.phase == .present)
    #expect(absent.snapshot.value == nil && absent.phase == .absent)
    #expect(transport.calls.isEmpty)
  }

  @Test @MainActor
  func entityIfMissingPlatforms() async throws {
    let transport = MockTransport()
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: transport)
    try DB.database.registerOperations(defsJson: #"[{"name":"ContainerShelfQuery","document":"query ContainerShelfQuery($shelf: String!) { containers(shelf: $shelf) { id title shelves } }"},{"name":"ContainerNode","document":"query ContainerNode($id: ID!) { node(id: $id) { ... on Container { id title shelves } } }"}]"#)
    transport.enqueue("ContainerShelfQuery", reply: #"{"data":{"containers":[{"id":"from-list","title":"local","shelves":["PRIMARY"]}]}}"#)
    try await Container.shelf(shelf: .primary).load()
    let present = Container.entity("from-list", fetch: .ifMissing)
    try await present.waitForInitialLoad()
    await eventually("the presence read settles") { present.read == .loaded }
    #expect(present.value?.title == "local")
    #expect(transport.calls.filter { $0.operationName == "ContainerNode" }.isEmpty)
    transport.enqueue("ContainerNode", reply: #"{"data":{"node":{"__typename":"Container","id":"missing","title":"node","shelves":["PRIMARY"]}}}"#)
    let absent = Container.entity("missing", fetch: .ifMissing)
    try await absent.waitForInitialLoad()
    #expect(absent.value?.title == "node")
    #expect(transport.calls.filter { $0.operationName == "ContainerNode" }.count == 1)
  }
}
