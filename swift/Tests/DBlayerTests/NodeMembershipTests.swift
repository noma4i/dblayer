import DBlayerTesting
import Testing
@testable import DBlayer

extension BridgeProbeTests {
  @Test(.timeLimit(.minutes(1)))
  func anEventNodeAddsAnExistingContainerToItsTypedShelf() async throws {
    let transport = MockTransport()
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: transport)
    try DB.database.registerOperations(defsJson: #"[{"name":"ContainerShelfQuery","document":"query ContainerShelfQuery($shelf: String!) { containers(shelf: $shelf) { id title shelves } }"},{"name":"ContainerNode","document":"query ContainerNode($id: ID!) { node(id: $id) { ... on Container { id title shelves } } }"},{"name":"RecordEvents","document":"subscription RecordEvents { recordEvents { entries { topic action recordId } } }"}]"#)
    transport.enqueue("ContainerShelfQuery", reply: #"{"data":{"containers":[]}}"#)
    let shelf = try Container.shelf(shelf: .primary)
    try await shelf.load()
    let rows = Cursor(try await shelf.live.stream())
    #expect(await rows.next()?.isEmpty == true)
    transport.enqueue("ContainerNode", reply: #"{"data":{"node":{"__typename":"Container","id":"c1","title":"same","shelves":["PRIMARY"]}}}"#)
    _ = try await Container.node("c1", force: true)
    #expect(try await shelf.live.current().isEmpty)
    let events = try await Container.events()
    defer { events.cancel() }
    transport.enqueue("ContainerNode", reply: #"{"data":{"node":{"__typename":"Container","id":"c1","title":"same","shelves":["PRIMARY"]}}}"#)
    try transport.emit("RecordEvents", json: #"{"data":{"recordEvents":{"entries":[{"topic":"CONTAINER","action":"UPDATED","recordId":"c1"}]}}}"#)
    #expect(await rows.first("container joins shelf") { !$0.isEmpty }?.map(\.id) == ["c1"])
    #expect(transport.calls.filter { $0.operationName == "ContainerShelfQuery" }.count == 1)
  }
}
