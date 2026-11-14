import DBlayerTesting
import Foundation
import Testing
@testable import DBlayer

private func historyPage(_ range: ClosedRange<Int>, more: Bool) -> String {
  let rows = range.map { n in
    #"{"__typename":"Record","id":"m\#(n)","containerId":"c1","authorId":"u1","text":"message \#(n)","createdAt":"2026-10-27T00:00:00Z","sequenceNumber":\#(n)}"#
  }.joined(separator: ",")
  return #"{"data":{"container":{"records":{"nodes":[\#(rows)],"pageInfo":{"hasNextPage":\#(more),"endCursor":"\#(range.lowerBound)"}}}}}"#
}

extension BridgeProbeTests {
  @Test @MainActor
  func bridge_window_two_pages_parity_c9() async throws {
    let transport = MockTransport()
    let document = "query ContainerThread($containerId: ID!, $first: Int, $after: String) { container(id: $containerId) { records(first: $first, after: $after) { nodes { id containerId authorId text createdAt sequenceNumber } pageInfo { hasNextPage endCursor } } } }"
    transport.respondWith("ContainerThread") { request in
      let variables = (try? JSONSerialization.jsonObject(with: Data(request.variablesJson.utf8))) as? [String: Any]
      switch variables?["after"] as? String {
      case nil: return historyPage(41...65, more: true)
      case "41": return historyPage(16...40, more: false)
      default: return nil
      }
    }
    try await openSigned(config: Config(path: nil), modelsJson: modelsJSON, transport: transport)
    try await registerStandOperations(DB.database, defsJson: JSONValue.array([.object(["name": .string("ContainerThread"), "document": .string(document)])]).text)
    let held = Projected<Record>(try DB.account().stream(Record.thread("c1")))
    await eventually("first page") { held.rows.count == 25 && held.state?.settled == true }
    #expect(held.state?.hasMore == true)
    #expect(transport.calls.filter { $0.operationName == "ContainerThread" }.count == 1)

    _ = try await held.loadMore()
    await eventually("second page") { held.rows.first?.id == "m16" && held.state?.settled == true }
    #expect(held.state?.hasMore == false)
    #expect(transport.calls.filter { $0.operationName == "ContainerThread" }.count == 2)

    await held.stream.cancel()
    await DB.closeForCleanup()
  }
}
