import Foundation
import DBlayerTesting
import Testing
@testable import DBlayer

extension BridgeProbeTests {
  @Test(.timeLimit(.minutes(1)))
  func keywordNamesPreserveStoredKeysAndNestedValues() async throws {
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: nil)
    let input = #"{"default":"nested","when":"now","class":"kind","container":"box","r":"reader"}"#
    let nested = try JSONDecoder().decode(KeywordValue.self, from: Data(input.utf8))
    let created = try await KeywordRecord.create(default: "row", class: "selected", value: nested).optimisticValue!
    let updated = try await created.update(default: "changed").optimisticValue!
    let rows = try await KeywordRecord.when("selected").live.current()
    let row = try #require(rows.first)
    #expect(rows.count == 1 && row.id == updated.id && row.default == "changed" && row.class == "selected")
    #expect(row.value == nested)
    #expect(row.value?.container == "box" && row.value?.r == "reader")
    #expect(row.value?.child.default == "d" && row.value?.child.when == "w" && row.value?.child.class == "c")
    let encoded = try JSONDecoder().decode(JSONValue.self, from: JSONEncoder().encode(nested))
    #expect(try encoded == JSONValue.read(#"{"default":"nested","when":"now","class":"kind","container":"box","r":"reader","child":{"default":"d","when":"w","class":"c"}}"#))
    let transport = MockTransport()
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 32), modelsJson: modelsJSON, transport: transport)
    try await DB.database.registerOperations(defsJson: #"[{"name":"KeywordCall","document":"query KeywordCall($class: String!) { keywordCall(class: $class) { default when container r } }"}]"#)
    transport.enqueue("KeywordCall", reply: #"{"data":{"keywordCall":{"default":"answer","when":"now","container":"box","r":"reader"}}}"#)
    let answer = try await Operations.when(class: "request")
    #expect(answer.default == "answer" && answer.when == "now" && answer.container == "box" && answer.r == "reader")
    let sent = try #require(transport.calls.first).variablesJson
    #expect(try JSONValue.read(sent) == JSONValue.read(#"{"class":"request"}"#))
  }
}
