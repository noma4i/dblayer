@testable import DBlayer
import Foundation
import Testing

private struct ProbeVariables: Encodable {
  let id: String
  let name: GraphQLNullable<String>

  enum CodingKeys: String, CodingKey {
    case id
    case name
  }

  func encode(to encoder: any Encoder) throws {
    var container = encoder.container(keyedBy: CodingKeys.self)
    try container.encode(id, forKey: .id)
    try container.encode(name, forKey: .name)
  }
}

@Suite(.serialized)
struct GraphQLNullableTests {
  @Test
  func omittedSkipsKey() throws {
    let encoder = JSONEncoder()
    encoder.outputFormatting = .sortedKeys
    let data = try encoder.encode(ProbeVariables(id: "1", name: .omitted))

    #expect(String(bytes: data, encoding: .utf8) == "{\"id\":\"1\"}")
  }

  @Test
  func nullWritesNull() throws {
    let encoder = JSONEncoder()
    encoder.outputFormatting = .sortedKeys
    let data = try encoder.encode(ProbeVariables(id: "1", name: .null))

    #expect(String(bytes: data, encoding: .utf8) == "{\"id\":\"1\",\"name\":null}")
  }

  @Test
  func valueWritesValue() throws {
    let encoder = JSONEncoder()
    encoder.outputFormatting = .sortedKeys
    let data = try encoder.encode(ProbeVariables(id: "1", name: .value("Ann")))

    #expect(String(bytes: data, encoding: .utf8) == "{\"id\":\"1\",\"name\":\"Ann\"}")
  }
}
