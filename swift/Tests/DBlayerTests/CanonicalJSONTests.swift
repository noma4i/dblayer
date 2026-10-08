import DBlayer
import CryptoKit
import Foundation
import Testing
@testable import DBlayer

@Suite(.serialized)
struct CanonicalJSONTests {
  @Test
  func sortsKeysByUtf8BytesKeepsArrayOrderAndNests() throws {
    #expect(
      try encodedString(.object(["b": .number(1), "A": .number(2), "a": .number(3)]))
        == "{\"A\":2,\"a\":3,\"b\":1}"
    )

    #expect(
      try encodedString(
        .object([
          "list": .array([
            .object(["z": .bool(true), "y": .null]),
            .string("second")
          ])
        ])
      ) == "{\"list\":[{\"y\":null,\"z\":true},\"second\"]}"
    )
  }

  @Test
  func escapesActiveSupportCharacters() throws {
    let expected = "\"Tom \\u0026 Jerry \\u003cb\\u003e\\u2028\\n\""
    let result = try encodedString(.string("Tom & Jerry <b>\u{2028}\n"))
    #expect(result == expected)
    #expect(result.count == 40)
  }

  @Test
  func printsIntegersWithoutFraction() throws {
    #expect(try encodedString(.number(1)) == "1")
    #expect(try encodedString(.number(1.5)) == "1.5")
    #expect(try encodedString(.number(0)) == "0")
    #expect(try encodedString(.number(Double.infinity)) == "null")
  }

  @Test
  func emptyObjectMatchesServerHashVector() {
    let data = CanonicalJSON.encode(.object([:]))
    #expect(String(data: data, encoding: .utf8) == "{}")
    let digest = SHA256.hash(data: data)
    let hex = digest.map { String(format: "%02x", $0) }.joined()
    #expect(hex == "44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a")
  }
}

private extension CanonicalJSONTests {
  func encodedString(_ value: JSONValue) throws -> String {
    try #require(String(data: CanonicalJSON.encode(value), encoding: .utf8))
  }
}
