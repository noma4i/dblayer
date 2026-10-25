import Foundation
import Testing
@testable import DBlayer

struct JSONValueNumberTests {
  private struct Refused: Encodable {
    struct Failure: Error {}
    func encode(to encoder: any Encoder) throws { throw Failure() }
  }

  @Test func invalidJSONCannotBecomeNull() throws {
    let invalid: [() throws -> Void] = [
      { _ = try JSONValue.encode(Double.infinity) },
      { _ = try JSONValue.encode(Double.nan) },
      { _ = try JSONValue.object(["value": .number(-Double.infinity)]).text },
    ]
    for encode in invalid {
      #expect(throws: (any Error).self) { try encode() }
    }
    #expect(throws: Refused.Failure.self) { _ = try JSONValue.encode(Refused()) }
    #expect(try JSONValue.encode(Optional<String>.none) == .null)
  }

  @Test func integersAboveTheDoubleRangeKeepEveryDigit() throws {
    let root = URL(fileURLWithPath: #filePath).deletingLastPathComponent().deletingLastPathComponent().deletingLastPathComponent().deletingLastPathComponent()
    let value = try JSONValue.decode(Data(contentsOf: root.appendingPathComponent("fixtures/numbers.json")))
    let texts = try value.objectValue.mapValues { try $0.text }
    #expect(texts == ["big": "9007199254740993", "negative": "-9007199254740993", "max": "9223372036854775807", "whole": "3", "real": "2.5", "list": "[9007199254740993,1.5]"])
    #expect(value.objectValue["big"]?.intValue == 9007199254740993)
    #expect(try Cell.int(value: 9007199254740993).asJSON.text == "9007199254740993")
    #expect(try JSONValue.encode(9007199254740993 as Int64).text == "9007199254740993")
  }
}
