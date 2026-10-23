import Foundation
import Testing
@testable import DBlayer

struct JSONValueNumberTests {
  @Test func integersAboveTheDoubleRangeKeepEveryDigit() throws {
    let root = URL(fileURLWithPath: #filePath).deletingLastPathComponent().deletingLastPathComponent().deletingLastPathComponent().deletingLastPathComponent()
    let value = try JSONValue.decode(Data(contentsOf: root.appendingPathComponent("fixtures/numbers.json")))
    let texts = value.objectValue.mapValues(\.text)
    #expect(texts == ["big": "9007199254740993", "negative": "-9007199254740993", "max": "9223372036854775807", "whole": "3", "real": "2.5", "list": "[9007199254740993,1.5]"])
    #expect(value.objectValue["big"]?.intValue == 9007199254740993)
    #expect(Cell.int(value: 9007199254740993).asJSON.text == "9007199254740993")
    #expect(JSONValue.encode(9007199254740993 as Int64).text == "9007199254740993")
  }
}
