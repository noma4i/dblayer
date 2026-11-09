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
    let texts = try (try value.objectValue).mapValues { try $0.text }
    #expect(texts == ["big": "9007199254740993", "negative": "-9007199254740993", "max": "9223372036854775807", "whole": "3", "real": "2.5", "list": "[9007199254740993,1.5]"])
    #expect((try value.objectValue)["big"]?.intValue == 9007199254740993)
    #expect(try Cell.int(value: 9007199254740993).asJSON.text == "9007199254740993")
    #expect(try JSONValue.encode(9007199254740993 as Int64).text == "9007199254740993")
  }

  @Test func aMistypedCellThrowsInsteadOfConverting() throws {
    expectDbErrorNow("Invalid") { try CellReader([.int(value: 7)]).req(0, \.asId) }
    expectDbErrorNow("Invalid") { try CellReader([.int(value: 2)]).req(0, \.asDouble) }
    expectDbErrorNow("Invalid") { try CellReader([.keyed(entries: [CellEntry(key: "k", value: .null), CellEntry(key: "k", value: .null)])]).json(0) }
    expectDbErrorNow("Invalid") { try CellReader([.seq(items: [.text(value: "f1"), .int(value: -1), .null, .null, .null])]).file(0) }
    #expect(throws: DecodingError.self) { _ = try JSONValue.integer(1).objectValue }
    #expect(JSONValue.number(3.0).intValue == nil)
  }

  @Test func aCorrectlyTypedRowDecodes() throws {
    let r = CellReader([.text(value: "7"), .real(value: 2), .seq(items: [.text(value: "f1"), .int(value: 10), .null, .text(value: "image/png"), .null])])
    #expect(try r.req(0, \.asId) == "7")
    #expect(try r.req(1, \.asDouble) == 2)
    #expect(try r.file(2) == FileInfo(id: "f1", size: 10, name: nil, mime: "image/png", sha256: nil))
  }
}
