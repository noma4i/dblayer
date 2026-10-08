
import Foundation

public enum CanonicalJSON {
  public static func encode(_ value: JSONValue) -> Data {
    var output = ""
    write(value, into: &output)
    return Data(output.utf8)
  }

  private static func write(_ value: JSONValue, into output: inout String) {
    switch value {
    case let .object(fields):
      output += "{"
      let sortedEntries = fields.sorted { lhs, rhs in
        Array(lhs.key.utf8).lexicographicallyPrecedes(Array(rhs.key.utf8))
      }
      for (index, entry) in sortedEntries.enumerated() {
        if index > 0 {
          output += ","
        }
        writeString(entry.key, into: &output)
        output += ":"
        write(entry.value, into: &output)
      }
      output += "}"
    case let .array(elements):
      output += "["
      for (index, element) in elements.enumerated() {
        if index > 0 {
          output += ","
        }
        write(element, into: &output)
      }
      output += "]"
    case let .string(string):
      writeString(string, into: &output)
    case let .number(number):
      output += formatNumber(number)
    case let .bool(bool):
      output += bool ? "true" : "false"
    case .null:
      output += "null"
    }
  }

  private static func formatNumber(_ number: Double) -> String {
    guard number.isFinite else {
      return "null"
    }
    if number.rounded(.down) == number, abs(number) < 9_007_199_254_740_992 {
      return String(Int64(number))
    }
    return String(number)
  }

  private static let namedEscapes: [UInt32: String] = [
    8: "\\b",
    9: "\\t",
    10: "\\n",
    12: "\\f",
    13: "\\r",
    Unicode.Scalar("\"").value: "\\\"",
    Unicode.Scalar("\\").value: "\\\\",
    Unicode.Scalar("<").value: "\\u003c",
    Unicode.Scalar(">").value: "\\u003e",
    Unicode.Scalar("&").value: "\\u0026",
    0x2028: "\\u2028",
    0x2029: "\\u2029"
  ]

  private static func writeString(_ string: String, into output: inout String) {
    output += "\""
    for scalar in string.unicodeScalars {
      if let escaped = escapedSequence(for: scalar) {
        output += escaped
      } else {
        output.unicodeScalars.append(scalar)
      }
    }
    output += "\""
  }

  private static func escapedSequence(for scalar: Unicode.Scalar) -> String? {
    if let named = namedEscapes[scalar.value] {
      return named
    }
    if scalar.value < 32 {
      return "\\u00" + String(format: "%02x", scalar.value)
    }
    return nil
  }
}
