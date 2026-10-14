import Foundation

public struct IDText: Decodable {
  public let value: String

  public init(from decoder: Decoder) throws {
    let container = try decoder.singleValueContainer()
    if let text = try? container.decode(String.self) {
      value = text
      return
    }
    value = String(try container.decode(Int64.self))
  }
}
