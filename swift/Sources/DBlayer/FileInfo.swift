import Foundation

public struct FileInfo: Codable, Sendable, Hashable {
  public let id: String
  public let size: UInt64
  public let name: String?
  public let mime: String?
  public let sha256: String?

  init(json: JSONValue) throws {
    guard case let .object(members) = json, case let .string(id)? = members["id"], case let .integer(size)? = members["size"], size >= 0 else {
      throw DecodingError.dataCorrupted(DecodingError.Context(codingPath: [], debugDescription: "File description needs id and size"))
    }
    func text(_ key: String) -> String? { if case let .string(value)? = members[key] { return value } else { return nil } }
    self.init(id: id, size: UInt64(size), name: text("name"), mime: text("mime"), sha256: text("sha256"))
  }

  init(id: String, size: UInt64, name: String?, mime: String?, sha256: String?) {
    self.id = id
    self.size = size
    self.name = name
    self.mime = mime
    self.sha256 = sha256
  }

  static func parse(_ text: String) throws -> FileInfo { try FileInfo(json: try JSONValue.read(text)) }
}

public enum NoFiles: String, Sendable {
  case none
}
