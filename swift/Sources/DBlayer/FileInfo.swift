import Foundation

public struct FileInfo: Codable, Sendable, Hashable {
  public let id: String
  public let size: UInt64
  public let name: String?
  public let mime: String?
  public let sha256: String?

  init(cells: [Cell]) throws {
    let r = CellReader(cells)
    guard let size = UInt64(exactly: try r.req(1, \.asInt)) else { throw DbError.Invalid(detail: "the file size is negative") }
    self.init(id: try r.req(0, \.asText), size: size, name: try r.opt(2, \.asText), mime: try r.opt(3, \.asText), sha256: try r.opt(4, \.asText))
  }

  init(wire: WireFile) { self.init(id: wire.id, size: wire.size, name: wire.name, mime: wire.mime, sha256: wire.sha256) }

  init(id: String, size: UInt64, name: String?, mime: String?, sha256: String?) {
    self.id = id
    self.size = size
    self.name = name
    self.mime = mime
    self.sha256 = sha256
  }
}

public enum NoFiles: String, Sendable {
  case none
}
