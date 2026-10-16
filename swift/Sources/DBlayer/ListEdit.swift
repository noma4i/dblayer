import Foundation

/// One step from the rows a reader holds to the next rows. It carries its own row value, so the steps of several deliveries replay in order.
public enum ListEdit<Model: Sendable>: Sendable {
  case rewrite(Int, Model)
  case insert(Int, Model)
  case remove(Int, String)

  static func composing(_ older: [ListEdit]?, _ newer: [ListEdit]?, within rows: Int) -> [ListEdit]? {
    guard let older, let newer, older.count + newer.count <= rows else { return nil }

    return older + newer
  }
}

extension Array {
  public mutating func replay<Model: Sendable>(_ edits: [ListEdit<Model>]) where Element == Model {
    for edit in edits {
      switch edit {
      case .rewrite(let index, let row): self[index] = row
      case .insert(let index, let row): insert(row, at: index)
      case .remove(let index, _): remove(at: index)
      }
    }
  }
}
