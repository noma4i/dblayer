import Foundation

/// The rows of a list with the edits since the last take, from one revision. `edits` is nil when the rows cannot be reached by edits: replace the mirror with `rows`. Empty `edits` mean no change.
public struct ListUpdate<Model: Sendable>: Sendable {
  public let rows: [Model]
  public let edits: [ListEdit<Model>]?
  public let revision: UInt64
}

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
  public mutating func replay(_ edits: [ListEdit<Element>]) where Element: Sendable {
    for edit in edits {
      switch edit {
      case .rewrite(let index, let row): self[index] = row
      case .insert(let index, let row): insert(row, at: index)
      case .remove(let index, _): remove(at: index)
      }
    }
  }
}
