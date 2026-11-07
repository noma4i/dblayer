import Foundation

/// One step from the rows a reader holds to the next rows. It carries its own row value. A `rewrite` that settles the swap of a temporary id carries that `temporary` id.
public enum ListEdit<Model: Sendable>: Sendable {
  case rewrite(Int, Model, temporary: String?)
  case insert(Int, Model)
  case remove(Int, String)
  case move(Int, Int)
}

extension Array {
  public mutating func replay(_ edits: [ListEdit<Element>]) where Element: Sendable {
    for edit in edits {
      switch edit {
      case .rewrite(let index, let row, _): self[index] = row
      case .insert(let index, let row): insert(row, at: index)
      case .remove(let index, _): remove(at: index)
      case .move(let from, let to): insert(remove(at: from), at: to)
      }
    }
  }
}
