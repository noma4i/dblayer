import Foundation

public struct ListUpdate<Model: Sendable>: Sendable {
  public let rows: [Model]
  public let edits: [ListEdit<Model>]?
  public let revision: UInt64
}

final class ListUpdates<Model: DBModel>: @unchecked Sendable {
  private struct Bundle {
    var rows: [Model]
    var edits: [ListEdit<Model>]?
    var revision: UInt64
    var generation: UInt64
  }

  private let lock = NSLock()
  private var bundle = Bundle(rows: [], edits: nil, revision: 0, generation: 0)
#if DEBUG
  private var publicationObserver: (@Sendable (UInt64) -> Void)?

  func observePublications(_ observer: @escaping @Sendable (UInt64) -> Void) { lock.withLock { publicationObserver = observer } }
#endif

  func publish(_ snapshot: Snapshot<Model>) {
    while true {
      let current = lock.withLock { bundle }
      let edits = ListEdit.composing(current.edits, snapshot.edits, within: snapshot.rows.count)
      let published = lock.withLock { () -> Bool in
        guard bundle.generation == current.generation else { return false }
        bundle = Bundle(rows: snapshot.rows, edits: edits, revision: snapshot.seq, generation: current.generation + 1)
        return true
      }
      if published {
#if DEBUG
        lock.withLock { publicationObserver }?(snapshot.seq)
#endif
        return
      }
    }
  }

  func take() -> ListUpdate<Model> {
    lock.withLock {
      let taken = bundle
      bundle.edits = []
      bundle.generation += 1
      return ListUpdate(rows: taken.rows, edits: taken.edits, revision: taken.revision)
    }
  }
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
