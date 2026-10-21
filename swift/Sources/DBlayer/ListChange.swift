import Foundation

public struct IndexedRow<Model: DBModel>: Sendable {
  public let index: Int
  public let row: Model
}

public struct ListChange<Model: DBModel>: Sendable {
  public let inserted: [IndexedRow<Model>]
  public let updated: [IndexedRow<Model>]
  public let removed: [String]
  public let moved: [Placement]
  public let replaced: [Replacement]
  public let revision: Int64
  public let size: Int
  public let failure: String?

  init(_ change: LiveChange) {
    var failure = change.error.map(String.init(describing:))
    let rows: [IndexedRow<Model>?] = change.rows.enumerated().map { position, wire in
      do { return IndexedRow(index: change.full ? position : Int(change.rowIndexes[position]), row: try Model.decode(wire)) }
      catch { failure = String(describing: error); return nil }
    }
    inserted = change.inserted.compactMap { rows[Int($0)] }
    updated = change.updated.compactMap { rows[Int($0)] }
    removed = change.removed
    moved = change.moved
    replaced = change.renamed
    revision = change.revision
    size = Int(change.size)
    self.failure = failure
  }
}

extension Query {
  public var changes: LiveSource<ListChange<Model>> {
    LiveSource(initial: ListChange(LiveChange(revision: 0, full: true, size: 0, rows: [], rowIndexes: [], removed: [], placed: [], replaced: [], renamed: [], count: nil, error: nil)), buffered: true) { emit in
      let subscription = try DB.database.liveQuery(queryJson: json, listener: ChangeListener { emit(ListChange($0)) })
      return { subscription.cancel() }
    }
  }
}
