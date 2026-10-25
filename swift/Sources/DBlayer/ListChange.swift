import Foundation

public struct IndexedRow<Model: DBModel>: Sendable {
  public let index: Int
  public let row: Model
}

public enum ListChangeKind: Sendable { case delta, replaced }

public struct ListChange<Model: DBModel>: Sendable {
  public let kind: ListChangeKind
  public let inserted: [IndexedRow<Model>]
  public let updated: [IndexedRow<Model>]
  public let removed: [String]
  public let moved: [Placement]
  public let replaced: [Replacement]
  public let revision: Int64
  public let size: Int
  public let failure: String?

  init(_ change: LiveChange) {
    kind = change.full ? .replaced : .delta
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

public struct QueryChanges<Model: DBModel>: Sendable {
  let query: Query<Model>

  public func stream() async throws -> LiveStream<ListChange<Model>> {
    guard !Task.isCancelled else { return .ended }
    let queue = try await Task.detached { try DB.database.changes(queryJson: query.json) }.value
    guard !Task.isCancelled else { queue.cancel(); return .ended }
    return LiveStream(receive: { await queue.next().map { ListChange($0) } }, cancel: { queue.cancel() })
  }
}

extension Query {
  public var changes: QueryChanges<Model> { QueryChanges(query: self) }
}
