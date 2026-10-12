import Foundation
import os

private let decodeLog = Logger(subsystem: "DBlayer", category: "decode")

/// The rows of one live list, rebuilt from its deliveries. Only the rows that a delivery carries are decoded.
final class ListFeed<Model: DBModel>: @unchecked Sendable {
  private let lock = NSLock()
  private let decoder = JSONDecoder()
  private var rows: [Model] = []
  private var decoded = 0

  /// Rows decoded so far.
  var decodedRows: Int { lock.withLock { decoded } }

  /// The list after a delivery, and whether its ids or their order changed.
  func apply(_ change: LiveChange) -> (rows: [Model], reordered: Bool) {
    lock.withLock {
      let carried: [(row: Model, index: Int)] = change.rows.enumerated().compactMap { position, text in
        decoded += 1
        do {
          let row = try decoder.decode(Model.self, from: Data(text.utf8))
          return (row, change.full ? position : Int(change.rowIndexes[position]))
        } catch {
          decodeLog.error("Row of \(Model.modelName, privacy: .public) failed to decode: \(String(describing: error), privacy: .public)")
          return nil
        }
      }
      if change.full {
        rows = carried.map(\.row)
        return (rows, true)
      }
      let byId = Dictionary(carried.map { ($0.row.id, $0.row) }, uniquingKeysWith: { _, newer in newer })
      for pair in change.replaced {
        if let at = rows.firstIndex(where: { $0.id == pair.from }), let row = byId[pair.to] { rows[at] = row }
      }
      if !change.removed.isEmpty {
        let gone = Set(change.removed)
        rows.removeAll { gone.contains($0.id) }
      }
      var taken: [String: Model] = [:]
      for place in change.placed {
        if let at = rows.firstIndex(where: { $0.id == place.id }) { taken[place.id] = rows.remove(at: at) }
      }
      for place in change.placed {
        guard let row = taken[place.id] ?? byId[place.id] else { continue }
        rows.insert(row, at: min(Int(place.index), rows.count))
      }
      for (row, index) in carried where index < rows.count { rows[index] = row }
      return (rows, !change.removed.isEmpty || !change.placed.isEmpty || !change.replaced.isEmpty)
    }
  }
}

extension LiveChange {
  /// Real id of a row that replaced a temporary one, to the temporary id.
  var replacedIds: [String: String] {
    Dictionary(replaced.map { ($0.to, $0.from) }, uniquingKeysWith: { first, _ in first })
  }
}
