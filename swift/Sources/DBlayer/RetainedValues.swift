import Foundation

@MainActor
final class RetainedValues {
  private let pool = RetentionPool()
  private var values: [UInt64: Any] = [:]

  func keep<Model: DBModel>(_ id: String, value: Retained<Model>, now: UInt64) {
    let change = pool.keep(model: Model.modelName, id: id, bytes: value.bytes, now: now)
    apply(change)
    if let token = change.selected { values[token] = value }
  }

  func take<Model: DBModel>(_ id: String, as _: Model.Type, now: UInt64) -> Retained<Model>? {
    let change = pool.take(model: Model.modelName, id: id, now: now)
    apply(change)
    guard let token = change.selected else { return nil }
    return values.removeValue(forKey: token) as? Retained<Model>
  }

  func count(_ model: String, now: UInt64) -> Int {
    let change = pool.count(model: model, now: now)
    apply(change)
    return Int(change.count)
  }

  func clear(_ model: String) { apply(pool.clear(model: model)) }

  private func apply(_ change: RetentionChange) {
    for token in change.evicted { values[token] = nil }
  }
}
