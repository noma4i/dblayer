import Foundation
import SwiftUI

final class ChangeListener: QueryListener, @unchecked Sendable {
  let body: @Sendable (LiveChange) -> Void
  init(_ body: @escaping @Sendable (LiveChange) -> Void) { self.body = body }
  func changed(change: LiveChange) { body(change) }
}

extension LiveChange {
  func decodeRows<Model: DBModel>() -> [Model] {
    let decoder = JSONDecoder()
    return rows.compactMap { try? decoder.decode(Model.self, from: Data($0.utf8)) }
  }
}

public struct LiveSource<Value: Sendable>: Sendable {
  public let initial: Value
  let attach: @Sendable (QueryListener) throws -> LiveSubscription
  let map: @Sendable (LiveChange) -> Value

  init(initial: Value, attach: @escaping @Sendable (QueryListener) throws -> LiveSubscription, map: @escaping @Sendable (LiveChange) -> Value) {
    self.initial = initial; self.attach = attach; self.map = map
  }

  public var stream: AsyncStream<Value> {
    AsyncStream { continuation in
      let map = map
      let subscription = try? attach(ChangeListener { continuation.yield(map($0)) })
      continuation.onTermination = { _ in subscription?.cancel() }
    }
  }

  public func current() async -> Value {
    for await value in stream { return value }
    return initial
  }
}

@MainActor
final class LiveBox<Value: Sendable>: ObservableObject {
  @Published var value: Value
  private var task: Task<Void, Never>?

  init(_ source: LiveSource<Value>) {
    value = source.initial
    task = Task { [weak self] in
      for await next in source.stream { self?.value = next }
    }
  }

  deinit { task?.cancel() }
}

@MainActor
@propertyWrapper
public struct Live<Value: Sendable>: DynamicProperty {
  @StateObject private var box: LiveBox<Value>
  public init(_ source: LiveSource<Value>) { _box = StateObject(wrappedValue: LiveBox(source)) }
  public var wrappedValue: Value { box.value }
}

extension Live {
  public init<Model: DBModel>(_ query: Query<Model>) where Value == [Model] { self.init(query.live) }
}

public struct LiveView<Value: Sendable, Content: View>: View {
  @Live private var value: Value
  private let content: (Value) -> Content
  public init(_ source: LiveSource<Value>, @ViewBuilder content: @escaping (Value) -> Content) {
    _value = Live(source); self.content = content
  }
  public init<Model: DBModel>(_ query: Query<Model>, @ViewBuilder content: @escaping ([Model]) -> Content) where Value == [Model] {
    self.init(query.live, content: content)
  }
  public var body: some View { content(value) }
}
