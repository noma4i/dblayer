import Foundation
import Observation
import os
import SwiftUI

private let decodeLog = Logger(subsystem: "DBlayer", category: "decode")

final class ChangeListener: QueryListener, @unchecked Sendable {
  let body: @Sendable (LiveChange) -> Void
  init(_ body: @escaping @Sendable (LiveChange) -> Void) { self.body = body }
  func changed(change: LiveChange) { body(change) }
}

extension LiveChange {
  func decodeRows<Model: DBModel>() -> [Model] {
    let decoder = JSONDecoder()
    return rows.compactMap { row in
      do {
        return try decoder.decode(Model.self, from: Data(row.utf8))
      } catch {
        decodeLog.error("Row of \(Model.modelName, privacy: .public) failed to decode: \(String(describing: error), privacy: .public)")
        return nil
      }
    }
  }
}

public struct LiveSource<Value: Sendable>: Sendable {
  public let initial: Value
  let subscribe: @Sendable (@escaping @Sendable (Value) -> Void) throws -> @Sendable () -> Void

  init(initial: Value, subscribe: @escaping @Sendable (@escaping @Sendable (Value) -> Void) throws -> @Sendable () -> Void) {
    self.initial = initial; self.subscribe = subscribe
  }

  init(initial: Value, attach: @escaping @Sendable (QueryListener) throws -> LiveSubscription, map: @escaping @Sendable (LiveChange) -> Value) {
    self.init(initial: initial) { emit in
      let subscription = try attach(ChangeListener { emit(map($0)) })
      return { subscription.cancel() }
    }
  }

  public var stream: AsyncStream<Value> {
    AsyncStream { continuation in
      let cancel = try? subscribe { continuation.yield($0) }
      continuation.onTermination = { _ in cancel?() }
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

/// A live query for view models: the rows, the load state of the relation, and the calls that move them.
@MainActor
@Observable
public final class LiveList<Model: DBModel> {
  public private(set) var rows: [Model] = []
  public private(set) var state = LoadState()
  @ObservationIgnored private let query: Query<Model>
  @ObservationIgnored private var tasks: [Task<Void, Never>] = []

  public init(_ query: Query<Model>) {
    self.query = query
    tasks = [
      Task { [weak self] in
        for await next in query.live.stream { self?.rows = next }
      },
      Task { [weak self] in
        for await next in query.state.stream { self?.state = next }
      }
    ]
  }

  public func refresh() async throws { try await query.refresh() }

  @discardableResult
  public func loadMore() async throws -> Bool { try await query.loadMore() }

  public func stop() {
    tasks.forEach { $0.cancel() }
    tasks = []
  }

  isolated deinit { stop() }
}

/// Live rows of one model looked up by id, for the records a list refers to (authors, last messages).
@MainActor
@Observable
public final class LiveRows<Model: DBModel> {
  public private(set) var byId: [String: Model] = [:]
  @ObservationIgnored private var ids: [String] = []
  @ObservationIgnored private var task: Task<Void, Never>?

  public init() {}

  public func track(_ nextIds: [String]) {
    let unique = Array(Set(nextIds)).sorted()
    guard unique != ids else { return }

    ids = unique
    task?.cancel()
    guard !unique.isEmpty else {
      byId = [:]
      return
    }

    task = Task { [weak self] in
      for await rows in Model.ids(unique).live.stream {
        self?.byId = Dictionary(rows.map { ($0.id, $0) }, uniquingKeysWith: { first, _ in first })
      }
    }
  }

  public func stop() {
    task?.cancel()
    task = nil
  }

  isolated deinit { stop() }
}
