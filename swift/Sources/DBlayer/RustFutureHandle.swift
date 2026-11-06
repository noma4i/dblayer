import Foundation

enum RuntimeLane {
  private static let queue = DispatchQueue(label: "com.noma4i.dblayer.runtime-lane", qos: .userInitiated)
  static func run(_ work: @escaping @Sendable () -> Void) { queue.async(execute: work) }
}

final class RustFutureHandle: @unchecked Sendable {
  let value: UInt64
  private var cancelled = false
  private var freed = false
  private let cancelCall: ((UInt64) -> Void)?
  private let freeCall: (UInt64) -> Void
  private let lane: (@escaping @Sendable () -> Void) -> Void

  init(_ value: UInt64, cancel: ((UInt64) -> Void)?, free: @escaping (UInt64) -> Void, lane: @escaping (@escaping @Sendable () -> Void) -> Void = RuntimeLane.run) {
    self.value = value
    cancelCall = cancel
    freeCall = free
    self.lane = lane
  }

  func cancel() {
    guard cancelCall != nil else { return }
    lane { [self] in
      guard !freed, !cancelled, let cancelCall else { return }
      cancelled = true
      cancelCall(value)
    }
  }

  func free() {
    lane { [self] in
      guard !freed else { return }
      freed = true
      freeCall(value)
    }
  }
}
