import Foundation

enum RuntimeLane {
  private static let queue = DispatchQueue(label: "com.noma4i.dblayer.runtime-lane", qos: .userInitiated)
  static func run(_ work: @escaping @Sendable () -> Void) { queue.async(execute: work) }
}

/// One Rust future. Cancel and free reserve their operation under the lock and run the FFI call after the lock is released. A free requested while a cancel is queued or running waits for that cancel, so the handle is never cancelled after it is freed and is freed exactly once.
final class RustFutureHandle: @unchecked Sendable {
  let value: UInt64
  private let lock = NSLock()
  private var cancelRequested = false
  private var cancelRunning = false
  private var freeRequested = false
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
    guard let cancelCall else { return }
    let reserved = lock.withLock { () -> Bool in
      guard !freeRequested, !cancelRequested else { return false }
      cancelRequested = true
      cancelRunning = true
      return true
    }
    guard reserved else { return }
    lane { [self] in
      cancelCall(value)
      let freeNow = lock.withLock { () -> Bool in
        cancelRunning = false
        return freeRequested
      }
      if freeNow { freeCall(value) }
    }
  }

  func free() {
    let freeNow = lock.withLock { () -> Bool in
      guard !freeRequested else { return false }
      freeRequested = true
      return !cancelRunning
    }
    if freeNow { freeCall(value) }
  }
}
