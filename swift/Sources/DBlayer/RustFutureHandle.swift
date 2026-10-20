import Foundation

final class RustFutureHandle: @unchecked Sendable {
  let value: UInt64
  private let lock = NSLock()
  private var freed = false
  private let cancelCall: ((UInt64) -> Void)?
  private let freeCall: (UInt64) -> Void

  init(_ value: UInt64, cancel: ((UInt64) -> Void)?, free: @escaping (UInt64) -> Void) {
    self.value = value
    cancelCall = cancel
    freeCall = free
  }

  func cancel() {
    lock.withLock {
      if freed { return }
      cancelCall?(value)
    }
  }

  func free() {
    lock.withLock {
      if freed { return }
      freed = true
      freeCall(value)
    }
  }
}
