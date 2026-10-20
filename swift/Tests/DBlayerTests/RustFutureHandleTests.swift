import Foundation
import Testing
@testable import DBlayer

private final class HandleCalls: @unchecked Sendable {
  private let lock = NSLock()
  private var recorded: [String] = []
  var calls: [String] { lock.withLock { recorded } }
  func cancel(_ handle: UInt64) { lock.withLock { recorded.append("cancel:\(handle)") } }
  func free(_ handle: UInt64) { lock.withLock { recorded.append("free:\(handle)") } }
}

struct RustFutureHandleTests {
  @Test
  func cancellationNeverUsesAFreedHandleAndFreeRunsOnce() async {
    for cancelFirst in [false, true] {
      let calls = HandleCalls()
      let handle = RustFutureHandle(42, cancel: calls.cancel, free: calls.free)
      if cancelFirst { handle.cancel() }
      handle.free()
      handle.cancel()
      handle.free()
      #expect(calls.calls == (cancelFirst ? ["cancel:42", "free:42"] : ["free:42"]))
    }
    let calls = HandleCalls()
    let handle = RustFutureHandle(42, cancel: calls.cancel, free: calls.free)
    await withTaskGroup(of: Void.self) { group in
      for index in 0..<64 {
        group.addTask { if index.isMultiple(of: 3) { handle.free() } else { handle.cancel() } }
      }
    }
    #expect(calls.calls.last == "free:42")
    #expect(calls.calls.filter { $0 == "free:42" }.count == 1)
  }
}
