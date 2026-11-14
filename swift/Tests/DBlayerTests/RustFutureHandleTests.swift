import Foundation
import Testing
@testable import DBlayer

private final class HandleCalls: @unchecked Sendable {
  private let lock = NSLock()
  private var recorded: [String] = []
  private var queued: [@Sendable () -> Void] = []
  private var reenter: (@Sendable () -> Void)?
  var calls: [String] { lock.withLock { recorded } }
  func onCancel(_ body: @escaping @Sendable () -> Void) { lock.withLock { reenter = body } }
  func cancel(_ handle: UInt64) {
    lock.withLock { recorded.append("cancel:\(handle)") }
    lock.withLock { reenter }?()
  }
  func free(_ handle: UInt64) { lock.withLock { recorded.append("free:\(handle)") } }
  func lane(_ work: @escaping @Sendable () -> Void) { lock.withLock { queued.append(work) } }
  func drainLane() {
    let work = lock.withLock { () -> [@Sendable () -> Void] in defer { queued = [] }; return queued }
    for item in work { item() }
  }
}

struct RustFutureHandleTests {
  @Test
  func bridge_prop_future_handle_cancelsAndFreesQueuedOnTheLaneReachTheFutureInOrderAndOnce() {
    let calls = HandleCalls()
    let handle = RustFutureHandle(42, cancel: calls.cancel, free: calls.free, lane: calls.lane)
    handle.cancel()
    handle.cancel()
    handle.free()
    handle.free()
    #expect(calls.calls == [])
    calls.drainLane()
    handle.cancel()
    handle.free()
    calls.drainLane()
    #expect(calls.calls == ["cancel:42", "free:42"])
  }

  @Test
  func bridge_prop_future_handle_aCancelAfterTheFreeNeverReachesTheFutureAndAFreeInsideACancelRunsOnce() {
    let calls = HandleCalls()
    let handle = RustFutureHandle(7, cancel: calls.cancel, free: calls.free, lane: calls.lane)
    handle.free()
    handle.cancel()
    handle.free()
    calls.drainLane()
    #expect(calls.calls == ["free:7"])

    let reentrant = HandleCalls()
    let inside = RustFutureHandle(9, cancel: reentrant.cancel, free: reentrant.free, lane: { $0() })
    reentrant.onCancel { inside.free() }
    inside.cancel()
    #expect(reentrant.calls == ["cancel:9", "free:9"])
  }

  @Test
  func bridge_prop_future_handle_concurrentCancelsAndFreesOnTheRuntimeLaneCallTheFutureOnceEach() async {
    let calls = HandleCalls()
    let handle = RustFutureHandle(42, cancel: calls.cancel, free: calls.free)
    await withTaskGroup(of: Void.self) { group in
      for index in 0..<64 {
        group.addTask { if index.isMultiple(of: 3) { handle.free() } else { handle.cancel() } }
      }
    }
    await withCheckedContinuation { continuation in RuntimeLane.enqueue { continuation.resume() } }
    #expect(calls.calls.filter { $0 == "free:42" }.count == 1)
    #expect(calls.calls.filter { $0 == "cancel:42" }.count <= 1)
    #expect(calls.calls.last == "free:42")
  }
}
