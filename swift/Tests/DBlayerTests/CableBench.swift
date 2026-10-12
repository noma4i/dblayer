import Foundation
import Testing
@testable import DBlayer

func settle() async {
  try? await Task.sleep(for: .milliseconds(50))
}

final class ManualCableClock: @unchecked Sendable {
  private struct Sleeper {
    let id: Int
    let due: TimeInterval
    let seconds: TimeInterval
    let continuation: CheckedContinuation<Void, Never>
  }

  private let lock = NSLock()
  private var elapsed: TimeInterval = 0
  private var sleepers: [Sleeper] = []
  private var nextId = 0
  private var fired = 0
  private var skew: TimeInterval = 0

  func now() -> Date {
    lock.withLock { Date(timeIntervalSince1970: elapsed + skew) }
  }

  func sleep(_ seconds: TimeInterval) async {
    let id = lock.withLock {
      nextId += 1
      return nextId
    }
    await withTaskCancellationHandler {
      await withCheckedContinuation { continuation in
        let resumeNow = lock.withLock {
          if Task.isCancelled {
            return true
          }
          sleepers.append(Sleeper(id: id, due: elapsed + seconds, seconds: seconds, continuation: continuation))
          return false
        }
        if resumeNow {
          continuation.resume()
        }
      }
    } onCancel: {
      let sleeper = lock.withLock { () -> Sleeper? in
        guard let index = sleepers.firstIndex(where: { $0.id == id }) else { return nil }
        return sleepers.remove(at: index)
      }
      sleeper?.continuation.resume()
    }
  }

  func advance(by seconds: TimeInterval) async {
    let due = lock.withLock {
      elapsed += seconds
      let due = sleepers.filter { $0.due <= elapsed }
      sleepers.removeAll { $0.due <= elapsed }
      fired += due.count
      return due
    }
    due.forEach { $0.continuation.resume() }
    await settle()
  }

  func waitForSleepers(_ count: Int, requestedSeconds: TimeInterval? = nil) async {
    await eventually { pending(requestedSeconds: requestedSeconds) >= count }
  }

  func sleeperDueTimes(before limit: TimeInterval) -> [TimeInterval] {
    lock.withLock { sleepers.map(\.due).filter { $0 < limit } }
  }
  func skewWallClock(by seconds: TimeInterval) {
    lock.withLock { skew += seconds }
  }

  func pendingDues() -> [TimeInterval] {
    lock.withLock { sleepers.map(\.due) }
  }

  func pendingSeconds() -> [TimeInterval] {
    lock.withLock { sleepers.map(\.seconds) }
  }

  var firedSleeps: Int {
    lock.withLock { fired }
  }

  func sleeperCount(requestedSeconds: TimeInterval) -> Int {
    pending(requestedSeconds: requestedSeconds)
  }

  private func pending(requestedSeconds: TimeInterval?) -> Int {
    lock.withLock {
      sleepers.count { sleeper in requestedSeconds.map { abs(sleeper.seconds - $0) < 0.0001 } ?? true }
    }
  }
}

final class BenchSocket: CableSocket, @unchecked Sendable {
  let token: String
  private let index: Int
  private let bench: CableBench
  private let lock = NSLock()
  private var queued: [String] = []
  private var failed = false
  private(set) var isClosed = false
  private var sentTexts: [String] = []
  private var pending: CheckedContinuation<String, any Error>?

  init(token: String, index: Int, bench: CableBench) {
    self.token = token
    self.index = index
    self.bench = bench
  }

  var sent: [String] {
    lock.withLock { sentTexts }
  }

  func open() async throws {
    bench.record("open \(token)")
    await bench.waitForOpenGate()
    if bench.consumeOpenFailure() {
      throw CableSocketError.closed
    }
  }

  func close() {
    let continuation = lock.withLock { () -> CheckedContinuation<String, any Error>? in
      guard !isClosed else { return nil }
      isClosed = true
      let continuation = pending
      pending = nil
      return continuation
    }
    bench.record("close \(index)")
    continuation?.resume(throwing: CableSocketError.closed)
  }

  func send(_ text: String) async throws {
    try lock.withLock {
      if isClosed {
        throw CableSocketError.closed
      }
      sentTexts.append(text)
    }
  }

  func receive() async throws -> String {
    try await withCheckedThrowingContinuation { continuation in
      let result = lock.withLock { () -> Result<String, any Error>? in
        if isClosed || failed {
          return .failure(CableSocketError.closed)
        }
        if !queued.isEmpty {
          return .success(queued.removeFirst())
        }
        pending = continuation
        return nil
      }
      if let result {
        continuation.resume(with: result)
      }
    }
  }

  func deliver(_ text: String) {
    let continuation = lock.withLock { () -> CheckedContinuation<String, any Error>? in
      guard let continuation = pending else {
        queued.append(text)
        return nil
      }
      pending = nil
      return continuation
    }
    continuation?.resume(returning: text)
  }

  func fail() {
    let continuation = lock.withLock { () -> CheckedContinuation<String, any Error>? in
      failed = true
      let continuation = pending
      pending = nil
      return continuation
    }
    continuation?.resume(throwing: CableSocketError.closed)
  }
}

final class CableBench: @unchecked Sendable {
  let clock = ManualCableClock()
  private let lock = NSLock()
  private var created: [BenchSocket] = []
  private var entries: [String] = []
  private var remainingFailures = 0
  private var channelIds: [String]
  private var recycleEntries: [CableRecycle] = []
  private var isGatingOpens = false
  private var gated: [CheckedContinuation<Void, Never>] = []

  init(channelIds: [String] = ["abc"]) {
    self.channelIds = channelIds
  }

  var sockets: [BenchSocket] {
    lock.withLock { created }
  }

  var log: [String] {
    lock.withLock { entries }
  }

  var recycles: [CableRecycle] {
    lock.withLock { recycleEntries }
  }

  var failOpens: Int {
    get { lock.withLock { remainingFailures } }
    set { lock.withLock { remainingFailures = newValue } }
  }

  func gateOpens() {
    lock.withLock { isGatingOpens = true }
  }

  func releaseOpens() {
    let waiting = lock.withLock {
      isGatingOpens = false
      defer { gated = [] }
      return gated
    }
    waiting.forEach { $0.resume() }
  }

  func waitForOpenGate() async {
    await withCheckedContinuation { continuation in
      let resumeNow = lock.withLock {
        guard isGatingOpens else { return true }
        gated.append(continuation)
        return false
      }
      if resumeNow {
        continuation.resume()
      }
    }
  }

  var gatedOpenCount: Int {
    lock.withLock { gated.count }
  }

  func record(_ entry: String) {
    lock.withLock { entries.append(entry) }
  }

  func consumeOpenFailure() -> Bool {
    lock.withLock {
      guard remainingFailures > 0 else { return false }
      remainingFailures -= 1
      return true
    }
  }

  var timers: CableTimers {
    CableTimers(
      now: { self.clock.now() },
      sleep: { await self.clock.sleep($0) },
      jitter: { _ in 0 },
      recycled: { recycle in self.lock.withLock { self.recycleEntries.append(recycle) } }
    )
  }

  func makeSocket(_: URL, token: String) -> any CableSocket {
    lock.withLock {
      let socket = BenchSocket(token: token, index: created.count, bench: self)
      created.append(socket)
      return socket
    }
  }

  func makeConnection() -> CableConnection {
    CableConnection(
      url: URL(fileURLWithPath: "/test"),
      token: "old-token",
      makeSocket: { self.makeSocket($0, token: $1) },
      timers: timers
    )
  }

  func makeClient() -> CableClient {
    CableClient(
      url: URL(fileURLWithPath: "/test"),
      token: "old-token",
      makeSocket: { self.makeSocket($0, token: $1) },
      makeChannelId: { self.nextChannelId() },
      pingInterval: 3600,
      timers: timers
    )
  }

  private func nextChannelId() -> String {
    lock.withLock { channelIds.count > 1 ? channelIds.removeFirst() : channelIds[0] }
  }
}

final class CallLog: @unchecked Sendable {
  private let lock = NSLock()
  private var items: [String] = []

  var entries: [String] {
    lock.withLock { items }
  }

  func add(_ entry: String) {
    lock.withLock { items.append(entry) }
  }
}

extension CableBench {
  var pingCount: Int {
    sockets.flatMap(\.sent).count { $0.contains("\\\"action\\\":\\\"ping\\\"") }
  }

  func startWelcomedClient(
    presenceUserId: String? = nil,
    subscribing document: String? = nil
  ) async -> (client: CableClient, start: Task<Void, Never>) {
    let client = makeClient()
    if let presenceUserId {
      await client.openPresence(userId: presenceUserId)
    }
    if let document {
      _ = await client.subscribeGraphQL(document: document, operationName: "X")
    }
    let start = Task { await client.start() }
    await eventually { sockets.count == 1 }
    sockets[0].deliver("{\"type\":\"welcome\"}")
    if let presenceUserId {
      await eventually { sockets[0].sent.count == 1 }
      sockets[0].deliver(confirmationFrame(identifier: OnlineChannel.identifier(userId: presenceUserId)))
      await eventually { pingCount == 1 }
    } else {
      await settle()
    }
    return (client, start)
  }
}
