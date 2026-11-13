import Foundation
import Testing
@testable import DBlayer

private final class Calls: @unchecked Sendable {
  private let lock = NSLock()
  private var lines = [String]()
  var all: [String] { lock.withLock { lines } }
  func add(_ line: String) { lock.withLock { lines.append(line) } }
}

private final class Handle: RoundHandle {
  let calls: Calls
  init(_ calls: Calls) { self.calls = calls }
  func progress(_ fraction: Double) { calls.add("progress \(fraction)") }
  func finish(success: Bool) { calls.add("finish \(success)") }
}

private final class Answers: RoundAnswers {
  let calls: Calls
  init(_ calls: Calls) { self.calls = calls }
  func roundStarted(requestId: UInt64, roundId: UInt32, epoch: UInt64) { calls.add("started \(requestId) \(roundId) \(epoch)") }
  func roundRefused(requestId: UInt64, epoch: UInt64, kind: RoundRefusal, detail: String) { calls.add("refused \(requestId) \(epoch) \(kind) \(detail)") }
  func roundEnded(roundId: UInt32, epoch: UInt64) { calls.add("ended \(roundId) \(epoch)") }
}

private struct Refusing: Error, LocalizedError { var errorDescription: String? { "no slot" } }

private final class Scheduler: RoundScheduler, @unchecked Sendable {
  let calls: Calls
  let handle: Handle
  let fails: Bool
  private let lock = NSLock()
  private var held: (launch: @Sendable (RoundHandle) -> Void, expire: @Sendable () -> Void)?
  init(_ calls: Calls, fails: Bool = false) { self.calls = calls; self.handle = Handle(calls); self.fails = fails }
  var launch: (@Sendable (RoundHandle) -> Void)? { lock.withLock { held?.launch } }
  var expire: (@Sendable () -> Void)? { lock.withLock { held?.expire } }
  func submit(_ job: RoundJob, launch: @escaping @Sendable (RoundHandle) -> Void, expire: @escaping @Sendable () -> Void) throws {
    calls.add("submit \(job.requestId) \(job.roundId) \(job.epoch)")
    if fails { throw Refusing() }
    lock.withLock { held = (launch, expire) }
  }
}

@Suite struct UploadRoundsTests {
  @Test func theMessagesOfTheCoreAndTheEventsOfTheSystemMapOneToOne() {
    let calls = Calls()
    let scheduler = Scheduler(calls)
    let answers = Answers(calls)
    let refusing = Scheduler(calls, fails: true)
    withExtendedLifetime(answers) {
      let rounds = UploadRounds(database: answers, scheduler: scheduler)
      rounds.scheduleJob(job: RoundJob(requestId: 7, roundId: 3, epoch: 9))
      scheduler.launch?(scheduler.handle)
      rounds.roundProgress(roundId: 3, fraction: 0.4)
      rounds.endJob(roundId: 3)
      rounds.scheduleJob(job: RoundJob(requestId: 8, roundId: 4, epoch: 9))
      scheduler.launch?(scheduler.handle)
      scheduler.expire?()
      UploadRounds(database: answers, scheduler: refusing).scheduleJob(job: RoundJob(requestId: 9, roundId: 5, epoch: 9))
    }
    #expect(calls.all == ["submit 7 3 9", "started 7 3 9", "progress 0.4", "finish true", "submit 8 4 9", "started 8 4 9", "finish false", "ended 4 9", "submit 9 5 9", "refused 9 9 schedulerFailed no slot"])
  }
}
