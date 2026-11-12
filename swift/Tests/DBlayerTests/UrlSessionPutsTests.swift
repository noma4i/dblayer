import Foundation
import Testing
@testable import DBlayer

private final class FakeTask: PutTask, @unchecked Sendable {
  let taskDescription: String?
  var state: URLSessionTask.State
  var response: URLResponse?
  var error: Error?
  private(set) var cancelled = false
  let cancelledSignal = Awaited<Void>()

  init(description: String?, state: URLSessionTask.State = .running, response: URLResponse? = nil) {
    taskDescription = description
    self.state = state
    self.response = response
  }

  func cancel() {
    cancelled = true
    Task { await cancelledSignal.set(()) }
  }
}

private final class FakeSession: PutSession, @unchecked Sendable {
  private(set) var uploads: [(URLRequest, URL, String)] = []
  var held: [FakeTask] = []

  func attach(events: any PutEvents) {}

  func upload(_ request: URLRequest, fromFile file: URL, description: String) { uploads.append((request, file, description)) }

  func tasks() async -> [any PutTask] { held }
}

private final class RecordingInputs: PutInputs, @unchecked Sendable {
  private let lock = NSLock()
  private var items: [String] = []
  private var waiter: CheckedContinuation<Void, Never>?
  private var want = 0

  func putEnded(identity: PutIdentity, outcome: RawOutcome) {
    var text = "ended \(identity.transferId).\(identity.attemptId)"
    if case let .response(status, etag, _) = outcome { text += " \(status) \(etag ?? "-")" }
    record(text)
  }

  func putMissing(identity: PutIdentity) { record("missing \(identity.transferId).\(identity.attemptId)") }

  private func record(_ text: String) {
    lock.lock()
    items.append(text)
    let ready = waiter.flatMap { items.count >= want ? $0 : nil }
    if ready != nil { waiter = nil }
    lock.unlock()
    ready?.resume()
  }

  /// The inputs once `count` of them came.
  func inputs(_ count: Int) async -> [String] {
    await withCheckedContinuation { (reader: CheckedContinuation<Void, Never>) in
      lock.lock()
      if items.count >= count { lock.unlock(); reader.resume() } else { want = count; waiter = reader; lock.unlock() }
    }
    return lock.withLock { items }
  }
}

private func answer(_ status: Int, etag: String?) -> HTTPURLResponse {
  HTTPURLResponse(url: URL(string: "https://storage.test/1")!, statusCode: status, httpVersion: nil, headerFields: etag.map { ["ETag": $0] })!
}

private let identity = PutIdentity(epoch: 3, transferId: 7, attemptId: 2)

private func executor(_ session: FakeSession, inputs: RecordingInputs, settled: @escaping @Sendable () async -> Void = {}) -> UrlSessionPuts {
  let puts = UrlSessionPuts(session: session, settled: settled)
  puts.attach(inputs)
  return puts
}

struct UrlSessionPutsTests {
  /// `StartPut` hands the system a task of the file with the identity as its description, the method and the headers of the target.
  @Test
  func startPutCreatesATaskNamedByTheIdentity() {
    let session = FakeSession()
    let puts = executor(session, inputs: RecordingInputs())
    puts.startPut(identity: identity, target: PutTarget(url: "https://storage.test/1", headers: ["Content-Type": "video/mp4"], connectMs: 1, requestMs: 2000, responseLimit: 10), file: PutFile(path: "/tmp/copy", offset: 0, length: 5))
    let (request, file, description) = session.uploads[0]
    #expect((request.httpMethod, request.value(forHTTPHeaderField: "Content-Type"), file.path, PutDescription.identity(description)) == ("PUT", "video/mp4", "/tmp/copy", identity))
  }

  /// The end of a task reaches the core as the outcome of the identity in its description, with the etag.
  @Test
  func theEndOfATaskReachesTheCoreWithItsEtag() async {
    let inputs = RecordingInputs()
    let puts = executor(FakeSession(), inputs: inputs)
    let task = FakeTask(description: PutDescription.text(identity), state: .completed, response: answer(200, etag: "\"abc\""))
    puts.ended(description: task.taskDescription, outcome: UrlSessionPuts.outcome(of: task))
    #expect(await inputs.inputs(1) == ["ended 7.2 200 \"abc\""])
  }

  /// `ResumePuts` answers for the tasks the system holds: a finished one with its outcome, a missing one with `PutMissing`, a running one with nothing. A task outside the list is cancelled.
  @Test
  func resumePutsAnswersFromTheTasksOfTheSystem() async {
    let session = FakeSession()
    let finished = FakeTask(description: PutDescription.text(PutIdentity(epoch: 1, transferId: 7, attemptId: 2)), state: .completed, response: answer(200, etag: "e"))
    let running = FakeTask(description: PutDescription.text(PutIdentity(epoch: 1, transferId: 8, attemptId: 1)))
    let foreign = FakeTask(description: PutDescription.text(PutIdentity(epoch: 1, transferId: 99, attemptId: 1)))
    session.held = [finished, running, foreign]
    let inputs = RecordingInputs()
    let puts = executor(session, inputs: inputs)
    puts.resumePuts(identities: [identity, PutIdentity(epoch: 3, transferId: 8, attemptId: 1), PutIdentity(epoch: 3, transferId: 9, attemptId: 1)])
    #expect(await inputs.inputs(2) == ["ended 7.2 200 e", "missing 9.1"])
    #expect((finished.cancelled, running.cancelled, foreign.cancelled) == (false, false, true))
  }

  /// `CancelPut` cancels the task with that description and no other.
  @Test
  func cancelPutCancelsTheTaskOfTheIdentity() async {
    let session = FakeSession()
    let mine = FakeTask(description: PutDescription.text(identity))
    let other = FakeTask(description: PutDescription.text(PutIdentity(epoch: 3, transferId: 8, attemptId: 1)))
    session.held = [mine, other]
    let puts = executor(session, inputs: RecordingInputs())
    puts.cancelPut(identity: identity)
    await mine.cancelledSignal.get()
    #expect(other.cancelled == false)
  }

  /// The completion of the app runs once, on the main thread, after the events of the system ended and the core is quiet, in either order of the two calls.
  @Test(arguments: [true, false])
  func theCompletionRunsOnceAfterTheEventsAreDelivered(handedFirst: Bool) async {
    let log = Locked<[String]>([])
    let puts = executor(FakeSession(), inputs: RecordingInputs(), settled: { log.update { $0.append("settled") } })
    await withCheckedContinuation { (done: CheckedContinuation<Void, Never>) in
      let completion: @Sendable () -> Void = {
        log.update { $0.append(Thread.isMainThread ? "main" : "thread") }
        done.resume()
      }
      if handedFirst { puts.handle(completion: completion); puts.eventsFinished() } else { puts.eventsFinished(); puts.handle(completion: completion) }
    }
    #expect(log.value == ["settled", "main"])
  }
}

private final class Locked<Value>: @unchecked Sendable {
  private let lock = NSLock()
  private var stored: Value

  init(_ value: Value) { stored = value }

  var value: Value { lock.withLock { stored } }

  func update(_ change: (inout Value) -> Void) { lock.withLock { change(&stored) } }
}
