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
  private let onQuiet: @Sendable () -> Void
  private let lock = NSLock()
  private var items: [String] = []
  private var waiter: CheckedContinuation<Void, Never>?
  private var want = 0

  init(onQuiet: @escaping @Sendable () -> Void = {}) { self.onQuiet = onQuiet }

  func quiet() async { onQuiet() }

  func putProgress(identity: PutIdentity, sent: UInt64) { record("sent \(identity.transferId).\(identity.cycleId).\(identity.attempt) \(sent)") }

  func putEnded(identity: PutIdentity, outcome: RawOutcome) {
    var text = "ended \(identity.transferId).\(identity.cycleId).\(identity.attempt)"
    if case let .response(status, etag, _) = outcome { text += " \(status) \(etag ?? "-")" }
    record(text)
  }

  func putMissing(identity: PutIdentity) { record("missing \(identity.transferId).\(identity.cycleId).\(identity.attempt)") }

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

private let identity = PutIdentity(transferId: 7, cycleId: 1, attempt: 2)

private let sessionIdentifier = "test.dblayer.uploads"

private func executor(_ session: FakeSession, inputs: RecordingInputs) -> UrlSessionPuts {
  let puts = UrlSessionPuts(identifier: sessionIdentifier, session: session)
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
    #expect(await inputs.inputs(1) == ["ended 7.1.2 200 \"abc\""])
  }

  /// The bytes the system reports for a task reach the core with the identity of its description.
  @Test
  func theSentBytesOfATaskReachTheCore() async {
    let inputs = RecordingInputs()
    let puts = executor(FakeSession(), inputs: inputs)
    puts.sent(description: PutDescription.text(identity), bytes: 4096)
    #expect(await inputs.inputs(1) == ["sent 7.1.2 4096"])
  }

  /// `ResumePuts` answers for the tasks the system holds: a finished one with its outcome, a missing one with `PutMissing`, a running one with nothing. A task outside the list is cancelled.
  @Test
  func resumePutsAnswersFromTheTasksOfTheSystem() async {
    let session = FakeSession()
    let finished = FakeTask(description: PutDescription.text(PutIdentity(transferId: 7, cycleId: 1, attempt: 2)), state: .completed, response: answer(200, etag: "e"))
    let running = FakeTask(description: PutDescription.text(PutIdentity(transferId: 8, cycleId: 1, attempt: 1)))
    let foreign = FakeTask(description: PutDescription.text(PutIdentity(transferId: 99, cycleId: 1, attempt: 1)))
    session.held = [finished, running, foreign]
    let inputs = RecordingInputs()
    let puts = executor(session, inputs: inputs)
    puts.resumePuts(identities: [identity, PutIdentity(transferId: 8, cycleId: 1, attempt: 1), PutIdentity(transferId: 9, cycleId: 1, attempt: 1)])
    #expect(await inputs.inputs(2) == ["ended 7.1.2 200 e", "missing 9.1.1"])
    #expect((finished.cancelled, running.cancelled, foreign.cancelled) == (false, false, true))
  }

  /// `CancelPut` cancels the task with that description and no other.
  @Test
  func cancelPutCancelsTheTaskOfTheIdentity() async {
    let session = FakeSession()
    let mine = FakeTask(description: PutDescription.text(identity))
    let other = FakeTask(description: PutDescription.text(PutIdentity(transferId: 8, cycleId: 1, attempt: 1)))
    session.held = [mine, other]
    let puts = executor(session, inputs: RecordingInputs())
    puts.cancelPut(identity: identity)
    await mine.cancelledSignal.get()
    #expect(other.cancelled == false)
  }

  /// Every completion the app handed over for a wake runs once on the main thread, after the events of the wake and the quiet of the core, in the order they were handed over.
  @Test
  func eachHandedCompletionRunsOnceAfterTheEventsAndTheQuiet() async {
    let log = Locked<[String]>([])
    let puts = executor(FakeSession(), inputs: RecordingInputs(onQuiet: { log.update { $0.append("quiet") } }))
    await withCheckedContinuation { (done: CheckedContinuation<Void, Never>) in
      let remaining = Locked(2)
      func completion(_ name: String) -> @Sendable () -> Void {
        { log.update { $0.append("\(name) \(Thread.isMainThread ? "main" : "thread")") }; if remaining.update({ $0 -= 1; return $0 }) == 0 { done.resume() } }
      }
      #expect(puts.handle(identifier: sessionIdentifier, completion: completion("first")))
      #expect(puts.handle(identifier: sessionIdentifier, completion: completion("second")))
      puts.eventsFinished()
    }
    #expect(log.value == ["quiet", "first main", "second main"])
  }

  /// A session of the app is not the session of the library: `false`, the completion is not taken and never runs.
  @Test
  func aForeignIdentifierIsNotTaken() async {
    let log = Locked<[String]>([])
    let inputs = RecordingInputs()
    let puts = executor(FakeSession(), inputs: inputs)
    #expect(puts.handle(identifier: "app.own.session", completion: { log.update { $0.append("called") } }) == false)
    puts.eventsFinished()
    puts.ended(description: PutDescription.text(identity), outcome: .failed(failure: RawFailure(domain: "t", code: nil, message: "")))
    _ = await inputs.inputs(1)
    #expect(log.value == [])
  }

  /// A call before `open` waits for the open database: the completion runs after the quiet of the core, which exists only once the database is attached.
  @Test
  func aCallBeforeOpenWaitsForTheDatabase() async {
    let log = Locked<[String]>([])
    let puts = UrlSessionPuts(identifier: sessionIdentifier, session: FakeSession())
    await withCheckedContinuation { (done: CheckedContinuation<Void, Never>) in
      #expect(puts.handle(identifier: sessionIdentifier, completion: { log.update { $0.append("completion") }; done.resume() }))
      puts.eventsFinished()
      puts.attach(RecordingInputs(onQuiet: { log.update { $0.append("quiet") } }))
    }
    #expect(log.value == ["quiet", "completion"])
  }

  /// The end of the events of a task that finished while the app was visible has no completion to run and is not remembered: the next call waits for the events of its own wake.
  @Test
  func anEventWithoutACompletionIsNotRemembered() async {
    let log = Locked<[String]>([])
    let ran = Awaited<Void>()
    let inputs = RecordingInputs(onQuiet: { log.update { $0.append("quiet") } })
    let puts = executor(FakeSession(), inputs: inputs)
    puts.eventsFinished()
    #expect(puts.handle(identifier: sessionIdentifier, completion: { log.update { $0.append("completion") }; Task { await ran.set(()) } }))
    puts.ended(description: PutDescription.text(identity), outcome: .failed(failure: RawFailure(domain: "t", code: nil, message: "")))
    _ = await inputs.inputs(1)
    #expect(log.value == [])
    puts.eventsFinished()
    await ran.get()
    #expect(log.value == ["quiet", "completion"])
  }

  /// `ResumePuts` matches a task by the whole identity: a task of the same transfer and attempt in another cycle is cancelled and the listed identity is missing.
  @Test
  func resumePutsMatchesTheWholeIdentity() async {
    let session = FakeSession()
    let pastCycle = FakeTask(description: PutDescription.text(PutIdentity(transferId: 7, cycleId: 1, attempt: 2)), state: .completed, response: answer(200, etag: "e"))
    session.held = [pastCycle]
    let inputs = RecordingInputs()
    let puts = executor(session, inputs: inputs)
    puts.resumePuts(identities: [PutIdentity(transferId: 7, cycleId: 2, attempt: 2)])
    #expect(await inputs.inputs(1) == ["missing 7.2.2"])
    #expect(pastCycle.cancelled)
  }
}

private final class Locked<Value>: @unchecked Sendable {
  private let lock = NSLock()
  private var stored: Value

  init(_ value: Value) { stored = value }

  var value: Value { lock.withLock { stored } }

  @discardableResult
  func update<Result>(_ change: (inout Value) -> Result) -> Result { lock.withLock { change(&stored) } }
}
