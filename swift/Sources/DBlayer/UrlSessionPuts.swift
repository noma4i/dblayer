import Foundation

/// A system task of the background session, as far as the executor reads it.
protocol PutTask: AnyObject, Sendable {
  var taskDescription: String? { get }
  var state: URLSessionTask.State { get }
  var response: URLResponse? { get }
  var error: Error? { get }
  func cancel()
}

extension URLSessionTask: PutTask {}

/// The background session that runs the PUTs outside the process. The system keeps the tasks, the executor keeps nothing.
protocol PutSession: AnyObject, Sendable {
  func attach(events: any PutEvents)
  func upload(_ request: URLRequest, fromFile file: URL, description: String)
  func tasks() async -> [any PutTask]
}

/// What the session reports: the progress and the end of a task and the end of the events the system delivered for a wake.
protocol PutEvents: AnyObject, Sendable {
  func sent(description: String?, bytes: UInt64)
  func ended(description: String?, outcome: RawOutcome)
  func eventsFinished()
}

/// The inputs of the core for the outcomes of the system tasks.
protocol PutInputs: Sendable {
  func putProgress(identity: PutIdentity, sent: UInt64)
  func putEnded(identity: PutIdentity, outcome: RawOutcome)
  func putMissing(identity: PutIdentity)
  /// Answers when the writes of the open session have no work that runs without a new external event.
  func quiet() async
}

extension Database: PutInputs {
  func quiet() async { await writesQuiet(epoch: sessionEpoch()) }
}

/// A value that arrives once: a reader before it waits for it.
actor Awaited<Value: Sendable> {
  private var value: Value?
  private var waiting: [CheckedContinuation<Value, Never>] = []

  func set(_ new: Value) {
    value = new
    let readers = waiting
    waiting = []
    readers.forEach { $0.resume(returning: new) }
  }

  func get() async -> Value {
    if let value { return value }
    return await withCheckedContinuation { waiting.append($0) }
  }
}

/// A value that several threads read and write under one lock.
final class Guarded<Value>: @unchecked Sendable {
  private let lock = NSLock()
  private var stored: Value

  init(_ value: Value) { stored = value }

  func update<Result>(_ change: (inout Value) -> Result) -> Result { lock.withLock { change(&stored) } }
}

/// The text of an identity in the description of a system task, and back.
enum PutDescription {
  private static let prefix = "dblayer-put "

  static func text(_ identity: PutIdentity) -> String { "\(prefix)\(identity.transferId).\(identity.cycleId).\(identity.attempt)" }

  static func identity(_ text: String?) -> PutIdentity? {
    guard let text, text.hasPrefix(prefix) else { return nil }
    let numbers = text.dropFirst(prefix.count).split(separator: ".").compactMap { UInt64($0) }
    guard numbers.count == 3 else { return nil }
    return PutIdentity(transferId: numbers[0], cycleId: numbers[1], attempt: numbers[2])
  }
}

/// What the delegate of the system session reports, in the order it came.
private enum PutEvent: Sendable {
  case sent(PutIdentity, UInt64)
  case ended(PutIdentity, RawOutcome)
  case finished([@Sendable () -> Void])
}

/// The executor of the PUTs outside the process: every call is one call of the system session or one input of the core. The delegate events reach the core in the order they came, and the completions of a wake run after the core is quiet.
final class UrlSessionPuts: PutExecutor, PutEvents, Sendable {
  /// The one executor of the process: one open is one background session.
  static let process: UrlSessionPuts? = available()

  /// The identifier of the background session of the library; the app has its own sessions.
  static var sessionIdentifier: String? { Bundle.main.bundleIdentifier.map { "\($0).dblayer.uploads" } }

  private let inputs = Awaited<any PutInputs>()
  private let identifier: String
  private let session: any PutSession
  private let completions = Guarded<[@Sendable () -> Void]>([])
  private let events: AsyncStream<PutEvent>.Continuation

  init(identifier: String, session: any PutSession) {
    let (stream, continuation) = AsyncStream.makeStream(of: PutEvent.self)
    self.identifier = identifier
    self.session = session
    events = continuation
    session.attach(events: self)
    Task { await self.deliver(stream) }
  }

  /// The system session exists on iOS with a bundle id: macOS and the tests of the host run the PUTs in the process.
  private static func available() -> UrlSessionPuts? {
    #if os(iOS)
    guard let identifier = sessionIdentifier else { return nil }
    return UrlSessionPuts(identifier: identifier, session: BackgroundPutSession(identifier: identifier))
    #else
    return nil
    #endif
  }

  func attach(_ database: any PutInputs) { Task { await inputs.set(database) } }

  func startPut(identity: PutIdentity, target: PutTarget, file: PutFile) {
    guard let address = URL(string: target.url) else {
      Task { await inputs.get().putMissing(identity: identity) }
      return
    }
    var request = URLRequest(url: address, timeoutInterval: TimeInterval(target.requestMs) / 1000)
    request.httpMethod = "PUT"
    target.headers.forEach { request.setValue($1, forHTTPHeaderField: $0) }
    session.upload(request, fromFile: URL(fileURLWithPath: file.path), description: PutDescription.text(identity))
  }

  func cancelPut(identity: PutIdentity) {
    let text = PutDescription.text(identity)
    Task { await session.tasks().filter { $0.taskDescription == text }.forEach { $0.cancel() } }
  }

  func resumePuts(identities: [PutIdentity]) {
    Task {
      let tasks = await session.tasks()
      let listed = Set(identities)
      var found: [PutIdentity: any PutTask] = [:]
      for task in tasks {
        guard let held = PutDescription.identity(task.taskDescription) else { continue }
        if listed.contains(held) { found[held] = task } else { task.cancel() }
      }
      let inputs = await inputs.get()
      for identity in identities {
        guard let task = found[identity] else { inputs.putMissing(identity: identity); continue }
        if task.state == .completed { inputs.putEnded(identity: identity, outcome: Self.outcome(of: task)) }
      }
    }
  }

  static func outcome(of task: any PutTask) -> RawOutcome {
    if let error = task.error { return .failed(error) }
    let http = task.response as? HTTPURLResponse
    return .response(status: UInt16(clamping: http?.statusCode ?? 0), etag: http?.value(forHTTPHeaderField: "ETag"), body: "")
  }

  func sent(description: String?, bytes: UInt64) {
    guard let identity = PutDescription.identity(description) else { return }
    events.yield(.sent(identity, bytes))
  }

  func ended(description: String?, outcome: RawOutcome) {
    guard let identity = PutDescription.identity(description) else { return }
    events.yield(.ended(identity, outcome))
  }

  /// The completions handed up to this event belong to it: one handed later waits for the events of its own wake.
  func eventsFinished() { events.yield(.finished(completions.update { taken in defer { taken = [] }; return taken })) }

  /// The app hands the completion of a wake over. A foreign identifier is a session of the app: `false`, nothing is taken. Otherwise `true`: the completion runs once on the main thread after the events of this wake and the quiet of the core.
  func handle(identifier: String, completion: @escaping @Sendable () -> Void) -> Bool {
    guard identifier == self.identifier else { return false }
    completions.update { $0.append(completion) }
    return true
  }

  private func deliver(_ stream: AsyncStream<PutEvent>) async {
    for await event in stream {
      switch event {
      case let .sent(identity, bytes): await inputs.get().putProgress(identity: identity, sent: bytes)
      case let .ended(identity, outcome): await inputs.get().putEnded(identity: identity, outcome: outcome)
      case let .finished(handed):
        guard !handed.isEmpty else { continue }
        await inputs.get().quiet()
        handed.forEach { DispatchQueue.main.async(execute: $0) }
      }
    }
  }
}

/// The background `URLSession`: the system keeps its tasks over the death of the process.
final class BackgroundPutSession: NSObject, PutSession, URLSessionTaskDelegate, Sendable {
  private struct Started { let events: any PutEvents; let session: URLSession }

  private let identifier: String
  private let started = Guarded<Started?>(nil)

  init(identifier: String) { self.identifier = identifier }

  /// The session starts once the executor listens: the system delivers the events of an earlier wake at once.
  func attach(events: any PutEvents) {
    let configuration = URLSessionConfiguration.background(withIdentifier: identifier)
    configuration.sessionSendsLaunchEvents = true
    configuration.waitsForConnectivity = true
    configuration.httpCookieStorage = nil
    configuration.httpShouldSetCookies = false
    configuration.urlCredentialStorage = nil
    configuration.urlCache = nil
    let session = URLSession(configuration: configuration, delegate: self, delegateQueue: nil)
    started.update { $0 = Started(events: events, session: session) }
  }

  private var events: (any PutEvents)? { started.update { $0?.events } }
  private var session: URLSession? { started.update { $0?.session } }

  func upload(_ request: URLRequest, fromFile file: URL, description: String) {
    guard let task = session?.uploadTask(with: request, fromFile: file) else { return }
    task.taskDescription = description
    task.resume()
  }

  func tasks() async -> [any PutTask] { await session?.allTasks ?? [] }

  func urlSession(_ session: URLSession, task: URLSessionTask, didSendBodyData bytesSent: Int64, totalBytesSent: Int64, totalBytesExpectedToSend: Int64) {
    events?.sent(description: task.taskDescription, bytes: UInt64(clamping: totalBytesSent))
  }

  func urlSession(_ session: URLSession, task: URLSessionTask, didCompleteWithError error: Error?) {
    events?.ended(description: task.taskDescription, outcome: UrlSessionPuts.outcome(of: task))
  }

  func urlSessionDidFinishEvents(forBackgroundURLSession session: URLSession) { events?.eventsFinished() }
}

extension DB {
  /// The app calls it from `application(_:handleEventsForBackgroundURLSession:completionHandler:)` with the identifier and the completion handler the system gave. An identifier that is not the session of the library returns `false` and the completion handler is not taken. Otherwise it returns `true` and the handler runs once on the main thread after the events of the wake and after the library has no work that runs without a new event. A call before `open` waits for the open database.
  @discardableResult
  public static func handleBackgroundEvents(identifier: String, completion: @escaping @Sendable () -> Void) -> Bool {
    UrlSessionPuts.process?.handle(identifier: identifier, completion: completion) ?? false
  }
}
