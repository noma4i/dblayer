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

/// What the delegate of the system session, the core and the app report, in the order it came.
private enum PutEvent: Sendable {
  case start(PutIdentity, PutTarget, PutFile)
  case cancel(PutIdentity)
  case resume([PutIdentity])
  case sent(PutIdentity, UInt64)
  case ended(PutIdentity, RawOutcome)
  case handed(@Sendable () -> Void)
  case finished
}

/// The one line the calls of the core, the events of the session and the completions of the app go through: its single reader sees them in the order they came.
final class PutFeed: PutEvents, Sendable {
  fileprivate let stream: AsyncStream<PutEvent>
  private let continuation: AsyncStream<PutEvent>.Continuation

  init() { (stream, continuation) = AsyncStream.makeStream(of: PutEvent.self) }

  func sent(description: String?, bytes: UInt64) {
    guard let identity = PutDescription.identity(description) else { return }
    continuation.yield(.sent(identity, bytes))
  }

  func ended(description: String?, outcome: RawOutcome) {
    guard let identity = PutDescription.identity(description) else { return }
    continuation.yield(.ended(identity, outcome))
  }

  func eventsFinished() { continuation.yield(.finished) }

  func handed(_ completion: @escaping @Sendable () -> Void) { continuation.yield(.handed(completion)) }

  fileprivate func start(_ identity: PutIdentity, _ target: PutTarget, _ file: PutFile) { continuation.yield(.start(identity, target, file)) }

  fileprivate func cancel(_ identity: PutIdentity) { continuation.yield(.cancel(identity)) }

  fileprivate func resume(_ identities: [PutIdentity]) { continuation.yield(.resume(identities)) }
}

/// The executor of the PUTs outside the process: every call is one call of the system session or one input of the core, and all of them run in the order of the feed. The events of the session reach the core in the order they came, and the completions of a wake run after the core is quiet.
final class UrlSessionPuts: PutExecutor, Sendable {
  /// The one executor of the process: one open is one background session.
  static let process: UrlSessionPuts? = available()

  /// The identifier of the background session of the library; the app has its own sessions.
  static var sessionIdentifier: String? { Bundle.main.bundleIdentifier.map { "\($0).dblayer.uploads" } }

  let feed: PutFeed
  private let inputs = Awaited<any PutInputs>()
  private let identifier: String
  private let session: any PutSession

  init(identifier: String, makeSession: (any PutEvents) -> any PutSession) {
    let feed = PutFeed()
    self.identifier = identifier
    self.feed = feed
    session = makeSession(feed)
    Task { await self.deliver(feed.stream) }
  }

  /// The system session exists on iOS with a bundle id: macOS and the tests of the host run the PUTs in the process.
  private static func available() -> UrlSessionPuts? {
    #if os(iOS)
    guard let identifier = sessionIdentifier else { return nil }
    return UrlSessionPuts(identifier: identifier) { BackgroundPutSession(identifier: identifier, events: $0) }
    #else
    return nil
    #endif
  }

  func attach(_ database: any PutInputs) { Task { await inputs.set(database) } }

  func startPut(identity: PutIdentity, target: PutTarget, file: PutFile) { feed.start(identity, target, file) }

  func cancelPut(identity: PutIdentity) { feed.cancel(identity) }

  func resumePuts(identities: [PutIdentity]) { feed.resume(identities) }

  static func outcome(of task: any PutTask) -> RawOutcome {
    if let error = task.error { return .failed(error) }
    let http = task.response as? HTTPURLResponse
    return .response(status: UInt16(clamping: http?.statusCode ?? 0), etag: http?.value(forHTTPHeaderField: "ETag"), body: "")
  }

  /// The app hands the completion of a wake over. A foreign identifier is a session of the app: `false`, nothing is taken. Otherwise `true`: the completion runs once on the main thread after the events of this wake and the quiet of the core. A completion handed after the last end of events waits for the next one.
  func handle(identifier: String, completion: @escaping @Sendable () -> Void) -> Bool {
    guard identifier == self.identifier else { return false }
    feed.handed(completion)
    return true
  }

  private func deliver(_ stream: AsyncStream<PutEvent>) async {
    var handed: [@Sendable () -> Void] = []
    var started: Set<PutIdentity> = []
    var ended: Set<PutIdentity> = []
    for await event in stream {
      switch event {
      case let .start(identity, target, file):
        guard let request = Self.request(target) else {
          await inputs.get().putEnded(identity: identity, outcome: .failed(failure: RawFailure(domain: "url", code: nil, message: "the PUT target is not a URL")))
          continue
        }
        started.insert(identity)
        session.upload(request, fromFile: URL(fileURLWithPath: file.path), description: PutDescription.text(identity))
      case let .cancel(identity):
        started.remove(identity)
        let text = PutDescription.text(identity)
        await session.tasks().filter { $0.taskDescription == text }.forEach { $0.cancel() }
      case let .resume(identities):
        let tasks = await session.tasks()
        let listed = Set(identities)
        var found: [PutIdentity: any PutTask] = [:]
        for task in tasks {
          guard let held = PutDescription.identity(task.taskDescription) else { continue }
          if listed.contains(held) { found[held] = task } else if !started.contains(held) { task.cancel() }
        }
        let inputs = await inputs.get()
        let delivered = ended
        ended = []
        for identity in identities where !delivered.contains(identity) {
          guard let task = found[identity] else { inputs.putMissing(identity: identity); continue }
          guard task.state == .completed else { continue }
          ended.insert(identity)
          inputs.putEnded(identity: identity, outcome: Self.outcome(of: task))
        }
      case let .sent(identity, bytes): await inputs.get().putProgress(identity: identity, sent: bytes)
      case let .ended(identity, outcome):
        started.remove(identity)
        guard ended.insert(identity).inserted else { continue }
        await inputs.get().putEnded(identity: identity, outcome: outcome)
      case let .handed(completion): handed.append(completion)
      case .finished:
        let ready = handed
        handed = []
        guard !ready.isEmpty else { continue }
        await inputs.get().quiet()
        ready.forEach { DispatchQueue.main.async(execute: $0) }
      }
    }
  }

  private static func request(_ target: PutTarget) -> URLRequest? {
    guard let address = URL(string: target.url) else { return nil }
    var request = URLRequest(url: address, timeoutInterval: TimeInterval(target.requestMs) / 1000)
    request.httpMethod = "PUT"
    target.headers.forEach { request.setValue($1, forHTTPHeaderField: $0) }
    return request
  }
}

/// The delegate of the background session: it reports to the feed and holds nothing else.
final class PutDelegate: NSObject, URLSessionTaskDelegate, Sendable {
  private let events: any PutEvents

  init(events: any PutEvents) { self.events = events }

  func urlSession(_ session: URLSession, task: URLSessionTask, didSendBodyData bytesSent: Int64, totalBytesSent: Int64, totalBytesExpectedToSend: Int64) {
    events.sent(description: task.taskDescription, bytes: UInt64(clamping: totalBytesSent))
  }

  func urlSession(_ session: URLSession, task: URLSessionTask, didCompleteWithError error: Error?) {
    events.ended(description: task.taskDescription, outcome: UrlSessionPuts.outcome(of: task))
  }

  func urlSessionDidFinishEvents(forBackgroundURLSession session: URLSession) { events.eventsFinished() }
}

/// The background `URLSession`: the system keeps its tasks over the death of the process. It starts with the executor, because the system delivers the events of an earlier wake at once.
final class BackgroundPutSession: PutSession, Sendable {
  private let session: URLSession

  init(identifier: String, events: any PutEvents) {
    let configuration = URLSessionConfiguration.background(withIdentifier: identifier)
    configuration.sessionSendsLaunchEvents = true
    configuration.waitsForConnectivity = true
    configuration.httpCookieStorage = nil
    configuration.httpShouldSetCookies = false
    configuration.urlCredentialStorage = nil
    configuration.urlCache = nil
    session = URLSession(configuration: configuration, delegate: PutDelegate(events: events), delegateQueue: nil)
  }

  func upload(_ request: URLRequest, fromFile file: URL, description: String) {
    let task = session.uploadTask(with: request, fromFile: file)
    task.taskDescription = description
    task.resume()
  }

  func tasks() async -> [any PutTask] { await session.allTasks }
}

extension DB {
  /// The PUT of a file in a background session is off: no identifier is the session of the library, so it returns `false` and the completion handler is not taken.
  @discardableResult
  public static func handleBackgroundEvents(identifier: String, completion: @escaping @Sendable () -> Void) -> Bool {
    false
  }
}
