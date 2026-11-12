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

/// What the session reports: the end of a task and the end of the events the system delivered for a wake.
protocol PutEvents: AnyObject, Sendable {
  func ended(description: String?, outcome: RawOutcome)
  func eventsFinished()
}

/// The inputs of the core for the outcomes of the system tasks.
protocol PutInputs: Sendable {
  func putEnded(identity: PutIdentity, outcome: RawOutcome)
  func putMissing(identity: PutIdentity)
}

extension Database: PutInputs {}

/// A value that arrives once: a reader before it waits for it.
final class Awaited<Value: Sendable>: @unchecked Sendable {
  private let lock = NSLock()
  private var value: Value?
  private var waiting: [CheckedContinuation<Value, Never>] = []

  func set(_ new: Value) {
    lock.lock()
    value = new
    let readers = waiting
    waiting = []
    lock.unlock()
    readers.forEach { $0.resume(returning: new) }
  }

  func get() async -> Value {
    await withCheckedContinuation { reader in
      lock.lock()
      if let value {
        lock.unlock()
        reader.resume(returning: value)
      } else {
        waiting.append(reader)
        lock.unlock()
      }
    }
  }
}

/// The text of an identity in the description of a system task, and back.
enum PutDescription {
  private static let prefix = "dblayer-put "

  static func text(_ identity: PutIdentity) -> String { "\(prefix)\(identity.epoch).\(identity.transferId).\(identity.attemptId)" }

  static func identity(_ text: String?) -> PutIdentity? {
    guard let text, text.hasPrefix(prefix) else { return nil }
    let numbers = text.dropFirst(prefix.count).split(separator: ".").compactMap { UInt64($0) }
    guard numbers.count == 3 else { return nil }
    return PutIdentity(epoch: numbers[0], transferId: numbers[1], attemptId: numbers[2])
  }

  /// A task of an earlier process names the same PUT with another epoch.
  static func key(_ identity: PutIdentity) -> String { "\(identity.transferId).\(identity.attemptId)" }
}

/// The executor of the PUTs outside the process: every call is one call of the system session or one input of the core.
final class UrlSessionPuts: PutExecutor, PutEvents, @unchecked Sendable {
  static let current = Awaited<UrlSessionPuts?>()

  private let inputs = Awaited<any PutInputs>()
  private let settled: @Sendable () async -> Void
  private let lock = NSLock()
  private var session: (any PutSession)?
  private var completion: (@Sendable () -> Void)?
  private var finished = false

  init(session: (UrlSessionPuts) -> any PutSession, settled: @escaping @Sendable () async -> Void = DB.settledTransfers) {
    self.settled = settled
    self.session = session(self)
  }

  /// The system session exists on iOS with a bundle id: macOS and the tests of the host run the PUTs in the process.
  static func available() -> UrlSessionPuts? {
    #if os(iOS)
    guard let bundle = Bundle.main.bundleIdentifier else { return nil }
    return UrlSessionPuts { BackgroundPutSession(identifier: "\(bundle).dblayer.uploads", events: $0) }
    #else
    return nil
    #endif
  }

  func attach(_ database: any PutInputs) { inputs.set(database) }

  private var live: any PutSession { lock.withLock { session! } }

  func startPut(identity: PutIdentity, target: PutTarget, file: PutFile) {
    guard let address = URL(string: target.url) else {
      Task { await inputs.get().putEnded(identity: identity, outcome: .failed(failure: RawFailure(domain: "url", code: nil, message: ""))) }
      return
    }
    var request = URLRequest(url: address, timeoutInterval: TimeInterval(target.requestMs) / 1000)
    request.httpMethod = "PUT"
    target.headers.forEach { request.setValue($1, forHTTPHeaderField: $0) }
    live.upload(request, fromFile: URL(fileURLWithPath: file.path), description: PutDescription.text(identity))
  }

  func cancelPut(identity: PutIdentity) {
    let text = PutDescription.text(identity)
    let session = live
    Task { await session.tasks().filter { $0.taskDescription == text }.forEach { $0.cancel() } }
  }

  func resumePuts(identities: [PutIdentity]) {
    let session = live
    Task {
      let tasks = await session.tasks()
      let listed = Set(identities.map(PutDescription.key))
      var found: [String: any PutTask] = [:]
      for task in tasks {
        guard let held = PutDescription.identity(task.taskDescription) else { continue }
        if listed.contains(PutDescription.key(held)) { found[PutDescription.key(held)] = task } else { task.cancel() }
      }
      let inputs = await inputs.get()
      for identity in identities {
        guard let task = found[PutDescription.key(identity)] else { inputs.putMissing(identity: identity); continue }
        if task.state == .completed { inputs.putEnded(identity: identity, outcome: Self.outcome(of: task)) }
      }
    }
  }

  static func outcome(of task: any PutTask) -> RawOutcome {
    if let error = task.error { return .failed(error) }
    let http = task.response as? HTTPURLResponse
    return .response(status: UInt16(clamping: http?.statusCode ?? 0), etag: http?.value(forHTTPHeaderField: "ETag"), body: "")
  }

  func ended(description: String?, outcome: RawOutcome) {
    guard let identity = PutDescription.identity(description) else { return }
    Task { await inputs.get().putEnded(identity: identity, outcome: outcome) }
  }

  /// The app handed the completion of the wake over: it runs once the events are delivered and the core is quiet.
  func handle(completion: @escaping @Sendable () -> Void) {
    let ready = lock.withLock { () -> Bool in
      guard finished else { self.completion = completion; return false }
      finished = false
      return true
    }
    if ready { run(completion) }
  }

  func eventsFinished() {
    let taken = lock.withLock { () -> (@Sendable () -> Void)? in
      guard let taken = completion else { finished = true; return nil }
      completion = nil
      return taken
    }
    if let taken { run(taken) }
  }

  private func run(_ completion: @escaping @Sendable () -> Void) {
    Task {
      await settled()
      DispatchQueue.main.async(execute: completion)
    }
  }
}

/// The background `URLSession`: the system keeps its tasks over the death of the process.
final class BackgroundPutSession: NSObject, PutSession, URLSessionTaskDelegate, @unchecked Sendable {
  private let events: any PutEvents
  private var session: URLSession!

  init(identifier: String, events: any PutEvents) {
    self.events = events
    super.init()
    let configuration = URLSessionConfiguration.background(withIdentifier: identifier)
    configuration.sessionSendsLaunchEvents = true
    configuration.waitsForConnectivity = true
    configuration.httpCookieStorage = nil
    configuration.httpShouldSetCookies = false
    configuration.urlCredentialStorage = nil
    configuration.urlCache = nil
    session = URLSession(configuration: configuration, delegate: self, delegateQueue: nil)
  }

  func upload(_ request: URLRequest, fromFile file: URL, description: String) {
    let task = session.uploadTask(with: request, fromFile: file)
    task.taskDescription = description
    task.resume()
  }

  func tasks() async -> [any PutTask] { await session.allTasks }

  func urlSession(_ session: URLSession, task: URLSessionTask, didCompleteWithError error: Error?) {
    events.ended(description: task.taskDescription, outcome: UrlSessionPuts.outcome(of: task))
  }

  func urlSessionDidFinishEvents(forBackgroundURLSession session: URLSession) { events.eventsFinished() }
}

extension DB {
  /// The app calls it from `application(_:handleEventsForBackgroundURLSession:completionHandler:)` with the identifier and the completion handler the system gave. Before `open` the call waits for it. The completion handler runs on the main thread once the library has the outcomes of the PUTs that ended while the app was away and their transfers have no step in the network.
  public static func handleBackgroundEvents(identifier: String, completion: @escaping @Sendable () -> Void) {
    Task {
      guard let puts = await UrlSessionPuts.current.get() else { return DispatchQueue.main.async(execute: completion) }
      puts.handle(completion: completion)
    }
  }

  /// The queue of operations has no transfer in a PUT or in the steps that follow it.
  @Sendable static func settledTransfers() async {
    guard let account = try? account() else { return }
    for await item in account.operations() {
      guard case let .queue(_, items) = item else { return }
      if !items.contains(where: busy) { return }
    }
  }

  private static func busy(_ operation: OperationInfo) -> Bool {
    guard let status = operation.upload?.status else { return false }
    return status == .uploading || status == .sending
  }
}
