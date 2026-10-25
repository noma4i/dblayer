import Foundation

public protocol UploadTaskSource: AnyObject, Sendable {
  var identifier: String { get }
  func attach(_ events: UploadTaskEvents)
  func liveTags() async -> [String]
  func start(_ upload: UploadTaskRequest)
  func cancel(tag: String)
}

public protocol UploadTaskEvents: AnyObject, Sendable {
  func progress(tag: String, sent: UInt64)
  func finished(tag: String, status: Int, etag: String?)
  func failed(tag: String, error: Error)
  func sessionEventsFinished()
}

public struct UploadTaskRequest: Sendable {
  public let request: URLRequest
  public let file: URL
  public let tag: String
}

protocol PutProgress: Sendable {
  func sent(bytes: UInt64)
  func isCancelled() -> Bool
}

extension ByteProgress: PutProgress {}

private struct Waiter {
  let id: UUID
  let progress: PutProgress
  let continuation: CheckedContinuation<PutReply, Error>
}

final class BackgroundPuts: UploadTaskEvents, @unchecked Sendable {
  let source: UploadTaskSource
  private let lock = NSLock()
  private var waiters: [String: [Waiter]] = [:]
  private var live: Set<String> = []
  private var buffered: [String: PutTask] = [:]
  private var sink: (([PutTask]) -> Void)?
  private var eventsDone = false
  private var delivered: Set<String> = []
  private var eventWaiters: [CheckedContinuation<Void, Never>] = []
  private var ready: Task<Void, Never>?

  init(source: UploadTaskSource) {
    self.source = source
    source.attach(self)
    ready = Task { [self] in
      let tags = await source.liveTags()
      lock.withLock { live.formUnion(tags) }
      report(tags.map { PutTask(tag: $0, state: .running) })
    }
  }

  func connect(_ sink: @escaping ([PutTask]) -> Void) {
    let held = lock.withLock { () -> [PutTask] in
      self.sink = sink
      defer { buffered = [:] }
      return Array(buffered.values)
    }
    if !held.isEmpty { sink(held) }
  }

  func deliveredTags() -> [String] { lock.withLock { Array(delivered) } }

  func eventsFinished() async {
    await withCheckedContinuation { continuation in
      let done = lock.withLock { () -> Bool in
        if !eventsDone { eventWaiters.append(continuation) }
        return eventsDone
      }
      if done { continuation.resume() }
    }
  }

  func put(url: String, headers: [String: String], file: PutFile, tag: String, progress: PutProgress) async throws -> PutReply {
    guard let target = URL(string: url) else { throw DbError.Invalid(detail: "bad upload url") }
    guard file.offset == 0 else { throw DbError.Invalid(detail: "a background upload sends a whole file") }
    var request = URLRequest(url: target)
    request.httpMethod = "PUT"
    headers.forEach { request.setValue($1, forHTTPHeaderField: $0) }
    request.setUserAgent()
    await ready?.value
    let id = UUID()
    return try await withTaskCancellationHandler {
      try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<PutReply, Error>) in
        enum Plan { case settled(PutTask), wait, start }
        let plan = lock.withLock { () -> Plan in
          if let task = buffered[tag], task.state != .running {
            buffered[tag] = nil
            return .settled(task)
          }
          waiters[tag, default: []].append(Waiter(id: id, progress: progress, continuation: continuation))
          return live.insert(tag).inserted ? .start : .wait
        }
        switch plan {
        case .settled(let task):
          lock.withLock { waiters[tag]?.removeAll { $0.id == id } }
          switch task.state {
          case .finished(let status, let etag): continuation.resume(returning: PutReply(status: status, etag: etag))
          case .failed(let detail): continuation.resume(throwing: TransportFailure.ofStoredTask(detail: detail))
          case .running: continuation.resume(throwing: DbError.Interrupted(detail: "the upload task is still running"))
          }
        case .start: source.start(UploadTaskRequest(request: request, file: URL(fileURLWithPath: file.path), tag: tag))
        case .wait: break
        }
        if Task.isCancelled { drop(tag: tag, id: id) }
      }
    } onCancel: {
      drop(tag: tag, id: id)
    }
  }

  private func drop(tag: String, id: UUID) {
    let dropped = lock.withLock { () -> Waiter? in
      guard let index = waiters[tag]?.firstIndex(where: { $0.id == id }) else { return nil }
      return waiters[tag]?.remove(at: index)
    }
    dropped?.continuation.resume(throwing: CancellationError())
  }

  func progress(tag: String, sent: UInt64) {
    let current = lock.withLock { waiters[tag] ?? [] }
    for waiter in current {
      waiter.progress.sent(bytes: sent)
      if waiter.progress.isCancelled() { source.cancel(tag: tag) }
    }
  }

  func finished(tag: String, status: Int, etag: String?) {
    lock.withLock { _ = delivered.insert(tag) }
    let current = settle(tag)
    if current.isEmpty {
      report([PutTask(tag: tag, state: .finished(status: UInt16(clamping: status), etag: etag))])
    }
    current.forEach { $0.continuation.resume(returning: PutReply(status: UInt16(clamping: status), etag: etag)) }
  }

  func failed(tag: String, error: Error) {
    lock.withLock { _ = delivered.insert(tag) }
    let current = settle(tag)
    if current.isEmpty {
      report([PutTask(tag: tag, state: .failed(detail: error.localizedDescription))])
    }
    current.forEach { $0.continuation.resume(throwing: TransportFailure.classifySocket(error)) }
  }

  func sessionEventsFinished() {
    let resumed = lock.withLock { () -> [CheckedContinuation<Void, Never>] in
      eventsDone = true
      defer { eventWaiters = [] }
      return eventWaiters
    }
    resumed.forEach { $0.resume() }
  }

  private func settle(_ tag: String) -> [Waiter] {
    lock.withLock {
      live.remove(tag)
      return waiters.removeValue(forKey: tag) ?? []
    }
  }

  private func report(_ tasks: [PutTask]) {
    guard !tasks.isEmpty else { return }
    let target = lock.withLock { () -> (([PutTask]) -> Void)? in
      if sink == nil { tasks.forEach { buffered[$0.tag] = $0 } }
      return sink
    }
    target?(tasks)
  }
}
