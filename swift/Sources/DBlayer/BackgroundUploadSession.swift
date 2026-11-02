import Foundation

final class BackgroundUploadSession: NSObject, UploadTaskSource, URLSessionTaskDelegate, @unchecked Sendable {
  let identifier: String
  private let lock = NSLock()
  private var events: UploadTaskEvents?
  private lazy var session = URLSession(configuration: configuration(), delegate: self, delegateQueue: nil)

  init(identifier: String = BackgroundUploadSession.defaultIdentifier) {
    self.identifier = identifier
    super.init()
  }

  static var defaultIdentifier: String { "\(Bundle.main.bundleIdentifier ?? "app").dblayer.uploads" }

  private func configuration() -> URLSessionConfiguration {
    let configuration = URLSessionConfiguration.background(withIdentifier: identifier)
    configuration.sessionSendsLaunchEvents = true
    configuration.waitsForConnectivity = true
    return configuration
  }

  func attach(_ events: UploadTaskEvents) { _ = lock.withLock { () -> UploadTaskEvents? in defer { self.events = events }; return self.events } }

  func liveTags() async -> [String] {
    await session.allTasks.compactMap { $0.state == .running || $0.state == .suspended ? $0.taskDescription : nil }
  }

  func start(_ upload: UploadTaskRequest) {
    let task = session.uploadTask(with: upload.request, fromFile: upload.file)
    task.taskDescription = upload.tag
    task.resume()
  }

  func cancel(tag: String) {
    Task { [session] in await session.allTasks.filter { $0.taskDescription == tag }.forEach { $0.cancel() } }
  }

  func urlSession(_ session: URLSession, task: URLSessionTask, didSendBodyData bytesSent: Int64, totalBytesSent: Int64, totalBytesExpectedToSend: Int64) {
    guard let tag = task.taskDescription else { return }
    lock.withLock { events }?.progress(tag: tag, sent: UInt64(totalBytesSent))
  }

  func urlSession(_ session: URLSession, task: URLSessionTask, didCompleteWithError error: Error?) {
    guard let tag = task.taskDescription, let events = lock.withLock({ events }) else { return }
    if let error {
      events.failed(tag: tag, error: error)
      return
    }
    let http = task.response as? HTTPURLResponse
    events.finished(tag: tag, status: http?.statusCode ?? 0, etag: http?.value(forHTTPHeaderField: "ETag"))
  }

  func urlSessionDidFinishEvents(forBackgroundURLSession session: URLSession) {
    lock.withLock { events }?.sessionEventsFinished()
  }
}
