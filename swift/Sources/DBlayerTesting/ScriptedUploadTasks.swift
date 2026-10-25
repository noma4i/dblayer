import DBlayer
import Foundation

public final class ScriptedUploadTasks: UploadTaskSource, @unchecked Sendable {
  public let identifier: String
  private let lock = NSLock()
  private var events: UploadTaskEvents?
  private var live: [String]
  private var started: [UploadTaskRequest] = []
  private var cancelled: [String] = []

  public init(identifier: String = "test.uploads", live: [String] = []) {
    self.identifier = identifier
    self.live = live
  }

  public var startedTasks: [UploadTaskRequest] { lock.withLock { started } }
  public var cancelledTags: [String] { lock.withLock { cancelled } }

  public func attach(_ events: UploadTaskEvents) { lock.withLock { self.events = events } }
  public func liveTags() async -> [String] { lock.withLock { live } }
  public func start(_ upload: UploadTaskRequest) { lock.withLock { started.append(upload) } }
  public func cancel(tag: String) { lock.withLock { cancelled.append(tag) } }

  public func sendProgress(tag: String, sent: UInt64) { lock.withLock { events }?.progress(tag: tag, sent: sent) }
  public func finish(tag: String, status: Int = 200, etag: String?) { lock.withLock { events }?.finished(tag: tag, status: status, etag: etag) }
  public func fail(tag: String, error: Error) { lock.withLock { events }?.failed(tag: tag, error: error) }
  public func finishSessionEvents() { lock.withLock { events }?.sessionEventsFinished() }
}
