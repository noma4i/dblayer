import Foundation

final class BackgroundWake: @unchecked Sendable {
  private let lock = NSLock()
  private var uploads: UrlSessionTransport?
  private var database: Database?
  private var attached = false
  private var waiting: [(identifier: String, completion: @Sendable () -> Void)] = []

  func handle(identifier: String, completion: @escaping @Sendable () -> Void) {
    let target = lock.withLock { () -> (UrlSessionTransport?, Database?)? in
      guard attached else {
        waiting.append((identifier, completion))
        return nil
      }
      return (uploads, database)
    }
    guard let target else { return }
    run(identifier: identifier, completion: completion, uploads: target.0, database: target.1)
  }

  func attach(uploads: UrlSessionTransport?, database: Database) {
    let (drained, previous) = lock.withLock { () -> ([(identifier: String, completion: @Sendable () -> Void)], (UrlSessionTransport?, Database?)) in
      let previous = (self.uploads, self.database)
      self.uploads = uploads
      self.database = database
      attached = true
      defer { waiting = [] }
      return (waiting, previous)
    }
    withExtendedLifetime(previous) {}
    for wake in drained { run(identifier: wake.identifier, completion: wake.completion, uploads: uploads, database: database) }
  }

  func detach() {
    let released = lock.withLock { () -> (UrlSessionTransport?, Database?) in
      defer { uploads = nil; database = nil; attached = false }
      return (uploads, database)
    }
    withExtendedLifetime(released) {}
  }

  private func run(identifier: String, completion: @escaping @Sendable () -> Void, uploads: UrlSessionTransport?, database: Database?) {
    guard let uploads, let database, uploads.uploadSessionIdentifier == identifier else {
      completion()
      return
    }
    Task {
      await uploads.uploadEventsFinished()
      do { try await database.resumeUploadsFor(tags: uploads.uploadDeliveredTags()) } catch { NSLog("dblayer: resumeUploads failed in the background wake: %@", String(describing: error)) }
      await MainActor.run { completion() }
    }
  }
}
