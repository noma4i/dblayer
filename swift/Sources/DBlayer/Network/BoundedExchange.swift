import Foundation

struct ResponseLimitError: Error {
  let limit: UInt64
}

/// One request whose answer is counted while it is read: the byte after `limit` aborts the request and the answer never sits in memory above the limit.
/// The state of the request is touched only on the delegate queue of the session, which is serial.
class BoundedExchange: RefuseRedirect, URLSessionDataDelegate, @unchecked Sendable {
  private let limit: UInt64
  private let progress: ByteProgress?
  private var queue: OperationQueue?
  private var task: URLSessionTask?
  private var continuation: CheckedContinuation<(Data, URLResponse), Error>?
  private var body = Data()
  private var failure: Error?

  init(limit: UInt64, progress: ByteProgress? = nil) {
    self.limit = limit
    self.progress = progress
    super.init()
  }

  func run(in session: URLSession, _ make: (URLSession) -> URLSessionTask) async throws -> (Data, URLResponse) {
    let task = make(session)
    task.delegate = self
    queue = session.delegateQueue
    self.task = task
    return try await withTaskCancellationHandler {
      try await withCheckedThrowingContinuation { continuation in
        self.continuation = continuation
        task.resume()
      }
    } onCancel: {
      task.cancel()
    }
  }

  func fail(_ error: Error) {
    queue?.addOperation { [self] in
      if failure == nil { failure = error }
      task?.cancel()
    }
  }

  func urlSession(_ session: URLSession, task: URLSessionTask, didSendBodyData bytesSent: Int64, totalBytesSent: Int64, totalBytesExpectedToSend: Int64) {
    progress?.sent(bytes: UInt64(totalBytesSent))
  }

  func urlSession(_ session: URLSession, dataTask: URLSessionDataTask, didReceive response: URLResponse, completionHandler: @escaping (URLSession.ResponseDisposition) -> Void) {
    if response.expectedContentLength > 0, UInt64(response.expectedContentLength) > limit {
      failure = ResponseLimitError(limit: limit)
      completionHandler(.cancel)
    } else {
      completionHandler(.allow)
    }
  }

  func urlSession(_ session: URLSession, dataTask: URLSessionDataTask, didReceive data: Data) {
    guard failure == nil else { return }
    if UInt64(body.count) + UInt64(data.count) > limit {
      failure = ResponseLimitError(limit: limit)
      body = Data()
      dataTask.cancel()
    } else {
      body.append(data)
    }
  }

  func urlSession(_ session: URLSession, task: URLSessionTask, didCompleteWithError error: Error?) {
    let continuation = continuation
    self.continuation = nil
    if let reason = failure ?? error {
      continuation?.resume(throwing: reason)
    } else if let response = task.response {
      continuation?.resume(returning: (body, response))
    } else {
      continuation?.resume(throwing: URLError(.badServerResponse))
    }
  }
}
