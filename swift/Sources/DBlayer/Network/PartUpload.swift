import Foundation

/// One PUT of a file range: the body leaves through a bound stream fed from the file in pieces of at most `chunk` bytes.
/// The state of the request is touched only on the delegate queue of the session, which is serial.
final class PartUpload: RefuseRedirect, URLSessionDataDelegate, @unchecked Sendable {
  static let chunk = 64 * 1024

  private let file: PutFile
  private let progress: ByteProgress
  private var queue: OperationQueue?
  private var task: URLSessionTask?
  private var continuation: CheckedContinuation<(Data, URLResponse), Error>?
  private var body = Data()
  private var failure: Error?

  init(file: PutFile, progress: ByteProgress) {
    self.file = file
    self.progress = progress
    super.init()
  }

  func run(_ request: URLRequest, in session: URLSession) async throws -> (Data, URLResponse) {
    var request = request
    request.setValue(String(file.length), forHTTPHeaderField: "Content-Length")
    let task = session.uploadTask(withStreamedRequest: request)
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

  func urlSession(_ session: URLSession, task: URLSessionTask, needNewBodyStream completionHandler: @escaping (InputStream?) -> Void) {
    completionHandler(open())
  }

  func urlSession(_ session: URLSession, task: URLSessionTask, didSendBodyData bytesSent: Int64, totalBytesSent: Int64, totalBytesExpectedToSend: Int64) {
    progress.sent(bytes: UInt64(totalBytesSent))
  }

  func urlSession(_ session: URLSession, dataTask: URLSessionDataTask, didReceive data: Data) {
    body.append(data)
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

  private func fail(_ error: Error) {
    queue?.addOperation { [self] in
      if failure == nil { failure = error }
      task?.cancel()
    }
  }

  private func open() -> InputStream? {
    var input: InputStream?
    var output: OutputStream?
    Stream.getBoundStreams(withBufferSize: Self.chunk, inputStream: &input, outputStream: &output)
    guard let input, let output else { return nil }
    let feed = Feed(output: output)
    let (path, offset, length) = (file.path, file.offset, file.length)
    Thread.detachNewThread { [weak self] in
      if let error = feed.pump(path: path, offset: offset, length: length) { self?.fail(error) }
    }
    return input
  }

  private final class Feed: @unchecked Sendable {
    let output: OutputStream

    init(output: OutputStream) { self.output = output }

    /// Returns the reason when the file cannot give the whole range. A reader that went away ends the pump without a reason.
    func pump(path: String, offset: UInt64, length: UInt64) -> Error? {
      output.open()
      defer { output.close() }
      let descriptor = Darwin.open(path, O_RDONLY)
      guard descriptor >= 0 else { return POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO) }
      defer { Darwin.close(descriptor) }
      var buffer = [UInt8](repeating: 0, count: PartUpload.chunk)
      var position = offset
      var remaining = length
      while remaining > 0 {
        let count = pread(descriptor, &buffer, Int(min(remaining, UInt64(buffer.count))), off_t(position))
        if count < 0 { return POSIXError(POSIXErrorCode(rawValue: errno) ?? .EIO) }
        if count == 0 { return URLError(.cannotDecodeRawData) }
        var done = 0
        while done < count {
          let written = buffer.withUnsafeBufferPointer { output.write($0.baseAddress! + done, maxLength: count - done) }
          if written <= 0 { return nil }
          done += written
        }
        position += UInt64(count)
        remaining -= UInt64(count)
      }
      return nil
    }
  }
}
