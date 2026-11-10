import Foundation

/// One PUT of a file range: the body leaves through a bound stream fed from the file in pieces of at most `chunk` bytes.
final class PartUpload: BoundedExchange, @unchecked Sendable {
  static let chunk = 64 * 1024

  private let file: PutFile

  init(file: PutFile, progress: ByteProgress, limit: UInt64) {
    self.file = file
    super.init(limit: limit, progress: progress)
  }

  func run(_ request: URLRequest, in session: URLSession) async throws -> (Data, URLResponse) {
    var request = request
    request.setValue(String(file.length), forHTTPHeaderField: "Content-Length")
    return try await run(in: session) { $0.uploadTask(withStreamedRequest: request) }
  }

  func urlSession(_ session: URLSession, task: URLSessionTask, needNewBodyStream completionHandler: @escaping (InputStream?) -> Void) {
    completionHandler(open())
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
