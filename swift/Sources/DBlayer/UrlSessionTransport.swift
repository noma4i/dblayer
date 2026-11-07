import Foundation

public final class UrlSessionTransport: Transport, @unchecked Sendable {
  private let address: URL
  private let http: UrlSessionHttp
  private let session: URLSession
  private let cable: UrlSessionCable

  public init(endpoint: URL, headers: @escaping @Sendable () -> [String: String] = { [:] }, session: URLSession = .shared) {
    self.address = endpoint
    self.session = session
    self.http = UrlSessionHttp(session: session, headers: headers)
    self.cable = UrlSessionCable(headers: headers) { session.webSocketTask(with: $0) }
  }

  public func endpoint() -> String { address.absoluteString }

  public func send(request: HttpRequest) async -> RawOutcome { await http.run(request) }

  public func openCable(url: String, connectMs: UInt64, events: CableEvents) async -> CableOutcome { await cable.open(url: url, connectMs: connectMs, events: events) }

  public func put(url: String, headers: [String: String], file: PutFile, identity: TransferIdentity, progress: ByteProgress) async -> RawOutcome {
    guard let target = URL(string: url) else { return .failed(.io, "invalid upload address") }
    var request = URLRequest(url: target)
    request.httpMethod = "PUT"
    headers.forEach { request.setValue($1, forHTTPHeaderField: $0) }
    request.setUserAgent()
    do {
      let delegate = PutProgress(progress)
      let size = try FileManager.default.attributesOfItem(atPath: file.path)[.size] as? UInt64 ?? 0
      let response: URLResponse
      if file.offset == 0 && file.length == size {
        (_, response) = try await session.upload(for: request, fromFile: URL(fileURLWithPath: file.path), delegate: delegate)
      } else {
        (_, response) = try await session.upload(for: request, from: try part(file), delegate: delegate)
      }
      let http = response as? HTTPURLResponse
      return .response(status: UInt16(clamping: http?.statusCode ?? 0), etag: http?.value(forHTTPHeaderField: "ETag"), body: "")
    } catch {
      return .failed(error)
    }
  }

  private func part(_ file: PutFile) throws -> Data {
    let handle = try FileHandle(forReadingFrom: URL(fileURLWithPath: file.path))
    defer { try? handle.close() }
    try handle.seek(toOffset: file.offset)
    let bytes = try handle.read(upToCount: Int(file.length)) ?? Data()
    guard UInt64(bytes.count) == file.length else { throw URLError(.cannotDecodeRawData) }
    return bytes
  }
}

private final class PutProgress: RefuseRedirect, @unchecked Sendable {
  private let progress: ByteProgress

  init(_ progress: ByteProgress) {
    self.progress = progress
    super.init()
  }

  func urlSession(_ session: URLSession, task: URLSessionTask, didSendBodyData bytesSent: Int64, totalBytesSent: Int64, totalBytesExpectedToSend: Int64) {
    progress.sent(bytes: UInt64(totalBytesSent))
    if progress.isCancelled() { task.cancel() }
  }
}
