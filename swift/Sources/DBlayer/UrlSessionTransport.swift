import Foundation

public final class UrlSessionTransport: Transport, EncryptedHttp, @unchecked Sendable {
  private let headers: @Sendable () -> [String: String]
  private let http: UrlSessionHttp
  private let session: URLSession
  private let cable: UrlSessionCable
  private let trace: Bool

  /// `trace: true` writes one `os_log` line per HTTP exchange, file PUT and cable event (subsystem `com.noma4i.dblayer`, category `dblayer.transport`): metadata only, for development.
  public init(headers: @escaping @Sendable () -> [String: String] = { [:] }, session: URLSession? = nil, trace: Bool = false) {
    let configuration = session?.configuration ?? .ephemeral
    configuration.httpAdditionalHeaders = nil
    configuration.httpCookieStorage = nil
    configuration.httpShouldSetCookies = false
    configuration.urlCredentialStorage = nil
    configuration.urlCache = nil
    configuration.requestCachePolicy = .reloadIgnoringLocalCacheData
    let own = URLSession(configuration: configuration)
    self.headers = headers
    self.session = own
    self.trace = trace
    self.http = UrlSessionHttp(session: own, trace: trace)
    self.cable = UrlSessionCable(trace: trace) { own.webSocketTask(with: $0) }
  }

  deinit { session.invalidateAndCancel() }

  public func appHeaders() -> [String: String] { headers() }

  public func send(request: HttpRequest) async -> RawOutcome { await http.exchange(request) }

  public func openCable(url: String, headers: [String: String], connectMs: UInt64, events: CableEvents) async -> CableOutcome { await cable.open(url: url, headers: headers, connectMs: connectMs, events: events) }

  /// URLSession has no connect timeout: `target.connectMs` is not applied. `target.requestMs` is the idle bound, a file has no total bound.
  public func put(target: PutTarget, file: PutFile, identity: TransferIdentity, progress: ByteProgress) async -> RawOutcome {
    guard let address = URL(string: target.url) else { return .failed(failure: RawFailure(domain: "url", code: nil, message: "")) }
    var request = URLRequest(url: address, timeoutInterval: TimeInterval(target.requestMs) / 1000)
    request.httpMethod = "PUT"
    target.headers.forEach { request.setValue($1, forHTTPHeaderField: $0) }
    do {
      let size = try FileManager.default.attributesOfItem(atPath: file.path)[.size] as? UInt64 ?? 0
      let response: URLResponse
      let body: Data
      if file.offset == 0 && file.length == size {
        (body, response) = try await BoundedExchange(limit: target.responseLimit, progress: progress, trace: trace ? TraceCall(put: address, identity: identity) : nil).run(in: session) { $0.uploadTask(with: request, fromFile: URL(fileURLWithPath: file.path)) }
      } else {
        (body, response) = try await PartUpload(file: file, progress: progress, limit: target.responseLimit, trace: trace ? TraceCall(put: address, identity: identity) : nil).run(request, in: session)
      }
      let http = response as? HTTPURLResponse
      return .response(status: UInt16(clamping: http?.statusCode ?? 0), etag: http?.value(forHTTPHeaderField: "ETag"), body: String(decoding: body, as: UTF8.self))
    } catch {
      return .failed(error)
    }
  }
}
