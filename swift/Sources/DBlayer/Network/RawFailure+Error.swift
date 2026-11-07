import Foundation

extension RawFailure {
  private static let tls: Set<URLError.Code> = [.secureConnectionFailed, .serverCertificateHasBadDate, .serverCertificateUntrusted, .serverCertificateHasUnknownRoot, .serverCertificateNotYetValid, .clientCertificateRejected, .clientCertificateRequired]
  private static let unreachable: Set<URLError.Code> = [.notConnectedToInternet, .cannotFindHost, .cannotConnectToHost, .dnsLookupFailed, .dataNotAllowed, .internationalRoamingOff]

  init(_ error: Error) {
    guard let failure = error as? URLError else {
      self.init(kind: error is CancellationError ? .cancelled : .io, code: nil, message: error.localizedDescription)
      return
    }
    let kind: RawFailureKind
    if failure.code == .cancelled { kind = .cancelled }
    else if Self.tls.contains(failure.code) { kind = .tls }
    else if Self.unreachable.contains(failure.code) { kind = .unreachable }
    else if failure.code == .timedOut { kind = .timeout }
    else { kind = .io }
    self.init(kind: kind, code: Int64(failure.code.rawValue), message: failure.localizedDescription)
  }
}

extension RawOutcome {
  static func failed(_ error: Error) -> RawOutcome { .failed(failure: RawFailure(error)) }
  static func failed(_ kind: RawFailureKind, _ message: String) -> RawOutcome { .failed(failure: RawFailure(kind: kind, code: nil, message: message)) }
}

struct UrlSessionHttp: Sendable {
  let session: URLSession
  let headers: @Sendable () -> [String: String]

  func run(_ request: HttpRequest) async -> RawOutcome {
    guard let url = URL(string: request.url) else { return .failed(.io, "invalid request address") }
    var urlRequest = URLRequest(url: url, timeoutInterval: TimeInterval(request.requestMs) / 1000)
    urlRequest.httpMethod = request.method
    urlRequest.httpShouldHandleCookies = false
    headers().forEach { urlRequest.setValue($1, forHTTPHeaderField: $0) }
    request.headers.forEach { urlRequest.setValue($1, forHTTPHeaderField: $0) }
    urlRequest.setUserAgent()
    urlRequest.httpBody = Data(request.body.utf8)
    do {
      let (data, response) = try await session.data(for: urlRequest, delegate: RefuseRedirect())
      let status = (response as? HTTPURLResponse)?.statusCode ?? 0
      return .response(status: UInt16(clamping: status), etag: nil, body: String(decoding: data, as: UTF8.self))
    } catch {
      return .failed(error)
    }
  }
}
