import Foundation

public final class UrlSessionEncryptedHttp: EncryptedHttp, @unchecked Sendable {
  private let origin: String
  private let session: URLSession
  private let headers: @Sendable () -> [String: String]

  public convenience init(origin: URL, headers: @escaping @Sendable () -> [String: String] = { [:] }, session: URLSession? = nil) {
    self.init(origin: origin.absoluteString, headers: headers, session: session)
  }

  init(origin: String, headers: @escaping @Sendable () -> [String: String] = { [:] }, session: URLSession? = nil) {
    self.origin = origin
    self.headers = headers
    let configuration = session?.configuration ?? .ephemeral
    configuration.timeoutIntervalForRequest = 30
    configuration.timeoutIntervalForResource = 30
    configuration.httpAdditionalHeaders = nil
    configuration.httpCookieStorage = nil
    configuration.httpShouldSetCookies = false
    configuration.urlCredentialStorage = nil
    configuration.urlCache = nil
    configuration.requestCachePolicy = .reloadIgnoringLocalCacheData
    self.session = URLSession(configuration: configuration)
  }

  deinit { session.invalidateAndCancel() }

  public func post(path: String, body: String, purpose: NetworkPurpose) async throws -> EncryptedReply {
    guard let base = URL(string: origin), let url = URL(string: path, relativeTo: base)?.absoluteURL,
          url.scheme == base.scheme, url.host == base.host, url.port == base.port,
          url.query == nil, url.fragment == nil else { throw DbError.Config(code: .invalidRequest, detail: "bad request path") }
    var request = URLRequest(url: url, timeoutInterval: 30)
    request.httpMethod = "POST"
    request.httpShouldHandleCookies = false
    headers().forEach { request.setValue($1, forHTTPHeaderField: $0) }
    request.setValue("application/jose", forHTTPHeaderField: "Content-Type")
    request.setValue("", forHTTPHeaderField: "Accept")
    request.setValue("", forHTTPHeaderField: "Accept-Language")
    request.setUserAgent()
    request.httpBody = Data(body.utf8)
    do {
      let (data, response) = try await session.data(for: request, delegate: RefuseRedirect())
      let status = (response as? HTTPURLResponse)?.statusCode ?? 0
      return EncryptedReply(status: UInt16(clamping: status), body: String(decoding: data, as: UTF8.self))
    } catch {
      throw TransportFailure.classifyEncryptedHttp(error)
    }
  }
}
