import Foundation

public final class UrlSessionEncryptedHttp: EncryptedHttp, @unchecked Sendable {
  private let session: URLSession
  private let http: UrlSessionHttp

  public convenience init(origin: URL, headers: @escaping @Sendable () -> [String: String] = { [:] }, session: URLSession? = nil) {
    self.init(origin: origin.absoluteString, headers: headers, session: session)
  }

  init(origin: String, headers: @escaping @Sendable () -> [String: String] = { [:] }, session: URLSession? = nil) {
    let configuration = session?.configuration ?? .ephemeral
    configuration.httpAdditionalHeaders = nil
    configuration.httpCookieStorage = nil
    configuration.httpShouldSetCookies = false
    configuration.urlCredentialStorage = nil
    configuration.urlCache = nil
    configuration.requestCachePolicy = .reloadIgnoringLocalCacheData
    self.session = URLSession(configuration: configuration)
    self.http = UrlSessionHttp(session: self.session, headers: headers)
  }

  deinit { session.invalidateAndCancel() }

  public func send(request: HttpRequest) async -> RawOutcome { await http.run(request) }
}
