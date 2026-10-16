import Foundation

public final class UrlSessionEncryptedHttp: EncryptedHttp, @unchecked Sendable {
  private let origin: URL
  private let session: URLSession

  public init(origin: URL, session: URLSession = .shared) {
    self.origin = origin
    self.session = session
  }

  public func post(path: String, body: String) async throws -> EncryptedReply {
    try await send(path: path, method: "POST", body: Data(body.utf8))
  }

  public func get(path: String) async throws -> EncryptedReply {
    try await send(path: path, method: "GET", body: nil)
  }

  private func send(path: String, method: String, body: Data?) async throws -> EncryptedReply {
    guard let url = URL(string: path, relativeTo: origin)?.absoluteURL else { throw DbError.Invalid(detail: "bad request path \(path)") }
    var request = URLRequest(url: url, timeoutInterval: 30)
    request.httpMethod = method
    request.httpShouldHandleCookies = false
    if let body {
      request.setValue("application/jose", forHTTPHeaderField: "Content-Type")
      request.httpBody = body
    }
    do {
      let (data, response) = try await session.data(for: request)
      let status = (response as? HTTPURLResponse)?.statusCode ?? 0
      return EncryptedReply(status: UInt16(clamping: status), body: String(decoding: data, as: UTF8.self))
    } catch {
      throw DbError.Network(detail: error.localizedDescription)
    }
  }
}
