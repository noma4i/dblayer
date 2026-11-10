import Foundation

extension RawFailure {
  init(_ error: Error) {
    if let limit = error as? ResponseLimitError {
      self.init(domain: "DBlayer.ResponseLimit", code: 1, message: "the answer is above the limit of \(limit.limit) bytes")
      return
    }
    let native = error as NSError
    self.init(domain: native.domain, code: Int64(native.code), message: error.localizedDescription)
  }
}

extension RawOutcome {
  static func failed(_ error: Error) -> RawOutcome { .failed(failure: RawFailure(error)) }
}

struct UrlSessionHttp: Sendable {
  let session: URLSession

  func exchange(_ request: HttpRequest) async -> RawOutcome {
    guard let url = URL(string: request.url) else { return .failed(failure: RawFailure(domain: "url", code: nil, message: "")) }
    var urlRequest = URLRequest(url: url, timeoutInterval: TimeInterval(request.requestMs) / 1000)
    urlRequest.httpMethod = request.method
    urlRequest.httpShouldHandleCookies = false
    request.headers.forEach { urlRequest.setValue($1, forHTTPHeaderField: $0) }
    urlRequest.httpBody = Data(request.body.utf8)
    do {
      let (data, response) = try await BoundedExchange(limit: request.responseLimit).run(in: session) { $0.dataTask(with: urlRequest) }
      let status = (response as? HTTPURLResponse)?.statusCode ?? 0
      return .response(status: UInt16(clamping: status), etag: nil, body: String(decoding: data, as: UTF8.self))
    } catch {
      return .failed(error)
    }
  }
}
