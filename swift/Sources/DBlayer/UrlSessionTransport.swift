import Foundation

public final class UrlSessionTransport: Transport, @unchecked Sendable {
  private let endpoint: URL
  private let headers: @Sendable () -> [String: String]
  private let session: URLSession
  private let encryptedCable: UrlSessionCable

  public init(endpoint: URL, headers: @escaping @Sendable () -> [String: String] = { [:] }, session: URLSession = .shared) {
    self.endpoint = endpoint
    self.headers = headers
    self.session = session
    self.encryptedCable = UrlSessionCable(headers: headers) { session.webSocketTask(with: $0) }
  }

  public func query(request: TransportRequest) async throws -> String { try await post(request, kind: .query) }
  public func mutate(request: TransportRequest) async throws -> String { try await post(request, kind: .mutation) }

  public func openCable(lease: CableLease, url: String) async throws { try lease.opened(token: try await encryptedCable.open(url: url)) }
  public func cableSend(token: String, text: String) async throws { try await encryptedCable.send(token: token, text: text) }
  public func cableNext(token: String) async -> EncryptedSocketEvent? { await encryptedCable.next(token: token) }
  public func cableClose(token: String) async { await encryptedCable.close(token: token) }

  private func body(_ request: TransportRequest) throws -> [String: Any] {
    let variables = try JSONSerialization.jsonObject(with: Data(request.variablesJson.utf8))
    return ["query": request.document, "operationName": request.operationName, "variables": variables]
  }

  private func post(_ request: TransportRequest, kind: GraphQLOperationKind) async throws -> String {
    var urlRequest = URLRequest(url: endpoint)
    urlRequest.httpMethod = "POST"
    urlRequest.setValue("application/json", forHTTPHeaderField: "Content-Type")
    headers().forEach { urlRequest.setValue($1, forHTTPHeaderField: $0) }
    urlRequest.setUserAgent()
    urlRequest.httpBody = try JSONSerialization.data(withJSONObject: body(request))
    let data: Data, response: URLResponse
    do { (data, response) = try await session.data(for: urlRequest, delegate: RefuseRedirect()) } catch { throw TransportFailure.classify(error, kind: kind) }
    if let http = response as? HTTPURLResponse, !(200..<300).contains(http.statusCode) {
      throw DbError.Http(status: UInt16(http.statusCode), detail: String(decoding: data, as: UTF8.self))
    }
    return String(decoding: data, as: UTF8.self)
  }

  public func put(url: String, headers: [String: String], file: PutFile, identity: TransferIdentity, progress: ByteProgress) async throws -> PutReply {
    guard let target = URL(string: url) else { throw DbError.Invalid(detail: "bad upload url") }
    guard file.offset == 0 else { throw DbError.Invalid(detail: "an upload sends a whole file") }
    var request = URLRequest(url: target)
    request.httpMethod = "PUT"
    headers.forEach { request.setValue($1, forHTTPHeaderField: $0) }
    request.setUserAgent()
    do {
      let (_, response) = try await session.upload(for: request, fromFile: URL(fileURLWithPath: file.path), delegate: PutProgress(progress))
      let http = response as? HTTPURLResponse
      return PutReply(status: UInt16(clamping: http?.statusCode ?? 0), etag: http?.value(forHTTPHeaderField: "ETag"))
    } catch {
      throw TransportFailure.classifySocket(error)
    }
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
