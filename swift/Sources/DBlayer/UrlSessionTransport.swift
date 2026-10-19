import Foundation

public final class UrlSessionTransport: Transport, @unchecked Sendable {
  private let endpoint: URL
  private let headers: @Sendable () -> [String: String]
  private let session: URLSession
  private let encryptedCable: UrlSessionCable
  private let lock = NSLock()
  private var sockets: [String: URLSessionWebSocketTask] = [:]

  public init(endpoint: URL, headers: @escaping @Sendable () -> [String: String] = { [:] }, session: URLSession = .shared) {
    self.endpoint = endpoint
    self.headers = headers
    self.session = session
    self.encryptedCable = UrlSessionCable(origin: endpoint, applicationUserAgent: {
      headers().first { $0.key.caseInsensitiveCompare("User-Agent") == .orderedSame }?.value
    }) { session.webSocketTask(with: $0) }
  }

  public convenience init(endpoint: URL, headers: [String: String]) {
    self.init(endpoint: endpoint, headers: { headers })
  }

  public func query(request: TransportRequest) async throws -> String { try await post(request, kind: .query) }
  public func mutate(request: TransportRequest) async throws -> String { try await post(request, kind: .mutation) }

  public func openCable() async throws -> String { try await encryptedCable.open() }
  public func cableSend(token: String, text: String) async throws { try await encryptedCable.send(token: token, text: text) }
  public func cableNext(token: String) async -> EncryptedSocketEvent? { await encryptedCable.next(token: token) }
  public func cableClose(token: String) async { await encryptedCable.close(token: token) }

  private func body(_ request: TransportRequest) -> [String: Any] {
    let variables = (try? JSONSerialization.jsonObject(with: Data(request.variablesJson.utf8))) ?? [String: Any]()
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
    do { (data, response) = try await session.data(for: urlRequest) } catch { throw TransportFailure.classify(error, kind: kind) }
    if let http = response as? HTTPURLResponse, !(200..<300).contains(http.statusCode) {
      throw DbError.Http(status: UInt16(http.statusCode), detail: String(decoding: data, as: UTF8.self))
    }
    return String(decoding: data, as: UTF8.self)
  }

  public func subscribe(request: TransportRequest, sink: SubscriptionSink) async throws -> String {
    var components = URLComponents(url: endpoint, resolvingAgainstBaseURL: false)
    components?.scheme = endpoint.scheme == "http" ? "ws" : "wss"
    guard let url = components?.url else { throw DbError.Invalid(detail: "bad subscription url") }
    var urlRequest = URLRequest(url: url)
    urlRequest.setValue(headers().first { $0.key.caseInsensitiveCompare("User-Agent") == .orderedSame }?.value, forHTTPHeaderField: "User-Agent")
    urlRequest.setUserAgent()
    urlRequest.setValue("graphql-transport-ws", forHTTPHeaderField: "Sec-WebSocket-Protocol")
    let socket = session.webSocketTask(with: urlRequest)
    let token = UUID().uuidString
    lock.withLock { sockets[token] = socket }
    socket.resume()
    let payload = body(request)
    let initial = try JSONSerialization.data(withJSONObject: ["type": "connection_init", "payload": headers()])
    try await socket.send(.string(String(decoding: initial, as: UTF8.self)))
    Task { await self.pump(socket, token: token, payload: payload, sink: sink) }
    return token
  }

  private func pump(_ socket: URLSessionWebSocketTask, token: String, payload: [String: Any], sink: SubscriptionSink) async {
    do {
      while true {
        let message = try await socket.receive()
        guard case .string(let text) = message, let object = try JSONSerialization.jsonObject(with: Data(text.utf8)) as? [String: Any] else { continue }
        switch object["type"] as? String {
        case "connection_ack":
          let start = try JSONSerialization.data(withJSONObject: ["id": token, "type": "subscribe", "payload": payload])
          try await socket.send(.string(String(decoding: start, as: UTF8.self)))
          sink.ready()
        case "next":
          if let next = object["payload"] { try sink.event(json: String(decoding: try JSONSerialization.data(withJSONObject: next), as: UTF8.self)) }
        case "error":
          if let errors = object["payload"] { try sink.event(json: String(decoding: try JSONSerialization.data(withJSONObject: ["errors": errors]), as: UTF8.self)) }
        case "complete":
          sink.closed(); return
        case "ping":
          try await socket.send(.string(#"{"type":"pong"}"#))
        default: break
        }
      }
    } catch {
      sink.failed(error: TransportFailure.classifySubscription(error))
      sink.closed()
    }
  }

  public func unsubscribe(token: String) {
    lock.withLock { sockets.removeValue(forKey: token) }?.cancel(with: .normalClosure, reason: nil)
  }

  public func put(url: String, headers: [String: String], bytes: Data, progress: ByteProgress) async throws -> PutReply {
    guard let target = URL(string: url) else { throw DbError.Invalid(detail: "bad upload url") }
    var urlRequest = URLRequest(url: target)
    urlRequest.httpMethod = "PUT"
    headers.forEach { urlRequest.setValue($1, forHTTPHeaderField: $0) }
    urlRequest.setUserAgent()
    let watcher = UploadWatcher(progress)
    do {
      let (_, response) = try await session.upload(for: urlRequest, from: bytes, delegate: watcher)
      let http = response as? HTTPURLResponse
      return PutReply(status: UInt16(http?.statusCode ?? 0), etag: http?.value(forHTTPHeaderField: "ETag"))
    } catch { throw TransportFailure.classifySocket(error) }
  }
}

private final class UploadWatcher: NSObject, URLSessionTaskDelegate, @unchecked Sendable {
  let progress: ByteProgress
  init(_ progress: ByteProgress) { self.progress = progress }

  func urlSession(_ session: URLSession, task: URLSessionTask, didSendBodyData bytesSent: Int64, totalBytesSent: Int64, totalBytesExpectedToSend: Int64) {
    progress.sent(bytes: UInt64(totalBytesSent))
    if progress.isCancelled() { task.cancel() }
  }
}
