import Foundation

protocol CableWebSocket: AnyObject, Sendable {
  var closeCode: URLSessionWebSocketTask.CloseCode { get }
  var closeReason: Data? { get }
  func open() async throws
  func send(_ message: URLSessionWebSocketTask.Message) async throws
  func receive() async throws -> URLSessionWebSocketTask.Message
  func cancel(with closeCode: URLSessionWebSocketTask.CloseCode, reason: Data?)
}

extension URLSessionWebSocketTask: CableWebSocket {
  func open() async throws {
    let (events, continuation) = AsyncThrowingStream<Void, Error>.makeStream()
    let opening = SocketOpening(continuation)
    delegate = opening
    try await withTaskCancellationHandler {
      resume()
      var iterator = events.makeAsyncIterator()
      guard try await iterator.next() != nil else { throw CancellationError() }
    } onCancel: {
      self.cancel(with: .normalClosure, reason: nil)
      continuation.finish(throwing: CancellationError())
    }
  }
}

private final class SocketOpening: NSObject, URLSessionWebSocketDelegate, @unchecked Sendable {
  private let events: AsyncThrowingStream<Void, Error>.Continuation

  init(_ events: AsyncThrowingStream<Void, Error>.Continuation) { self.events = events }

  func urlSession(_ session: URLSession, webSocketTask: URLSessionWebSocketTask, didOpenWithProtocol protocol: String?) {
    events.yield(())
    events.finish()
  }

  func urlSession(_ session: URLSession, webSocketTask: URLSessionWebSocketTask, didCloseWith closeCode: URLSessionWebSocketTask.CloseCode, reason: Data?) {
    events.finish(throwing: URLError(.networkConnectionLost))
  }

  func urlSession(_ session: URLSession, task: URLSessionTask, didCompleteWithError error: Error?) {
    events.finish(throwing: error ?? URLError(.networkConnectionLost))
  }
}

final class UrlSessionCable: @unchecked Sendable {
  private let origin: URL?
  private let cablePath: String?
  private let applicationUserAgent: @Sendable () -> String?
  private let makeSocket: @Sendable (URLRequest) -> any CableWebSocket
  private let lock = NSLock()
  private var sockets: [String: any CableWebSocket] = [:]

  init(origin: URL?, cablePath: String?, applicationUserAgent: @escaping @Sendable () -> String? = { nil }, makeSocket: @escaping @Sendable (URLRequest) -> any CableWebSocket) {
    self.origin = origin
    self.cablePath = cablePath
    self.applicationUserAgent = applicationUserAgent
    self.makeSocket = makeSocket
  }

  deinit {
    for socket in sockets.values { socket.cancel(with: .goingAway, reason: nil) }
  }

  func open() async throws -> String {
    do { try Task.checkCancellation() } catch { throw TransportFailure.classifySocket(error) }
    guard let cablePath, let origin, var url = URLComponents(url: origin, resolvingAgainstBaseURL: false),
          ["https", "http"].contains(url.scheme), url.host != nil else {
      throw DbError.Invalid(detail: "cable requires an HTTP origin")
    }
    url.scheme = url.scheme == "https" ? "wss" : "ws"
    url.percentEncodedPath = cablePath
    url.query = nil
    url.fragment = nil
    guard let target = url.url else { throw DbError.Invalid(detail: "invalid cable origin") }
    var request = URLRequest(url: target)
    request.setValue(applicationUserAgent(), forHTTPHeaderField: "User-Agent")
    request.setUserAgent()
    let socket = makeSocket(request)
    let token = UUID().uuidString
    lock.withLock { sockets[token] = socket }
    do {
      try await socket.open()
      try Task.checkCancellation()
      return token
    } catch {
      remove(token)?.cancel(with: .goingAway, reason: nil)
      throw TransportFailure.classifySocket(error)
    }
  }

  func send(token: String, text: String) async throws {
    guard let socket = lock.withLock({ sockets[token] }) else {
      throw DbError.Interrupted(detail: "cable socket is closed")
    }
    do {
      try await withTaskCancellationHandler {
        try Task.checkCancellation()
        try await socket.send(.string(text))
      } onCancel: { self.remove(token)?.cancel(with: .normalClosure, reason: nil) }
    } catch { throw TransportFailure.classifySocket(error) }
  }

  func next(token: String) async -> EncryptedSocketEvent? {
    guard let socket = lock.withLock({ sockets[token] }) else { return nil }
    do {
      let message = try await withTaskCancellationHandler {
        try Task.checkCancellation()
        return try await socket.receive()
      } onCancel: { self.remove(token)?.cancel(with: .normalClosure, reason: nil) }
      guard lock.withLock({ sockets[token] != nil }) else { return nil }
      switch message {
      case let .string(text): return .text(text: text)
      case .data:
        let reason = "binary cable frames are not supported"
        remove(token)?.cancel(with: .unsupportedData, reason: Data(reason.utf8))
        return .closed(code: 1003, reason: reason)
      @unknown default:
        remove(token)?.cancel(with: .protocolError, reason: nil)
        return .closed(code: 1002, reason: "unknown cable frame")
      }
    } catch {
      guard remove(token) != nil else { return nil }
      let code = socket.closeCode == .invalid ? 1006 : UInt16(clamping: socket.closeCode.rawValue)
      let reason = socket.closeReason.map { String(decoding: $0, as: UTF8.self) } ?? error.localizedDescription
      socket.cancel(with: .goingAway, reason: nil)
      return .closed(code: code, reason: reason)
    }
  }

  func close(token: String) async {
    remove(token)?.cancel(with: .normalClosure, reason: nil)
  }

  private func remove(_ token: String) -> (any CableWebSocket)? {
    lock.withLock { sockets.removeValue(forKey: token) }
  }
}
