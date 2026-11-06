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

private actor OpenSockets {
  private var sockets: [String: any CableWebSocket] = [:]

  func add(_ token: String, _ socket: any CableWebSocket) { sockets[token] = socket }
  func socket(_ token: String) -> (any CableWebSocket)? { sockets[token] }
  func remove(_ token: String) -> (any CableWebSocket)? { sockets.removeValue(forKey: token) }
}

final class UrlSessionCable: Sendable {
  private let headers: @Sendable () -> [String: String]
  private let makeSocket: @Sendable (URLRequest) -> any CableWebSocket
  private let sockets = OpenSockets()

  init(headers: @escaping @Sendable () -> [String: String] = { [:] }, makeSocket: @escaping @Sendable (URLRequest) -> any CableWebSocket) {
    self.headers = headers
    self.makeSocket = makeSocket
  }

  func open(url: String) async throws -> String {
    do { try Task.checkCancellation() } catch { throw TransportFailure.classifySocket(error) }
    guard let target = URL(string: url), ["ws", "wss"].contains(target.scheme), target.host != nil else { throw DbError.Invalid(detail: "invalid cable url") }
    var request = URLRequest(url: target)
    headers().forEach { request.setValue($1, forHTTPHeaderField: $0) }
    request.setUserAgent()
    let socket = makeSocket(request)
    let token = UUID().uuidString
    await sockets.add(token, socket)
    do {
      try await socket.open()
      try Task.checkCancellation()
      return token
    } catch {
      await sockets.remove(token)?.cancel(with: .goingAway, reason: nil)
      throw TransportFailure.classifySocket(error)
    }
  }

  func send(token: String, text: String) async throws {
    guard let socket = await sockets.socket(token) else {
      throw DbError.Interrupted(detail: "cable socket is closed")
    }
    do {
      try await withTaskCancellationHandler {
        try Task.checkCancellation()
        try await socket.send(.string(text))
      } onCancel: { socket.cancel(with: .normalClosure, reason: nil) }
    } catch {
      if Task.isCancelled { await sockets.remove(token) }
      throw TransportFailure.classifySocket(error)
    }
  }

  func next(token: String) async -> EncryptedSocketEvent? {
    guard let socket = await sockets.socket(token) else { return nil }
    do {
      let message = try await withTaskCancellationHandler {
        try Task.checkCancellation()
        return try await socket.receive()
      } onCancel: { socket.cancel(with: .normalClosure, reason: nil) }
      guard await sockets.socket(token) != nil else { return nil }
      switch message {
      case let .string(text): return .text(text: text)
      case .data:
        let reason = "binary cable frames are not supported"
        await sockets.remove(token)?.cancel(with: .unsupportedData, reason: Data(reason.utf8))
        return .closed(code: 1003, reason: reason)
      @unknown default:
        await sockets.remove(token)?.cancel(with: .protocolError, reason: nil)
        return .closed(code: 1002, reason: "unknown cable frame")
      }
    } catch {
      guard await sockets.remove(token) != nil else { return nil }
      let code = socket.closeCode == .invalid ? 1006 : UInt16(clamping: socket.closeCode.rawValue)
      let reason = socket.closeReason.map { String(decoding: $0, as: UTF8.self) } ?? error.localizedDescription
      socket.cancel(with: .goingAway, reason: nil)
      return .closed(code: code, reason: reason)
    }
  }

  func close(token: String) async {
    await sockets.remove(token)?.cancel(with: .normalClosure, reason: nil)
  }
}
