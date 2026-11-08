import Foundation

protocol CableWebSocket: AnyObject, Sendable {
  var closeCode: URLSessionWebSocketTask.CloseCode { get }
  var closeReason: Data? { get }
  var handshakeStatus: Int? { get }
  func open() async throws
  func send(_ message: URLSessionWebSocketTask.Message, completion: @escaping @Sendable (Error?) -> Void)
  func receive() async throws -> URLSessionWebSocketTask.Message
  func cancel(with closeCode: URLSessionWebSocketTask.CloseCode, reason: Data?)
  func cancel()
}

extension URLSessionWebSocketTask: CableWebSocket {
  var handshakeStatus: Int? { (response as? HTTPURLResponse)?.statusCode }

  func send(_ message: URLSessionWebSocketTask.Message, completion: @escaping @Sendable (Error?) -> Void) {
    send(message, completionHandler: completion)
  }

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

private final class UrlSessionCableConnection: PlatformSocket, @unchecked Sendable {
  private let socket: any CableWebSocket
  private let events: CableEvents
  private var receiver: Task<Void, Never>?

  init(socket: any CableWebSocket, events: CableEvents) {
    self.socket = socket
    self.events = events
    receiver = Task { [socket] in
      while !Task.isCancelled {
        do {
          switch try await socket.receive() {
          case let .string(text): events.frame(text: text)
          case let .data(bytes): events.binary(bytes: bytes)
          @unknown default: events.ended(closeCode: nil, reason: nil, failure: RawFailure(domain: "cable", code: nil, message: "unknown cable frame"))
          }
        } catch {
          if Task.isCancelled { return }
          let code = socket.closeCode
          events.ended(closeCode: code == .invalid ? nil : UInt16(clamping: code.rawValue), reason: socket.closeReason.map { String(decoding: $0, as: UTF8.self) }, failure: RawFailure(error))
          return
        }
      }
    }
  }

  func send(text: String) -> Bool {
    guard socket.closeCode == .invalid else { return false }
    socket.send(.string(text)) { [events] error in
      if let error { events.ended(closeCode: nil, reason: nil, failure: RawFailure(error)) }
    }
    return true
  }

  func close(code: UInt16, reason: String) {
    receiver?.cancel()
    socket.cancel(with: URLSessionWebSocketTask.CloseCode(rawValue: Int(code)) ?? .normalClosure, reason: Data(reason.utf8))
  }

  func cancel() {
    receiver?.cancel()
    socket.cancel()
  }
}

final class UrlSessionCable: Sendable {
  private let makeSocket: @Sendable (URLRequest) -> any CableWebSocket

  init(makeSocket: @escaping @Sendable (URLRequest) -> any CableWebSocket) {
    self.makeSocket = makeSocket
  }

  func open(url: String, headers: [String: String], connectMs: UInt64, events: CableEvents) async -> CableOutcome {
    guard let target = URL(string: url) else { return .failed(failure: RawFailure(domain: "url", code: nil, message: ""), handshakeStatus: nil) }
    var request = URLRequest(url: target, timeoutInterval: TimeInterval(connectMs) / 1000)
    headers.forEach { request.setValue($1, forHTTPHeaderField: $0) }
    let socket = makeSocket(request)
    do {
      try await socket.open()
      try Task.checkCancellation()
      return .open(connection: UrlSessionCableConnection(socket: socket, events: events))
    } catch {
      let status = socket.handshakeStatus
      socket.cancel(with: .goingAway, reason: nil)
      return .failed(failure: RawFailure(error), handshakeStatus: status.map { UInt16(clamping: $0) })
    }
  }
}
