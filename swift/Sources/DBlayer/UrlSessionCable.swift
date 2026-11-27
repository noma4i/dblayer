import Foundation

protocol CableWebSocket: AnyObject, Sendable {
  var closeCode: URLSessionWebSocketTask.CloseCode { get }
  var closeReason: Data? { get }
  var handshakeStatus: Int? { get }
  func open() async throws
  func send(_ message: URLSessionWebSocketTask.Message, completion: @escaping @Sendable (Error?) -> Void)
  func receive() async throws -> URLSessionWebSocketTask.Message
  func terminal(after error: Error) async -> CableTerminal
  func cancel(with closeCode: URLSessionWebSocketTask.CloseCode, reason: Data?)
  func cancel()
}

struct CableTerminal {
  let code: URLSessionWebSocketTask.CloseCode
  let reason: Data?
  let error: Error
}

extension CableWebSocket {
  func terminal(after error: Error) async -> CableTerminal {
    CableTerminal(code: closeCode, reason: closeReason, error: error)
  }
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

  func terminal(after error: Error) async -> CableTerminal {
    guard let opening = delegate as? SocketOpening, let result = await opening.terminal() else {
      return CableTerminal(code: closeCode, reason: closeReason, error: error)
    }
    return CableTerminal(code: result.code, reason: result.reason, error: result.error ?? error)
  }
}

private final class SocketOpening: NSObject, URLSessionWebSocketDelegate, @unchecked Sendable {
  private let events: AsyncThrowingStream<Void, Error>.Continuation
  private let results = AsyncStream<(code: URLSessionWebSocketTask.CloseCode, reason: Data?, error: Error?)>.makeStream()
  // Only the session's serial delegate queue accesses the close frame.
  private var close: (URLSessionWebSocketTask.CloseCode, Data?)?

  init(_ events: AsyncThrowingStream<Void, Error>.Continuation) { self.events = events }

  func urlSession(_ session: URLSession, webSocketTask: URLSessionWebSocketTask, didOpenWithProtocol protocol: String?) {
    events.yield(())
    events.finish()
  }

  func urlSession(_ session: URLSession, webSocketTask: URLSessionWebSocketTask, didCloseWith closeCode: URLSessionWebSocketTask.CloseCode, reason: Data?) {
    close = (closeCode, reason)
    events.finish(throwing: URLError(.networkConnectionLost))
  }

  func urlSession(_ session: URLSession, task: URLSessionTask, didCompleteWithError error: Error?) {
    events.finish(throwing: error ?? URLError(.networkConnectionLost))
    let socket = task as? URLSessionWebSocketTask
    let result = close ?? (socket?.closeCode ?? .invalid, socket?.closeReason)
    results.continuation.yield((result.0, result.1, error))
    results.continuation.finish()
  }

  func terminal() async -> (code: URLSessionWebSocketTask.CloseCode, reason: Data?, error: Error?)? {
    var iterator = results.stream.makeAsyncIterator()
    return await iterator.next()
  }
}

/// The trace lines of one cable (see `TransportTrace`): connect start, open, every frame, close and failure.
private struct CableTrace: Sendable {
  private let target: String

  init(url: URL) {
    target = TransportTrace.target(url)
    emit("connect")
  }

  func opened(handshake: Date) { emit("open handshake=\(TransportTrace.ms(handshake, Date()))ms") }
  func frame(_ direction: String, bytes: Int) { emit("frame \(direction) bytes=\(bytes)") }
  func closed(code: Int?, reason: String?, initiator: String) { emit("close code=\(code.map { String($0) } ?? "-") reason=\(reason ?? "-") initiator=\(initiator)") }
  func failed(_ error: Error) { emit("failure \(TransportTrace.outcome(error, status: nil))") }

  private func emit(_ event: String) {
    TransportTrace.emit("cable \(event) at=\(Int(Date().timeIntervalSince1970 * 1000)) \(target)")
  }
}

private final class UrlSessionCableConnection: PlatformSocket, @unchecked Sendable {
  private let socket: any CableWebSocket
  private let events: CableEvents
  private let trace: CableTrace?
  private var receiver: Task<Void, Never>?

  init(socket: any CableWebSocket, events: CableEvents, trace: CableTrace?) {
    self.socket = socket
    self.events = events
    self.trace = trace
    receiver = Task { [socket] in
      while !Task.isCancelled {
        do {
          switch try await socket.receive() {
          case let .string(text):
            trace?.frame("in", bytes: text.utf8.count)
            guard UInt64(text.utf8.count) <= cableFrameLimit() else { return Self.refuse(socket, events, trace) }
            events.frame(text: text)
          case let .data(bytes):
            trace?.frame("in", bytes: bytes.count)
            guard UInt64(bytes.count) <= cableFrameLimit() else { return Self.refuse(socket, events, trace) }
            events.binary(bytes: bytes)
          @unknown default:
            events.ended(closeCode: nil, reason: nil, failure: RawFailure(domain: "cable", code: nil, message: "unknown cable frame"))
            return
          }
        } catch {
          if Task.isCancelled { return }
          let terminal = await socket.terminal(after: error)
          if Task.isCancelled { return }
          let code = terminal.code
          let reason = terminal.reason.map { String(decoding: $0, as: UTF8.self) }
          if code == .invalid { trace?.failed(terminal.error) } else { trace?.closed(code: code.rawValue, reason: reason, initiator: "remote") }
          events.ended(closeCode: code == .invalid ? nil : UInt16(clamping: code.rawValue), reason: reason, failure: RawFailure(terminal.error))
          return
        }
      }
    }
  }

  private static func refuse(_ socket: any CableWebSocket, _ events: CableEvents, _ trace: CableTrace?) {
    trace?.closed(code: nil, reason: "frame above the limit", initiator: "local")
    socket.cancel()
    events.ended(closeCode: nil, reason: nil, failure: RawFailure(domain: "DBlayer.ResponseLimit", code: 1, message: "the cable frame is above the limit of \(cableFrameLimit()) bytes"))
  }

  func send(text: String) -> Bool {
    guard socket.closeCode == .invalid else { return false }
    trace?.frame("out", bytes: text.utf8.count)
    socket.send(.string(text)) { [trace] error in
      if let error { trace?.failed(error) }
    }
    return true
  }

  func close(code: UInt16, reason: String) {
    receiver?.cancel()
    trace?.closed(code: Int(code), reason: reason, initiator: "local")
    socket.cancel(with: URLSessionWebSocketTask.CloseCode(rawValue: Int(code)) ?? .normalClosure, reason: Data(reason.utf8))
  }

  func cancel() {
    receiver?.cancel()
    trace?.closed(code: nil, reason: nil, initiator: "local")
    socket.cancel()
  }
}

final class UrlSessionCable: Sendable {
  private let makeSocket: @Sendable (URLRequest) -> any CableWebSocket
  private let trace: Bool

  init(trace: Bool = false, makeSocket: @escaping @Sendable (URLRequest) -> any CableWebSocket) {
    self.trace = trace
    self.makeSocket = makeSocket
  }

  func open(url: String, headers: [String: String], connectMs: UInt64, events: CableEvents) async -> CableOutcome {
    guard let target = URL(string: url) else { return .failed(failure: RawFailure(domain: "url", code: nil, message: ""), handshakeStatus: nil) }
    var request = URLRequest(url: target, timeoutInterval: TimeInterval(connectMs) / 1000)
    headers.forEach { request.setValue($1, forHTTPHeaderField: $0) }
    let socket = makeSocket(request)
    let trace = trace ? CableTrace(url: target) : nil
    let started = Date()
    do {
      try await socket.open()
      try Task.checkCancellation()
      trace?.opened(handshake: started)
      return .open(connection: UrlSessionCableConnection(socket: socket, events: events, trace: trace))
    } catch {
      trace?.failed(error)
      let status = socket.handshakeStatus
      socket.cancel(with: .goingAway, reason: nil)
      return .failed(failure: RawFailure(error), handshakeStatus: status.map { UInt16(clamping: $0) })
    }
  }
}
