import Foundation

public protocol CableSocket: Sendable {
  func open() async throws
  func close()
  func send(_ text: String) async throws
  func receive() async throws -> String
}

public enum CableSocketError: Error, Sendable {
  case unsupportedFrame
  case closed
}

private final class CableSocketDelegate: NSObject, URLSessionWebSocketDelegate, @unchecked Sendable {
  private let protocolStream: AsyncThrowingStream<String?, Error>
  private var continuation: AsyncThrowingStream<String?, Error>.Continuation?

  override init() {
    var streamContinuation: AsyncThrowingStream<String?, Error>.Continuation?
    protocolStream = AsyncThrowingStream { continuation in
      streamContinuation = continuation
    }
    continuation = streamContinuation
    super.init()
  }

  func waitForProtocol() async throws -> String? {
    var iterator = protocolStream.makeAsyncIterator()
    guard let negotiatedProtocol = try await iterator.next() else {
      return nil
    }
    return negotiatedProtocol
  }

  func urlSession(
    _: URLSession,
    webSocketTask _: URLSessionWebSocketTask,
    didOpenWithProtocol negotiatedProtocol: String?
  ) {
    continuation?.yield(negotiatedProtocol)
  }

  func urlSession(_: URLSession, task _: URLSessionTask, didCompleteWithError error: Error?) {
    if let error {
      continuation?.finish(throwing: error)
    } else {
      continuation?.finish()
    }
  }
}

public final class URLSessionCableSocket: CableSocket, @unchecked Sendable {
  private let url: URL
  private let token: String
  private let session: URLSession
  private let delegate: CableSocketDelegate
  private var task: URLSessionWebSocketTask?
  private var pendingMessage: URLSessionWebSocketTask.Message?

  public init(url: URL, token: String) {
    self.url = url
    self.token = token
    let delegate = CableSocketDelegate()
    self.delegate = delegate
    session = URLSession(configuration: .default, delegate: delegate, delegateQueue: nil)
  }

  public static func isSupported(protocol value: String?) -> Bool {
    value == "actioncable-v1-json"
  }

  public func open() async throws {
    var request = URLRequest(url: url)
    request.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
    request.setValue(
      "actioncable-v1-json, actioncable-unsupported",
      forHTTPHeaderField: "Sec-WebSocket-Protocol"
    )
    let task = session.webSocketTask(with: request)
    self.task = task
    task.resume()
    let message = try await task.receive()
    let negotiatedProtocol = try await delegate.waitForProtocol()
    guard Self.isSupported(protocol: negotiatedProtocol) else {
      close()
      throw CableSocketError.unsupportedFrame
    }
    pendingMessage = message
  }

  public func close() {
    task?.cancel(with: .normalClosure, reason: nil)
    task = nil
    pendingMessage = nil
  }

  public func send(_ text: String) async throws {
    guard let task else {
      throw CableSocketError.closed
    }
    try await task.send(.string(text))
  }

  public func receive() async throws -> String {
    guard let task else {
      throw CableSocketError.closed
    }
    let message: URLSessionWebSocketTask.Message
    if let pendingMessage {
      self.pendingMessage = nil
      message = pendingMessage
    } else {
      message = try await task.receive()
    }
    switch message {
    case let .string(text):
      return text
    case .data:
      throw CableSocketError.unsupportedFrame
    @unknown default:
      throw CableSocketError.unsupportedFrame
    }
  }
}
