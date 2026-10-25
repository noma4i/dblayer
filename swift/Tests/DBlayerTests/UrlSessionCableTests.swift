import Foundation
import Testing
@testable import DBlayer

struct UrlSessionCableTests {
  @Test(arguments: ["missing-origin", "wrong-scheme", "missing-path"])
  func invalidOriginsAndMissingPathsAreTyped(kind: String) async throws {
    let origin = kind == "missing-origin" ? nil : URL(string: kind == "wrong-scheme" ? "ftp://example.test" : "https://example.test")
    let cable = UrlSessionCable(origin: origin, cablePath: kind == "missing-path" ? nil : "/api/cable") { _ in
      Issue.record("invalid cable configuration created a socket")
      return Socket()
    }
    do {
      _ = try await cable.open()
      Issue.record("invalid cable configuration opened a socket")
    } catch let error as DbError {
      guard case .Invalid = error else {
        Issue.record("expected Invalid for cable configuration, got \(error)")
        return
      }
    }
  }

  @Test(.timeLimit(.minutes(1)))
  func cancelledHandshakeClosesTheNativeConnection() async throws {
    let server = try BootstrapListener(holdHeaders: true)
    defer { server.listener.cancel() }
    let origin = try await server.start()
    let session = URLSession(configuration: .ephemeral)
    defer { session.invalidateAndCancel() }
    let transport = UrlSessionTransport(endpoint: origin, session: session, cablePath: "/api/cable")
    let lease = RecordingLease(noHandle: .init())
    let outcome = AsyncStream<String>.makeStream()
    let task = Task {
      do {
        try await transport.openCable(lease: lease)
        outcome.continuation.yield("opened")
      } catch let error as DbError {
        outcome.continuation.yield(TransportFailureTests.shape(error))
      } catch {
        outcome.continuation.yield("untyped")
      }
    }
    await eventually("cable handshake request", timeout: .seconds(2)) { server.requests.count == 1 }
    task.cancel()
    let finished = Cursor(outcome.stream)
    #expect(await finished.next("cancelled handshake outcome", timeout: .seconds(2)) == "Interrupted")
    await eventually("cancelled handshake disconnected", timeout: .seconds(2)) { server.disconnected }
    #expect(lease.token == nil)
    session.invalidateAndCancel()
    await task.value
  }

  final class Socket: CableWebSocket, @unchecked Sendable {
    private let lock = NSLock()
    var closeCode: URLSessionWebSocketTask.CloseCode = .invalid
    var closeReason: Data?
    var incoming: Result<URLSessionWebSocketTask.Message, Error> = .success(.string("server frame"))
    var sendError: Error?
    var openError: Error?
    private(set) var sent: [String] = []
    private(set) var resumed = false
    private(set) var cancelled: URLSessionWebSocketTask.CloseCode?

    func resume() { lock.withLock { resumed = true } }
    func open() async throws {
      resume()
      if let openError { throw openError }
    }
    func send(_ message: URLSessionWebSocketTask.Message) async throws {
      try lock.withLock {
        if let sendError { throw sendError }
        if case let .string(text) = message { sent.append(text) }
      }
    }
    func receive() async throws -> URLSessionWebSocketTask.Message { try lock.withLock { try incoming.get() } }
    func cancel(with closeCode: URLSessionWebSocketTask.CloseCode, reason: Data?) {
      lock.withLock { cancelled = closeCode }
    }
  }

  @Test func textUsesCableURLAndSeparateTokens() async throws {
    let socket = Socket()
    let cable = UrlSessionCable(origin: URL(string: "https://example.com:8443/ignored?secret=value"), cablePath: "/api/cable") { request in
      #expect(request.url?.absoluteString == "wss://example.com:8443/api/cable")
      #expect(request.value(forHTTPHeaderField: "Sec-WebSocket-Protocol") == nil)
      return socket
    }
    let first = try await cable.open()
    let second = try await cable.open()
    #expect(first != second)
    #expect(socket.resumed)
    try await cable.send(token: first, text: "client frame")
    #expect(socket.sent == ["client frame"])
    #expect(await cable.next(token: first) == .text(text: "server frame"))
    await cable.close(token: first)
    await cable.close(token: second)
  }

  @Test(arguments: [4401, 1006, 1003])
  func closesPreservePeerCodeAndRejectBinary(code: Int) async throws {
    let socket = Socket()
    if code == 4401 {
      socket.closeCode = URLSessionWebSocketTask.CloseCode(rawValue: code)!
      socket.closeReason = Data("UNAUTHENTICATED".utf8)
    }
    socket.incoming = code == 1003 ? .success(.data(Data([1]))) : .failure(URLError(.networkConnectionLost))
    let cable = UrlSessionCable(origin: URL(string: "https://example.com"), cablePath: "/api/cable") { _ in socket }
    let token = try await cable.open()
    guard case let .closed(actual, reason) = await cable.next(token: token) else {
      Issue.record("expected a close event")
      return
    }
    #expect(actual == UInt16(code))
    #expect(!reason.isEmpty)
    if code == 4401 { #expect(reason == "UNAUTHENTICATED") }
    if code == 1003 { #expect(socket.cancelled == .unsupportedData) }
    #expect(await cable.next(token: token) == nil)
  }

  @Test func cancelClosesAndRemovesOnlyItsSocket() async throws {
    let socket = Socket()
    let cable = UrlSessionCable(origin: URL(string: "https://example.com"), cablePath: "/api/cable") { _ in socket }
    let token = try await cable.open()
    await cable.close(token: token)
    #expect(socket.cancelled == .normalClosure)
    #expect(await cable.next(token: token) == nil)
    await #expect(throws: DbError.self) { try await cable.send(token: token, text: "late") }
  }

  @Test(arguments: [URLError.Code.cancelled, .cannotConnectToHost])
  func sendFailureIsTyped(code: URLError.Code) async throws {
    let socket = Socket()
    socket.sendError = URLError(code)
    let cable = UrlSessionCable(origin: URL(string: "https://example.com"), cablePath: "/api/cable") { _ in socket }
    let token = try await cable.open()
    do {
      try await cable.send(token: token, text: "frame")
      Issue.record("send must fail")
    } catch let error as DbError {
      #expect(TransportFailureTests.shape(error) == (code == .cancelled ? "Interrupted" : "Network"))
    }
  }

  @Test func cancelledOpenFailsWithTypedErrorBeforeCreatingSocket() async throws {
    let cable = UrlSessionCable(origin: URL(string: "https://example.com"), cablePath: "/api/cable") { _ in
      Issue.record("cancelled open created a socket")
      return Socket()
    }
    let task = Task {
      withUnsafeCurrentTask { $0?.cancel() }
      do {
        _ = try await cable.open()
        Issue.record("cancelled open succeeded")
      } catch let error as DbError {
        #expect(TransportFailureTests.shape(error) == "Interrupted")
      } catch {
        Issue.record("untyped open error: \(error)")
      }
    }
    await task.value
  }

  @Test func connectionFailureRejectsOpenAndClosesSocket() async throws {
    let socket = Socket()
    socket.openError = URLError(.cannotConnectToHost)
    let cable = UrlSessionCable(origin: URL(string: "https://example.com"), cablePath: "/api/cable") { _ in socket }
    do {
      _ = try await cable.open()
      Issue.record("open returned before the WebSocket connected")
    } catch let error as DbError {
      #expect(TransportFailureTests.shape(error) == "Network")
    }
    #expect(socket.cancelled != nil)
  }
}
