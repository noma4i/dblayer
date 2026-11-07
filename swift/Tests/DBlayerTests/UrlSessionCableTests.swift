import Foundation
import Testing
@testable import DBlayer

struct UrlSessionCableTests {
  @Test
  func anAddressThatIsNoUrlFailsWithoutASocket() async throws {
    let cable = UrlSessionCable { _ in
      Issue.record("an invalid cable address created a socket")
      return Socket()
    }
    guard case let .failed(failure) = await cable.open(url: "http://[::1", connectMs: 10_000, events: RecordingEvents()) else {
      Issue.record("an invalid cable address opened a socket")
      return
    }
    #expect(failure.kind == .io)
  }

  @Test(.timeLimit(.minutes(1)))
  func cancelledHandshakeClosesTheNativeConnection() async throws {
    let server = try BootstrapListener(holdHeaders: true)
    defer { server.listener.cancel() }
    let origin = try await server.start()
    let session = URLSession(configuration: .ephemeral)
    defer { session.invalidateAndCancel() }
    let transport = UrlSessionTransport(endpoint: origin, session: session)
    let outcome = AsyncStream<RawFailureKind?>.makeStream()
    let task = Task {
      switch await transport.openCable(url: cableURL(origin), connectMs: 10_000, events: RecordingEvents()) {
      case .open: outcome.continuation.yield(nil)
      case let .failed(failure): outcome.continuation.yield(failure.kind)
      }
    }
    await eventually("cable handshake request", timeout: .seconds(2)) { server.requests.count == 1 }
    task.cancel()
    var finished = outcome.stream.makeAsyncIterator()
    #expect(await finished.next() == .some(.cancelled))
    await eventually("cancelled handshake disconnected", timeout: .seconds(2)) { server.disconnected }
    session.invalidateAndCancel()
    await task.value
  }

  final class Socket: CableWebSocket, @unchecked Sendable {
    private let lock = NSLock()
    var closeCode: URLSessionWebSocketTask.CloseCode = .invalid
    var closeReason: Data?
    var handshakeStatus: Int?
    var incoming: [Result<URLSessionWebSocketTask.Message, Error>] = [.success(.string("server frame"))]
    var openError: Error?
    private(set) var sent: [String] = []
    private(set) var resumed = false
    private(set) var cancelled: URLSessionWebSocketTask.CloseCode?

    func open() async throws {
      lock.withLock { resumed = true }
      if let openError { throw openError }
    }
    func send(_ message: URLSessionWebSocketTask.Message, completion: @escaping @Sendable (Error?) -> Void) {
      lock.withLock { if case let .string(text) = message { sent.append(text) } }
      completion(nil)
    }
    func receive() async throws -> URLSessionWebSocketTask.Message {
      let next = lock.withLock { incoming.isEmpty ? nil : incoming.removeFirst() }
      guard let next else {
        try await Task.sleep(for: .seconds(60))
        throw CancellationError()
      }
      return try next.get()
    }
    func cancel(with closeCode: URLSessionWebSocketTask.CloseCode, reason: Data?) {
      lock.withLock { cancelled = closeCode }
    }
  }

  @Test func frameGoesToTheCoreAndTextToTheSocket() async throws {
    let socket = Socket()
    let cable = UrlSessionCable { request in
      #expect(request.url?.absoluteString == "wss://example.com:8443/api/cable")
      #expect(request.timeoutInterval == 10)
      #expect(request.value(forHTTPHeaderField: "Sec-WebSocket-Protocol") == nil)
      return socket
    }
    let events = RecordingEvents()
    guard case let .open(connection) = await cable.open(url: "wss://example.com:8443/api/cable", connectMs: 10_000, events: events) else {
      Issue.record("the cable did not open")
      return
    }
    #expect(socket.resumed)
    #expect(connection.send(text: "client frame"))
    #expect(socket.sent == ["client frame"])
    #expect(await events.next() == .frame("server frame"))
    connection.close(code: 1000, reason: "done")
    #expect(socket.cancelled == .normalClosure)
  }

  @Test(arguments: [4401, 1006, 1003])
  func theEndOfTheSocketIsReportedAsTheSocketShowsIt(code: Int) async throws {
    let socket = Socket()
    if code == 4401 {
      socket.closeCode = URLSessionWebSocketTask.CloseCode(rawValue: code)!
      socket.closeReason = Data("UNAUTHENTICATED".utf8)
    }
    socket.incoming = [code == 1003 ? .success(.data(Data([1]))) : .failure(URLError(.networkConnectionLost))]
    let events = RecordingEvents()
    let cable = UrlSessionCable { _ in socket }
    guard case let .open(connection) = await cable.open(url: "wss://example.com:8443/api/cable", connectMs: 10_000, events: events) else {
      Issue.record("the cable did not open")
      return
    }
    switch code {
    case 4401: #expect(await events.next() == .closed(4401, "UNAUTHENTICATED"))
    case 1003: #expect(await events.next() == .binary(Data([1])))
    default: #expect(await events.next() == .failed(.io))
    }
    connection.close(code: 1000, reason: "")
  }

  @Test func theHandshakeStatusAndTheNativeFailureReachTheCore() async throws {
    let refused = Socket()
    refused.openError = URLError(.badServerResponse)
    refused.handshakeStatus = 403
    let refusing = UrlSessionCable { _ in refused }
    guard case let .failed(status) = await refusing.open(url: "wss://example.com/api/cable", connectMs: 10_000, events: RecordingEvents()) else {
      Issue.record("a refused handshake opened")
      return
    }
    #expect((status.kind, status.code) == (.protocol, 403))
    let unreachable = Socket()
    unreachable.openError = URLError(.cannotConnectToHost)
    let unreachableCable = UrlSessionCable { _ in unreachable }
    guard case let .failed(failure) = await unreachableCable.open(url: "wss://example.com/api/cable", connectMs: 10_000, events: RecordingEvents()) else {
      Issue.record("open returned before the WebSocket connected")
      return
    }
    #expect(failure.kind == .unreachable)
    #expect(unreachable.cancelled == .goingAway)
  }
}
