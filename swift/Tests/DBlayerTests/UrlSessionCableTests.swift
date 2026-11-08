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
    guard case let .failed(failure, status) = await cable.open(url: "http://[::1", headers: [:], connectMs: 10_000, events: RecordingEvents()) else {
      Issue.record("an invalid cable address opened a socket")
      return
    }
    #expect((failure.domain, failure.code, status) == ("url", nil, nil))
  }

  @Test(.timeLimit(.minutes(1)))
  func cancelledHandshakeClosesTheNativeConnection() async throws {
    let server = try BootstrapListener(holdHeaders: true)
    defer { server.listener.cancel() }
    let origin = try await server.start()
    let session = URLSession(configuration: .ephemeral)
    defer { session.invalidateAndCancel() }
    let transport = UrlSessionTransport(session: session)
    let outcome = AsyncStream<RawFailure?>.makeStream()
    let task = Task {
      switch await transport.openCable(url: cableURL(origin), headers: [:], connectMs: 10_000, events: RecordingEvents()) {
      case .open: outcome.continuation.yield(nil)
      case let .failed(failure, _): outcome.continuation.yield(failure)
      }
    }
    await eventually("cable handshake request", timeout: .seconds(2)) { server.requests.count == 1 }
    task.cancel()
    let finished = Cursor(outcome.stream)
    let failure = await finished.next("the handshake outcome") ?? nil
    #expect(failure?.domain == "Swift.CancellationError" || (failure?.domain, failure?.code) == (NSURLErrorDomain, Int64(URLError.cancelled.rawValue)), "\(String(describing: failure))")
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
    var sendError: Error?
    private(set) var sent: [String] = []
    private(set) var resumed = false
    private(set) var cancelled: URLSessionWebSocketTask.CloseCode?
    private(set) var hardCancelled = false

    func open() async throws {
      lock.withLock { resumed = true }
      if let openError { throw openError }
    }
    func send(_ message: URLSessionWebSocketTask.Message, completion: @escaping @Sendable (Error?) -> Void) {
      lock.withLock { if case let .string(text) = message { sent.append(text) } }
      completion(sendError)
    }
    func receive() async throws -> URLSessionWebSocketTask.Message {
      let next = lock.withLock { incoming.isEmpty ? nil : incoming.removeFirst() }
      guard let next else {
        _ = await AsyncStream<Void> { _ in }.first { _ in true }
        throw CancellationError()
      }
      return try next.get()
    }
    func cancel(with closeCode: URLSessionWebSocketTask.CloseCode, reason: Data?) {
      lock.withLock { cancelled = closeCode }
    }
    func cancel() { lock.withLock { hardCancelled = true } }
  }

  @Test func frameGoesToTheCoreAndTextToTheSocket() async throws {
    let socket = Socket()
    let cable = UrlSessionCable { request in
      #expect(request.url?.absoluteString == "wss://example.com:8443/api/cable")
      #expect(request.timeoutInterval == 10)
      #expect(request.value(forHTTPHeaderField: "Sec-WebSocket-Protocol") == nil)
      #expect(request.value(forHTTPHeaderField: "X-Core") == "yes")
      return socket
    }
    let events = RecordingEvents()
    guard case let .open(connection) = await cable.open(url: "wss://example.com:8443/api/cable", headers: ["X-Core": "yes"], connectMs: 10_000, events: events) else {
      Issue.record("the cable did not open")
      return
    }
    #expect(socket.resumed)
    #expect(connection.send(text: "client frame"))
    #expect(socket.sent == ["client frame"])
    #expect(await events.next() == .frame("server frame"))
    connection.close(code: 1000, reason: "done")
    #expect(socket.cancelled == .normalClosure)
    #expect(!socket.hardCancelled)
    connection.cancel()
    #expect(socket.hardCancelled)
    socket.closeCode = .goingAway
    #expect(!connection.send(text: "late"))
    #expect(socket.sent == ["client frame"])
  }

  /// bridge_prop_raw_io (P-BR-08): the failure of a send completion reaches the core as the end of the socket, with the native failure.
  @Test func aFailedSendReachesTheCoreWithTheNativeFailure() async throws {
    let socket = Socket()
    socket.incoming = []
    socket.sendError = URLError(.networkConnectionLost)
    let events = RecordingEvents()
    let cable = UrlSessionCable { _ in socket }
    guard case let .open(connection) = await cable.open(url: "wss://example.com/api/cable", headers: [:], connectMs: 10_000, events: events) else {
      Issue.record("the cable did not open")
      return
    }
    _ = connection.send(text: "client frame")
    guard case let .ended(closeCode, _, failure)? = await events.next() else { Issue.record("the failed send did not reach the core"); return }
    #expect(closeCode == nil)
    #expect((failure?.domain, failure?.code) == (NSURLErrorDomain, Int64(URLError.networkConnectionLost.rawValue)))
    connection.cancel()
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
    guard case let .open(connection) = await cable.open(url: "wss://example.com:8443/api/cable", headers: [:], connectMs: 10_000, events: events) else {
      Issue.record("the cable did not open")
      return
    }
    switch code {
    case 4401:
      guard case let .ended(closeCode, reason, _)? = await events.next() else { Issue.record("no end"); return }
      #expect((closeCode, reason) == (4401, "UNAUTHENTICATED"))
    case 1003: #expect(await events.next() == .binary(Data([1])))
    default:
      guard case let .ended(closeCode, _, failure)? = await events.next() else { Issue.record("no end"); return }
      #expect(closeCode == nil)
      #expect((failure?.domain, failure?.code) == (NSURLErrorDomain, Int64(URLError.networkConnectionLost.rawValue)))
    }
    connection.close(code: 1000, reason: "")
  }

  @Test func theHandshakeStatusAndTheNativeFailureReachTheCore() async throws {
    let refused = Socket()
    refused.openError = URLError(.badServerResponse)
    refused.handshakeStatus = 403
    let refusing = UrlSessionCable { _ in refused }
    guard case let .failed(refusal, status) = await refusing.open(url: "wss://example.com/api/cable", headers: [:], connectMs: 10_000, events: RecordingEvents()) else {
      Issue.record("a refused handshake opened")
      return
    }
    #expect((refusal.domain, refusal.code, status) == (NSURLErrorDomain, Int64(URLError.badServerResponse.rawValue), 403))
    let unreachable = Socket()
    unreachable.openError = URLError(.cannotConnectToHost)
    let unreachableCable = UrlSessionCable { _ in unreachable }
    guard case let .failed(failure, noStatus) = await unreachableCable.open(url: "wss://example.com/api/cable", headers: [:], connectMs: 10_000, events: RecordingEvents()) else {
      Issue.record("open returned before the WebSocket connected")
      return
    }
    #expect((failure.domain, failure.code, noStatus) == (NSURLErrorDomain, Int64(URLError.cannotConnectToHost.rawValue), nil))
    #expect(unreachable.cancelled == .goingAway)
  }
}
