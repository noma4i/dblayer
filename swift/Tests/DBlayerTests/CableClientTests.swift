import DBlayer
import Foundation
import Testing
@testable import DBlayer

struct CableClientTests {
  @Test
  func sendsExecuteAfterConfirmation() async throws {
    let identifier = GraphQLChannel.identifier(channelId: "abc")
    let socket = TestCableClientSocket(
      frames: [
        "{\"type\":\"welcome\"}",
        confirmationFrame(identifier: identifier)
      ]
    )
    let client = makeClient(socket: socket)
    let stream = await client.subscribeGraphQL(document: "subscription X { x }", operationName: "X")
    let startTask = Task { await client.start() }

    await socket.waitForRecordedCount(2)
    #expect(
      socket.recordedTexts()
        == [
          CableCommand.subscribe(identifier: identifier).json,
          GraphQLChannel.execute(
            channelId: "abc",
            attempt: 1,
            document: "subscription X { x }",
            operationName: "X"
          ).json
        ]
    )

    await client.stop()
    let events = try await collectEvents(from: stream)
    await startTask.value
    #expect(events.isEmpty)
  }

  @Test
  func emitsReadyOnceThenPayloadEvents() async throws {
    let identifier = GraphQLChannel.identifier(channelId: "abc")
    let readyPayload = #"{"result":{"data":null},"more":true,"requestId":"abc:1","subscriptionReady":true}"#
    let payload = #"{"result":{"data":{"chatUpdated":{"id":"1"}}},"more":true}"#
    let socket = TestCableClientSocket(
      frames: [
        "{\"type\":\"welcome\"}",
        confirmationFrame(identifier: identifier),
        messageFrame(identifier: identifier, payload: readyPayload),
        messageFrame(identifier: identifier, payload: readyPayload),
        messageFrame(identifier: identifier, payload: payload),
        rejectionFrame(identifier: identifier)
      ]
    )
    let client = makeClient(socket: socket)
    let stream = await client.subscribeGraphQL(document: "subscription X { x }", operationName: "X")
    let startTask = Task { await client.start() }

    let events = try await collectEvents(from: stream, ignoringFailure: true)
    await client.stop()
    await startTask.value

    #expect(
      events
        == [.ready, .payload(JSONValue.object(["chatUpdated": .object(["id": .string("1")])]))]
    )
  }

  @Test
  func finishesOnReject() async throws {
    let identifier = GraphQLChannel.identifier(channelId: "abc")
    let socket = TestCableClientSocket(
      frames: [
        "{\"type\":\"welcome\"}",
        rejectionFrame(identifier: identifier)
      ]
    )
    let client = makeClient(socket: socket)
    let stream = await client.subscribeGraphQL(document: "subscription X { x }", operationName: "X")
    let startTask = Task { await client.start() }

    var failure: (any Error)?
    do {
      _ = try await collectEvents(from: stream)
    } catch {
      failure = error
    }
    await client.stop()
    await startTask.value

    #expect(failure as? CableClientError == .rejected)
  }

  @Test
  func finishesAndUnsubscribesOnMoreFalse() async throws {
    let identifier = GraphQLChannel.identifier(channelId: "abc")
    let socket = TestCableClientSocket(
      frames: [
        "{\"type\":\"welcome\"}",
        confirmationFrame(identifier: identifier),
        messageFrame(identifier: identifier, payload: #"{"result":{"data":null},"more":false}"#)
      ]
    )
    let client = makeClient(socket: socket)
    let stream = await client.subscribeGraphQL(document: "subscription X { x }", operationName: "X")
    let startTask = Task { await client.start() }

    let events = try await collectEvents(from: stream)
    await socket.waitForRecordedCount(3)
    await client.stop()
    await startTask.value

    #expect(events.isEmpty)
    #expect(socket.recordedTexts().last == CableCommand.unsubscribe(identifier: identifier).json)
  }

  @Test
  func subscribesPresenceAfterWelcomeAndPingsOnConfirmation() async {
    let identifier = OnlineChannel.identifier(userId: "54")
    let socket = TestCableClientSocket(
      frames: ["{\"type\":\"welcome\"}", confirmationFrame(identifier: identifier)]
    )
    let client = CableClient(
      url: URL(fileURLWithPath: "/test"),
      token: "token",
      makeSocket: { _, _ in socket },
      makeChannelId: { "abc" },
      pingInterval: 0.01
    )
    await client.openPresence(userId: "54")
    let startTask = Task { await client.start() }

    await socket.waitForRecordedCount(4)
    await client.stop()
    await startTask.value

    let texts = socket.recordedTexts()
    #expect(texts.first == OnlineChannel.subscribe(userId: "54").json)
    #expect(texts.dropFirst().allSatisfy { $0.contains("\\\"action\\\":\\\"ping\\\"") })
  }

  @Test
  func resubscribesAfterSecondWelcome() async throws {
    let identifier = GraphQLChannel.identifier(channelId: "abc")
    let socket = TestCableClientSocket(
      frames: [
        "{\"type\":\"welcome\"}",
        confirmationFrame(identifier: identifier),
        "{\"type\":\"welcome\"}",
        confirmationFrame(identifier: identifier)
      ]
    )
    let client = makeClient(socket: socket)
    let stream = await client.subscribeGraphQL(document: "subscription X { x }", operationName: "X")
    let startTask = Task { await client.start() }

    await socket.waitForRecordedCount(4)
    #expect(
      socket.recordedTexts()
        == [
          CableCommand.subscribe(identifier: identifier).json,
          GraphQLChannel.execute(
            channelId: "abc",
            attempt: 1,
            document: "subscription X { x }",
            operationName: "X"
          ).json,
          CableCommand.subscribe(identifier: identifier).json,
          GraphQLChannel.execute(
            channelId: "abc",
            attempt: 2,
            document: "subscription X { x }",
            operationName: "X"
          ).json
        ]
    )

    await client.stop()
    let events = try await collectEvents(from: stream)
    await startTask.value
    #expect(events.isEmpty)
  }

  private func makeClient(socket: TestCableClientSocket) -> CableClient {
    CableClient(
      url: URL(fileURLWithPath: "/test"),
      token: "token",
      makeSocket: { _, _ in socket },
      makeChannelId: { "abc" }
    )
  }

  private func collectEvents(
    from stream: AsyncThrowingStream<GraphQLSubscriptionEvent, Error>,
    ignoringFailure: Bool = false
  ) async throws -> [GraphQLSubscriptionEvent] {
    var events: [GraphQLSubscriptionEvent] = []
    do {
      for try await event in stream {
        events.append(event)
      }
    } catch where ignoringFailure {
      return events
    }
    return events
  }
}

func confirmationFrame(identifier: String) -> String {
  let escapedIdentifier = identifier.replacingOccurrences(of: "\"", with: "\\\"")
  return "{\"type\":\"confirm_subscription\",\"identifier\":\"\(escapedIdentifier)\"}"
}

func rejectionFrame(identifier: String) -> String {
  let escapedIdentifier = identifier.replacingOccurrences(of: "\"", with: "\\\"")
  return "{\"type\":\"reject_subscription\",\"identifier\":\"\(escapedIdentifier)\"}"
}

func messageFrame(identifier: String, payload: String) -> String {
  let escapedIdentifier = identifier.replacingOccurrences(of: "\"", with: "\\\"")
  return "{\"identifier\":\"\(escapedIdentifier)\",\"message\":\(payload)}"
}

final class TestCableClientSocket: CableSocket, @unchecked Sendable {
  private let lock = NSLock()
  private var frames: [String]
  private var recordedText: [String] = []
  private var closed = false
  private var pendingReceive: CheckedContinuation<String, any Error>?
  private var pendingRecordedWaiters: [(Int, CheckedContinuation<Void, Never>)] = []

  init(frames: [String]) {
    self.frames = frames
  }

  func open() async throws {}

  func close() {
    let continuation = lock.withLock {
      closed = true
      let continuation = pendingReceive
      pendingReceive = nil
      return continuation
    }
    continuation?.resume(throwing: CableSocketError.closed)
  }

  func send(_ text: String) async throws {
    let result = lock.withLock { () -> Result<[CheckedContinuation<Void, Never>], CableSocketError> in
      guard !closed else {
        return .failure(.closed)
      }
      recordedText.append(text)
      let waiters = pendingRecordedWaiters.filter { $0.0 <= recordedText.count }.map(\.1)
      pendingRecordedWaiters.removeAll { $0.0 <= recordedText.count }
      return .success(waiters)
    }
    switch result {
    case let .success(waiters):
      waiters.forEach { $0.resume() }
    case let .failure(error):
      throw error
    }
  }

  func receive() async throws -> String {
    try await withCheckedThrowingContinuation { continuation in
      let result = lock.withLock { () -> Result<String, any Error>? in
        if closed {
          return .failure(CableSocketError.closed)
        }
        if !frames.isEmpty {
          return .success(frames.removeFirst())
        }
        pendingReceive = continuation
        return nil
      }
      if let result {
        continuation.resume(with: result)
      }
    }
  }

  func recordedTexts() -> [String] {
    lock.withLock { recordedText }
  }

  func waitForRecordedCount(_ count: Int) async {
    await withCheckedContinuation { continuation in
      let resumeNow = lock.withLock {
        if recordedText.count >= count {
          return true
        }
        pendingRecordedWaiters.append((count, continuation))
        return false
      }
      if resumeNow {
        continuation.resume()
      }
    }
  }
}
