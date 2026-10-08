import Foundation
import Testing
@testable import DBlayer

@Suite(.serialized)
struct CableConnectionTests {
  @Test
  func emitsParsedFramesAndSkipsUnknownText() async throws {
    let parsed = makeConnection(
      socket: TestCableSocket(frames: [
        "{\"type\":\"welcome\"}",
        "{\"type\":\"ping\",\"message\":1700000000}"
      ])
    )
    let skipping = makeConnection(socket: TestCableSocket(frames: ["nonsense", "{\"type\":\"welcome\"}"]))

    let parsedFrames = try await receivedFrames(from: parsed, count: 2)
    let skippedFrames = try await receivedFrames(from: skipping, count: 1)

    #expect(parsedFrames == [.welcome, .ping(Date(timeIntervalSince1970: 1_700_000_000))])
    #expect(skippedFrames == [.welcome])
  }

  @Test
  func sendFailsBeforeOpenAndWritesCommandJSONAfter() async throws {
    let socket = TestCableSocket(frames: [])
    let connection = makeConnection(socket: socket)

    do {
      try await connection.send(.subscribe(identifier: "chan"))
      Issue.record("Expected CableSocketError.closed")
    } catch let error as CableSocketError {
      guard case .closed = error else {
        Issue.record("Expected CableSocketError.closed")
        return
      }
    } catch {
      Issue.record("Expected CableSocketError.closed")
    }
    #expect(socket.recordedTexts().isEmpty)

    await connection.open()
    try await connection.send(.subscribe(identifier: "chan"))

    #expect(socket.recordedTexts() == ["{\"command\":\"subscribe\",\"identifier\":\"chan\"}"])
    await connection.close()
  }

  private func makeConnection(socket: TestCableSocket) -> CableConnection {
    CableConnection(
      url: URL(fileURLWithPath: "/test"),
      token: "token",
      makeSocket: { _, _ in socket }
    )
  }

  private func receivedFrames(
    from connection: CableConnection,
    count: Int
  ) async throws -> [CableFrame] {
    let stream = await connection.frames
    await connection.open()
    var frames: [CableFrame] = []
    for await frame in stream {
      frames.append(frame)
      if frames.count == count {
        await connection.close()
      }
    }
    return frames
  }
}

private final class TestCableSocket: CableSocket, @unchecked Sendable {
  private let lock = NSLock()
  private var frames: [String]
  private var recordedText: [String] = []
  private var closed = false
  private var pendingReceive: CheckedContinuation<String, any Error>?

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
    let isClosed = lock.withLock {
      if closed {
        return true
      }
      recordedText.append(text)
      return false
    }
    if isClosed {
      throw CableSocketError.closed
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
}
