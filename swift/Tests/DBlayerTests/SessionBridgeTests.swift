import Foundation
import Testing
@testable import DBlayer

private struct PhoneBody: Codable, Sendable, Equatable { let phone: String }
private struct TicketAnswer: Codable, Sendable, Equatable { let ticket: String }

private final class RecordingSession: Database, @unchecked Sendable {
  private let lock = NSLock()
  private var recorded: [String] = []
  var calls: [String] { lock.withLock { recorded } }
  private func record(_ line: String) { lock.withLock { recorded.append(line) } }

  override func sessionCall(operation: String, bodyJson: String) async throws -> String {
    record("call \(operation) \(bodyJson)")
    return #"{"ticket":"t-1"}"#
  }
  override func sessionResume() async throws -> String? {
    record("resume")
    return #"{"ticket":"t-2"}"#
  }
  override func sessionReset() async throws { record("reset") }
  override func retry(epoch: UInt64, model: String, rowId: String, action: String?) async throws -> UInt64 {
    record("retry \(model) \(rowId) \(action ?? "nil")")
    return 3
  }
  override func retryAll() async throws -> UInt64 {
    record("retryAll")
    return 7
  }
}

extension BridgeProbeTests {
  @MainActor private func withRecordingSession(_ body: (RecordingSession) async throws -> Void) async throws {
    try await reopen(config: Config(path: nil, dataVersion: nil, cacheRows: 8), modelsJson: modelsJSON, transport: nil)
    let real = try DB.database
    defer { DB.current = real }
    let recording = RecordingSession(noHandle: .init())
    DB.current = recording
    try await body(recording)
  }

  @Test @MainActor func aSessionCallSendsTheEncodedBodyAndDecodesTheAnswer() async throws {
    try await withRecordingSession { recording in
      let answer = try await DB.sessionCall("otp", body: PhoneBody(phone: "+61400000000"), as: TicketAnswer.self)
      #expect(answer == TicketAnswer(ticket: "t-1"))
      try await DB.sessionCall("otp", body: PhoneBody(phone: "+61411111111"))
      #expect(recording.calls == [#"call otp {"phone":"+61400000000"}"#, #"call otp {"phone":"+61411111111"}"#])
    }
  }

  @Test @MainActor func sessionResumeDecodesTheAnswerAndResetReachesTheCore() async throws {
    try await withRecordingSession { recording in
      #expect(try await DB.sessionResume(as: TicketAnswer.self) == TicketAnswer(ticket: "t-2"))
      try await DB.sessionReset()
      #expect(recording.calls == ["resume", "reset"])
    }
  }

  @Test @MainActor func retryByIdAndRetryAllReachTheCoreWithTheirCounts() async throws {
    try await withRecordingSession { recording in
      let liked = try await Moment.retry(id: "42", action: "like")
      let plain = try await Moment.retry(id: "43", action: nil)
      let all = try await DB.retryAll()
      #expect([liked, plain, all] == [3, 3, 7])
      #expect(recording.calls == ["retry Moment 42 like", "retry Moment 43 nil", "retryAll"])
    }
  }
}
