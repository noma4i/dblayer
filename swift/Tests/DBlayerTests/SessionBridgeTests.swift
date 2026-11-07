import Foundation
import Testing
@_spi(Testing) @testable import DBlayer

private struct AccountBody: Codable, Sendable, Equatable { let account: String }
private struct SessionAnswer: Codable, Sendable, Equatable { let sessionId: String }

extension BridgeProbeTests {
  @Test(.timeLimit(.minutes(1))) @MainActor func aSessionCallSendsTheEncodedBodyAndDecodesTheAnswerAfterAResetOfTheSession() async throws {
    try await reopenSigned(config: Config(path: nil, dataVersion: nil), modelsJson: modelsJSON)
    try await DB.sessionReset()
    try DB.database.setOnline(online: true)
    let answer = try await DB.sessionCall("login_link", body: AccountBody(account: "stand"), as: SessionAnswer.self)
    #expect(answer == SessionAnswer(sessionId: "stand"))
    try await DB.sessionCall("login_link", body: AccountBody(account: "stand"))
  }

  @Test @MainActor func retryByIdReachesTheCoreAndItsErrorComesBack() async throws {
    try await reopenSigned(config: Config(path: nil, dataVersion: nil), modelsJson: modelsJSON)
    try await DB.close()
    for action in ["like", nil] {
      do { try await Moment.retry(id: "42", action: action); Issue.record("retry on a closed database succeeded") } catch DbError.Closed(_) {} catch { Issue.record("retry surfaced as \(error), not as the closed error of the core") }
    }
  }
}
