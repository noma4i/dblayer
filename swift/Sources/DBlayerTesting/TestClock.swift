@_spi(Testing) import DBlayer
import Foundation

extension TestClock {
  public func advance(by interval: TimeInterval) { advance(millis: UInt64(interval * 1000)) }
}

extension DB {
  /// Opens the database on a clock that only `advance(by:)` moves, so freshness windows, retry delays and expiry run without sleeping.
  @discardableResult
  public static func openTesting(transport: Transport?, modelsJSON: String, path: String? = nil, operations: [any GraphQLOperation.Type] = [], startingAt start: Date = Date(), encrypted: EncryptedConfig? = nil, credentials: CredentialStore? = nil, http: EncryptedHttp? = nil) async throws -> TestClock {
    let clock = TestClock(startMillis: UInt64(start.timeIntervalSince1970 * 1000))
    let database = try await openOnClock(path: path, modelsJson: modelsJSON, transport: transport, endpoint: scriptedEndpoint, clock: clock, encrypted: encrypted, credentials: credentials, http: http)
    try await install(database, transport: transport, operations: operations)
    return clock
  }
}
