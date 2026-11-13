@_spi(Testing) import DBlayer
import Foundation

extension TestClock {
  public func advance(by interval: TimeInterval) {
    advance(millis: UInt64(interval * 1000))
    TestPulse.shared.pulse()
  }
}

extension DB {
  /// Opens the database on a clock that only `advance(by:)` moves, so freshness windows, retry delays and expiry run without sleeping. The platform reports online at once unless the test passes its own `observers`.
  @discardableResult
  public static func openTesting(transport: Transport?, modelsJSON: String, path: String? = nil, operations: [any GraphQLOperation.Type] = [], startingAt start: Date = Date(), encrypted: EncryptedConfig? = nil, credentials: CredentialStore? = nil, http: EncryptedHttp? = nil, observers: (any PlatformObserver)? = nil, preparer: Preparer? = ScriptedPreparer()) async throws -> TestClock {
    let clock = TestClock(startMillis: UInt64(start.timeIntervalSince1970 * 1000))
    let definitions = operations.map { OperationDefinition(name: $0.operationName, document: $0.document) }
    do { try await DB.opener.opened().closeDatabase() } catch DbError.NotOpen(_) {}
    let database = try await DB.opener.openWithClock(config: Config(path: path), modelsJson: modelsJSON, ports: OpenPorts(transport: transport, encrypted: encrypted, credentials: credentials, http: http, endpoint: scriptedEndpoint, preparer: preparer, partialOperations: true), clock: clock, options: OpenOptions(operations: definitions), observers: observers ?? OnlineAtOpen())
    await ChangePump.start(database)
    return clock
  }
}
