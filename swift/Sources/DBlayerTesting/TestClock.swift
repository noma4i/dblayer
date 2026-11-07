@_spi(Testing) import DBlayer
import Foundation

extension TestClock {
  public func advance(by interval: TimeInterval) {
    advance(millis: UInt64(interval * 1000))
    TestPulse.shared.pulse()
  }
}

private let opener = OpenDatabase()

private final class OnlineAtOnce: PlatformObserver {
  func start(sink: OnlineSink) { sink.setOnline(online: true) }
  func stop() {}
}

extension DB {
  /// Opens the database on a clock that only `advance(by:)` moves, so freshness windows, retry delays and expiry run without sleeping. The platform reports online at once unless the test passes its own `observers`.
  @discardableResult
  public static func openTesting(transport: Transport?, modelsJSON: String, path: String? = nil, operations: [any GraphQLOperation.Type] = [], startingAt start: Date = Date(), encrypted: EncryptedConfig? = nil, credentials: CredentialStore? = nil, http: EncryptedHttp? = nil, observers: (any PlatformObserver)? = nil) async throws -> TestClock {
    let clock = TestClock(startMillis: UInt64(start.timeIntervalSince1970 * 1000))
    let definitions = operations.map { OperationDefinition(name: $0.operationName, document: $0.document) }
    let database = try await opener.openWithClock(config: Config(path: path, dataVersion: nil), modelsJson: modelsJSON, ports: OpenPorts(transport: transport, encrypted: encrypted, credentials: credentials, http: http, endpoint: scriptedEndpoint), clock: clock, options: OpenOptions(operations: definitions), observers: observers ?? OnlineAtOnce())
    adopt(database)
    await ChangePump.start(database)
    return clock
  }
}
