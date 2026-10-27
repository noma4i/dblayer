@_spi(Testing) import DBlayer
import Foundation

extension TestClock {
  public func advance(by interval: TimeInterval) { advance(millis: UInt64(interval * 1000)) }
}

extension DB {
  /// Opens the database on a clock that only `advance(by:)` moves, so freshness windows, retry delays and expiry run without sleeping. Scope preloads send no request unless `preload` is true.
  @discardableResult
  public static func openTesting(transport: Transport?, modelsJSON: String, path: String? = nil, operations: [any GraphQLOperation.Type] = [], uploads: UploadOperations? = nil, startingAt start: Date = Date(), preload: Bool = false, listening: Bool = true) async throws -> TestClock {
    let clock = TestClock(startMillis: UInt64(start.timeIntervalSince1970 * 1000))
    let database = try await openOnClock(config: Config(path: path, dataVersion: nil, cacheRows: 2000), modelsJson: modelsJSON, transport: transport, uploads: uploads, clock: clock, preload: preload, listening: listening)
    try install(database, transport: transport, operations: operations)
    return clock
  }
}
