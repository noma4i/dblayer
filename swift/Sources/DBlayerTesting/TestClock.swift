@_spi(Testing) import DBlayer
import Foundation

extension TestClock {
  public func advance(by interval: TimeInterval) { advance(millis: UInt64(interval * 1000)) }
}

extension DB {
  /// Opens the database on a clock that only `advance(by:)` moves, so freshness windows, retry delays and expiry run without sleeping.
  @discardableResult
  public static func openTesting(transport: Transport?, modelsJSON: String, path: String? = nil, operations: [any GraphQLOperation.Type] = [], startingAt start: Date = Date()) throws -> TestClock {
    let clock = TestClock(startMillis: UInt64(start.timeIntervalSince1970 * 1000))
    let database = try Database.openWithClock(config: Config(path: path, dataVersion: nil, cacheRows: 2000), modelsJson: modelsJSON, transport: transport, clock: clock)
    try install(database, transport: transport, operations: operations)
    return clock
  }
}
