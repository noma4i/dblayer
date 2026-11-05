import Foundation
import os

final class Opened: Sendable {
  let database: Database
  private let stops: [@Sendable () -> Void]

  init(database: Database, stops: [@Sendable () -> Void]) {
    self.database = database
    self.stops = stops
  }

  func stopObservers() { stops.forEach { $0() } }
  func standingIn(_ database: Database) -> Opened { Opened(database: database, stops: stops) }
}

public enum DB {
  private static let slot = OSAllocatedUnfairLock<Opened?>(initialState: nil)
  private static let opener = OpenDatabase()
  private static let cacheRows: UInt32 = 2000

  static var database: Database {
    get throws {
      guard let opened = slot.withLock({ $0 }) else { throw DatabaseNotOpen() }
      return opened.database
    }
  }

  /// The database and the epoch a command belongs to: the pair the scope holds when the caller has one, else the open database and the epoch of its session now.
  static func admitted(_ scope: AccountScope?) throws -> (Database, UInt64) {
    if let scope { return (scope.database, scope.epoch) }
    let database = try database
    return (database, database.sessionEpoch())
  }

  /// Closes the database. Every request, load and subscription ends, and the call returns after the library released its resources. A call on the closed database gets the closed error of the core.
  @MainActor public static func close() async throws {
    guard let opened = slot.withLock({ $0 }) else { return }
    opened.stopObservers()
    do { try await opened.database.closeDatabase() } catch DbError.Closed(_) {}
  }

  public static func subscriptionCensus() throws -> SubscriptionCensus { try database.subscriptionCensus() }

  public static func openReport() throws -> OpenReport { try database.openReport() }

  @concurrent public static func run<Answer: Decodable & Sendable>(_ operation: String, variables: [String: JSONValue], as _: Answer.Type, in scope: AccountScope? = nil) async throws -> Answer {
    let (database, epoch) = try admitted(scope)
    return try JSONDecoder().decode(Answer.self, from: Data(try await database.run(epoch: epoch, operation: operation, variablesJson: JSONValue.object(variables).text).utf8))
  }

  @concurrent public static func run(_ operation: String, variables: [String: JSONValue], in scope: AccountScope? = nil) async throws {
    let (database, epoch) = try admitted(scope)
    _ = try await database.run(epoch: epoch, operation: operation, variablesJson: JSONValue.object(variables).text)
  }

  @concurrent public static func sessionCall<Body: Encodable & Sendable, Answer: Decodable & Sendable>(_ operation: String, body: Body, as _: Answer.Type) async throws -> Answer {
    let answer = try JSONDecoder().decode(Answer.self, from: Data(try await database.sessionCall(operation: operation, bodyJson: sessionBodyText(body)).utf8))
    return answer
  }

  @concurrent public static func sessionCall<Body: Encodable & Sendable>(_ operation: String, body: Body) async throws {
    _ = try await database.sessionCall(operation: operation, bodyJson: sessionBodyText(body))
  }

  /// Ends the session: work of the old session ends, the credential and the data of the account go. Call it for every sign-out; the library clears the data itself.
  public static func sessionReset() async throws {
    try await database.sessionReset()
  }

  private static func sessionBodyText<Body: Encodable>(_ body: Body) throws -> String {
    String(decoding: try JSONEncoder().encode(body), as: UTF8.self)
  }

  public static func serverConfig(encrypted: EncryptedConfig, platform: AppPlatform, version: String, build: String? = nil, http: EncryptedHttp? = nil, retries: RetryProgress? = nil) async throws -> ServerConfig {
    let connectivity = Connectivity(online: false)
    let stop = Reachability.observe { connectivity.setOnline(online: $0) }
    defer { stop() }
    return try await fetchServerConfig(encrypted: encrypted, platform: platform, version: version, build: build, http: http ?? UrlSessionEncryptedHttp(origin: encrypted.origin), retries: retries, connectivity: connectivity)
  }

  @MainActor public static func open(endpoint: URL? = nil, headers: @escaping @Sendable () -> [String: String] = { [:] }, encrypted: EncryptedConfig? = nil, credentials: CredentialStore? = nil, transport: Transport? = nil, modelsJSON: String, path: String? = nil, operations: [any GraphQLOperation.Type] = [], uploads: UploadOperations? = nil, http: EncryptedHttp? = nil, retries: RetryProgress? = nil) async throws {
    let definitions = operations.map { JSONValue.object(["name": .string($0.operationName), "document": .string($0.document)]) }
    let origin = encrypted.flatMap { URL(string: $0.origin) }
    let selectedTransport = transport ?? (endpoint ?? origin).map { UrlSessionTransport(endpoint: $0, headers: headers) }
    let opened = try await opener.open(config: Config(path: path, dataVersion: nil, cacheRows: cacheRows), modelsJson: modelsJSON, options: OpenOptions(operations: JSONValue.array(definitions).text, uploads: uploads?.wire, powerSaving: ProcessInfo.processInfo.isLowPowerModeEnabled, lowRam: PowerSaving.lowRam), transport: selectedTransport, encrypted: encrypted, credentials: credentials, http: http ?? origin.map { UrlSessionEncryptedHttp(origin: $0) }, retries: retries)
    try attach(opened)
  }

  @_spi(Testing) public static func openOnClock(path: String?, modelsJson: String, transport: Transport?, uploads: UploadOperations? = nil, clock: TestClock, encrypted: EncryptedConfig? = nil, credentials: CredentialStore? = nil, http: EncryptedHttp? = nil, retries: RetryProgress? = nil) async throws -> Database {
    try await opener.openWithClock(config: Config(path: path, dataVersion: nil, cacheRows: cacheRows), modelsJson: modelsJson, transport: transport, uploads: uploads?.wire, clock: clock, encrypted: encrypted, credentials: credentials, http: http, retries: retries)
  }

  @_spi(Testing) public static func install(_ database: Database, transport: Transport?, operations: [any GraphQLOperation.Type]) throws {
    let definitions = operations.map { JSONValue.object(["name": .string($0.operationName), "document": .string($0.document)]) }
    if transport != nil { try database.registerOperations(defsJson: JSONValue.array(definitions).text) }
    try attach(database)
  }

  /// Hands a stand-in database to the calls and keeps the observers of the open one.
  static func use(_ database: Database) {
    slot.withLock { $0 = $0?.standingIn(database) ?? Opened(database: database, stops: []) }
  }

  private static func attach(_ database: Database) throws {
    let sink = try database.onlineSink()
    let opened = Opened(database: database, stops: [
      Reachability.observe { sink.setOnline(online: $0) },
      ForegroundEvents.observe { sink.enterForeground() },
      PowerSaving.observe { sink.setPowerSaving(saving: $0) }
    ])
    let previous = slot.withLock { current -> Opened? in
      defer { current = opened }
      return current
    }
    previous?.stopObservers()
  }
}

public struct DatabaseNotOpen: Error, LocalizedError, CustomStringConvertible {
  public init() {}
  public var description: String { "database is not open; call DBlayer.open once at app start" }
  public var errorDescription: String? { description }
}
