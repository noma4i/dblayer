import Foundation

public enum DB {
  nonisolated(unsafe) static var reachability = Reachability.shared
  nonisolated(unsafe) static var current: Database?
  public static var database: Database {
    get throws {
      guard let current else { throw DatabaseNotOpen() }
      return current
    }
  }
  private static let opener = OpenDatabase()
  static let wake = BackgroundWake()

  @MainActor public static func logout() async throws {
    try await database.logout()
    EntityRegistries.resetAll()
  }

  @MainActor public static func close() {
    EntityRegistries.closeAll()
    ForegroundEvents.shared.stop()
    PowerSaving.shared.stop()
    wake.detach()
    current?.shutdown()
    current = nil
  }

  public static func subscriptionCensus() throws -> SubscriptionCensus { try database.subscriptionCensus() }

  public static func entityCensus() throws -> EntityCensus { try database.entityCensus() }

  @MainActor public static func entityLeaseFailures() -> Int { EntityRegistries.leaseFailures() }

  public static func openReport() throws -> OpenReport { try database.openReport() }

  /// Reloads the stale relations that have readers. The library already reloads stale windows when the app returns to the foreground, when the network returns and after a sign-in. Call this for a manual refresh: it also restarts stopped subscriptions and singleton entities.
  @concurrent public static func resume() async throws {
    try await database.resume()
    await EntityRegistries.resumeAll()
  }

  @concurrent public static func run<Answer: Decodable & Sendable>(_ operation: String, variables: [String: JSONValue], as _: Answer.Type) async throws -> Answer {
    try JSONDecoder().decode(Answer.self, from: Data(try await database.run(operation: operation, variablesJson: JSONValue.object(variables).text).utf8))
  }

  @concurrent public static func run(_ operation: String, variables: [String: JSONValue]) async throws {
    _ = try await database.run(operation: operation, variablesJson: JSONValue.object(variables).text)
  }

  @concurrent public static func sessionCall<Body: Encodable & Sendable, Answer: Decodable & Sendable>(_ operation: String, body: Body, as _: Answer.Type) async throws -> Answer {
    let answer = try JSONDecoder().decode(Answer.self, from: Data(try await database.sessionCall(operation: operation, bodyJson: sessionBodyText(body)).utf8))
    await EntityRegistries.resumeAll()
    return answer
  }

  @concurrent public static func sessionCall<Body: Encodable & Sendable>(_ operation: String, body: Body) async throws {
    _ = try await database.sessionCall(operation: operation, bodyJson: sessionBodyText(body))
    await EntityRegistries.resumeAll()
  }

  @concurrent public static func sessionResume<Answer: Decodable & Sendable>(as _: Answer.Type) async throws -> Answer? {
    try await database.sessionResume().map { try JSONDecoder().decode(Answer.self, from: Data($0.utf8)) }
  }

  public static func sessionReset() throws { try database.sessionReset() }

  /// Reloads every `preload` scope now, fresh or not, and returns when all have settled. Offline it throws without a request. When some scopes fail the error names them. Call it for pull to refresh instead of refreshing each reference list.
  @concurrent public static func refreshPreloads() async throws { try await database.refreshPreloads() }

  @discardableResult @concurrent public static func retryAll() async throws -> UInt64 { try await database.retryAll() }

  private static func sessionBodyText<Body: Encodable>(_ body: Body) throws -> String {
    String(decoding: try JSONEncoder().encode(body), as: UTF8.self)
  }

  public static func serverConfig(encrypted: EncryptedConfig, platform: AppPlatform, version: String, build: String? = nil, http: EncryptedHttp? = nil, retries: RetryProgress? = nil) async throws -> ServerConfig {
    try await fetchServerConfig(encrypted: encrypted, platform: platform, version: version, build: build, http: http ?? UrlSessionEncryptedHttp(origin: encrypted.origin), retries: retries, connectivity: reachability.start())
  }

  @MainActor public static func open(endpoint: URL? = nil, headers: @escaping @Sendable () -> [String: String] = { [:] }, encrypted: EncryptedConfig? = nil, credentials: CredentialStore? = nil, transport: Transport? = nil, modelsJSON: String, path: String? = nil, operations: [any GraphQLOperation.Type] = [], uploads: UploadOperations? = nil, http: EncryptedHttp? = nil, retries: RetryProgress? = nil) async throws {
    let definitions = operations.map { JSONValue.object(["name": .string($0.operationName), "document": .string($0.document)]) }
    let origin = encrypted.flatMap { URL(string: $0.origin) }
    let selectedTransport = transport ?? (endpoint ?? origin).map { UrlSessionTransport(endpoint: $0, headers: headers, cablePath: encrypted?.endpoints.cable) }
    let opened = try await opener.open(config: Config(path: path, dataVersion: nil, cacheRows: 2000), modelsJson: modelsJSON, options: OpenOptions(operations: JSONValue.array(definitions).text, uploads: uploads?.wire, powerSaving: ProcessInfo.processInfo.isLowPowerModeEnabled, lowRam: false, preload: true), transport: selectedTransport, encrypted: encrypted, credentials: credentials, http: http ?? origin.map { UrlSessionEncryptedHttp(origin: $0) }, retries: retries)
    let sink = try opened.onlineSink()
    reachability.start(sink)
    ForegroundEvents.shared.start { sink.enterForeground() }
    PowerSaving.shared.start { sink.setPowerSaving(saving: $0) }
    current = opened
    attachUploads(selectedTransport, to: opened)
  }

  public static func handleBackgroundSession(identifier: String, completion: @escaping @Sendable () -> Void) {
    wake.handle(identifier: identifier, completion: completion)
  }

  static func attachUploads(_ transport: Transport?, to database: Database) {
    let uploads = transport as? UrlSessionTransport
    uploads?.connectUploads { database.putTasks(tasks: $0) }
    wake.attach(uploads: uploads, database: database)
  }

  @_spi(Testing) public static func openOnClock(config: Config, modelsJson: String, transport: Transport?, uploads: UploadOperations? = nil, clock: TestClock, preload: Bool) async throws -> Database {
    try await opener.openWithClock(config: config, modelsJson: modelsJson, transport: transport, uploads: uploads?.wire, clock: clock, preload: preload)
  }

  @_spi(Testing) public static func install(_ database: Database, transport: Transport?, operations: [any GraphQLOperation.Type]) throws {
    let definitions = operations.map { JSONValue.object(["name": .string($0.operationName), "document": .string($0.document)]) }
    if transport != nil { try database.registerOperations(defsJson: JSONValue.array(definitions).text) }
    let sink = try database.onlineSink()
    reachability.start(sink)
    ForegroundEvents.shared.start { sink.enterForeground() }
    PowerSaving.shared.start { sink.setPowerSaving(saving: $0) }
    current = database
  }
}

public struct DatabaseNotOpen: Error, LocalizedError, CustomStringConvertible {
  public init() {}
  public var description: String { "database is not open; call DBlayer.open once at app start" }
  public var errorDescription: String? { description }
}
