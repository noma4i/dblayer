import Foundation

public enum DB {
  nonisolated(unsafe) static var reachability = Reachability.shared
  nonisolated(unsafe) static var current: Database?
  /// The one database of the process. Throws `DatabaseNotOpen` before `open` and after `close`.
  public static var database: Database {
    get throws {
      guard let current else { throw DatabaseNotOpen() }
      return current
    }
  }
  private static let opener = OpenDatabase()
  static let wake = BackgroundWake()

  /// Ends the session: clears the stored data. Entities that readers hold become empty and receive the next session's rows.
  @MainActor public static func logout() async throws {
    try await database.logout()
    EntityRegistries.resetAll()
  }

  /// Closes the database. Every entity becomes closed, and `open` may open the database again. Meant for tests and the end of the process.
  @MainActor public static func close() {
    EntityRegistries.closeAll()
    ForegroundEvents.shared.stop()
    wake.detach()
    current?.shutdown()
    current = nil
  }

  /// The live subscriptions of the database, for probes of how many a screen opens.
  public static func subscriptionCensus() throws -> SubscriptionCensus { try database.subscriptionCensus() }

  /// What the entity observers did: facts told, deliveries skipped as not newer than the row, reads that failed. A failed read leaves the id dirty and the core reads it again.
  public static func entityCensus() throws -> EntityCensus { try database.entityCensus() }

  /// Lease calls the core refused and rows that failed to decode for an entity, for probes that tell a refused lease from a lost delivery.
  @MainActor public static func entityLeaseFailures() -> Int { EntityRegistries.leaseFailures() }

  /// What the open discarded: the reason the database file was recreated, and the models whose rows a schema change cleared. Empty when nothing was discarded.
  public static func openReport() throws -> OpenReport { try database.openReport() }

  /// Opens the subscriptions that models declare with `listen`. Call it after sign-in, stop it with `unlisten()`.
  public static func listen() throws { try database.listen() }
  public static func unlisten() throws { try database.unlisten() }
  /// Reloads the stale relations that have readers. The library already reloads stale windows when the app returns to the foreground and when the network returns. Call this for a manual refresh and after sign-in: it also restarts stopped subscriptions and singleton entities.
  @concurrent public static func resume() async throws {
    try await database.resume()
    await EntityRegistries.resumeAll()
  }

  /// Sends one registered operation that no model owns and decodes the value at its root field. The generated `Operations` calls use it.
  @concurrent public static func run<Answer: Decodable & Sendable>(_ operation: String, variables: [String: JSONValue], as _: Answer.Type) async throws -> Answer {
    try JSONDecoder().decode(Answer.self, from: Data(try await database.run(operation: operation, variablesJson: JSONValue.object(variables).text).utf8))
  }

  @concurrent public static func run(_ operation: String, variables: [String: JSONValue]) async throws {
    _ = try await database.run(operation: operation, variablesJson: JSONValue.object(variables).text)
  }

  /// Sends a session operation declared in the encrypted config and decodes its answer. A refusal throws the typed error.
  @concurrent public static func sessionCall<Body: Encodable & Sendable, Answer: Decodable & Sendable>(_ operation: String, body: Body, as _: Answer.Type) async throws -> Answer {
    try JSONDecoder().decode(Answer.self, from: Data(try await database.sessionCall(operation: operation, bodyJson: sessionBodyText(body)).utf8))
  }

  @concurrent public static func sessionCall<Body: Encodable & Sendable>(_ operation: String, body: Body) async throws {
    _ = try await database.sessionCall(operation: operation, bodyJson: sessionBodyText(body))
  }

  /// Restores the stored session. Returns the answer of the resume call, or `nil` when there is nothing to resume.
  @concurrent public static func sessionResume<Answer: Decodable & Sendable>(as _: Answer.Type) async throws -> Answer? {
    try await database.sessionResume().map { try JSONDecoder().decode(Answer.self, from: Data($0.utf8)) }
  }

  /// Drops the stored session and its credential, so the next sign-in starts clean.
  public static func sessionReset() throws { try database.sessionReset() }

  /// Sends every failed and delivery-unknown change again. Returns the number of reset operations or upload items.
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
    let opened = try await opener.open(config: Config(path: path, dataVersion: nil, cacheRows: 2000), modelsJson: modelsJSON, options: OpenOptions(operations: JSONValue.array(definitions).text, uploads: uploads?.wire), transport: selectedTransport, encrypted: encrypted, credentials: credentials, http: http ?? origin.map { UrlSessionEncryptedHttp(origin: $0) }, retries: retries)
    let sink = try opened.onlineSink()
    reachability.start(sink)
    ForegroundEvents.shared.start { sink.enterForeground() }
    current = opened
    attachUploads(selectedTransport, to: opened)
  }

  /// The one line the app adds to `application(_:handleEventsForBackgroundURLSession:completionHandler:)`.
  public static func handleBackgroundSession(identifier: String, completion: @escaping @Sendable () -> Void) {
    wake.handle(identifier: identifier, completion: completion)
  }

  static func attachUploads(_ transport: Transport?, to database: Database) {
    let uploads = transport as? UrlSessionTransport
    uploads?.connectUploads { database.putTasks(tasks: $0) }
    wake.attach(uploads: uploads, database: database)
  }

  @_spi(Testing) public static func openOnClock(config: Config, modelsJson: String, transport: Transport?, uploads: UploadOperations? = nil, clock: TestClock) async throws -> Database {
    try await opener.openWithClock(config: config, modelsJson: modelsJson, transport: transport, uploads: uploads?.wire, clock: clock)
  }

  @_spi(Testing) public static func install(_ database: Database, transport: Transport?, operations: [any GraphQLOperation.Type]) throws {
    let definitions = operations.map { JSONValue.object(["name": .string($0.operationName), "document": .string($0.document)]) }
    if transport != nil { try database.registerOperations(defsJson: JSONValue.array(definitions).text) }
    let sink = try database.onlineSink()
    reachability.start(sink)
    ForegroundEvents.shared.start { sink.enterForeground() }
    current = database
  }
}

public struct DatabaseNotOpen: Error, LocalizedError, CustomStringConvertible {
  public init() {}
  public var description: String { "database is not open; call DBlayer.open once at app start" }
  public var errorDescription: String? { description }
}
