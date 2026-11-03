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

  /// Closes the database. Every request, load and subscription ends, and the call returns after the library released its resources.
  @MainActor public static func close() async throws {
    ForegroundEvents.shared.stop()
    PowerSaving.shared.stop()
    reachability.detach()
    let closing = current
    current = nil
    try await closing?.closeDatabase()
  }

  public static func subscriptionCensus() throws -> SubscriptionCensus { try database.subscriptionCensus() }

  public static func openReport() throws -> OpenReport { try database.openReport() }

  /// Reloads the stale relations that have readers. The library already reloads stale windows when the app returns to the foreground, when the network returns and after a sign-in. Call this for a manual refresh: it also restarts stopped subscriptions.
  @concurrent public static func resume() async throws {
    try await database.resume()
  }

  @concurrent public static func run<Answer: Decodable & Sendable>(_ operation: String, variables: [String: JSONValue], as _: Answer.Type) async throws -> Answer {
    try JSONDecoder().decode(Answer.self, from: Data(try await database.run(operation: operation, variablesJson: JSONValue.object(variables).text).utf8))
  }

  @concurrent public static func run(_ operation: String, variables: [String: JSONValue]) async throws {
    _ = try await database.run(operation: operation, variablesJson: JSONValue.object(variables).text)
  }

  @concurrent public static func sessionCall<Body: Encodable & Sendable, Answer: Decodable & Sendable>(_ operation: String, body: Body, as _: Answer.Type) async throws -> Answer {
    let answer = try JSONDecoder().decode(Answer.self, from: Data(try await database.sessionCall(operation: operation, bodyJson: sessionBodyText(body)).utf8))
    return answer
  }

  @concurrent public static func sessionCall<Body: Encodable & Sendable>(_ operation: String, body: Body) async throws {
    _ = try await database.sessionCall(operation: operation, bodyJson: sessionBodyText(body))
  }

  @concurrent public static func sessionResume<Answer: Decodable & Sendable>(as _: Answer.Type) async throws -> Answer? {
    try await database.sessionResume().map { try JSONDecoder().decode(Answer.self, from: Data($0.utf8)) }
  }

  /// Ends the session: work of the old session ends, the credential and the data of the account go. Call it for every sign-out; the library clears the data itself.
  public static func sessionReset() async throws {
    try await database.sessionReset()
  }

  /// Tells the library whether the app lets the model subscriptions run. `open(listening: false)` starts none from a committed credential until `setListening(true)`. Allowed with a committed credential, they start once. `setListening(false)` and `sessionReset()` stop them. Call it from the app state that gates subscriptions, for example registration complete.
  public static func setListening(_ allowed: Bool) throws { try database.setListening(allowed: allowed) }

  /// Reloads every `preload` scope now, fresh or not, and returns when all have settled. Offline it throws without a request. When some scopes fail the error names them. Call it for pull to refresh instead of refreshing each reference list.
  @concurrent public static func refreshPreloads() async throws { try await database.refreshPreloads() }

  @discardableResult @concurrent public static func retryAll() async throws -> UInt64 { try await database.retryAll() }

  private static func sessionBodyText<Body: Encodable>(_ body: Body) throws -> String {
    String(decoding: try JSONEncoder().encode(body), as: UTF8.self)
  }

  public static func serverConfig(encrypted: EncryptedConfig, platform: AppPlatform, version: String, build: String? = nil, http: EncryptedHttp? = nil, retries: RetryProgress? = nil) async throws -> ServerConfig {
    try await fetchServerConfig(encrypted: encrypted, platform: platform, version: version, build: build, http: http ?? UrlSessionEncryptedHttp(origin: encrypted.origin), retries: retries, connectivity: reachability.start())
  }

  @MainActor public static func open(endpoint: URL? = nil, headers: @escaping @Sendable () -> [String: String] = { [:] }, encrypted: EncryptedConfig? = nil, credentials: CredentialStore? = nil, transport: Transport? = nil, modelsJSON: String, path: String? = nil, operations: [any GraphQLOperation.Type] = [], uploads: UploadOperations? = nil, http: EncryptedHttp? = nil, retries: RetryProgress? = nil, listening: Bool = true) async throws {
    let definitions = operations.map { JSONValue.object(["name": .string($0.operationName), "document": .string($0.document)]) }
    let origin = encrypted.flatMap { URL(string: $0.origin) }
    let selectedTransport = transport ?? (endpoint ?? origin).map { UrlSessionTransport(endpoint: $0, headers: headers, cablePath: encrypted?.endpoints.cable) }
    let opened = try await opener.open(config: Config(path: path, dataVersion: nil, cacheRows: 2000), modelsJson: modelsJSON, options: OpenOptions(operations: JSONValue.array(definitions).text, uploads: uploads?.wire, powerSaving: ProcessInfo.processInfo.isLowPowerModeEnabled, lowRam: false, preload: true, listening: listening), transport: selectedTransport, encrypted: encrypted, credentials: credentials, http: http ?? origin.map { UrlSessionEncryptedHttp(origin: $0) }, retries: retries)
    let sink = try opened.onlineSink()
    reachability.start(sink)
    ForegroundEvents.shared.start { sink.enterForeground() }
    PowerSaving.shared.start { sink.setPowerSaving(saving: $0) }
    current = opened
  }

  @_spi(Testing) public static func openOnClock(config: Config, modelsJson: String, transport: Transport?, uploads: UploadOperations? = nil, clock: TestClock, preload: Bool, listening: Bool = true, encrypted: EncryptedConfig? = nil, credentials: CredentialStore? = nil, http: EncryptedHttp? = nil, retries: RetryProgress? = nil) async throws -> Database {
    try await opener.openWithClock(config: config, modelsJson: modelsJson, transport: transport, uploads: uploads?.wire, clock: clock, preload: preload, listening: listening, encrypted: encrypted, credentials: credentials, http: http, retries: retries)
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
