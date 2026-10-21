import Foundation

public enum DB {
  static let reachability = Reachability()
  public nonisolated(unsafe) static var database: Database!
  @MainActor private static var opening: (OpenArguments, Task<Void, Error>)?

  private struct OpenArguments: Equatable {
    let endpoint: URL?
    let headers: [String: String]
    let encrypted: EncryptedConfig?
    let references: [AnyHashable?]
    let models: String
    let path: String?
    let operations: [String]
  }

  /// Ends the session: clears the stored data and drops every entity, lease and retained value.
  @MainActor public static func logout() async throws {
    try await database.logout()
    EntityRegistries.resetAll()
  }

  /// The live subscriptions of the database, for probes of how many a screen opens.
  public static func subscriptionCensus() -> SubscriptionCensus { database.subscriptionCensus() }

  /// What the open discarded: the reason the database file was recreated, and the models whose rows a schema change cleared. Empty when nothing was discarded.
  public static func openReport() -> OpenReport { database.openReport() }

  /// Opens the subscriptions that models declare with `listen`. Call it after sign-in, stop it with `unlisten()`.
  public static func listen() throws { try database.listen() }
  public static func unlisten() throws { try database.unlisten() }
  /// Reloads the stale relations that have readers. Call it when the app returns to the foreground.
  public static func resume() async throws {
    try await database.resume()
    await EntityRegistries.resumeAll()
  }

  /// Sends one registered operation that no model owns and decodes the value at its root field. The generated `Operations` calls use it.
  public static func run<Answer: Decodable & Sendable>(_ operation: String, variables: [String: JSONValue], as _: Answer.Type) async throws -> Answer {
    try JSONDecoder().decode(Answer.self, from: Data(try await database.run(operation: operation, variablesJson: JSONValue.object(variables).text).utf8))
  }

  public static func run(_ operation: String, variables: [String: JSONValue]) async throws {
    _ = try await database.run(operation: operation, variablesJson: JSONValue.object(variables).text)
  }

  public static func serverConfig(encrypted: EncryptedConfig, platform: AppPlatform, version: String, build: String? = nil, http: EncryptedHttp? = nil, retryListener: RetryListener? = nil) async throws -> ServerConfig {
    try await fetchServerConfig(encrypted: encrypted, platform: platform, version: version, build: build, http: http ?? UrlSessionEncryptedHttp(origin: encrypted.origin), retryListener: retryListener)
  }

  private static func reference(_ object: AnyObject) -> AnyHashable {
    (object as? AnyHashable) ?? AnyHashable(ObjectIdentifier(object))
  }

  @MainActor public static func open(endpoint: URL? = nil, headers: [String: String] = [:], encrypted: EncryptedConfig? = nil, credentials: CredentialStore? = nil, transport: Transport? = nil, modelsJSON: String, path: String? = nil, operations: [any GraphQLOperation.Type] = [], http: EncryptedHttp? = nil) async throws {
    let arguments = OpenArguments(endpoint: endpoint, headers: headers, encrypted: encrypted, references: [credentials.map(reference), transport.map(reference), http.map(reference)], models: modelsJSON, path: path, operations: operations.flatMap { [$0.operationName, $0.document] })
    if let (previous, task) = opening {
      guard previous == arguments else { throw DbError.Invalid(detail: "The database is already opening or open with different parameters") }
      return try await task.value
    }
    let task = Task { @MainActor in
      do {
        let origin = encrypted.flatMap { URL(string: $0.origin) }
        let transport = transport ?? (endpoint ?? origin).map { UrlSessionTransport(endpoint: $0, headers: headers, cablePath: encrypted?.endpoints.cable) }
        let database = try await Database.open(config: Config(path: path, dataVersion: nil, cacheRows: 2000), modelsJson: modelsJSON, transport: transport, encrypted: encrypted, credentials: credentials, http: http ?? origin.map { UrlSessionEncryptedHttp(origin: $0) })
        try install(database, transport: transport, operations: operations)
      } catch {
        opening = nil
        throw error
      }
    }
    opening = (arguments, task)
    try await task.value
  }

  @_spi(Testing) public static func install(_ database: Database, transport: Transport?, operations: [any GraphQLOperation.Type]) throws {
    let definitions = operations.map { JSONValue.object(["name": .string($0.operationName), "document": .string($0.document)]) }
    if transport != nil { try database.registerOperations(defsJson: JSONValue.array(definitions).text) }
    reachability.start(try database.onlineSink())
    self.database = database
  }
}
