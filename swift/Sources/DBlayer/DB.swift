import Foundation

public enum DB {
  private static var slot: Database?
  private static let opener = OpenDatabase()

  static var database: Database {
    get throws {
      guard let database = slot else { throw DbError.NotOpen(detail: "database is not open; call DBlayer.open once at app start") }
      return database
    }
  }

  /// The database and the epoch a command belongs to: the epoch the account scope holds, else the epoch the core admits for the open database.
  static func admitted(_ scope: AccountScope?) throws -> (Database, UInt64) {
    let target = try scope?.database ?? database
    return (target, try target.admit(accountEpoch: scope?.epoch))
  }

  /// Closes the database. Every request, load and subscription ends, and the call returns after the library released its resources. A call on the closed database, a second close included, gets the closed error of the core.
  public static func close() async throws {
    try await database.closeDatabase()
  }

  public static func subscriptionCensus() async throws -> SubscriptionCensus { try await database.subscriptionCensus() }

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

  /// The answer of the session call that the killed process left running, taken once: `nil` when there is none.
  @concurrent public static func interruptedSessionAnswer<Answer: Decodable & Sendable>(as _: Answer.Type) async throws -> Answer? {
    try await database.interruptedSessionAnswer().map { try JSONDecoder().decode(Answer.self, from: Data($0.utf8)) }
  }

  /// Ends the session: work of the old session ends, the credential and the data of the account go. Call it for every sign-out; the library clears the data itself.
  public static func sessionReset() async throws {
    try await database.sessionReset()
  }

  private static func sessionBodyText<Body: Encodable>(_ body: Body) throws -> String {
    String(decoding: try JSONEncoder().encode(body), as: UTF8.self)
  }

  public static func serverConfig(encrypted: EncryptedConfig, platform: AppPlatform, version: String, build: String? = nil, http: EncryptedHttp? = nil, retries: RetryProgress? = nil) async throws -> ServerConfig {
    try await fetchServerConfig(encrypted: encrypted, platform: platform, version: version, build: build, http: http ?? UrlSessionEncryptedHttp(), retries: retries, observers: SystemObservers())
  }

  public static func open(endpoint: URL? = nil, headers: @escaping @Sendable () -> [String: String] = { [:] }, encrypted: EncryptedConfig? = nil, credentials: CredentialStore? = nil, transport: Transport? = nil, modelsJSON: String, path: String? = nil, operations: [any GraphQLOperation.Type] = [], http: EncryptedHttp? = nil) async throws {
    let origin = encrypted.flatMap { URL(string: $0.origin) }
    let ports = OpenPorts(transport: transport ?? (endpoint ?? origin).map { _ in UrlSessionTransport(headers: headers) }, encrypted: encrypted, credentials: credentials, http: http ?? (encrypted == nil ? nil : UrlSessionEncryptedHttp(headers: headers)), endpoint: (endpoint ?? origin)?.absoluteString)
    let database = try await opener.open(config: Config(path: path, dataVersion: nil), modelsJson: modelsJSON, options: OpenOptions(operations: definitions(operations), dataSyncDeclared: nil), ports: ports, observers: SystemObservers())
    slot = database
  }

  /// The one test hook: makes `database` the open database of the calls, or none.
  @_spi(Testing) public static func adopt(_ database: Database?) { slot = database }

  private static func definitions(_ operations: [any GraphQLOperation.Type]) -> [OperationDefinition] {
    operations.map { OperationDefinition(name: $0.operationName, document: $0.document) }
  }
}
