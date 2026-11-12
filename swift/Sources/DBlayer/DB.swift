import Foundation

public enum DB {
  @_spi(Testing) public static let opener = OpenDatabase()

  static var database: Database {
    get throws { try opener.opened() }
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

  @concurrent public static func call<Answer: Decodable & Sendable>(_ operation: String, variables: [String: JSONValue], as _: Answer.Type, in scope: AccountScope? = nil) async throws -> Answer {
    let (database, epoch) = try admitted(scope)
    return try JSONDecoder().decode(Answer.self, from: Data(try await database.call(epoch: epoch, operation: operation, variablesJson: JSONValue.object(variables).text).utf8))
  }

  @concurrent public static func call(_ operation: String, variables: [String: JSONValue], in scope: AccountScope? = nil) async throws {
    let (database, epoch) = try admitted(scope)
    _ = try await database.call(epoch: epoch, operation: operation, variablesJson: JSONValue.object(variables).text)
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

  /// The session now: epoch, identity of its credential and phase. Read it again after a stream ends with the session or after `sessionCall` and `sessionReset`.
  public static func sessionStatus() throws -> SessionStatus {
    try database.sessionStatus()
  }

  private static func sessionBodyText<Body: Encodable>(_ body: Body) throws -> String {
    String(decoding: try JSONEncoder().encode(body), as: UTF8.self)
  }

  public static func serverConfig(encrypted: EncryptedConfig, platform: AppPlatform, version: String, build: String? = nil, http: EncryptedHttp? = nil, retries: RetryProgress? = nil) async throws -> ServerConfig {
    _ = NetworkStatus()
    return try await fetchServerConfig(encrypted: encrypted, platform: platform, version: version, build: build, http: http ?? UrlSessionTransport(), retries: retries, observers: nil)
  }

  public static func open(endpoint: URL? = nil, headers: @escaping @Sendable () -> [String: String] = { [:] }, encrypted: EncryptedConfig? = nil, credentials: CredentialStore? = nil, transport: Transport? = nil, modelsJSON: String, path: String? = nil, operations: [any GraphQLOperation.Type] = [], http: EncryptedHttp? = nil) async throws {
    _ = NetworkStatus()
    let puts = UrlSessionPuts.available()
    UrlSessionPuts.current.set(puts)
    let ports = OpenPorts(transport: transport, encrypted: encrypted, credentials: credentials, http: http, endpoint: endpoint?.absoluteString, executor: UrlSessionTransport(headers: headers), dataSyncDeclared: nil, preparer: SystemPreparer(), detachedPut: puts != nil, putExecutor: puts)
    let database = try await opener.open(config: Config(path: path, dataVersion: nil), modelsJson: modelsJSON, options: OpenOptions(operations: definitions(operations)), ports: ports, observers: SystemObservers())
    puts?.attach(database)
  }

  private static func definitions(_ operations: [any GraphQLOperation.Type]) -> [OperationDefinition] {
    operations.map { OperationDefinition(name: $0.operationName, document: $0.document) }
  }
}
