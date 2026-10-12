import Foundation

public enum DB {
  static let reachability = Reachability()
  public nonisolated(unsafe) static var database: Database!

  /// Ends the session: clears the stored data and drops every entity, lease and retained value.
  @MainActor public static func logout() throws {
    try database.logout()
    EntityRegistries.resetAll()
  }

  /// The live subscriptions of the database, for probes of how many a screen opens.
  public static func subscriptionCensus() -> SubscriptionCensus { database.subscriptionCensus() }

  /// Opens the subscriptions that models declare with `listen`. Call it after sign-in, stop it with `unlisten()`.
  public static func listen() throws { try database.listen() }
  public static func unlisten() throws { try database.unlisten() }
  /// Reloads the stale relations that have readers. Call it when the app returns to the foreground.
  public static func resume() async throws { try await database.resume() }

  public static func open(endpoint: URL, headers: [String: String] = [:], modelsJSON: String, path: String? = nil, operations: [any GraphQLOperation.Type] = []) throws {
    try open(transport: UrlSessionTransport(endpoint: endpoint, headers: headers), modelsJSON: modelsJSON, path: path, operations: operations)
  }

  public static func open(transport: Transport?, modelsJSON: String, path: String? = nil, operations: [any GraphQLOperation.Type] = []) throws {
    let database = try Database.open(config: Config(path: path, dataVersion: nil, cacheRows: 2000), modelsJson: modelsJSON, transport: transport)
    let definitions = operations.map { JSONValue.object(["name": .string($0.operationName), "document": .string($0.document)]) }
    if transport != nil { try database.registerOperations(defsJson: JSONValue.array(definitions).text) }
    reachability.start(try database.onlineSink())
    self.database = database
  }
}
