import Foundation

public enum DB {
  static let reachability = Reachability()
  public nonisolated(unsafe) static var database: Database!

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
