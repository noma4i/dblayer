import Foundation

public enum DB {
  static let reachability = Reachability()
  public nonisolated(unsafe) static var database: Database!

  public static func open(endpoint: URL, headers: [String: String] = [:], modelsJSON: String, path: String? = nil, operations: [JSONValue] = []) throws {
    let transport = UrlSessionTransport(endpoint: endpoint, headers: headers)
    let database = try Database.open(config: Config(path: path, dataVersion: nil, cacheRows: 2000), modelsJson: modelsJSON, transport: transport)
    try database.registerOperations(defsJson: JSONValue.array(operations).text)
    reachability.start(try database.onlineSink())
    self.database = database
  }
}
