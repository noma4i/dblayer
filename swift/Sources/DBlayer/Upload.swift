import Foundation

public struct UploadRequest: Sendable {
  public var path: String
  public var name: String?
  public var mime: String?
  public var variables: [String: JSONValue]

  public init(path: String, name: String? = nil, mime: String? = nil, variables: [String: JSONValue] = [:]) {
    (self.path, self.name, self.mime, self.variables) = (path, name, mime, variables)
  }

  var wire: WireUploadRequest {
    get throws { WireUploadRequest(path: path, name: name, mime: mime, variablesJson: try JSONValue.object(variables).text) }
  }
}

public struct UploadItem: Sendable {
  public var values: [String: JSONValue]
  public var path: String
  public var name: String?
  public var mime: String?
  public var variables: [String: JSONValue]

  public init(values: [String: JSONValue], path: String, name: String? = nil, mime: String? = nil, variables: [String: JSONValue] = [:]) {
    (self.values, self.path, self.name, self.mime, self.variables) = (values, path, name, mime, variables)
  }

  var wire: WireUploadItem {
    get throws { WireUploadItem(valuesJson: try JSONValue.object(values).text, path: path, name: name, mime: mime, variablesJson: try JSONValue.object(variables).text) }
  }
}

public struct UploadBatchRequest: Sendable {
  public var operation: String
  public var variables: [String: JSONValue]

  public init(operation: String, variables: [String: JSONValue] = [:]) {
    (self.operation, self.variables) = (operation, variables)
  }

  var wire: WireUploadBatch {
    get throws { WireUploadBatch(operation: operation, variablesJson: try JSONValue.object(variables).text) }
  }
}

extension DBModel {
  @concurrent public func cancelUpload(action: String, in scope: AccountScope? = nil) async throws -> Bool {
    let (database, epoch) = try DB.admitted(scope)
    return try await database.cancelUpload(epoch: epoch, model: Self.modelName, rowId: id, action: action)
  }
}
