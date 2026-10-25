import Foundation

/// A registered operation of the upload declaration. `rootPath` names the response field the operation answers under; nil keeps the first field of its document.
public struct UploadOperation: Sendable {
  public var name: String
  public var rootPath: String?

  public init(_ name: String, rootPath: String? = nil) {
    (self.name, self.rootPath) = (name, rootPath)
  }

  var wire: WireUploadOperation { WireUploadOperation(name: name, rootPath: rootPath) }
}

/// The operations that move a file to storage, declared once in `DBlayer.open`. A file takes the single request through `createBlob` or the multipart requests, by its size.
public struct UploadOperations: Sendable {
  public struct Multipart: Sendable {
    public var initiate: UploadOperation, presign: UploadOperation, complete: UploadOperation, abort: UploadOperation
    public init(initiate: UploadOperation, presign: UploadOperation, complete: UploadOperation, abort: UploadOperation) {
      (self.initiate, self.presign, self.complete, self.abort) = (initiate, presign, complete, abort)
    }

    var wire: WireMultipartOperations { WireMultipartOperations(initiate: initiate.wire, presign: presign.wire, complete: complete.wire, abort: abort.wire) }
  }

  public var createBlob: UploadOperation?
  public var multipart: Multipart?

  public init(createBlob: UploadOperation? = nil, multipart: Multipart? = nil) {
    (self.createBlob, self.multipart) = (createBlob, multipart)
  }

  var wire: WireUploadOperations { WireUploadOperations(createBlob: createBlob?.wire, multipart: multipart?.wire) }
}

/// A local file sent to storage, then the content mutation of the declared upload action, which carries the stored blob as `signedId`.
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
  @concurrent public static func uploadCreate(items: [UploadItem], request: UploadBatchRequest) async throws -> [Self] {
    try await DB.database.uploadCreateBatch(model: modelName, items: items.map { try $0.wire }, request: request.wire).map(decode)
  }

  /// Creates the record locally at once and sends its file in the background through the declared upload `action`. The record carries `uploads[action]` progress, and the mutation creates it on the server when the file is stored. A failed upload leaves the record failed: `retry(action:)` sends again, `discard(action:)` removes it.
  @concurrent public static func create(values: [String: JSONValue], action: String, uploading request: UploadRequest) async throws -> Self {
    try decode(try await DB.database.uploadCreate(model: modelName, action: action, valuesJson: JSONValue.object(values).text, request: request.wire))
  }

  /// Sends the file of an existing record through the declared upload `action`, then runs its content mutation.
  @concurrent public func upload(action: String, _ request: UploadRequest) async throws { try await DB.database.upload(model: Self.modelName, rowId: id, action: action, request: request.wire) }

  @concurrent public func cancelUpload(action: String) async throws -> Bool { try await DB.database.cancelUpload(model: Self.modelName, rowId: id, action: action) }
}
