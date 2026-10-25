import Foundation

/// A local file sent to storage, then one content mutation that carries the stored blob as `signedId`.
public struct UploadRequest: Sendable {
  public struct Multipart: Sendable {
    public var initiate: String, presign: String, complete: String, abort: String
    public init(initiate: String, presign: String, complete: String, abort: String) {
      (self.initiate, self.presign, self.complete, self.abort) = (initiate, presign, complete, abort)
    }
  }

  public var path: String
  public var mutation: String
  public var createBlob: String?
  public var multipart: Multipart?
  public var name: String?
  public var mime: String?
  public var variables: [String: JSONValue]

  public init(path: String, mutation: String, createBlob: String? = nil, multipart: Multipart? = nil, name: String? = nil, mime: String? = nil, variables: [String: JSONValue] = [:]) {
    (self.path, self.mutation, self.createBlob, self.multipart, self.name, self.mime, self.variables) = (path, mutation, createBlob, multipart, name, mime, variables)
  }

  var json: String {
    get throws { try JSONValue.object(["path": .string(path), "name": name.map(JSONValue.string) ?? .null, "mime": mime.map(JSONValue.string) ?? .null, "variables": .object(variables), "steps": Self.steps(mutation: mutation, createBlob: createBlob, multipart: multipart)]).text }
  }

  static func steps(mutation: String, createBlob: String?, multipart: Multipart?) -> JSONValue {
    let single = createBlob.map { JSONValue.object(["create": .string($0)]) } ?? .null
    let parts = multipart.map { JSONValue.object(["init": .string($0.initiate), "presign": .string($0.presign), "complete": .string($0.complete), "abort": .string($0.abort)]) } ?? .null
    return .object(["single": single, "multipart": parts, "mutation": .string(mutation), "signed_id_variable": .string("signedId"), "blob_id_variable": .null])
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

  var value: JSONValue {
    .object(["values": .object(values), "path": .string(path), "name": name.map(JSONValue.string) ?? .null, "mime": mime.map(JSONValue.string) ?? .null, "variables": .object(variables)])
  }
}

public struct UploadBatchRequest: Sendable {
  public var operation: String
  public var createBlob: String?
  public var multipart: UploadRequest.Multipart?
  public var variables: [String: JSONValue]

  public init(operation: String, createBlob: String? = nil, multipart: UploadRequest.Multipart? = nil, variables: [String: JSONValue] = [:]) {
    (self.operation, self.createBlob, self.multipart, self.variables) = (operation, createBlob, multipart, variables)
  }

  var json: String {
    get throws {
      let steps = UploadRequest.steps(mutation: "", createBlob: createBlob, multipart: multipart)
      return try JSONValue.object(["operation": .string(operation), "steps": steps, "variables": .object(variables)]).text
    }
  }
}

extension DBModel {
  public static func uploadCreate(items: [UploadItem], request: UploadBatchRequest) async throws -> [Self] {
    try await DB.database.uploadCreateBatch(model: modelName, itemsJson: JSONValue.array(items.map(\.value)).text, inputJson: request.json).map(decode)
  }

  /// Creates the record locally at once and sends its file in the background. The record carries `upload` progress, and the mutation creates it on the server when the file is stored. A failed upload leaves the record failed: `retry()` sends again, `discard()` removes it.
  public static func create(values: [String: JSONValue], uploading request: UploadRequest) async throws -> Self {
    try decode(try await DB.database.uploadCreate(model: modelName, valuesJson: JSONValue.object(values).text, inputJson: request.json))
  }

  /// Sends the file of an existing record, then runs the content mutation.
  public func upload(_ request: UploadRequest) async throws { try await DB.database.upload(model: Self.modelName, rowId: id, inputJson: request.json) }

  public func cancelUpload() async throws -> Bool { try await DB.database.cancelUpload(model: Self.modelName, rowId: id) }
}
