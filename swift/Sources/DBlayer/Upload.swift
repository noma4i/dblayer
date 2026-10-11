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
    let single = createBlob.map { JSONValue.object(["create": .string($0)]) } ?? .null
    let parts = multipart.map { JSONValue.object(["init": .string($0.initiate), "presign": .string($0.presign), "complete": .string($0.complete), "abort": .string($0.abort)]) } ?? .null
    let steps = JSONValue.object(["single": single, "multipart": parts, "mutation": .string(mutation), "signed_id_variable": .string("signedId"), "blob_id_variable": .null])
    return JSONValue.object(["path": .string(path), "name": name.map(JSONValue.string) ?? .null, "mime": mime.map(JSONValue.string) ?? .null, "variables": .object(variables), "steps": steps]).text
  }
}

extension DBModel {
  /// Creates the record locally at once and sends its file in the background. The record carries `upload` progress, and the mutation creates it on the server when the file is stored. A failed upload leaves the record failed: `retry()` sends again, `discard()` removes it.
  public static func create(values: [String: JSONValue], uploading request: UploadRequest) async throws -> Self {
    try decode(try await DB.database.uploadCreate(model: modelName, valuesJson: JSONValue.object(values).text, inputJson: request.json))
  }

  /// Sends the file of an existing record, then runs the content mutation.
  public func upload(_ request: UploadRequest) async throws { try await DB.database.upload(model: Self.modelName, rowId: id, inputJson: request.json) }

  public func cancelUpload() async throws -> Bool { try await DB.database.cancelUpload(model: Self.modelName, rowId: id) }
}
