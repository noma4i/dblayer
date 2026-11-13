import Foundation

public struct Prepare: Sendable {
  let wire: WirePrepare

  public static func video(limits: VideoLimits? = nil, mirror: Bool = false, onCannotDecode: CannotDecode = .fail, maxBytes: UInt64? = nil) -> Self { Self(wire: .video(limits: limits, mirror: mirror, onCannotDecode: onCannotDecode, maxBytes: maxBytes)) }
  public static func image(edit: PhotoEdit, quality: Double? = nil, format: ImageFormat = .jpeg, maxBytes: UInt64? = nil) -> Self { Self(wire: .image(edit: edit, quality: quality, format: format, maxBytes: maxBytes)) }
}

public struct UploadRequest: Sendable {
  public var path: String?
  public var name: String?
  public var mime: String?
  public var variables: [String: JSONValue]
  public var prepare: Prepare?

  public init(path: String, name: String? = nil, mime: String? = nil, variables: [String: JSONValue] = [:], prepare: Prepare? = nil) {
    (self.path, self.name, self.mime, self.variables, self.prepare) = (path, name, mime, variables, prepare)
  }

  private init(variables: [String: JSONValue]) {
    (path, name, mime, self.variables, prepare) = (nil, nil, nil, variables, nil)
  }

  public static func pending(variables: [String: JSONValue] = [:]) -> Self { Self(variables: variables) }

  var wire: WireUploadRequest {
    get throws { WireUploadRequest(source: path.map { .file(path: $0, name: name, mime: mime, prepare: prepare?.wire) } ?? .pending, variablesJson: try JSONValue.object(variables).text) }
  }
}

/// A file sent before its row exists. `id` is the id of the row `commit` creates.
public struct StagedUpload<Model: DBModel>: Sendable {
  public let id: String
  public let action: String
  let database: Database
  let epoch: UInt64

  /// Creates the row with `values` and sends the final mutation with `variables`. A second call throws `Invalid`.
  nonisolated(nonsending) public func commit(values: [String: JSONValue], variables: [String: JSONValue] = [:]) async throws -> MutationReceipt<Model> {
    try await DB.ordered(database, epoch: epoch) { root, database, epoch in
      let receipt = try await database.commitStaged(epoch: epoch, model: Model.modelName, rowId: id, action: action, valuesJson: JSONValue.object(values).text, variablesJson: JSONValue.object(variables).text)
      return try MutationReceipt(database: root, wire: receipt)
    }
  }

  /// Removes the transfer and its files. Returns the value of the core.
  nonisolated(nonsending) public func discard() async throws -> Bool {
    try await DB.ordered(database, epoch: epoch) { root, database, epoch in
      try await database.cancelUpload(epoch: epoch, model: Model.modelName, rowId: id, action: action)
    }
  }
}

public struct UploadItem: Sendable {
  public var values: [String: JSONValue]
  public var path: String?
  public var name: String?
  public var mime: String?
  public var variables: [String: JSONValue]
  public var prepare: Prepare?

  public init(values: [String: JSONValue], path: String, name: String? = nil, mime: String? = nil, variables: [String: JSONValue] = [:], prepare: Prepare? = nil) {
    (self.values, self.path, self.name, self.mime, self.variables, self.prepare) = (values, path, name, mime, variables, prepare)
  }

  public init(values: [String: JSONValue], request: UploadRequest) {
    (self.values, path, name, mime, variables, prepare) = (values, request.path, request.name, request.mime, request.variables, request.prepare)
  }

  var wire: WireUploadItem {
    get throws { WireUploadItem(valuesJson: try JSONValue.object(values).text, source: path.map { .file(path: $0, name: name, mime: mime, prepare: prepare?.wire) } ?? .pending, variablesJson: try JSONValue.object(variables).text) }
  }
}

public struct UploadSource: Sendable {
  public var path: String
  public var name: String?
  public var mime: String?
  public var values: [String: JSONValue]
  public var variables: [String: JSONValue]
  public var prepare: Prepare?

  public init(path: String, name: String? = nil, mime: String? = nil, values: [String: JSONValue] = [:], variables: [String: JSONValue] = [:], prepare: Prepare? = nil) {
    (self.path, self.name, self.mime, self.values, self.variables, self.prepare) = (path, name, mime, values, variables, prepare)
  }

  var wire: WireUploadSource {
    get throws { WireUploadSource(path: path, name: name, mime: mime, valuesJson: try JSONValue.object(values).text, variablesJson: try JSONValue.object(variables).text, prepare: prepare?.wire) }
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
  nonisolated(nonsending) public func supplySource(action: String, source: UploadSource, in scope: AccountScope? = nil) async throws { try await Self.supplySource(id: id, action: action, source: source, in: scope) }
  nonisolated(nonsending) public static func supplySource(id: String, action: String, source: UploadSource, in scope: AccountScope? = nil) async throws {
    try await DB.ordered(scope) { root, database, epoch in
      try await database.supplySource(epoch: epoch, model: modelName, rowId: id, action: action, source: source.wire)
    }
  }
  nonisolated(nonsending) public func failSource(action: String, message: String, in scope: AccountScope? = nil) async throws { try await Self.failSource(id: id, action: action, message: message, in: scope) }
  nonisolated(nonsending) public static func failSource(id: String, action: String, message: String, in scope: AccountScope? = nil) async throws {
    try await DB.ordered(scope) { root, database, epoch in
      try await database.failSource(epoch: epoch, model: modelName, rowId: id, action: action, message: message)
    }
  }
  nonisolated(nonsending) public func cancelUpload(action: String, in scope: AccountScope? = nil) async throws -> Bool {
    try await DB.ordered(scope) { root, database, epoch in
      return try await database.cancelUpload(epoch: epoch, model: Self.modelName, rowId: id, action: action)
    }
  }
}
