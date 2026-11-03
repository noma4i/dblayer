import Foundation
import Observation

public struct BlobUpload: Sendable {
  public let name: String
  public let create: String
  public init(name: String, create: String) { self.name = name; self.create = create }
}

public typealias BlobSnapshot = WireBlobSnapshot

/// The status of one media upload. It reads the status stream of its account until it is released, and releasing it ends the stream in the core.
@Observable @MainActor
public final class Blob: Encodable, @unchecked Sendable {
  public nonisolated let id: String
  public private(set) var snapshot: BlobSnapshot
  @ObservationIgnored nonisolated private let database: Database
  @ObservationIgnored nonisolated private let epoch: UInt64
  @ObservationIgnored private let status: UploadStatusStream
  @ObservationIgnored private var task: Task<Void, Never>?

  init(database: Database, epoch: UInt64, snapshot: BlobSnapshot, status: UploadStatusStream) {
    self.database = database
    self.epoch = epoch
    self.snapshot = snapshot
    self.id = snapshot.id
    self.status = status
    task = Task { [weak self, status] in
      for await next in status {
        guard let self else { return }
        self.snapshot = next
      }
    }
  }

  isolated deinit { task?.cancel(); status.stream.cancel() }
  public nonisolated func encode(to encoder: Encoder) throws { var value = encoder.singleValueContainer(); try value.encode(id) }
  @concurrent public func waitUntilUploaded() async throws -> String { try await database.waitBlob(epoch: epoch, id: id) }
  @concurrent public func retry() async throws { try await database.retryBlob(epoch: epoch, id: id) }
  @concurrent public func discard() async throws { try await database.discardBlob(epoch: epoch, id: id) }
}

extension DB {
  @concurrent public static func upload(_ upload: BlobUpload, request: UploadRequest) async throws -> Blob {
    let scope = try AccountScope()
    let database = try DB.database
    let snapshot = try await database.uploadBlob(epoch: scope.epoch, name: upload.name, create: upload.create, request: request.wire)
    let status = scope.uploadStatus(snapshot.id)
    return await Blob(database: database, epoch: scope.epoch, snapshot: snapshot, status: status)
  }
  @concurrent public static func blob(_ id: String) async throws -> Blob {
    let scope = try AccountScope()
    let database = try DB.database
    let snapshot = try await database.blob(epoch: scope.epoch, id: id)
    let status = scope.uploadStatus(id)
    return await Blob(database: database, epoch: scope.epoch, snapshot: snapshot, status: status)
  }
}
