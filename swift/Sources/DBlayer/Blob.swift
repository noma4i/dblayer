import Foundation
import Observation

public struct BlobUpload: Sendable {
  public let name: String
  public let create: String
  public init(name: String, create: String) { self.name = name; self.create = create }
}

public typealias BlobSnapshot = WireBlobSnapshot

@Observable @MainActor
public final class Blob: Encodable, @unchecked Sendable {
  public nonisolated let id: String
  public private(set) var snapshot: BlobSnapshot
  @ObservationIgnored nonisolated private let database: Database
  @ObservationIgnored private let changes: BlobChanges
  @ObservationIgnored private var task: Task<Void, Never>?

  init(database: Database, snapshot: BlobSnapshot, changes: BlobChanges) {
    self.database = database
    self.snapshot = snapshot
    self.id = snapshot.id
    self.changes = changes
    task = Task { [weak self, changes] in
      do {
        while let next = try await changes.next() {
          guard let self else { return }
          self.snapshot = next
        }
      } catch is CancellationError {} catch {
        guard let self else { return }
        self.snapshot = BlobSnapshot(id: self.id, state: .failed, progress: self.snapshot.progress, sentBytes: self.snapshot.sentBytes, totalBytes: self.snapshot.totalBytes, error: String(describing: error), signedId: nil)
      }
    }
  }

  deinit { task?.cancel(); changes.cancel() }
  public nonisolated func encode(to encoder: Encoder) throws { var value = encoder.singleValueContainer(); try value.encode(id) }
  @concurrent public func waitUntilUploaded() async throws -> String { try await database.waitBlob(id: id) }
  @concurrent public func retry() async throws { try await database.retryBlob(id: id) }
  @concurrent public func discard() async throws { try await database.discardBlob(id: id) }
}

extension DB {
  @concurrent public static func upload(_ upload: BlobUpload, request: UploadRequest) async throws -> Blob {
    let database = try DB.database
    let snapshot = try await database.uploadBlob(name: upload.name, create: upload.create, request: request.wire)
    let changes = try await database.blobChanges(id: snapshot.id)
    return await Blob(database: database, snapshot: snapshot, changes: changes)
  }
  @concurrent public static func blob(_ id: String) async throws -> Blob {
    let database = try DB.database
    let snapshot = try await database.blob(id: id)
    let changes = try await database.blobChanges(id: id)
    return await Blob(database: database, snapshot: snapshot, changes: changes)
  }
}
