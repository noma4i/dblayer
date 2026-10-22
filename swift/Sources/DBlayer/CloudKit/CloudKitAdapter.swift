import CloudKit
import Foundation

public final class CloudKitAdapter: CloudIo, @unchecked Sendable {
  private let wire: CloudKitWire
  private let zoneID: CKRecordZone.ID

  init(wire: CloudKitWire, zoneID: CKRecordZone.ID) {
    self.wire = wire
    self.zoneID = zoneID
  }

  public convenience init(container: CKContainer) {
    let wire = CloudKitContainerWire(container: container)
    self.init(wire: wire, zoneID: wire.recordZoneID)
  }

  public func account() async throws -> CloudAccount {
    do {
      switch try await wire.accountStatus() {
      case .available: return .available
      case .noAccount: return .signedOut
      case .restricted: return .restricted
      case .temporarilyUnavailable: return .needsConsent
      case .couldNotDetermine: throw CloudError.Unavailable(detail: "the iCloud account status is unknown")
      @unknown default: throw CloudError.Unavailable(detail: "the iCloud account status is unknown")
      }
    } catch {
      throw CloudKitErrors.map(error)
    }
  }

  public func pull(cursor: Data?, limit: UInt32) async throws -> CloudPage {
    do {
      try await wire.prepare()
      let token = cursor.flatMap { $0.isEmpty ? nil : $0 }
      let fetched: CloudKitFetch
      do {
        fetched = try await wire.fetch(token: token, limit: Int(limit))
      } catch let error as CKError where error.code == .changeTokenExpired {
        fetched = try await wire.fetch(token: nil, limit: Int(limit))
      }
      return CloudPage(blobs: try fetched.records.map(CloudKitRecords.blob), cursor: fetched.token, more: fetched.more)
    } catch {
      throw CloudKitErrors.map(error)
    }
  }

  public func push(blobs: [CloudBlob]) async throws {
    if blobs.isEmpty { return }
    do {
      try await wire.prepare()
      let records = try blobs.map { try CloudKitRecords.record(for: $0, zoneID: zoneID) }
      defer { CloudKitRecords.discardAssets(of: records) }
      try await wire.save(records)
    } catch {
      throw CloudKitErrors.map(error)
    }
  }
}
