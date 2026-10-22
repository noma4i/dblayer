import CloudKit
import Foundation

enum CloudKitRecords {
  static let zoneName = "DBlayerCloud"
  static let recordType = "Change"
  static let subscriptionID = "dblayer-cloud"
  static let bytesField = "bytes"
  static let assetField = "asset"
  static let inlineLimit = 900_000

  static func record(for blob: CloudBlob, zoneID: CKRecordZone.ID) throws -> CKRecord {
    let record = CKRecord(recordType: recordType, recordID: CKRecord.ID(recordName: blob.key, zoneID: zoneID))
    if blob.bytes.count <= inlineLimit {
      record[bytesField] = blob.bytes
    } else {
      let file = FileManager.default.temporaryDirectory.appendingPathComponent(UUID().uuidString)
      try blob.bytes.write(to: file)
      record[assetField] = CKAsset(fileURL: file)
    }
    return record
  }

  static func blob(from record: CKRecord) throws -> CloudBlob {
    guard record.recordType == recordType else {
      throw CloudError.Rejected(detail: "malformed cloud record \(record.recordID.recordName)")
    }
    let key = record.recordID.recordName
    if let bytes = record[bytesField] as? Data { return CloudBlob(key: key, bytes: bytes) }
    if let url = (record[assetField] as? CKAsset)?.fileURL, let bytes = try? Data(contentsOf: url) {
      return CloudBlob(key: key, bytes: bytes)
    }
    throw CloudError.Rejected(detail: "malformed cloud record \(key)")
  }

  static func discardAssets(of records: [CKRecord]) {
    for record in records {
      if let url = (record[assetField] as? CKAsset)?.fileURL { try? FileManager.default.removeItem(at: url) }
    }
  }
}
