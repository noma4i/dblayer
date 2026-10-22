import CloudKit
import Foundation
import Testing
@testable import DBlayer

private let zone = CKRecordZone.ID(zoneName: CloudKitRecords.zoneName, ownerName: CKCurrentUserDefaultName)

private func sized(_ count: Int) -> CloudBlob {
  CloudBlob(key: "a1b2", bytes: Data((0..<count).map { UInt8(truncatingIfNeeded: $0 &* 31) }))
}

struct CloudKitRecordsTests {
  @Test
  func smallBlobsTravelInlineAndLargeOnesAsAnAssetWithTheBytesUnchanged() throws {
    for count in [0, 1, CloudKitRecords.inlineLimit, CloudKitRecords.inlineLimit + 1] {
      let original = sized(count)
      let record = try CloudKitRecords.record(for: original, zoneID: zone)
      #expect(record.recordID.recordName == original.key)
      #expect(record.recordID.zoneID == zone)
      #expect(record.allKeys().count == 1)
      #expect((record[CloudKitRecords.assetField] != nil) == (count > CloudKitRecords.inlineLimit))
      #expect(try CloudKitRecords.blob(from: record) == original)
      CloudKitRecords.discardAssets(of: [record])
    }
  }

  @Test
  func recordsWithoutBytesOrOfAnotherTypeAreRejected() throws {
    let empty = CKRecord(recordType: CloudKitRecords.recordType, recordID: CKRecord.ID(recordName: "x", zoneID: zone))
    #expect(throws: CloudError.self) { try CloudKitRecords.blob(from: empty) }
    let foreign = CKRecord(recordType: "Other", recordID: CKRecord.ID(recordName: "x", zoneID: zone))
    foreign[CloudKitRecords.bytesField] = Data([1])
    #expect(throws: CloudError.self) { try CloudKitRecords.blob(from: foreign) }
  }
}
