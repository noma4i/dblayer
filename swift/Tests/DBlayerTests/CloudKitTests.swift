import CloudKit
import Foundation
import Testing
@testable import DBlayer

struct CloudKitTests {
  @Test func recordPayloadStaysEncryptedAndSystemFieldsStayScoped() throws {
    let zone = CKRecordZone.ID(zoneName: "fixture", ownerName: CKCurrentUserDefaultName)
    let other = CKRecordZone.ID(zoneName: "other", ownerName: CKCurrentUserDefaultName)
    let original = try CloudKitRecord.make(zone: zone, key: "note", payload: Data([0, 1, 2]), systemFields: nil)
    #expect(original["payload"] == nil)
    let read = try CloudKitRecord.values(original, zone: zone)
    #expect(read.key == "note")
    #expect(read.payload == Data([0, 1, 2]))

    let updated = try CloudKitRecord.make(zone: zone, key: read.key, payload: Data([3, 4]), systemFields: read.systemFields)
    #expect(updated.recordID == original.recordID)
    #expect(updated["payload"] == nil)
    #expect(try CloudKitRecord.values(updated, zone: zone).payload == Data([3, 4]))
    #expect(throws: CloudKitRecord.Failure.self) {
      _ = try CloudKitRecord.make(zone: other, key: read.key, payload: Data(), systemFields: read.systemFields)
    }
    #expect(throws: CloudKitRecord.Failure.self) {
      _ = try CloudKitRecord.values(original, zone: other)
    }
  }
}
