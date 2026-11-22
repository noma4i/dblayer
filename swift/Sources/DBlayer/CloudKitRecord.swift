import CloudKit
import Foundation

enum CloudKitRecord {
  enum Failure: Error {
    case invalidRecord
  }

  static func make(zone: CKRecordZone.ID, key: String, payload: Data, systemFields: Data?) throws -> CKRecord {
    let id = CKRecord.ID(recordName: key, zoneID: zone)
    let record: CKRecord
    if let systemFields {
      let decoder = try NSKeyedUnarchiver(forReadingFrom: systemFields)
      decoder.requiresSecureCoding = true
      decoder.decodingFailurePolicy = .setErrorAndReturn
      let decoded = CKRecord(coder: decoder)
      decoder.finishDecoding()
      guard decoder.error == nil, let decoded, decoded.recordID == id,
        decoded.recordType == "DBlayerRecord" else { throw Failure.invalidRecord }
      record = decoded
    } else {
      record = CKRecord(recordType: "DBlayerRecord", recordID: id)
    }
    record.encryptedValues["payload"] = payload as NSData
    return record
  }

  static func values(_ record: CKRecord, zone: CKRecordZone.ID) throws -> (key: String, payload: Data, systemFields: Data) {
    guard record.recordID.zoneID == zone, record.recordType == "DBlayerRecord",
      let payload = record.encryptedValues["payload"] as? Data else { throw Failure.invalidRecord }
    let encoder = NSKeyedArchiver(requiringSecureCoding: true)
    record.encodeSystemFields(with: encoder)
    encoder.finishEncoding()
    return (record.recordID.recordName, payload, encoder.encodedData)
  }
}
