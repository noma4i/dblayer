use crate::cloud::{CloudField, CloudRecord, RecordError, CloudStamp, StampedValue, decode, encode, record_key};
use serde_json::json;

#[test]
fn cloud_record_has_canonical_bytes_and_rejects_mismatched_identity() {
    let stamp = CloudStamp { millis: 1000, counter: 0, replica: [1; 16] };
    let make = |name: &str, value| CloudField {
        path: vec!["body".into(), name.into()],
        data: StampedValue { value, stamp },
    };
    let title = make("title", json!("hello"));
    let subtitle = make("subtitle", json!(null));
    let record = CloudRecord {
        model: "note:archive".into(), row_id: "n1".into(), server_revision: Some(9), deleted: false,
        fields: vec![subtitle.clone(), title.clone()],
    };
    let key = record_key(&record.model, &record.row_id);
    let encoded = encode(&record).unwrap();
    assert_eq!(decode(&key, &encoded), Ok(record.clone()));
    assert_eq!(encode(&CloudRecord { fields: vec![title.clone(), subtitle], ..record.clone() }), Ok(encoded.clone()));
    let wrong_key = record_key("note", "archive:n1");
    assert_ne!(key, wrong_key);
    assert_eq!(decode(&wrong_key, &encoded), Err(RecordError::Invalid));
    assert_eq!(encode(&CloudRecord { fields: vec![title.clone(), title], ..record.clone() }), Err(RecordError::Invalid));
    assert_eq!(encode(&CloudRecord { deleted: true, ..record.clone() }), Err(RecordError::Invalid));
    let oversized = CloudRecord { fields: vec![make("title", json!("a".repeat(900_001)))], ..record };
    assert_eq!(encode(&oversized), Err(RecordError::TooLarge));
}
