use super::*;
use fixtures::*;
use crate::store::operation::QuarantineReason;
use serde_json::json;

#[test]
fn no_table_keeps_a_blob_column_except_the_file_bytes() {
    let (store, _) = booted_store();
    let blobs = "SELECT COUNT(*) FROM sqlite_master m, pragma_table_info(m.name) p WHERE m.type = 'table' AND p.type = 'BLOB' AND NOT (m.name = 'file' AND p.name = 'data')";
    assert_eq!(count(&store, blobs), 0);
}

#[test]
fn a_quarantined_payload_is_kept_as_value_rows() {
    let (store, _) = booted_store();
    let seq = crate::store::operation::quarantine_add(&store.lock().connection, Some("messages"), &json!({"a": [1, "x"]}), QuarantineReason::SchemaChanged).unwrap();
    assert_eq!(count(&store, &format!("SELECT COUNT(*) FROM quarantine_value WHERE seq = {seq}")), 4);
}

#[test]
fn the_reset_intent_is_columns_and_the_fingerprints_are_hashes() {
    let (store, _) = booted_store();
    super::reset::write_intent(&store.lock().connection, "cache_only", &["messages".to_string(), "items".to_string()], 5).unwrap();
    assert_eq!(count(&store, "SELECT COUNT(*) FROM reset_intent WHERE mode = 'cache_only' AND started_at = 5"), 1);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM reset_intent_model WHERE (pos = 0 AND model = 'messages') OR (pos = 1 AND model = 'items')"), 2);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM manifest WHERE key = 'reset_intent'"), 0);
    assert_eq!(store.reset_intent().unwrap(), Some("cache_only".to_string()));
    assert_eq!(count(&store, "SELECT COUNT(*) FROM manifest WHERE key LIKE 'fingerprint:%'"), 2);
    assert_eq!(count(&store, "SELECT COUNT(*) FROM manifest WHERE key LIKE 'fingerprint:%' AND length(value) != 64"), 0);
}
