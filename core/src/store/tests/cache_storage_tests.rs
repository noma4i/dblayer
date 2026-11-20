use super::*;
use fixtures::*;
use crate::store::operation::QuarantineReason;
use serde_json::json;

#[test]
fn blob_columns_keep_only_file_bytes_and_opaque_preparation_descriptors() {
    let (store, _) = booted_store();
    let blobs = "SELECT COUNT(*) FROM sqlite_master m, pragma_table_info(m.name) p WHERE m.type = 'table' AND p.type = 'BLOB' AND NOT (m.name = 'file' AND p.name = 'data') AND NOT (m.name = 'sync_upload_prepare' AND p.name = 'descriptor')";
    assert_eq!(count(&store, blobs), 0);
}

#[test]
fn a_quarantined_payload_is_kept_as_value_rows() {
    let (store, _) = booted_store();
    let seq = crate::store::operation::quarantine_add(&store.lock().connection, Some("messages"), &json!({"a": [1, "x"]}), QuarantineReason::SchemaChanged).unwrap();
    assert_eq!(count(&store, &format!("SELECT COUNT(*) FROM quarantine_value WHERE seq = {seq}")), 4);
}

