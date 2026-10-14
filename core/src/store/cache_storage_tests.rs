use super::fixtures::*;
use super::*;
use crate::operation::QuarantineReason;
use serde_json::json;

fn window(key: &str, ids: &[&str]) -> WindowRecord {
    WindowRecord { key: key.into(), cursor: Some("c1".into()), has_next: true, total: Some(9), ids: ids.iter().map(|id| id.to_string()).collect(), issued: 1, applied: 1, generation: 0 }
}

#[test]
fn no_table_keeps_a_blob_column_except_the_file_bytes() {
    let (store, _) = booted_store();
    let blobs = "SELECT COUNT(*) FROM sqlite_master m, pragma_table_info(m.name) p WHERE m.type = 'table' AND p.type = 'BLOB' AND NOT (m.name = 'file' AND p.name = 'data')";
    assert_eq!(count(&store, blobs), 0);
}

#[test]
fn a_window_keeps_its_ids_in_a_child_table_in_order() {
    let (store, _) = booted_store();
    store.save_window(&window("w1", &["b", "a", "c"])).unwrap();
    assert_eq!(count(&store, "SELECT COUNT(*) FROM query_window_row WHERE key = 'w1'"), 3);
    assert_eq!(store.window("w1").unwrap().unwrap().ids, ["b", "a", "c"]);
    store.save_window(&window("w1", &["z"])).unwrap();
    assert_eq!(count(&store, "SELECT COUNT(*) FROM query_window_row WHERE key = 'w1'"), 1);
    assert_eq!(store.window("w1").unwrap().unwrap().ids, ["z"]);
}

#[test]
fn clearing_the_cache_clears_the_window_ids() {
    let (store, _) = booted_store();
    store.save_window(&window("w1", &["a"])).unwrap();
    store.reset_incompatible().unwrap();
    assert_eq!(count(&store, "SELECT COUNT(*) FROM query_window_row"), 0);
}

#[test]
fn a_quarantined_payload_is_kept_as_value_rows() {
    let (store, _) = booted_store();
    let seq = crate::operation::quarantine_add(&store.lock().connection, Some("messages"), &json!({"a": [1, "x"]}), QuarantineReason::SchemaChanged).unwrap();
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
