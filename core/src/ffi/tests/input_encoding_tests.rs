use std::sync::atomic::Ordering;

use serde_json::{json, Value};

use super::failure_tests::rig;
use super::DbError;

const BEYOND_2_POW_53: i64 = 9_007_199_254_740_993;

async fn sent_through_run(variables: &str) -> Result<Value, DbError> {
    let (db, transport) = rig(Ok(r#"{"data":{"moments":[]}}"#)).await;
    db.run(db.session_epoch(), "Feed".into(), variables.into()).await?;
    let sent = transport.variables.lock().unwrap_or_else(std::sync::PoisonError::into_inner).clone();
    assert_eq!(sent.len(), 1, "one request");
    Ok(serde_json::from_str(&sent[0]).unwrap())
}

#[tokio::test]
async fn data_operation_input_encoding() {
    let sent = sent_through_run(&format!(r#"{{"cleared": null, "set": {{"text": "a"}}, "big": {BEYOND_2_POW_53}, "choice": {{"emoji": {{"code": "x"}}}}}}"#)).await.unwrap();
    assert_eq!(sent, json!({"cleared": null, "set": {"text": "a"}, "big": BEYOND_2_POW_53, "choice": {"emoji": {"code": "x"}}}), "null stays a key with null, an omitted field has no key, an integer beyond 2^53 and a one-of key are sent as written");
    assert!(sent.as_object().unwrap().get("omitted").is_none());
    for (case, bad) in [("NaN", r#"{"a": NaN}"#), ("Infinity", r#"{"a": Infinity}"#), ("broken JSON", r#"{"a": "#)] {
        let (db, transport) = rig(Ok(r#"{"data":{"moments":[]}}"#)).await;
        let error = db.run(db.session_epoch(), "Feed".into(), bad.into()).await.unwrap_err();
        assert!(matches!(error, DbError::Invalid { .. }), "{case}: {error:?}");
        assert_eq!(transport.calls.load(Ordering::SeqCst), 0, "{case}: no request is sent");
    }
}

#[tokio::test]
async fn data_list_param_null_items() {
    let sent = sent_through_run(r#"{"widgets": [{"kind": "EMOJI"}, null, {"kind": "GIF"}]}"#).await.unwrap();
    assert_eq!(sent["widgets"], json!([{"kind": "EMOJI"}, null, {"kind": "GIF"}]), "a null item keeps its position in the request");
}
