//! `removed_by` through `Database` (C4): a row of the named model that lands removes the rows of the model that match it, in the same commit.
use serde_json::json;

use super::rig_tests::bound;
use crate::ffi::{Config, Database, OpenPorts};

const SPEC: &str = include_str!("../../../tests/corpus/matrix/removed_by.spec.json");

#[tokio::test(flavor = "multi_thread")]
async fn removed_by_removes_the_matching_rows_when_the_named_row_lands() {
    let spec: serde_json::Value = serde_json::from_str(SPEC).unwrap();
    let directory = tempfile::tempdir().unwrap();
    let config = Config { path: Some(directory.path().join("removed.sqlite").to_string_lossy().into()), data_version: None };
    let database = Database::open(config, crate::ffi::specs(spec["models"].to_string()), OpenPorts::default()).await.unwrap();
    let epoch = database.session_epoch();
    let mut marks = Vec::new();
    for note in ["n1", "n2"] {
        let receipt = bound(database.create(epoch, "NoteMark".into(), json!({"noteId": note}).to_string(), "{}".into())).await.unwrap();
        marks.push(receipt.optimistic_value.unwrap().id);
    }
    bound(database.create(epoch, "Note".into(), json!({"id": "n1", "title": "t"}).to_string(), "{}".into())).await.unwrap();
    let kept = (
        bound(database.read_row(epoch, "NoteMark".into(), marks[0].clone())).await.unwrap().is_some(),
        bound(database.read_row(epoch, "NoteMark".into(), marks[1].clone())).await.unwrap().is_some(),
    );
    assert_eq!(kept, (false, true), "the mark of n1 leaves with the landing of Note n1, the mark of n2 stays");
}
