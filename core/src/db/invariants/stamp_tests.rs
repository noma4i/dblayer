use serde_json::json;

use super::fixtures::*;
use crate::commit::{OperationRecord, OperationTransition};
use crate::operation::OperationStatus;
use crate::db::fixtures::*;
use crate::db::Db;
use crate::syncstate::{FailureKind, OperationFailure};
use crate::upload::{UploadState, UploadStatus};

type Cause = (&'static str, fn(&Db), fn(&Db));

fn nothing(_: &Db) {}

fn pending_update(db: &Db) {
    db.update(&db.admit().unwrap(), "moments", &rid("m1"), obj(json!({"text": "two"}))).unwrap();
}

fn upload_at(db: &Db, sent: u64) {
    let seq = db.outbox().unwrap().iter().find(|entry| entry.operation.upload.is_some()).map(|entry| entry.operation.id.0);
    let state = UploadState { phase: UploadStatus::Uploading, sent_bytes: sent, total_bytes: 10, ..UploadState::queued() };
    match seq {
        Some(seq) => apply(db, OperationTransition::Progress { seq, item_position: 0, state: Box::new(state) }),
        None => {
            let record = OperationRecord {
                seq: None,
                action_key: "moments:upload:upload".to_string(),
                status: OperationStatus::Pending,
                row_ids: vec![rid("m1")],
                temp_id: None,
                input: json!({}),
                transport_started: false,
                base_revision: None,
                created_at: 0,
                dedupe_key: None,
                rollback: None,
                upload: Some(state),
            };
            apply(db, OperationTransition::Begin(Box::new(record)));
        }
    }
}

const CAUSES: [Cause; 5] = [
    ("sync state", pending_update, |db| start(db, only_seq(db))),
    ("action list", pending_update, |db| {
        db.act(&db.admit().unwrap(), "moments", "like", &rid("m1"), obj(json!({})), obj(json!({}))).unwrap();
    }),
    ("attempts and error", |db| {
        pending_update(db);
        start(db, only_seq(db));
    }, |db| {
        apply(db, OperationTransition::Postpone { seq: only_seq(db), failure: OperationFailure::new(FailureKind::Network, "down") });
    }),
    ("upload bytes", |db| upload_at(db, 5), |db| upload_at(db, 6)),
    ("included row sync", nothing, |db| {
        db.update(&db.admit().unwrap(), "users", &rid("u1"), obj(json!({"name": "Bea"}))).unwrap();
    }),
];

#[test]
fn inv_observe_01_every_stamp_field_change_reaches_the_row_observer_and_the_list_observer() {
    for (cause, setup, change) in CAUSES {
        let db = booted();
        put(&db, "users", json!({"id": "u1", "name": "Ann"}));
        put(&db, "moments", json!({"id": "m1", "userId": "u1", "text": "one"}));
        setup(&db);
        let (row_log, row_listener) = recorder();
        let (list_log, list_listener) = recorder();
        let _row = db.observe_row("moments", &rid("m1"), row_listener).unwrap();
        let _list = db.observe(crate::query::Query::new("moments").including(&["author"]), list_listener).unwrap();
        db.await_revision(db.revision_seq().unwrap());
        let (row_before, list_before) = (changes(&row_log).len(), changes(&list_log).len());
        change(&db);
        db.await_revision(db.revision_seq().unwrap());
        let row_after = changes(&row_log);
        if cause != "included row sync" {
            assert!(row_after.len() > row_before, "{cause}: the row observer heard nothing");
            assert!(row_after.last().unwrap().diff.updated.contains(&rid("m1")), "{cause}: the row is not in updated");
        }
        let list_after = changes(&list_log);
        assert!(list_after.len() > list_before, "{cause}: the list observer heard nothing");
        assert!(list_after.last().unwrap().diff.updated.contains(&rid("m1")), "{cause}: the row is not in the list updates");
    }
}
