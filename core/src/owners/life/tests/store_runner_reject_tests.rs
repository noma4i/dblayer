use std::sync::{Arc, Mutex};

use super::super::runners::StoreRunner;
use super::super::Wiring;
use crate::owners::contract::{BatchAuthor, RejectReason};
use crate::owners::store::{RowValue, StoreBatch, StoreEffect, StoreInput, StoreOp, StoreWriter};
use crate::runtime::{Back, EffectRunner, EpochStamp};
use crate::store::fixtures::booted;

fn batch(author: BatchAuthor, rows: Vec<(&str, &str)>, ops: Vec<StoreOp>) -> StoreBatch {
    StoreBatch { author, id: 1, stamp: EpochStamp::Control, rows: rows.into_iter().map(|(model, text)| (model.to_string(), text.to_string())).collect(), intents: 0, credential: None, ops }
}

/// The places of the events refused alone, or the refusal of the batch.
fn answer(db: &Arc<crate::store::Store>, batch: StoreBatch) -> Result<Vec<usize>, RejectReason> {
    let wiring = Arc::new(Wiring::new(Default::default()));
    let mut runner = StoreRunner { wiring, db: db.clone(), secret: None, exchange: None, executor: tokio::runtime::Handle::current(), restore_error: None, session_open: None, session_cloud: false };
    let answers = Arc::new(Mutex::new(Vec::new()));
    let sink = answers.clone();
    let back = Back::<StoreWriter>::new(move |_, input| sink.lock().unwrap().push(input), |_| {});
    runner.run(EpochStamp::Control, StoreEffect::Write { batch, min_pin_revision: None }, &back);
    let mut answers = answers.lock().unwrap();
    match answers.pop() {
        Some(StoreInput::Written { result, .. }) if answers.is_empty() => result.map(|done| done.refused),
        other => panic!("one Written answer expected: {other:?} and {answers:?}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn store_error_kinds_are_classified_once() {
    let db = booted();
    let feed = |payload: &'static str| batch(BatchAuthor::Feed, vec![("moments", payload)], Vec::new());
    let rows = |text: &'static str| batch(BatchAuthor::Outbox, vec![("moments", text)], Vec::new());
    let data: Vec<(&str, StoreBatch, Result<Vec<usize>, RejectReason>)> = vec![
        ("unknown event operation", feed(r#"{"op":"bogus","id":"a"}"#), Ok(vec![0])),
        ("refused event", feed(r#"{"op":"invalid","reason":"bad"}"#), Ok(vec![0])),
        ("event without a row", feed(r#"{"op":"upsert","id":"a"}"#), Ok(vec![0])),
        ("row without an object", rows(r#""text""#), Err(RejectReason::Invalid("text".into()))),
        ("empty id", batch(BatchAuthor::Outbox, vec![("moments", r#"{"id":""}"#)], Vec::new()), Err(RejectReason::Invalid("text".into()))),
        ("unreadable json", rows("not json"), Err(RejectReason::Invalid("text".into()))),
        ("unknown model", batch(BatchAuthor::Outbox, vec![("nowhere", r#"{"id":"a"}"#)], Vec::new()), Err(RejectReason::Invalid("text".into()))),
        ("plan fault", batch(BatchAuthor::Outbox, Vec::new(), vec![StoreOp::Rows { model: "moments".into(), rows: vec![RowValue { row_id: "a".into(), values: r#"{"id":"b"}"#.into() }] }]), Err(RejectReason::Invalid("text".into()))),
    ];
    let mut wrong = Vec::new();
    for (case, batch, want) in data {
        let got = answer(&db, batch).map_err(|reason| match reason { RejectReason::Invalid(text) if !text.is_empty() => RejectReason::Invalid("text".into()), other => other });
        if got != want {
            wrong.push(format!("{case}: the executor answered {got:?}, the spec asks {want:?}"));
        }
    }
    db.lock().ready = false;
    let closed = answer(&db, rows(r#"{"id":"a"}"#));
    if !matches!(closed, Err(RejectReason::Storage(_))) {
        wrong.push(format!("closed database: the executor answered {closed:?}, the spec asks Rejected(Storage)"));
    }
    assert!(wrong.is_empty(), "{} wrong answers:\n{}", wrong.len(), wrong.join("\n"));
}

/// P-DATA-97: the SQLite codes of the class of data are `Invalid`, the codes of a failure of the storage are `Storage`.
#[test]
fn store_sqlite_codes_are_classified() {
    use rusqlite::ffi;
    let invalid = |reason: &RejectReason| matches!(reason, RejectReason::Invalid(_));
    let data = [("CONSTRAINT", ffi::SQLITE_CONSTRAINT), ("MISMATCH", ffi::SQLITE_MISMATCH), ("TOOBIG", ffi::SQLITE_TOOBIG), ("RANGE", ffi::SQLITE_RANGE)];
    let storage = [("IOERR", ffi::SQLITE_IOERR), ("BUSY", ffi::SQLITE_BUSY), ("LOCKED", ffi::SQLITE_LOCKED), ("FULL", ffi::SQLITE_FULL), ("READONLY", ffi::SQLITE_READONLY), ("CORRUPT", ffi::SQLITE_CORRUPT), ("CANTOPEN", ffi::SQLITE_CANTOPEN), ("NOTADB", ffi::SQLITE_NOTADB)];
    let mut wrong = Vec::new();
    for (name, code, want_invalid) in data.iter().map(|(name, code)| (name, code, true)).chain(storage.iter().map(|(name, code)| (name, code, false))) {
        let error = crate::store::StoreError::from(rusqlite::Error::SqliteFailure(ffi::Error::new(*code), None));
        let got = error.reject_reason();
        if invalid(&got) != want_invalid {
            wrong.push(format!("{name}: the classifier answered {got:?}, the spec asks {}", if want_invalid { "Invalid" } else { "Storage" }));
        }
    }
    assert!(wrong.is_empty(), "{} wrong codes:\n{}", wrong.len(), wrong.join("\n"));
}

/// P-DATA-101: the refusal of a data error carries the text of the error.
#[test]
fn store_invalid_reason_carries_the_error_text() {
    use crate::store::StoreError;
    let data = [
        StoreError::Constraint("missing membership field lists".into()),
        StoreError::UnknownModel("nowhere".into()),
        StoreError::Invalid("the id is empty".into()),
        StoreError::Plan(crate::store::PlanFault::RowIdMismatch),
        StoreError::Json(serde_json::from_str::<serde_json::Value>("not json").unwrap_err()),
    ];
    for error in data {
        let text = error.to_string();
        assert_eq!(error.reject_reason(), RejectReason::Invalid(text));
    }
}
