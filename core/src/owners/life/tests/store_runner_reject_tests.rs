use std::sync::{Arc, Mutex};

use super::super::runners::StoreRunner;
use super::super::Wiring;
use crate::owners::contract::{BatchAuthor, RejectReason};
use crate::owners::store::{RowValue, StoreBatch, StoreEffect, StoreInput, StoreOp, StoreWriter};
use crate::runtime::{Back, EffectRunner, EpochStamp};
use crate::store::fixtures::booted;

fn batch(author: BatchAuthor, rows: Vec<(&str, &str)>, ops: Vec<StoreOp>) -> StoreBatch {
    StoreBatch { author, id: 1, stamp: EpochStamp::Control, basis: Default::default(), rows: rows.into_iter().map(|(model, text)| (model.to_string(), text.to_string())).collect(), intents: 0, credential: None, ops }
}

fn answer(db: &Arc<crate::store::Store>, batch: StoreBatch) -> Result<(), RejectReason> {
    let wiring = Arc::new(Wiring::new(Default::default()));
    let mut runner = StoreRunner { wiring, db: db.clone(), secret: None, exchange: None, executor: tokio::runtime::Handle::current(), restore_error: None, session_open: None };
    let answers = Arc::new(Mutex::new(Vec::new()));
    let sink = answers.clone();
    let back = Back::<StoreWriter>::new(move |_, input| sink.lock().unwrap().push(input), |_| {});
    runner.run(EpochStamp::Control, StoreEffect::Write { batch, min_pin_revision: None }, &back);
    let mut answers = answers.lock().unwrap();
    match answers.pop() {
        Some(StoreInput::Written { result, .. }) if answers.is_empty() => result.map(|_| ()),
        other => panic!("one Written answer expected: {other:?} and {answers:?}"),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn store_error_kinds_are_classified_once() {
    let db = booted();
    let feed = |payload: &'static str| batch(BatchAuthor::Feed, vec![("moments", payload)], Vec::new());
    let rows = |text: &'static str| batch(BatchAuthor::Outbox, vec![("moments", text)], Vec::new());
    let data: Vec<(&str, StoreBatch)> = vec![
        ("unknown event operation", feed(r#"{"op":"bogus","id":"a"}"#)),
        ("refused event", feed(r#"{"op":"invalid","reason":"bad"}"#)),
        ("event without a row", feed(r#"{"op":"upsert","id":"a"}"#)),
        ("row without an object", rows(r#""text""#)),
        ("empty id", batch(BatchAuthor::Outbox, vec![("moments", r#"{"id":""}"#)], Vec::new())),
        ("unreadable json", rows("not json")),
        ("unknown model", batch(BatchAuthor::Outbox, vec![("nowhere", r#"{"id":"a"}"#)], Vec::new())),
        ("plan fault", batch(BatchAuthor::Outbox, Vec::new(), vec![StoreOp::Rows { model: "moments".into(), rows: vec![RowValue { row_id: "a".into(), values: r#"{"id":"b"}"#.into() }] }])),
    ];
    let mut wrong = Vec::new();
    for (case, batch) in data {
        let got = answer(&db, batch);
        if got != Err(RejectReason::Invalid) {
            wrong.push(format!("{case}: the executor answered {got:?}, the spec asks Rejected(Invalid)"));
        }
    }
    db.lock().ready = false;
    let closed = answer(&db, rows(r#"{"id":"a"}"#));
    if !matches!(closed, Err(RejectReason::Storage(_))) {
        wrong.push(format!("closed database: the executor answered {closed:?}, the spec asks Rejected(Storage)"));
    }
    assert!(wrong.is_empty(), "{} wrong answers:\n{}", wrong.len(), wrong.join("\n"));
}
