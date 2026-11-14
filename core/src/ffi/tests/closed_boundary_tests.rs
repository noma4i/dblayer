use std::sync::Arc;

use super::close_tests::with_transport;
use super::upload_wire::WireUploadSource;
use super::{CommandRequest, DbError};
use crate::runtime::ManualClock;

/// boundary_closed_call_precedence (P-LIFE-20, D-189): after the close every public entry answers `Closed` before it reads its arguments, reads data or sends.
#[tokio::test]
async fn boundary_closed_call_precedence() {
    let (database, transport) = with_transport(Arc::new(ManualClock::new(std::time::Duration::from_secs(1)))).await;
    database.close_now();
    let (model, epoch) = ("Notice".to_string(), 1);
    let source = WireUploadSource { path: String::new(), name: None, mime: None, values_json: "{".into(), variables_json: "{".into(), prepare: None, max_bytes: None };
    let answers: Vec<(&str, Result<(), DbError>)> = vec![
        ("begin_load", database.begin_load(epoch, model.clone(), None, "{}".into()).map(drop)),
        ("refresh", database.refresh(epoch, model.clone(), None, "{}".into()).await),
        ("begin_load_all", database.begin_load_all(epoch, model.clone(), None, "{}".into(), false).map(drop)),
        ("load_more", database.load_more(epoch, model.clone(), None, "{}".into()).await.map(drop)),
        ("node", database.node(epoch, "Unknown".into(), "missing".into(), "".into(), false).await.map(drop)),
        ("update", database.update(epoch, "Unknown".into(), "r1".into(), "{".into(), "{}".into()).await.map(drop)),
        ("act", database.act(epoch, "Unknown".into(), "r1".into(), "x".into(), "{}".into(), "{}".into()).await.map(drop)),
        ("destroy", database.destroy(epoch, "Unknown".into(), "r1".into()).await.map(drop)),
        ("send", database.send(CommandRequest { epoch, model: "Unknown".into(), values_json: "{".into(), args_json: "{}".into(), upload: None }).await.map(drop)),
        ("edge_values", database.edge_values(epoch, model.clone(), None, "{}".into(), Vec::new()).await.map(drop)),
        ("file", database.file(epoch, model.clone(), String::new(), "f".into()).await.map(drop)),
        ("url", database.url(epoch, model.clone(), String::new(), "f".into()).await.map(drop)),
        ("retry", database.retry(epoch, model.clone(), "r1".into(), None, Some(source)).await.map(drop)),
        ("session_call", database.session_call("op".into(), "{broken".into()).await.map(drop)),
    ];
    let wrong: Vec<_> = answers.iter().filter(|(_, answer)| !matches!(answer, Err(DbError::Closed { .. }))).collect();
    assert!(wrong.is_empty(), "entries that did not answer Closed: {wrong:?}");
    assert_eq!(answers.len(), 14);
    assert_eq!(transport.calls().len(), 0, "a closed database sends nothing");
}

#[tokio::test]
async fn call_refusal_order_table() {
    let mut model = crate::schema::ModelSpec::new("Note", vec![crate::schema::FieldSpec::scalar("id", crate::value::ScalarKind::Id)]);
    model.storage = crate::schema::Storage::Local;
    let database = super::Database::open(super::Config { path: None }, crate::ffi::specs(serde_json::to_string(&vec![model]).unwrap()), super::OpenPorts::default()).await.unwrap();
    let receipt = database.create(database.session_epoch(), "Note".into(), "{}".into(), "{}".into()).await.unwrap();
    let epoch = database.session_epoch();
    let answers = [
        database.attach_file(epoch, "Note".into(), "missing".into(), "unknown".into(), "missing".into(), "x".into(), "image/png".into()).await.map(drop),
        database.detach(epoch, "Note".into(), "missing".into(), "unknown".into()).await.map(drop),
    ];
    for answer in answers { assert!(matches!(answer, Err(DbError::Invalid { .. }))); }
    let mut changed = receipt.clone();
    changed.epoch += 1;
    database.close_database().await.unwrap();
    for receipt in [receipt, changed] {
        assert!(matches!(database.confirmed(receipt, false).await, Err(DbError::Closed { .. })));
    }
}

/// boundary_resolved_error_survives_epoch_end (P-BND-11, D-172): the work of a call answers `Storage` and the epoch ends before the boundary returns; the caller gets the resolved `Storage`, not the end of the epoch.
#[tokio::test]
async fn boundary_resolved_error_survives_epoch_end() {
    let (database, _transport) = with_transport(Arc::new(ManualClock::new(std::time::Duration::from_secs(1)))).await;
    let epoch = database.session_epoch();
    let resolved = database.in_epoch::<()>(epoch, async {
        database.session_reset().await.unwrap();
        Err(DbError::Storage { detail: "disk full".into() })
    }).await;
    assert!(matches!(resolved, Err(DbError::Storage { ref detail }) if detail == "disk full"), "the resolved error is returned as is after the epoch ended: {resolved:?}");
}
