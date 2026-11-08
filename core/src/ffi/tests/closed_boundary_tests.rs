use std::sync::Arc;

use super::close_tests::with_transport;
use super::upload_wire::WireUploadRequest;
use super::{CommandRequest, DbError};
use crate::runtime::ManualClock;

/// boundary_closed_call_precedence (P-LIFE-20, D-189): after the close every public entry answers `Closed` before it reads its arguments, reads data or sends.
#[tokio::test]
async fn every_entry_with_an_invalid_argument_answers_closed_after_the_close() {
    let (database, transport) = with_transport(Arc::new(ManualClock::new(std::time::Duration::from_secs(1)))).await;
    database.close_now();
    let (model, epoch) = ("Notice".to_string(), 1);
    let source = WireUploadRequest { path: String::new(), name: None, mime: None, variables_json: "{".into() };
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
        ("read", database.read(epoch, model.clone(), String::new(), "f".into()).await.map(drop)),
        ("url", database.url(epoch, model.clone(), String::new(), "f".into()).await.map(drop)),
        ("retry", database.retry(epoch, model.clone(), "r1".into(), None, Some(source)).await.map(drop)),
        ("session_call", database.session_call("op".into(), "{broken".into()).await.map(drop)),
    ];
    let wrong: Vec<_> = answers.iter().filter(|(_, answer)| !matches!(answer, Err(DbError::Closed { .. }))).collect();
    assert!(wrong.is_empty(), "entries that did not answer Closed: {wrong:?}");
    assert_eq!(answers.len(), 15);
    assert_eq!(transport.calls().len(), 0, "a closed database sends nothing");
}
