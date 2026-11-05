use std::sync::Arc;

use super::{Config, Database};
use crate::schema::{FieldSpec, ModelSpec, Storage};
use crate::value::ScalarKind;

async fn local() -> Arc<Database> {
    let mut model = ModelSpec::new("Note", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("text", ScalarKind::Str)]);
    model.storage = Storage::Local;
    Database::open(Config { path: None, data_version: None, cache_rows: 16 }, serde_json::to_string(&vec![model]).unwrap(), None, None, None, None, None, None).await.unwrap()
}

#[tokio::test]
async fn a_database_without_a_transport_hands_out_an_online_sink_that_ignores_the_signal() {
    let database = local().await;
    database.online_sink().unwrap().set_online(false);
    database.set_online(true).unwrap();
}

#[tokio::test]
async fn a_retry_needs_a_session_without_a_transport() {
    let database = local().await;
    let refused = database.retry(database.session_epoch(), "Note".into(), "1".into(), None, None).await;
    assert!(matches!(refused, Err(super::DbError::NoSession { .. })), "{refused:?}");
}

#[tokio::test]
async fn a_call_that_needs_the_server_still_fails_without_a_transport() {
    let database = local().await;
    let refused = database.refresh(database.session_epoch(), "Note".into(), String::new(), "{}".into()).await;
    assert!(matches!(refused, Err(super::DbError::NoSession { .. })));
}
