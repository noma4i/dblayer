use std::sync::Arc;

use super::{Config, Database};
use crate::schema::{FieldSpec, ModelSpec, Storage};
use crate::value::ScalarKind;

async fn local() -> Arc<Database> {
    let mut model = ModelSpec::new("Note", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("text", ScalarKind::Str)]);
    model.storage = Storage::Local;
    Database::open(Config { path: None }, crate::ffi::specs(serde_json::to_string(&vec![model]).unwrap()), crate::ffi::OpenPorts::default()).await.unwrap()
}

#[tokio::test]
async fn a_database_without_a_transport_hands_out_an_online_sink_that_ignores_the_signal() {
    let database = local().await;
    database.online_sink().unwrap().set_online(false);
    database.set_online(true).await.unwrap();
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
    let refused = database.refresh(database.session_epoch(), "Note".into(), Some("recent".into()), "{}".into()).await;
    assert!(matches!(refused, Err(super::DbError::NoSession { .. })));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_write_after_a_boot_that_raised_the_generation_is_not_stale() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("generation.sqlite").to_string_lossy().into_owned();
    let mut model = ModelSpec::new("Note", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("text", ScalarKind::Str)]);
    model.storage = Storage::Local;
    let models = serde_json::to_string(&vec![model]).unwrap();
    let config = || Config { path: Some(path.clone()) };
    let first = Database::open(config(), crate::ffi::specs(models.clone()), crate::ffi::OpenPorts::default()).await.unwrap();
    first.close_database().await.unwrap();
    drop(first);
    rusqlite::Connection::open(&path).unwrap().execute("UPDATE manifest SET value = '2' WHERE key = 'saved_generation'", []).unwrap();
    let second = Database::open(config(), crate::ffi::specs(models), crate::ffi::OpenPorts::default()).await.unwrap();
    let created = crate::test_wait::within(second.create_row(second.session_epoch(), "Note".into(), r#"{"text":"kept"}"#.into(), "{}".into())).await;
    assert!(created.is_ok(), "{created:?}");
}
