use std::sync::Arc;

use super::{Config, Database};
use crate::schema::{FieldSpec, ModelSpec, Storage};
use crate::value::ScalarKind;

fn local() -> Arc<Database> {
    let mut model = ModelSpec::new("Note", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("text", ScalarKind::Str)]);
    model.storage = Storage::Local;
    Database::open(Config { path: None, data_version: None, cache_rows: 16 }, serde_json::to_string(&vec![model]).unwrap(), None).unwrap()
}

#[test]
fn a_database_without_a_transport_hands_out_an_online_sink_that_ignores_the_signal() {
    let database = local();
    database.online_sink().unwrap().set_online(false);
    database.set_online(true).unwrap();
}

#[test]
fn the_app_lifecycle_calls_do_nothing_without_a_transport() {
    let database = local();
    database.listen().unwrap();
    database.unlisten().unwrap();
    super::database::executor().unwrap().block_on(async {
        database.resume().await.unwrap();
        database.flush().await.unwrap();
        database.retry_all().await.unwrap();
    });
}

#[test]
fn a_call_that_needs_the_server_still_fails_without_a_transport() {
    let database = local();
    let refused = super::database::executor().unwrap().block_on(database.refresh("Note".into(), String::new(), "{}".into()));
    assert!(matches!(refused, Err(super::DbError::Invalid { .. })));
}
