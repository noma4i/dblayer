//! Owner `Outbox`, row out_retry_no_identity (A-8): Retry and Discard on a database without identity answer `NoSession` and leave it open.

use std::sync::Arc;

use crate::ffi::{Config, Database, DbError, OpenPorts};

fn code(result: &Result<(), DbError>) -> &'static str {
    match result {
        Ok(()) => "ok",
        Err(DbError::Closed { .. }) => "Closed",
        Err(DbError::NoSession { .. }) => "NoSession",
        Err(DbError::Invalid { .. }) => "Invalid",
        Err(DbError::NotFound { .. }) => "NotFound",
        _ => "other",
    }
}

async fn opened() -> Arc<Database> {
    let spec = crate::schema::ModelSpec::new("Moment", vec![crate::schema::FieldSpec::scalar("id", crate::value::ScalarKind::Id)]);
    let models = serde_json::to_string(&vec![spec]).unwrap();
    let config = Config { path: None, data_version: None };
    Database::open(config, crate::ffi::specs(models), OpenPorts::default()).await.unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn out_retry_no_identity() {
    let database = opened().await;
    let epoch = database.session_epoch();
    let retry = database.retry(epoch, "Moment".into(), "r1".into(), None, None).await.map(|_| ());
    let discard = database.discard(epoch, "Moment".into(), "r1".into(), None).await;
    assert_eq!((code(&retry), code(&discard)), ("NoSession", "NoSession"), "Retry and Discard without identity answer NoSession: retry {retry:?}, discard {discard:?}");
    let again = database.retry(epoch, "Moment".into(), "r1".into(), None, None).await.map(|_| ());
    assert_eq!(code(&again), "NoSession", "the database stays open: the repeat still answers NoSession, not Closed: {again:?}");
}
