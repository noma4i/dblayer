use std::future::Future;
use std::sync::{mpsc, Arc};
use std::task::{Context, Waker};
use std::time::Duration;

use super::{Config, Database};
use crate::schema::{FieldSpec, ModelSpec, Storage};
use crate::value::ScalarKind;
use crate::test_wait::join_within;

#[tokio::test]
async fn a_write_yields_while_sqlite_is_busy_and_finishes_after_release() {
    let mut model = ModelSpec::new("Note", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("text", ScalarKind::Str)]);
    model.storage = Storage::Local;
    let database = Database::open(Config { path: None, data_version: None }, serde_json::to_string(&vec![model]).unwrap(), crate::ffi::OpenPorts::default()).await.unwrap();
    let writer = database.db.lock();
    let writing = Arc::clone(&database);
    let (sent, received) = mpsc::channel();
    let thread = std::thread::spawn(move || {
        let mut future = Box::pin(async { writing.create(writing.session_epoch(), "Note".into(), r#"{"text":"saved"}"#.into(), "{}".into()).await });
        let polled = future.as_mut().poll(&mut Context::from_waker(Waker::noop()));
        sent.send(polled.is_pending()).unwrap();
        match polled {
            std::task::Poll::Ready(row) => row,
            std::task::Poll::Pending => super::database::executor().unwrap().block_on(future),
        }
    });
    let yielded = received.recv_timeout(Duration::from_secs(2));
    drop(writer);
    let row = join_within(thread).unwrap().optimistic_value.unwrap();
    assert!(yielded.unwrap());
    assert_eq!(database.db.find("Note", &crate::identity::RowId::new(&row.id).unwrap()).unwrap().unwrap().get("text"), Some(&serde_json::json!("saved")));
}
