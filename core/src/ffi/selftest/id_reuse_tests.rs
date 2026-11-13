use std::sync::Arc;

use serde_json::json;

use super::rig_models::plain_models;
use super::rig_tests::{bound, Switchable, World};
use crate::ffi::tests::session_guards_tests::WatchedHeld;
use crate::ffi::{Config, Database, EncryptedOperationClass, EncryptedSessionOperation, OpenPorts};

async fn open(path: &std::path::Path, world: Arc<World>, credentials: Arc<WatchedHeld>) -> Arc<Database> {
    let mut encrypted = crate::transport::encrypted::tests::protocol_tests::config();
    encrypted.endpoints.session_operations.insert("login_link".into(), EncryptedSessionOperation { path: "/api/auth/login_link".into(), class: EncryptedOperationClass::Authenticated });
    let transport = Switchable::new(Some(world.clone()), false);
    let database = bound(Database::open(Config { path: Some(path.to_string_lossy().into_owned()), data_version: None }, crate::ffi::specs(plain_models()), OpenPorts { partial_operations: true, transport: Some(transport), encrypted: Some(encrypted), credentials: Some(credentials), http: Some(world), endpoint: Some(crate::ffi::FAKE_ENDPOINT.into()), ..Default::default() })).await.unwrap();
    database.set_online(true).await.unwrap();
    database
}

#[tokio::test(flavor = "multi_thread")]
async fn session_request_after_reopen_does_not_take_the_saved_answer() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("db.sqlite");
    let world = World::new("server", "A", false);
    let credentials = Arc::new(WatchedHeld::default());
    let database = open(&path, world.clone(), credentials.clone()).await;
    bound(database.session_call("login_link".into(), json!({"account":"A"}).to_string())).await.unwrap();
    database.close_now();
    let database = open(&path, world.clone(), credentials.clone()).await;
    let held = world.hold_next("login_link");
    let calling = database.clone();
    let first = tokio::spawn(async move { calling.session_call("login_link".into(), json!({"account":"first"}).to_string()).await });
    held.entered().await;
    database.close_now();
    held.release();
    let _ = bound(first).await;
    let reopened = open(&path, world.clone(), credentials).await;
    crate::test_wait::settled(&reopened, "the saved call is replayed", || world.log().iter().filter(|entry| entry.body.contains("\"first\"")).count() == 2).await;
    let second_held = world.hold_next("login_link");
    let calling = reopened.clone();
    let second = tokio::spawn(async move { calling.session_call("login_link".into(), json!({"account":"second"}).to_string()).await });
    second_held.entered().await;
    second_held.release();
    let answer = bound(second).await.unwrap().unwrap();
    assert_eq!(serde_json::from_str::<serde_json::Value>(&answer).unwrap()["sessionId"], "second");
    let sent = world.log();
    let first_requests: Vec<_> = sent.iter().filter(|entry| entry.body.contains("\"first\"")).collect();
    assert_eq!(first_requests.len(), 2, "the saved request goes out again after open");
    assert_eq!((&first_requests[0].request_id, first_requests[0].sequence), (&first_requests[1].request_id, first_requests[1].sequence));
    for (index, left) in sent.iter().enumerate() {
        for right in &sent[index + 1..] {
            if left.request_id == right.request_id && left.sequence == right.sequence { assert_eq!(left.body, right.body); }
        }
    }
    reopened.close_now();
}
