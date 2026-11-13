use std::sync::Arc;

use serde_json::json;

use super::rig_models::{plain_models, plain_operations};
use super::rig_tests::{bound, Switchable, World};
use crate::ffi::tape::Rows;
use crate::ffi::{Config, EncryptedSessionOperation, OpenDatabase, ReadRequest, TestClock};

/// Requirement D-402: the first network value gives each owner one reaction. Input: a database that restarts with a signed session, a queued write and no network value, a watched window of a server model, an open event stream of the same model; then the first `set_online(first)`. Observable: requests of the window, sends of the write, sockets of the Feed, counted at the transport. Catches: the first value reaching an owner twice (broadcast and answer to its waiting request) and loading, connecting or sending twice.
#[tokio::test(flavor = "multi_thread")]
async fn net_first_value_gives_one_reaction() {
    let mut table = Vec::new();
    for first_is_false in [false, true] {
        let directory = tempfile::tempdir().unwrap();
        let config = || Config { path: Some(directory.path().join("cache.sqlite").to_string_lossy().into_owned()), data_version: None };
        let world = World::new("server", "A", true);
        let switch = Switchable::new(Some(world.clone()), false);
        let mut encrypted = crate::transport::encrypted::tests::protocol_tests::config();
        encrypted.endpoints.session_operations.insert("login_link".into(), EncryptedSessionOperation { path: "/api/auth/login_link".into(), class: crate::transport::EncryptedOperationClass::Authenticated });
        let credentials = Arc::new(crate::ffi::tests::session_guards_tests::WatchedHeld::default());
        let mut models: serde_json::Value = serde_json::from_str(&plain_models()).unwrap();
        super::rig_events::events(|_| {})(&mut models);
        let specs = crate::ffi::specs(models.to_string());
        let opener = OpenDatabase::new();
        let open = || opener.open_with_clock(config(), specs.clone(), crate::ffi::OpenPorts { partial_operations: true, transport: Some(switch.clone()), encrypted: Some(encrypted.clone()), credentials: Some(credentials.clone()), http: Some(world.clone()), endpoint: Some(crate::ffi::FAKE_ENDPOINT.into()), ..Default::default() }, TestClock::new(1_000_000), crate::ffi::OpenOptions::default(), None);

        let database = bound(open()).await.unwrap();
        bound(database.register_operations(plain_operations())).await.unwrap();
        database.set_online(true).await.unwrap();
        bound(database.session_call("login_link".into(), json!({"account": "A"}).to_string())).await.unwrap();
        database.set_online(false).await.unwrap();
        let epoch = database.session_epoch();
        bound(database.create(epoch, "Message".into(), json!({"chatId":"c1","text":"t","at":1}).to_string(), "{}".into())).await.unwrap();
        bound(database.close_database()).await.unwrap();
        let before = (world.count("Thread"), world.count("SendMessage"), switch.events.peer.connects());

        let database = bound(open()).await.unwrap();
        bound(database.register_operations(plain_operations())).await.unwrap();
        let epoch = database.session_epoch();
        let rows = Arc::new(Rows::default());
        let _window = database.watch_query(r#"{"model":"Message","relation":"thread","params":{"chatId":"c1"}}"#, rows.clone());
        let _events = database.subscribe_values(epoch, ReadRequest::Events { model: "Message".into() });
        crate::test_wait::settled(&database, "the owners heard every input", || true).await;
        assert_eq!((world.count("Thread"), world.count("SendMessage"), switch.events.peer.connects()), before, "no value yet: nothing leaves");
        if first_is_false {
            database.set_online(false).await.unwrap();
            crate::test_wait::settled(&database, "the owners heard offline", || true).await;
        }
        database.set_online(true).await.unwrap();
        crate::test_wait::settled(&database, "the write left", || world.count("SendMessage") > before.1).await;
        crate::ffi::settled(&database).await.unwrap();
        table.push((first_is_false, world.count("Thread") - before.0, world.count("SendMessage") - before.1, switch.events.peer.connects() - before.2));
        bound(database.close_database()).await.unwrap();
    }
    assert_eq!(table, vec![(false, 1, 1, 1), (true, 1, 1, 1)], "(first value false, window requests, sends, sockets)");
}
