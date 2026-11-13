use std::sync::Arc;

use super::encrypted_tests::{Held, SubscriptionsOnly, ThroughServer};
use super::{Config, Database, EncryptedConfig};
use crate::transport::encrypted::tests::fake_server_tests::{FakeServer, MemoryCredentials};
use crate::transport::CredentialSlot;
use crate::transport::encrypted::tests::vectors_tests::{origin, pin, public_key_der_b64};

const SECRET: &str = "the-secret-of-the-older-session";

/// The layout of a device before 6.8.0 (`encrypted_pending` without `upload`, old tables and an old `format_version`).
fn plant_older_layout(path: &std::path::Path, digest: &str) {
    let connection = rusqlite::Connection::open(path).unwrap();
    connection.execute_batch(&format!(
        "CREATE TABLE manifest (key TEXT PRIMARY KEY NOT NULL, value TEXT NOT NULL) STRICT;
         INSERT INTO manifest VALUES ('format_version', 'older');
         CREATE TABLE encrypted_session (id INTEGER PRIMARY KEY CHECK (id = 1), credential_digest TEXT NOT NULL, sequence INTEGER NOT NULL) STRICT;
         INSERT INTO encrypted_session VALUES (1, '{digest}', 7);
         CREATE TABLE encrypted_pending (id INTEGER PRIMARY KEY CHECK (id = 1), credential_digest TEXT NOT NULL, request_id TEXT NOT NULL, response_key TEXT NOT NULL, sequence INTEGER NOT NULL, path TEXT NOT NULL, body TEXT NOT NULL, owner_seq INTEGER, item_position INTEGER, stage TEXT) STRICT;
         CREATE TABLE op_once (id INTEGER PRIMARY KEY);
         CREATE TABLE scope_edge (id INTEGER PRIMARY KEY);
         CREATE TABLE scope_member (id INTEGER PRIMARY KEY);
         CREATE TABLE sync_action (id INTEGER PRIMARY KEY);
         CREATE TABLE sync_field (id INTEGER PRIMARY KEY);
         CREATE TABLE tombstone (id INTEGER PRIMARY KEY);
         PRAGMA user_version = {};",
        crate::store::ddl::SCHEMA_VERSION
    )).unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_database_of_an_older_layout_opens_with_its_session_and_without_pending() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("older.sqlite");
    let digest = crate::transport::encrypted::exchange::digest_of(SECRET);
    plant_older_layout(&path, &digest);
    let credentials = Arc::new(MemoryCredentials::default());
    credentials.set(SECRET).unwrap();
    let server = FakeServer::new(Arc::new(crate::runtime::SystemClock));
    let encrypted = EncryptedConfig { origin: origin().to_string(), server_key: public_key_der_b64().to_string(), pin: pin().to_string(), ..crate::transport::encrypted::tests::protocol_tests::config() };

    let database = Database::open(Config { path: Some(path.to_string_lossy().into_owned()) }, crate::ffi::EMPTY_SPECS.to_string(), crate::ffi::OpenPorts { partial_operations: true, transport: Some(Arc::new(SubscriptionsOnly)), encrypted: Some(encrypted), credentials: Some(Arc::new(Held(credentials))), http: Some(Arc::new(ThroughServer(server))), endpoint: Some(crate::ffi::FAKE_ENDPOINT.into()), ..Default::default() }).await.expect("the database opens");

    assert_eq!(database.life.published().identity, crate::store::identity_of(&digest), "the session identity is published");
    assert_eq!(crate::transport::encrypted::credential::stored_pending(&database.db).unwrap(), None, "no pending request");
}
