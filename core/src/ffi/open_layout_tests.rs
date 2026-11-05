use std::sync::Arc;

use super::encrypted_tests::{Held, SubscriptionsOnly, ThroughServer};
use super::{Config, Database, EncryptedConfig};
use crate::transport::encrypted::fake_server_tests::{FakeServer, MemoryCredentials};
use crate::transport::encrypted::vectors_tests::{origin, pin, public_key_der_b64};

const DIGEST: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

/// The layout of a device before 6.8.0 (`encrypted_pending` without `upload`, old tables and an old `format_version`).
fn plant_older_layout(path: &std::path::Path) {
    let connection = rusqlite::Connection::open(path).unwrap();
    connection.execute_batch(&format!(
        "CREATE TABLE manifest (key TEXT PRIMARY KEY NOT NULL, value TEXT NOT NULL) STRICT;
         INSERT INTO manifest VALUES ('format_version', 'older');
         CREATE TABLE encrypted_session (id INTEGER PRIMARY KEY CHECK (id = 1), credential_digest TEXT NOT NULL, sequence INTEGER NOT NULL) STRICT;
         INSERT INTO encrypted_session VALUES (1, '{DIGEST}', 7);
         CREATE TABLE encrypted_pending (id INTEGER PRIMARY KEY CHECK (id = 1), credential_digest TEXT NOT NULL, request_id TEXT NOT NULL, response_key TEXT NOT NULL, sequence INTEGER NOT NULL, path TEXT NOT NULL, body TEXT NOT NULL, owner_seq INTEGER, item_position INTEGER, stage TEXT) STRICT;
         CREATE TABLE op_once (id INTEGER PRIMARY KEY);
         CREATE TABLE scope_edge (id INTEGER PRIMARY KEY);
         CREATE TABLE scope_member (id INTEGER PRIMARY KEY);
         CREATE TABLE sync_action (id INTEGER PRIMARY KEY);
         CREATE TABLE sync_field (id INTEGER PRIMARY KEY);
         CREATE TABLE tombstone (id INTEGER PRIMARY KEY);
         PRAGMA user_version = {};",
        crate::ddl::SCHEMA_VERSION
    )).unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_database_of_an_older_layout_opens_with_its_session_and_without_pending() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("older.sqlite");
    plant_older_layout(&path);
    let server = FakeServer::new(Arc::new(crate::pacing::SystemClock));
    let encrypted = EncryptedConfig { origin: origin().to_string(), server_key: public_key_der_b64().to_string(), pin: pin().to_string(), ..crate::transport::encrypted::protocol_tests::config() };

    let database = Database::open(Config { path: Some(path.to_string_lossy().into_owned()), data_version: None, cache_rows: 8 }, "[]".to_string(), Some(Arc::new(SubscriptionsOnly)), Some(encrypted), Some(Arc::new(Held(Arc::new(MemoryCredentials::default())))), Some(Arc::new(ThroughServer(server))), None, None).await.expect("the database opens");

    assert_eq!(database.life.published().identity, crate::store::identity_of(DIGEST), "the session identity is published");
    assert_eq!(database.session.as_ref().unwrap().stored_pending().unwrap(), None, "no pending request");
}
