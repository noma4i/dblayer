use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};

use super::fake_server_tests::{FakeServer, MemoryCredentials};
use super::state;
use super::vectors_tests::*;
use super::*;
use crate::db::Db;
use crate::pacing::ManualClock;

const SOCIAL: &str = "/api/auth/social";
const GRAPHQL: &str = "/api/graphql";

#[derive(Default)]
struct WatchedCredentials {
    inner: MemoryCredentials,
    sets: AtomicUsize,
    clears: AtomicUsize,
    fail_next_clear: AtomicBool,
}

impl CredentialSlot for WatchedCredentials {
    fn get(&self) -> Result<Option<String>, TransportError> {
        self.inner.get()
    }

    fn set(&self, credential: &str) -> Result<(), TransportError> {
        self.sets.fetch_add(1, Ordering::SeqCst);
        self.inner.set(credential)
    }

    fn clear(&self) -> Result<(), TransportError> {
        self.clears.fetch_add(1, Ordering::SeqCst);
        if self.fail_next_clear.swap(false, Ordering::SeqCst) {
            return Err(TransportError::Network("the app could not clear the credential".into()));
        }
        self.inner.clear()
    }
}

struct Rig {
    server: Arc<FakeServer>,
    credentials: Arc<WatchedCredentials>,
    clock: Arc<ManualClock>,
}

impl Rig {
    fn new() -> Self {
        let clock = Arc::new(ManualClock::new(Duration::from_secs(1_800_000_000)));
        Self { server: FakeServer::new(clock.clone()), credentials: Arc::default(), clock }
    }

    fn client(&self, db: &Db) -> EncryptedClient {
        let config = EncryptedConfig { origin: origin().to_string(), server_key: public_key_der_b64().to_string(), pin: pin().to_string(), ..crate::transport::encrypted::protocol_tests::config() };
        EncryptedClient::new(&config, self.server.clone(), self.credentials.clone(), db.clone(), self.clock.clone()).unwrap()
    }

    fn touched(&self) -> (usize, usize) {
        (self.credentials.sets.load(Ordering::SeqCst), self.credentials.clears.load(Ordering::SeqCst))
    }
}

fn file_db(path: &Path) -> Db {
    Db::open(path, Vec::new(), None).unwrap()
}

fn rows(db: &Db) -> (Option<state::Session>, Option<state::Pending>) {
    let inner = db.shared.store.lock();
    (state::session(&inner.connection).unwrap(), state::pending(&inner.connection).unwrap())
}

fn signed_in() -> Value {
    json!({ "provider": "google", "idToken": "t" })
}

#[tokio::test]
async fn a_key_that_does_not_match_the_pin_leaves_the_credential_and_the_session_row_alone() {
    let rig = Rig::new();
    let db = Db::open_in_memory(Vec::new(), None).unwrap();
    let client = rig.client(&db);
    client.mutate(&client.admit("test call").unwrap(), SOCIAL, signed_in()).await.unwrap();
    let (credential, session) = (rig.credentials.inner.value(), rows(&db).0);
    let before = rig.touched();

    let wrong_pin = EncryptedConfig { origin: origin().to_string(), server_key: public_key_der_b64().to_string(), pin: short_key_pin().to_string(), ..crate::transport::encrypted::protocol_tests::config() };
    assert!(EncryptedClient::new(&wrong_pin, rig.server.clone(), rig.credentials.clone(), db.clone(), rig.clock.clone()).is_err());

    assert_eq!((rig.touched(), rig.credentials.inner.value(), rows(&db).0), (before, credential, session));
}

#[tokio::test]
async fn a_refusal_code_keeps_the_credential_the_session_row_and_the_pending_request() {
    let codes = ["ACCOUNT_UNAVAILABLE", "UNAUTHENTICATED", "PROVIDER_REJECTED", "BLOCKED"];
    for code in codes {
        for confirmed in [false, true] {
            let rig = Rig::new();
            let db = Db::open_in_memory(Vec::new(), None).unwrap();
            let client = rig.client(&db);
            client.mutate(&client.admit("test call").unwrap(), SOCIAL, signed_in()).await.unwrap();
            let (credential, digest) = (rig.credentials.inner.value().unwrap(), rows(&db).0.unwrap().credential_digest);
            rig.server.refuse_next(code, confirmed);

            let outcome = client.mutate(&client.admit("test call").unwrap(), GRAPHQL, json!({ "query": "mutation { m }" })).await;

            let case = format!("{code} confirmed={confirmed}");
            match confirmed {
                true => assert_eq!(outcome.unwrap().body, json!({ "error": code }), "{case}"),
                false => assert_eq!(outcome, Err(TransportError::refusal(code)), "{case}"),
            }
            let (session, pending) = rows(&db);
            assert_eq!((rig.credentials.inner.value(), session.unwrap().credential_digest, pending.is_some(), rig.touched()), (Some(credential), digest, !confirmed, (1, 0)), "{case}");
        }
    }
}

#[tokio::test]
async fn a_request_that_never_left_is_sent_once_by_resume_after_a_restart() {
    let rig = Rig::new();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("session.sqlite");
    let first = rig.client(&file_db(&path));
    rig.server.drop_next_request();
    super::client_tests::cancel_pending(first.mutate(&first.admit("test call").unwrap(), SOCIAL, signed_in()));
    assert_eq!(rig.server.seen().len(), 0);
    drop(first);

    let db = file_db(&path);
    let answer = rig.client(&db).resume(&rig.client(&db).admit("test call").unwrap()).await.unwrap().unwrap();

    assert_eq!(answer.body, json!({ "nextStep": "authenticated" }));
    assert_eq!((rig.server.executed().len(), rows(&db).0.unwrap().sequence, rows(&db).1), (1, 1, None));
}

#[tokio::test]
async fn an_answer_that_was_executed_but_never_committed_is_confirmed_once_by_resume_after_a_restart() {
    let rig = Rig::new();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("session.sqlite");
    let first = rig.client(&file_db(&path));
    first.mutate(&first.admit("test call").unwrap(), SOCIAL, signed_in()).await.unwrap();
    rig.server.lose_next_answer();
    super::client_tests::cancel_pending(first.mutate(&first.admit("test call").unwrap(), GRAPHQL, json!({ "query": "mutation { m }" })));
    assert_eq!(rig.server.executed().len(), 2);
    drop(first);

    let db = file_db(&path);
    let answer = rig.client(&db).resume(&rig.client(&db).admit("test call").unwrap()).await.unwrap().unwrap();

    assert_eq!(answer.body, json!({ "data": { "done": "mutation { m }" } }));
    assert_eq!((rig.server.executed().len(), rows(&db).0.unwrap().sequence, rows(&db).1), (2, 2, None));
}

#[tokio::test]
async fn a_stored_credential_without_matching_sequence_state_is_reported_without_replacement() {
    for case in ["missing", "mismatched", "cached"] {
        let rig = Rig::new();
        let db = Db::open_in_memory(Vec::new(), None).unwrap();
        let client = rig.client(&db);
        if case == "cached" {
            client.mutate(&client.admit("test call").unwrap(), SOCIAL, signed_in()).await.unwrap();
            db.shared.store.lock().connection.execute("DELETE FROM encrypted_session", []).unwrap();
        } else {
            rig.credentials.inner.set("saved-before-the-crash").unwrap();
            if case == "mismatched" {
                state::start(&db.shared.store.lock().connection, "another-credential").unwrap();
            }
        }
        let before = (rig.touched(), rig.credentials.inner.value(), rows(&db), rig.server.seen().len());

        let error = client.mutate(&client.admit("test call").unwrap(), SOCIAL, signed_in()).await.unwrap_err();

        assert_eq!(crate::transport::retry::RetryClass::of_error(&error), crate::transport::retry::RetryClass::Fatal, "{case}");
        assert_eq!(crate::ffi::DbError::from(crate::sync::SyncError::Transport(error)), crate::ffi::DbError::Session { code: crate::transport::SessionCode::StateLost, detail: "lost session sequence state for the stored credential".into() }, "{case}");
        assert_eq!((rig.touched(), rig.credentials.inner.value(), rows(&db), rig.server.seen().len()), before, "{case}");
    }
}

fn session_model() -> crate::schema::ModelSpec {
    crate::schema::ModelSpec::new("messages", vec![crate::schema::FieldSpec::scalar("id", crate::value::ScalarKind::Id)])
}

async fn session_at_seven(rig: &Rig, db: &Db) -> EncryptedClient {
    let client = rig.client(db);
    client.mutate(&client.admit("test call").unwrap(), SOCIAL, signed_in()).await.unwrap();
    for _ in 1..7 {
        client.mutate(&client.admit("test call").unwrap(), GRAPHQL, json!({ "query": "mutation { m }" })).await.unwrap();
    }
    client
}

#[tokio::test]
async fn session_sequence_and_pending_survive_every_database_clearing_path() {
    const PREVIOUS_FORMAT: &str = "8741b87cadfb19835eed5eb86a4388f0ed57e651b054460628431c2e6aad312c";
    for case in ["format", "schema", "boot_format", "data", "model", "reset", "reset_all", "interrupted_cache", "interrupted_all", "close", "start"] {
        for pending in [false, true] {
            let rig = Rig::new();
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("session.sqlite");
            let db = Db::open(&path, vec![session_model()], None).unwrap();
            let client = session_at_seven(&rig, &db).await;
            if pending {
                rig.server.lose_next_answer();
                super::client_tests::cancel_pending(client.mutate(&client.admit("test call").unwrap(), GRAPHQL, json!({ "query": "mutation { pending }" })));
            }
            let before = (rows(&db), rig.credentials.inner.value(), rig.touched());
            assert_eq!(before.0.0.as_ref().unwrap().sequence, 7);
            let request = rig.server.seen().last().unwrap().clone();
            match case {
                "format" | "boot_format" => {
                    assert_ne!(PREVIOUS_FORMAT, crate::ddl::format_version());
                    db.shared.store.lock().connection.execute("UPDATE manifest SET value = ?1 WHERE key = 'format_version'", [PREVIOUS_FORMAT]).unwrap();
                    if case == "boot_format" { db.shared.store.boot(&[session_model()], None).unwrap(); }
                }
                "schema" => db.shared.store.lock().connection.pragma_update(None, "user_version", crate::ddl::SCHEMA_VERSION + 1).unwrap(),
                "data" => { db.shared.store.boot(&[session_model()], Some("next")).unwrap(); }
                "model" => {
                    let mut model = session_model();
                    model.fields.push(crate::schema::FieldSpec::scalar("body", crate::value::ScalarKind::Str).optional());
                    db.shared.store.boot(&[model], None).unwrap();
                }
                "reset" => db.reset(&["messages"]).unwrap(),
                "reset_all" => db.reset_all().unwrap(),
                "interrupted_cache" | "interrupted_all" => {
                    {
                        let inner = db.shared.store.lock();
                        inner.connection.execute("INSERT INTO reset_intent (id, mode, started_at) VALUES (1, ?1, 0)", [if case == "interrupted_cache" { "cache_only" } else { "incompatible" }]).unwrap();
                        inner.connection.execute("INSERT INTO reset_intent_model (pos, model) VALUES (0, 'messages')", []).unwrap();
                    }
                    assert!(db.shared.store.boot(&[session_model()], None).unwrap().finished_interrupted_reset);
                }
                "start" => state::start(&db.shared.store.lock().connection, &before.0.0.as_ref().unwrap().credential_digest).unwrap(),
                "close" => {}
                _ => unreachable!(),
            }
            drop(client);
            drop(db);

            let db = Db::open(&path, vec![session_model()], None).unwrap();
            let client = rig.client(&db);
            assert_eq!((rows(&db), rig.credentials.inner.value(), rig.touched()), before, "{case}, pending={pending}");
            let format: String = db.shared.store.lock().connection.query_row("SELECT value FROM manifest WHERE key = 'format_version'", [], |row| row.get(0)).unwrap();
            assert_eq!(format, crate::ddl::format_version());
            let answer = if pending { client.resume(&client.admit("test call").unwrap()).await.unwrap().unwrap() } else { client.mutate(&client.admit("test call").unwrap(), GRAPHQL, json!({ "query": "mutation { next }" })).await.unwrap() };
            assert_eq!(answer.sequence, Some(8), "{case}, pending={pending}");
            assert_eq!(rig.credentials.inner.value(), before.1);
            assert_eq!(rig.touched(), before.2);
            assert_eq!(rig.server.executed().len(), 8, "{case}, pending={pending}");
            if pending {
                let replay = rig.server.seen().last().unwrap().clone();
                for field in ["credential", "sequence", "requestId", "responseKey", "path", "body"] {
                    assert_eq!(replay[field], request[field], "{case}: {field}");
                }
            }
            assert_eq!(rows(&db).0.unwrap().sequence, 8);
            assert_eq!(rows(&db).1, None);
        }
    }
}

#[tokio::test]
async fn changed_session_table_ddl_migrates_the_sequence_and_complete_pending_request() {
    let rig = Rig::new();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("session.sqlite");
    let db = file_db(&path);
    let client = session_at_seven(&rig, &db).await;
    rig.server.lose_next_answer();
    let owner = crate::transport::RequestOwner { seq: 4, item_position: 2, stage: "mutation".into() };
    assert!(client.mutate_owned(&client.admit("test call").unwrap(), GRAPHQL, json!({ "query": "mutation { pending }" }), Some(owner), true).await.is_err());
    let before = (rows(&db), rig.credentials.inner.value(), rig.touched());
    db.shared.store.lock().connection.execute_batch(
        "ALTER TABLE encrypted_session ADD COLUMN retired TEXT;
         ALTER TABLE encrypted_pending ADD COLUMN retired TEXT;
         UPDATE manifest SET value = 'previous-session-ddl' WHERE key = 'format_version'",
    ).unwrap();
    drop(client);
    drop(db);

    let db = file_db(&path);

    assert_eq!((rows(&db), rig.credentials.inner.value(), rig.touched()), before);
    for table in ["encrypted_session", "encrypted_pending"] {
        let retired: i64 = db.shared.store.lock().connection.query_row("SELECT count(*) FROM pragma_table_info(?1) WHERE name = 'retired'", [table], |row| row.get(0)).unwrap();
        assert_eq!(retired, 0);
    }
    {
        let inner = db.shared.store.lock();
        inner.connection.execute("INSERT INTO op (action_key, status, created_at, since) VALUES ('messages:save', 'pending', 0, 0)", []).unwrap();
        assert!(inner.connection.last_insert_rowid() > before.0.1.as_ref().unwrap().owner.as_ref().unwrap().seq);
    }
    let answer = rig.client(&db).resume(&rig.client(&db).admit("test call").unwrap()).await.unwrap().unwrap();
    assert_eq!(answer.sequence, Some(8));
    assert_eq!(rig.server.executed().len(), 8);
    assert_eq!(rig.touched(), before.2);
}

#[tokio::test]
async fn explicit_session_reset_clears_credential_sequence_and_pending_after_reopen() {
    let rig = Rig::new();
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("session.sqlite");
    let db = file_db(&path);
    let client = session_at_seven(&rig, &db).await;
    rig.server.lose_next_answer();
    super::client_tests::cancel_pending(client.mutate(&client.admit("test call").unwrap(), GRAPHQL, json!({ "query": "mutation { pending }" })));
    assert!(rows(&db).1.is_some());

    client.reset().await.unwrap();
    drop(client);
    drop(db);

    let db = file_db(&path);
    assert_eq!((rig.credentials.inner.value(), rows(&db)), (None, (None, None)));
    assert_eq!(rig.touched(), (1, 1));
    assert_eq!(rig.client(&db).resume(&rig.client(&db).admit("test call").unwrap()).await, Ok(None));
}

#[tokio::test]
async fn a_session_row_without_its_credential_drops_the_old_pending_request_and_starts_again() {
    let rig = Rig::new();
    let db = Db::open_in_memory(Vec::new(), None).unwrap();
    rig.server.drop_next_request();
    super::client_tests::cancel_pending(rig.client(&db).mutate(&rig.client(&db).admit("test call").unwrap(), SOCIAL, signed_in()));
    rig.credentials.inner.clear().unwrap();

    let restored = rig.client(&db);

    assert_eq!(restored.resume(&restored.admit("test call").unwrap()).await, Ok(None));
    assert_eq!((rig.server.seen().len(), rows(&db).1), (0, None));
    restored.mutate(&restored.admit("test call").unwrap(), SOCIAL, signed_in()).await.unwrap();
    assert_eq!((rig.server.executed().len(), rows(&db).0.unwrap().sequence), (1, 1));
}

#[tokio::test]
async fn a_logout_whose_credential_could_not_be_cleared_is_finished_by_resume_without_a_second_effect() {
    let rig = Rig::new();
    let db = Db::open_in_memory(Vec::new(), None).unwrap();
    let client = rig.client(&db);
    client.mutate(&client.admit("test call").unwrap(), SOCIAL, signed_in()).await.unwrap();
    rig.credentials.fail_next_clear.store(true, Ordering::SeqCst);

    assert!(client.mutate(&client.admit("test call").unwrap(), "/api/logout", json!({})).await.is_err());
    let resumed = client.resume(&client.admit("test call").unwrap()).await.unwrap();

    assert_eq!(resumed.map(|answer| answer.body), Some(json!({ "revoked": true })));
    assert_eq!((rig.credentials.inner.value(), rows(&db)), (None, (None, None)));
    assert_eq!(rig.server.executed().len(), 2);
}
