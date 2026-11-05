//! Tests of the executors of direction A on the threaded `Runtime` and a real `Db`: every input enters through `Life`, the observable is
//! the published value, the answer of the waiter and the rows of the database. Each wait has a limit of 5 s.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use serde_json::json;

use super::{Life, LifeReply as Reply, Ports};
use crate::schema::{FieldSpec, ModelSpec};
use crate::value::ScalarKind;
use crate::db::{Db, DbConfig};
use crate::owners::calls::CallError;
use crate::owners::lifecycle::LifecycleError;
use crate::runtime::Waiter;
use crate::transport::{MockTransport, Reply as MockReply};

const LIMIT: Duration = Duration::from_secs(5);
const DIGEST: &str = "00000000000000010000000000000000000000000000000000000000000000ff";
const IDENTITY: u64 = 1;

fn ports(db: Db, transport: Arc<MockTransport>) -> Ports {
    Ports {
        db,
        transport: Some(transport),
        executor: tokio::runtime::Handle::current(),
        operations: Arc::new(|name| (name == "Op").then(|| ("query Op { feed }".to_string(), "feed".to_string()))),
        credentials: None,
        session: None,
    }
}

fn moments_spec() -> ModelSpec { ModelSpec::new("moments", vec![FieldSpec::scalar("id", ScalarKind::Id)]) }

fn open(path: &std::path::Path) -> Db { Db::open_with(Some(path), vec![moments_spec()], None, DbConfig::default()).unwrap() }

fn session_row(db: &Db) {
    let inner = db.shared.store.lock();
    crate::transport::encrypted::state::start(&inner.connection, DIGEST).unwrap();
    crate::transport::encrypted::state::commit(&inner.connection, 1).unwrap();
}

fn stored_digest(db: &Db) -> Option<String> {
    let inner = db.shared.store.lock();
    crate::transport::encrypted::state::session(&inner.connection).unwrap().map(|session| session.credential_digest)
}

async fn answer(waiter: Waiter<Reply>) -> Reply {
    match tokio::time::timeout(LIMIT, waiter.wait()).await.expect("the owner answered within 5 s") {
        crate::runtime::Outcome::Value(reply) => reply,
        other => panic!("the waiter ended without a value: {other:?}"),
    }
}

async fn start(db: &Db, transport: &Arc<MockTransport>) -> Life {
    let (db, transport) = (db.clone(), transport.clone());
    tokio::task::spawn_blocking(move || Life::start(ports(db, transport), LIMIT).expect("the first publish comes")).await.unwrap()
}

async fn closed(life: &Life) {
    assert_eq!(answer(life.close()).await, Reply::Life(Ok(())));
    assert!(life.wait_store_closed(LIMIT), "the store closed its connections");
}

/// life_open_no_session: a new database opens at epoch 1 without a session and the epoch is saved for the next open.
#[tokio::test(flavor = "multi_thread")]
async fn life_open_no_session_saves_the_epoch() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("db.sqlite");
    let transport = Arc::new(MockTransport::new());
    let db = open(&path);
    let life = start(&db, &transport).await;
    let published = life.published();
    assert_eq!((published.epoch, published.identity, published.generation, published.closed), (1, None, 0, false));
    closed(&life).await;
    assert!(life.faults().is_empty(), "no executor failed: {:?}", life.faults());
    drop(life);
    drop(db);
    let reopened = open(&path);
    assert_eq!(reopened.shared.store.saved().unwrap(), (1, 0, None), "Published was saved by the Store executor");
}

/// P-LIFE-05: after `close` and `shutdown` no owner thread of the database runs.
#[tokio::test(flavor = "multi_thread")]
async fn close_stops_every_owner_thread_of_the_database() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("db.sqlite");
    let transport = Arc::new(MockTransport::new());
    let db = open(&path);
    let life = Arc::new(start(&db, &transport).await);
    assert_eq!(life.owners_alive(), 4, "lifecycle, store, network and calls run");
    closed(&life).await;
    let stopping = life.clone();
    tokio::task::spawn_blocking(move || stopping.shutdown()).await.unwrap();
    assert_eq!(life.owners_alive(), 0);
}

/// life_open_session: a stored session opens as `Session{saved_epoch + 1, identity}` and `saved_epoch` grows with every open.
#[tokio::test(flavor = "multi_thread")]
async fn life_open_session_continues_the_saved_epoch() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("db.sqlite");
    let transport = Arc::new(MockTransport::new());
    {
        let db = open(&path);
        session_row(&db);
        let life = start(&db, &transport).await;
        assert_eq!((life.published().epoch, life.published().identity), (1, Some(IDENTITY)));
        closed(&life).await;
    }
    let db = open(&path);
    let life = start(&db, &transport).await;
    assert_eq!((life.published().epoch, life.published().identity), (2, Some(IDENTITY)), "the second open continues from the saved epoch");
    closed(&life).await;
}

/// net_set_online_changed: `set_online` answers and the old `online` mirror follows the Network owner.
#[tokio::test(flavor = "multi_thread")]
async fn net_set_online_reaches_the_mirror() {
    let db = Db::open_in_memory(vec![moments_spec()], None).unwrap();
    let transport = Arc::new(MockTransport::new());
    let life = start(&db, &transport).await;
    assert_eq!(answer(life.set_online(true)).await, Reply::Online { closed: false });
    assert!(db.connectivity().is_online());
    assert_eq!(answer(life.set_online(false)).await, Reply::Online { closed: false });
    assert!(!db.connectivity().is_online());
    closed(&life).await;
    assert_eq!(answer(life.set_online(true)).await, Reply::Online { closed: true }, "net_closed");
}

/// call_run, call_run_offline, call_run_stale: the answer of the transport, offline refusal and the epoch rule.
#[tokio::test(flavor = "multi_thread")]
async fn call_run_answers_by_epoch_and_network() {
    let db = Db::open_in_memory(vec![moments_spec()], None).unwrap();
    let transport = Arc::new(MockTransport::new());
    let life = start(&db, &transport).await;
    life.declare(vec!["Op".into()]);
    assert_eq!(answer(life.set_online(false)).await, Reply::Online { closed: false });
    assert_eq!(answer(life.run(1, "Op".into(), "{}".into())).await, Reply::Call(Err(CallError::NoSession)), "an epoch without a session has no Calls");
    life.credential_committed(IDENTITY);
    let published = loop {
        let published = life.published();
        if published.identity.is_some() { break published; }
        tokio::time::sleep(Duration::from_millis(5)).await;
    };
    assert_eq!(published.epoch, 2);
    assert_eq!(answer(life.run(2, "Op".into(), "{}".into())).await, Reply::Call(Err(CallError::Offline)));
    assert_eq!(answer(life.run(1, "Op".into(), "{}".into())).await, Reply::Call(Err(CallError::NoSession)), "call_run_stale");
    assert_eq!(answer(life.set_online(true)).await, Reply::Online { closed: false });
    transport.enqueue("Op", MockReply::data(json!({ "feed": [1, 2] })));
    assert_eq!(answer(life.run(2, "Op".into(), "{}".into())).await, Reply::Call(Ok("[1,2]".into())));
    assert_eq!(answer(life.run(2, "Missing".into(), "{}".into())).await, Reply::Call(Err(CallError::Invalid)), "call_run_unknown");
    closed(&life).await;
    assert_eq!(answer(life.run(2, "Op".into(), "{}".into())).await, Reply::Closed, "life_call_closed");
}

/// life_session_reset, life_prop_session_kept: `session_reset` erases the credential rows and ends the epoch, `IdentityChanged` keeps them.
#[tokio::test(flavor = "multi_thread")]
async fn life_session_reset_erases_the_credential_and_identity_change_keeps_it() {
    let transport = Arc::new(MockTransport::new());
    let db = Db::open_in_memory(vec![moments_spec()], None).unwrap();
    session_row(&db);
    let life = start(&db, &transport).await;
    assert_eq!(life.published().identity, Some(IDENTITY));
    let revision = db.revision_seq().unwrap();
    assert_eq!(answer(life.session_reset()).await, Reply::Life(Ok(())));
    let published = life.published();
    assert_eq!((published.epoch, published.identity, published.generation), (2, None, 1));
    assert_eq!(stored_digest(&db), None, "SessionReset erases encrypted_session");
    assert!(db.revision_seq().unwrap() > revision, "the clearing is a commit");
    closed(&life).await;

    let other = Db::open_in_memory(vec![moments_spec()], None).unwrap();
    session_row(&other);
    let life = start(&other, &transport).await;
    life.credential_committed(2);
    let published = loop {
        let published = life.published();
        if published.epoch == 3 { break published; }
        tokio::time::sleep(Duration::from_millis(5)).await;
    };
    assert_eq!((published.identity, published.generation), (Some(2), 1));
    assert_eq!(stored_digest(&other).as_deref(), Some(DIGEST), "IdentityChanged keeps encrypted_session");
    closed(&life).await;
}

/// life_resetting_cleared: `reset_models` clears the cache of the models and raises the generation, intents stay.
#[tokio::test(flavor = "multi_thread")]
async fn life_reset_models_raises_the_generation() {
    let transport = Arc::new(MockTransport::new());
    let db = Db::open_in_memory(vec![moments_spec()], None).unwrap();
    let life = start(&db, &transport).await;
    let models: BTreeSet<String> = ["moments".to_string()].into();
    assert_eq!(answer(life.reset_models(models)).await, Reply::Life(Ok(())));
    assert_eq!((life.published().epoch, life.published().generation), (1, 1));
    let bad: BTreeSet<String> = ["unknown".to_string()].into();
    assert_eq!(answer(life.reset_models(bad)).await, Reply::Life(Err(LifecycleError::Storage)), "an erase failure answers Err(Storage)");
    assert_eq!(life.published().generation, 1, "a failed clearing keeps the generation");
    closed(&life).await;
}
