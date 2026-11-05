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
        transfer: None,
        read_operations: None,
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
    tokio::task::spawn_blocking(move || Life::start(ports(db, transport)).expect("the first publish comes")).await.unwrap()
}

async fn closed(life: &Life) {
    assert_eq!(answer(life.close()).await, Reply::Life(Ok(())), "close is answered after the store closed its connections");
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
    assert_eq!(life.owners_alive(), 7, "lifecycle, store, network, calls, windows, deliveries and feed run");
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
    assert_eq!(answer(life.run(1, "Op".into(), "{}".into())).await, Reply::Call(Err(CallError::Offline)), "a database without a session client hands the call to Calls in every state");
    life.credential_committed(IDENTITY);
    let published = loop {
        let published = life.published();
        if published.identity.is_some() { break published; }
        tokio::time::sleep(Duration::from_millis(5)).await;
    };
    assert_eq!(published.epoch, 2);
    assert_eq!(answer(life.run(2, "Op".into(), "{}".into())).await, Reply::Call(Err(CallError::Offline)));
    assert!(matches!(life.run(1, "Op".into(), "{}".into()).wait().await, crate::runtime::Outcome::EpochEnded(_)), "call_run_stale: the wait of an ended epoch ends with the end of the epoch");
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

fn paged_spec() -> ModelSpec {
    let mut spec = ModelSpec::new("moments", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("userId", ScalarKind::Id).nullable().indexed(), FieldSpec::scalar("text", ScalarKind::Str).optional()]);
    let mut mine = crate::schema::RelationSpec::new("mine", &["userId"]);
    mine.remote = Some(crate::schema::RemoteSpec {
        shape: crate::schema::RemoteShape::Connection { direction: crate::schema::PageDirection::Forward, coverage: crate::schema::Coverage::Page, max_pages: None, cursor: None },
        operation: "Mine".into(),
        stale_time: "1.minute".into(),
        empty_stale_time: None,
        selects_page_info: true,
        page_size: None,
        variables: Default::default(),
        root: None,
        total_from: None,
    });
    spec.relations.push(mine);
    spec
}

/// win_loading_response, win_landing_committed (P-WIN-02): the answer of the server goes through Windows, the real Store executor and the Write of `Db`; the caller hears `Loaded` and the rows are in the database.
#[tokio::test(flavor = "multi_thread")]
async fn a_loaded_page_lands_through_the_store_and_the_caller_hears_loaded() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("db.sqlite");
    let transport = Arc::new(MockTransport::new());
    let db = Db::open_with(Some(&path), vec![paged_spec()], None, DbConfig::default()).unwrap();
    session_row(&db);
    transport.enqueue("Mine", MockReply::data(json!({"mine": {"edges": [{"node": {"id": "m1", "userId": "u1", "text": "one"}}, {"node": {"id": "m2", "userId": "u1", "text": "two"}}], "pageInfo": {"hasNextPage": false, "endCursor": null}}})));
    let mut setup = ports(db.clone(), transport.clone());
    let operations = crate::owners::read::ReadOperations::default();
    operations.declare([crate::owners::read::ReadOperation { name: "Mine".into(), document: "query Mine { mine { edges { node { id userId text } } pageInfo { hasNextPage endCursor } } }".into(), root_path: "mine".into(), page_size: 20 }]);
    setup.read_operations = Some(operations);
    let life = tokio::task::spawn_blocking(move || Life::start(setup).expect("the first publish comes")).await.unwrap();
    assert_eq!(answer(life.set_online(true)).await, Reply::Online { closed: false });
    let key = crate::owners::read::windows::WindowKey::relation("moments", "mine", json!({"userId": "u1"}).as_object().unwrap());
    let epoch = life.published().epoch;
    life.credential_committed(2);
    crate::test_wait::yield_until("the next identity was published", || life.published().identity == Some(2)).await;
    let old_key = crate::owners::read::windows::WindowKey::relation("moments", "mine", json!({"userId": "old"}).as_object().unwrap());
    let (stale, _) = life.load(epoch, old_key, crate::owners::read::windows::Goal::First, true, 0);
    assert!(matches!(crate::test_wait::within(stale.wait()).await, crate::runtime::Outcome::EpochEnded(_)));
    let (waiter, _) = life.load(life.published().epoch, key, crate::owners::read::windows::Goal::First, false, 0);
    let reply = answer(waiter).await;
    assert!(matches!(reply, Reply::Window(crate::owners::read::windows::WindowOutcome::Loaded { .. })), "the load ends as loaded: {reply:?} {:?}", life.faults());
    let stored: Vec<_> = ["m1", "m2"].iter().map(|id| db.find("moments", &crate::identity::RowId::new(id).unwrap()).unwrap().map(|row| row.text("text").map(str::to_string))).collect();
    assert_eq!(stored, [Some(Some("one".to_string())), Some(Some("two".to_string()))]);
    assert_eq!(transport.calls().len(), 1, "the old epoch never starts a request in the new identity");
    closed(&life).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn net_prop_no_request_before_snapshot() {
    let db = Db::open_in_memory(vec![moments_spec()], None).unwrap();
    let transport = Arc::new(MockTransport::new());
    let life = start(&db, &transport).await;
    life.declare(vec!["Op".into()]);
    assert_eq!(answer(life.run(1, "Op".into(), "{}".into())).await, Reply::Call(Err(CallError::Offline)));
    assert!(transport.calls().is_empty(), "no request left before the network said it is online");
    assert_eq!(answer(life.set_online(true)).await, Reply::Online { closed: false });
    transport.enqueue("Op", MockReply::data(json!({ "feed": [1] })));
    assert_eq!(answer(life.run(1, "Op".into(), "{}".into())).await, Reply::Call(Ok("[1]".into())));
    assert_eq!(transport.calls().len(), 1);
    closed(&life).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn network_events_reach_the_listeners_with_the_snapshot_of_the_epoch() {
    use crate::owners::contract::NetworkEvent;
    use crate::owners::lifecycle::Peer;
    use std::sync::Mutex;
    let db = Db::open_in_memory(vec![moments_spec()], None).unwrap();
    let transport = Arc::new(MockTransport::new());
    let life = start(&db, &transport).await;
    let heard: Arc<Mutex<Vec<(Peer, NetworkEvent)>>> = Arc::default();
    for owner in [Peer::Rounds, Peer::Session, Peer::Transfer] {
        let heard = heard.clone();
        life.on_network(owner, move |event| heard.lock().unwrap().push((owner, event)));
    }
    assert_eq!(answer(life.foreground()).await, Reply::Online { closed: false });
    assert_eq!(answer(life.set_power_saving(true)).await, Reply::Online { closed: false });
    assert_eq!(answer(life.set_power_saving(true)).await, Reply::Online { closed: false });
    assert_eq!(answer(life.foreground()).await, Reply::Online { closed: false });
    assert_eq!(
        *heard.lock().unwrap(),
        vec![
            (Peer::Rounds, NetworkEvent::Foreground(1)),
            (Peer::Session, NetworkEvent::Foreground(1)),
            (Peer::Transfer, NetworkEvent::PowerSaving { saving: true, epoch: None }),
            (Peer::Rounds, NetworkEvent::Foreground(2)),
            (Peer::Session, NetworkEvent::Foreground(2)),
        ],
        "each change goes out once, to the owners the table names"
    );
    heard.lock().unwrap().clear();
    life.credential_committed(IDENTITY);
    let epoch = loop {
        let published = life.published();
        if published.epoch == 2 { break published.epoch; }
        tokio::time::sleep(Duration::from_millis(5)).await;
    };
    let started = std::time::Instant::now();
    while !heard.lock().unwrap().iter().any(|(owner, event)| *owner == Peer::Transfer && matches!(event, NetworkEvent::PowerSaving { epoch: Some(_), .. })) {
        assert!(started.elapsed() < LIMIT, "the snapshot of the new epoch reached Transfer");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let heard = heard.lock().unwrap().clone();
    assert!(heard.contains(&(Peer::Transfer, NetworkEvent::Online { online: false, epoch: Some(epoch) })));
    assert!(heard.contains(&(Peer::Transfer, NetworkEvent::PowerSaving { saving: true, epoch: Some(epoch) })), "Transfer gets `saving` with its first snapshot");
    closed(&life).await;
}

/// net_prop_snapshot_after_create: a snapshot addressed to an epoch that is already past reaches no receiver.
#[tokio::test(flavor = "multi_thread")]
async fn a_snapshot_of_a_past_epoch_reaches_no_receiver() {
    use crate::owners::contract::NetworkEvent;
    use crate::owners::lifecycle::Peer;
    use crate::owners::network::NetworkInput;
    use std::sync::Mutex;
    let db = Db::open_in_memory(vec![moments_spec()], None).unwrap();
    let transport = Arc::new(MockTransport::new());
    let life = start(&db, &transport).await;
    let heard: Arc<Mutex<Vec<NetworkEvent>>> = Arc::default();
    let sink = heard.clone();
    life.on_network(Peer::Transfer, move |event| sink.lock().unwrap().push(event));
    let epoch = life.published().epoch;
    life.wiring.to_network(NetworkInput::SnapshotRequest { owner: Peer::Transfer, epoch: epoch - 1 });
    life.wiring.to_network(NetworkInput::SnapshotRequest { owner: Peer::Transfer, epoch });
    let started = std::time::Instant::now();
    while !heard.lock().unwrap().iter().any(|event| matches!(event, NetworkEvent::Online { epoch: Some(heard), .. } if *heard == epoch)) {
        assert!(started.elapsed() < LIMIT, "the snapshot of the current epoch arrived");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(!heard.lock().unwrap().iter().any(|event| matches!(event, NetworkEvent::Online { epoch: Some(heard), .. } if *heard != epoch)), "the snapshot of the past epoch was dropped");
    closed(&life).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn the_rows_of_a_batch_land_through_the_common_path() {
    use crate::owners::contract::BatchAuthor;
    use crate::owners::store::{StoreBatch, StoreInput, StoreReply};
    let db = Db::open_in_memory(vec![moments_spec()], None).unwrap();
    let transport = Arc::new(MockTransport::new());
    let life = start(&db, &transport).await;
    let (sender, receiver) = std::sync::mpsc::channel();
    life.on_store_reply(BatchAuthor::Window, move |batch, reply| { let _ = sender.send((batch, reply)); });
    let epoch = life.published().epoch;
    let batch = |id, row: &str| StoreBatch { author: BatchAuthor::Window, id, epoch, basis: [("moments".to_string(), 0)].into(), rows: vec![("moments".into(), row.into())], intents: 0, credential: None, ops: Vec::new() };
    life.wiring.to_store(StoreInput::Commit(batch(1, r#"{"id":"m1"}"#)));
    let (id, reply) = receiver.recv_timeout(LIMIT).expect("the store answered");
    assert_eq!(id, 1);
    assert!(matches!(reply, StoreReply::Committed { .. }), "{reply:?}");
    assert_eq!(db.shared.store.row_count("moments").unwrap(), 1);
    life.wiring.to_store(StoreInput::Commit(batch(2, r#"{"name":"no id"}"#)));
    let (id, reply) = receiver.recv_timeout(LIMIT).expect("the store answered");
    assert_eq!(id, 2);
    assert!(matches!(reply, StoreReply::Rejected(_)), "a row without an id is refused: {reply:?}");
    assert_eq!(db.shared.store.row_count("moments").unwrap(), 1);
    closed(&life).await;
}

/// store_commit_ok: every subscriber hears one `Committed` with the revision and the models of the batch.
#[tokio::test(flavor = "multi_thread")]
async fn a_committed_batch_is_broadcast_to_every_subscriber() {
    use crate::owners::contract::{BatchAuthor, Models};
    use crate::owners::store::{StoreBatch, StoreInput, StoreReply};
    let db = Db::open_in_memory(vec![moments_spec()], None).unwrap();
    let transport = Arc::new(MockTransport::new());
    let life = start(&db, &transport).await;
    let (replies, reply) = std::sync::mpsc::channel();
    life.on_store_reply(BatchAuthor::Window, move |_, reply| { let _ = replies.send(reply); });
    let (first, heard_first) = std::sync::mpsc::channel();
    let (second, heard_second) = std::sync::mpsc::channel();
    life.on_committed(move |broadcast| { let _ = first.send(broadcast); });
    life.on_committed(move |broadcast| { let _ = second.send(broadcast); });
    let epoch = life.published().epoch;
    let batch = StoreBatch { author: BatchAuthor::Window, id: 1, epoch, basis: [("moments".to_string(), 0)].into(), rows: vec![("moments".into(), r#"{"id":"m1"}"#.into())], intents: 0, credential: None, ops: Vec::new() };
    life.wiring.to_store(StoreInput::Commit(batch));
    let StoreReply::Committed { revision } = reply.recv_timeout(LIMIT).expect("the author got its reply") else { panic!("the batch was refused") };
    for heard in [heard_first, heard_second] {
        let broadcast = heard.recv_timeout(LIMIT).expect("the subscriber heard the broadcast");
        assert_eq!((broadcast.revision, broadcast.models), (revision, Models::Set(["moments".to_string()].into())));
    }
    closed(&life).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unreadable_queue_is_reported_for_outbox_and_open_still_ends() {
    use crate::owners::lifecycle::Peer;
    let transport = Arc::new(MockTransport::new());
    let db = Db::open_in_memory(vec![moments_spec()], None).unwrap();
    session_row(&db);
    {
        let inner = db.shared.store.lock();
        inner.connection.execute_batch("INSERT INTO op (action_key, status, created_at, since) VALUES ('moments:send', 'pending', 1, 1)").unwrap();
    }
    let life = start(&db, &transport).await;
    let started = std::time::Instant::now();
    while life.restore_failures().is_empty() {
        assert!(started.elapsed() < LIMIT, "the failure of the queue was reported");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let failures = life.restore_failures();
    assert_eq!(failures.len(), 1);
    assert_eq!(failures[0].0, Peer::Outbox);
    assert_eq!(life.published().identity, Some(IDENTITY));
    closed(&life).await;
}

#[test]
fn outbox_and_transfer_answer_storage_while_their_queue_is_unreadable() {
    use crate::owners::contract::RejectReason;
    use crate::owners::write::outbox::{Fault, Outbox, OutboxAnswer, OutboxEffect, OutboxInput, OutboxPeers, OutboxState};
    use crate::owners::write::transfer::{TransferAnswer, TransferEffect, TransferInput, TransferPeers, TransferState, Transfers};
    use crate::runtime::{Address, Effect, StateOwner};
    let mut outbox = OutboxState::new(1, OutboxPeers { store: Address::new(0), session: Address::new(1), transfer: None });
    assert!(Outbox::step(&mut outbox, OutboxInput::RestoreFailed { error: "disk".into() }).is_empty());
    let effects = Outbox::step(&mut outbox, OutboxInput::Retry { model: "moments".into(), row_id: "m1".into(), reply: 5 });
    assert!(matches!(effects.as_slice(), [Effect::Run(OutboxEffect::Reply { reply: 5, answer: OutboxAnswer::Err(Fault::Rejected(RejectReason::Storage(error))) })] if error == "disk"), "{effects:?}");
    let mut transfer = TransferState::new(1, 1, TransferPeers { store: Address::new(0), outbox: Address::new(2) });
    assert!(Transfers::step(&mut transfer, TransferInput::RestoreFailed { error: "disk".into() }).is_empty());
    let effects = Transfers::step(&mut transfer, TransferInput::Cancel { key: "k".into(), reply: 6 });
    assert!(matches!(effects.as_slice(), [Effect::Run(TransferEffect::Reply { reply: 6, answer: TransferAnswer::Err(Fault::Rejected(RejectReason::Storage(error))) })] if error == "disk"), "{effects:?}");
}

#[test]
fn open_waits_for_restore_classified_of_the_installed_session() {
    use super::restore_participants;
    use crate::owners::lifecycle::{Lifecycle, LifecycleEffect, LifecycleInput, LifecycleState, Peer};
    use crate::runtime::{Effect, StateOwner};
    assert!(restore_participants(false).is_empty());
    let mut state = LifecycleState::new().with_restore_participants(restore_participants(true));
    let run = |state: &mut LifecycleState, input| Lifecycle::step(state, input).into_iter().filter_map(|effect| if let Effect::Run(run) = effect { Some(run) } else { None }).collect::<Vec<_>>();
    let published = run(&mut state, LifecycleInput::Open { saved_epoch: 4, saved_generation: 2, saved_identity: Some(9), reply: Some(7) });
    let publication_id = state.publishing().first().expect("a publication waits for Saved").id;
    assert!(published.iter().all(|effect| !matches!(effect, LifecycleEffect::RestoreReady { .. })));
    let after_saved = run(&mut state, LifecycleInput::Saved { publication_id });
    assert!(after_saved.iter().all(|effect| !matches!(effect, LifecycleEffect::RestoreReady { .. })), "{after_saved:?}");
    let epoch = state.restoring().expect("the restore is open").epoch;
    let started = run(&mut state, LifecycleInput::Started { owner: Peer::Session, epoch });
    assert!(started.iter().all(|effect| !matches!(effect, LifecycleEffect::RestoreReady { .. })), "{started:?}");
    let ready = run(&mut state, LifecycleInput::RestoreClassified { owner: Peer::Session, epoch });
    assert!(ready.iter().any(|effect| matches!(effect, LifecycleEffect::RestoreReady { .. })), "{ready:?}");
}
