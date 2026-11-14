//! Tests of the executors of direction A on the threaded `Runtime` and a real `Store`: every input enters through `Life`, the observable is
//! the published value, the answer of the waiter and the rows of the database. Each wait has a limit of 5 s.

use std::sync::Arc;

use serde_json::json;

use super::{Life, LifeReply as Reply, Ports};
use crate::schema::{FieldSpec, ModelSpec};
use crate::value::ScalarKind;
use crate::store::Store;
use crate::owners::calls::CallError;
use crate::runtime::Waiter;
use crate::transport::{MockTransport, Reply as MockReply};

const DIGEST: &str = "00000000000000010000000000000000000000000000000000000000000000ff";
const IDENTITY: u64 = 1;

fn ports(db: std::sync::Arc<Store>, transport: Arc<MockTransport>) -> Ports {
    Ports {
        db,
        transport: Some(transport),
        executor: tokio::runtime::Handle::current(),
        operations: Arc::new(|name| (name == "Op").then(|| ("query Op { feed }".to_string(), "feed".to_string()))),
        secret: None,
        exchange: None,
        seal: None,
        transfer: None,
        cable: None,
        read_operations: None,
    }
}

fn moments_spec() -> ModelSpec { ModelSpec::new("moments", vec![FieldSpec::scalar("id", ScalarKind::Id)]) }

pub(in super::super) fn open(path: &std::path::Path) -> Arc<Store> { Store::open_models(Some(path), vec![moments_spec()], std::sync::Arc::new(crate::runtime::SystemClock)).map(std::sync::Arc::new).unwrap() }

fn session_row(db: &Store) {
    let inner = db.lock();
    crate::store::session_state::start(&inner.connection, DIGEST).unwrap();
    crate::store::session_state::commit(&inner.connection, 1).unwrap();
}

fn stored_digest(db: &Store) -> Option<String> {
    let inner = db.lock();
    crate::store::session_state::session(&inner.connection).unwrap().map(|session| session.credential_digest)
}

async fn answer(waiter: Waiter<Reply>) -> Reply {
    match crate::test_wait::within(waiter.wait()).await {
        crate::runtime::Outcome::Value(reply) => reply,
        other => panic!("the waiter ended without a value: {other:?}"),
    }
}

pub(in super::super) async fn start(db: &Arc<Store>, transport: &Arc<MockTransport>) -> Life {
    let (db, transport) = (db.clone(), transport.clone());
    tokio::task::spawn_blocking(move || Life::start(ports(db, transport)).expect("the first publish comes")).await.unwrap()
}

pub(in super::super) async fn closed(life: &Life) {
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
    assert_eq!(reopened.saved().unwrap(), (1, 0, None), "Published was saved by the Store executor");
}

/// P-LIFE-05: after `close` and `shutdown` no owner thread of the database runs.
#[tokio::test(flavor = "multi_thread")]
async fn close_stops_every_owner_thread_of_the_database() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("db.sqlite");
    let transport = Arc::new(MockTransport::new());
    let db = open(&path);
    let life = Arc::new(start(&db, &transport).await);
    assert_eq!(life.owners_alive(), 8, "lifecycle, store, network, calls, outbox, windows, deliveries and feed run");
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

/// call_run, call_run_offline, call_run_stale: the answer of the transport, offline refusal and the epoch rule.
#[tokio::test(flavor = "multi_thread")]
async fn call_run_answers_by_epoch_and_network() {
    let db = Store::open_models(None, vec![moments_spec()], std::sync::Arc::new(crate::runtime::SystemClock)).map(std::sync::Arc::new).unwrap();
    let transport = Arc::new(MockTransport::new());
    let life = start(&db, &transport).await;
    life.register_operations(vec![("Op".into(), crate::owners::calls::CallKind::Query)], Vec::new(), false);
    assert_eq!(answer(life.set_online(false, crate::ffi::next_stamp())).await, Reply::Online { closed: false });
    assert_eq!(answer(life.run(1, "Op".into(), "{}".into())).await, Reply::Call(Err(CallError::Offline)), "a database without a session client hands the call to Calls in every state");
    life.credential_committed(IDENTITY);
    crate::test_wait::settled(&life, "the identity was published", || life.published().identity.is_some()).await;
    let published = life.published();
    assert_eq!(published.epoch, 2);
    assert_eq!(answer(life.run(2, "Op".into(), "{}".into())).await, Reply::Call(Err(CallError::Offline)));
    assert!(matches!(crate::test_wait::within(life.run(1, "Op".into(), "{}".into()).wait()).await, crate::runtime::Outcome::EpochEnded(_)), "call_run_stale: the wait of an ended epoch ends with the end of the epoch");
    assert_eq!(answer(life.set_online(true, crate::ffi::next_stamp())).await, Reply::Online { closed: false });
    transport.enqueue("Op", MockReply::data(json!({ "feed": [1, 2] })));
    assert_eq!(answer(life.run(2, "Op".into(), "{}".into())).await, Reply::Call(Ok("[1,2]".into())));
    assert_eq!(answer(life.run(2, "Missing".into(), "{}".into())).await, Reply::Call(Err(CallError::Invalid)), "call_run_unknown");
    closed(&life).await;
    assert_eq!(answer(life.run(2, "Op".into(), "{}".into())).await, Reply::Closed, "life_call_closed");
}

#[tokio::test(flavor = "multi_thread")]
async fn call_awaiting_snapshot_keeps_document() {
    use crate::owners::calls::{CallKind, Calls, CallsInput, CallsState};
    use crate::runtime::{DetScheduler, EpochStamp};
    const ORIGINAL: &str = "query Op { original }";
    for replacement in [Some(("query Op { changed }".to_string(), "changed".to_string())), None] {
        let db = Store::open_models(None, vec![moments_spec()], std::sync::Arc::new(crate::runtime::SystemClock)).map(Arc::new).unwrap();
        let transport = Arc::new(MockTransport::new());
        let documents = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
        let seen = documents.clone();
        let received = Arc::new(tokio::sync::Notify::new());
        let signal = received.clone();
        transport.respond_with(move |request| {
            seen.lock().unwrap().push(request.document.clone());
            signal.notify_one();
            Some(MockReply::data(json!({"original": true, "changed": true})))
        });
        let lookup = Arc::new(std::sync::RwLock::new(Some((ORIGINAL.to_string(), "original".to_string()))));
        let source = lookup.clone();
        let mut setup = ports(db.clone(), transport.clone());
        setup.operations = Arc::new(move |_| source.read().unwrap().clone());
        let mut runner_ports = ports(db, transport.clone());
        runner_ports.operations = setup.operations.clone();
        let life = tokio::task::spawn_blocking(move || Life::start(setup).unwrap()).await.unwrap();
        let runner = super::calls_runner::CallsRunner::new(life.wiring.clone(), &runner_ports);
        let mut det = DetScheduler::new(9);
        let calls = det.add::<Calls>("calls", CallsState::new(1, None, &[("Op", CallKind::Query)]), runner);
        det.start_epoch(1);
        det.run_until_quiet(1000).unwrap();
        det.post(calls, EpochStamp::Control, CallsInput::Declare(vec![("Op".into(), crate::owners::calls::CallDefinition { kind: CallKind::Query, document: ORIGINAL.into(), root: "original".into() })]));
        det.run_until_quiet(1000).unwrap();
        det.post(calls, EpochStamp::Epoch(1), CallsInput::Run { epoch: 1, operation: "Op".into(), variables: "{}".into(), reply: 1 });
        det.run_until_quiet(1000).unwrap();
        *lookup.write().unwrap() = replacement;
        det.post(calls, EpochStamp::Epoch(1), CallsInput::Snapshot(crate::owners::contract::Online(true)));
        det.run_until_quiet(1000).unwrap();
        crate::test_wait::signalled(&received, "the request document arrived", || !documents.lock().unwrap().is_empty()).await;
        assert_eq!(*documents.lock().unwrap(), vec![ORIGINAL.to_string()]);
        closed(&life).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn call_mutate_plain_goes_through_epoch_gate() {
    use crate::transport::{GraphQLRequest, PutReply, PutRequest, Response, TransportError, Upstream};
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct HeldMutation { entered: tokio::sync::Notify, release: tokio::sync::Notify, calls: AtomicUsize }
    #[async_trait::async_trait]
    impl Upstream for HeldMutation {
        async fn query(&self, _: GraphQLRequest) -> Result<Response, TransportError> { unreachable!() }
        async fn mutate(&self, _: GraphQLRequest) -> Result<Response, TransportError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.entered.notify_one();
            crate::test_wait::within(self.release.notified()).await;
            Ok(Response::data(json!({"change": true})))
        }
        async fn put(&self, _: PutRequest) -> Result<PutReply, TransportError> { unreachable!() }
    }
    let db = Store::open_models(None, vec![moments_spec()], std::sync::Arc::new(crate::runtime::SystemClock)).map(Arc::new).unwrap();
    let mock = Arc::new(MockTransport::new());
    let held = Arc::new(HeldMutation { entered: tokio::sync::Notify::new(), release: tokio::sync::Notify::new(), calls: AtomicUsize::new(0) });
    let mut setup = ports(db, mock);
    setup.transport = Some(held.clone());
    setup.operations = Arc::new(|name| (name == "Change").then(|| ("mutation Change { change }".into(), "change".into())));
    let life = tokio::task::spawn_blocking(move || Life::start(setup).unwrap()).await.unwrap();
    life.register_operations(vec![("Change".into(), crate::owners::calls::CallKind::Mutate)], Vec::new(), false);
    life.credential_committed(IDENTITY);
    crate::test_wait::settled(&life, "the identity was published", || life.published().identity == Some(IDENTITY)).await;
    assert_eq!(answer(life.set_online(true, crate::ffi::next_stamp())).await, Reply::Online { closed: false });
    let epoch = life.published().epoch;
    let first = life.run(epoch, "Change".into(), "{}".into());
    crate::test_wait::signalled(&held.entered, "mutation entered the transport", || held.calls.load(Ordering::SeqCst) == 1).await;
    life.credential_committed(2);
    crate::test_wait::settled(&life, "the next epoch was published", || life.published().identity == Some(2)).await;
    assert!(matches!(crate::test_wait::within(first.wait()).await, crate::runtime::Outcome::EpochEnded(_)));
    let waiting = life.waiting_calls();
    held.release.notify_one();
    life.wiring.to_calls_at(crate::runtime::EpochStamp::Epoch(epoch), crate::owners::calls::CallsInput::Response { call_id: 1, body: Some("true".into()), data: None, errors: Vec::new(), kind: crate::transport::AnswerKind::Answered { sequence: None } });
    crate::test_wait::settled(&life, "the late response was discarded", || life.waiting_calls() == waiting).await;
    let old = life.run(epoch, "Change".into(), "{}".into());
    assert!(matches!(crate::test_wait::within(old.wait()).await, crate::runtime::Outcome::EpochEnded(_) | crate::runtime::Outcome::Value(Reply::Call(Err(CallError::NoSession)))));
    crate::test_wait::settled(&life, "old mutation must not reach transport", || held.calls.load(Ordering::SeqCst) == 1).await;
    closed(&life).await;
}

/// life_session_reset, life_prop_session_kept: `session_reset` erases the credential rows and ends the epoch, `IdentityChanged` keeps them.
#[tokio::test(flavor = "multi_thread")]
async fn life_session_reset_erases_the_credential_and_identity_change_keeps_it() {
    let transport = Arc::new(MockTransport::new());
    let db = Store::open_models(None, vec![moments_spec()], std::sync::Arc::new(crate::runtime::SystemClock)).map(std::sync::Arc::new).unwrap();
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

    let other = Store::open_models(None, vec![moments_spec()], std::sync::Arc::new(crate::runtime::SystemClock)).map(std::sync::Arc::new).unwrap();
    session_row(&other);
    let life = start(&other, &transport).await;
    life.credential_committed(2);
    crate::test_wait::settled(&life, "the third epoch was published", || life.published().epoch == 3).await;
    let published = life.published();
    assert_eq!((published.identity, published.generation), (Some(2), 1));
    assert_eq!(stored_digest(&other).as_deref(), Some(DIGEST), "IdentityChanged keeps encrypted_session");
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
    let db = Store::open_models(Some(&path), vec![paged_spec()], std::sync::Arc::new(crate::runtime::SystemClock)).map(std::sync::Arc::new).unwrap();
    session_row(&db);
    transport.enqueue("Mine", MockReply::data(json!({"mine": {"edges": [{"node": {"id": "m1", "userId": "u1", "text": "one"}}, {"node": {"id": "m2", "userId": "u1", "text": "two"}}], "pageInfo": {"hasNextPage": false, "endCursor": null}}})));
    let mut setup = ports(db.clone(), transport.clone());
    let operations = crate::owners::read::ReadOperations::default();
    operations.declare([crate::owners::read::ReadOperation { name: "Mine".into(), document: "query Mine { mine { edges { node { id userId text } } pageInfo { hasNextPage endCursor } } }".into(), root_path: "mine".into(), page_size: 20 }]);
    setup.read_operations = Some(operations);
    let life = tokio::task::spawn_blocking(move || Life::start(setup).expect("the first publish comes")).await.unwrap();
    assert_eq!(answer(life.set_online(true, crate::ffi::next_stamp())).await, Reply::Online { closed: false });
    let key = crate::owners::read::windows::WindowKey::relation("moments", "mine", json!({"userId": "u1"}).as_object().unwrap());
    let epoch = life.published().epoch;
    life.credential_committed(2);
    crate::test_wait::settled(&life, "the next identity was published", || life.published().identity == Some(2)).await;
    let old_key = crate::owners::read::windows::WindowKey::relation("moments", "mine", json!({"userId": "old"}).as_object().unwrap());
    let stale = reserved_load(&life, epoch, old_key, true);
    assert!(matches!(crate::test_wait::within(stale.wait()).await, crate::runtime::Outcome::EpochEnded(_)));
    let waiter = reserved_load(&life, life.published().epoch, key, false);
    let reply = answer(waiter).await;
    assert!(matches!(&reply, Reply::Window(outcome) if matches!(**outcome, crate::owners::read::windows::WindowOutcome::Loaded { .. })), "the load ends as loaded: {reply:?} {:?}", life.faults());
    let stored: Vec<_> = ["m1", "m2"].iter().map(|id| db.find("moments", &crate::identity::RowId::new(id).unwrap()).unwrap().map(|row| row.text("text").map(str::to_string))).collect();
    assert_eq!(stored, [Some(Some("one".to_string())), Some(Some("two".to_string()))]);
    assert_eq!(transport.calls().len(), 1, "the old epoch never starts a request in the new identity");
    closed(&life).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn net_open_power_saving() {
    use crate::owners::contract::NetworkEvent;
    use crate::owners::lifecycle::Peer;
    use crate::owners::network::NetworkInput;
    use std::sync::Mutex;
    let slot = crate::ffi::OpenDatabase::new();
    let config = crate::ffi::Config { path: None };
    let options = crate::ffi::OpenOptions::default();
    let transport = Arc::new(crate::ffi::tests::database_tests::Plain);
    struct Saving;
    impl crate::ffi::PlatformObserver for Saving {
        fn start(&self, sink: Arc<crate::ffi::OnlineSink>) { sink.set_power_saving(true); }
        fn stop(&self) {}
    }
    let database = slot.open(config, crate::ffi::EMPTY_SPECS.into(), options, crate::ffi::OpenPorts { transport: Some(transport), endpoint: Some(crate::ffi::FAKE_ENDPOINT.into()), ..Default::default() }, Some(Arc::new(Saving))).await.unwrap();
    let life = database.life_handle();
    let heard = Arc::new(Mutex::new(Vec::new()));
    let sink = heard.clone();
    life.on_network(Peer::Windows, move |event| sink.lock().unwrap().push(event));
    let epoch = life.published().epoch;
    life.wiring.to_network(NetworkInput::SnapshotRequest { owner: Peer::Windows, epoch });
    database.set_online(true).await.unwrap();
    crate::test_wait::settled(&database, "the snapshot contains power saving", || heard.lock().unwrap().contains(&NetworkEvent::PowerSaving { saving: true, epoch: Some(epoch) })).await;
    database.close_database().await.unwrap();
}

/// net_prop_connectivity_not_database: after the open only `SetOnline` changes the `online` of the owners.
#[tokio::test(flavor = "multi_thread")]
async fn net_prop_connectivity_not_database() {
    let db = Store::open_models(None, vec![moments_spec()], std::sync::Arc::new(crate::runtime::SystemClock)).map(std::sync::Arc::new).unwrap();
    let transport = Arc::new(MockTransport::new());
    let life = start(&db, &transport).await;
    life.register_operations(vec![("Op".into(), crate::owners::calls::CallKind::Query)], Vec::new(), false);
    assert_eq!(answer(life.set_online(false, crate::ffi::next_stamp())).await, Reply::Online { closed: false });
    assert_eq!(answer(life.run(1, "Op".into(), "{}".into())).await, Reply::Call(Err(CallError::Offline)));
    assert_eq!(answer(life.set_online(true, crate::ffi::next_stamp())).await, Reply::Online { closed: false });
    transport.enqueue("Op", MockReply::data(json!({ "feed": [1] })));
    assert_eq!(answer(life.run(1, "Op".into(), "{}".into())).await, Reply::Call(Ok("[1]".into())));
    assert_eq!(transport.calls().len(), 1);
    closed(&life).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn net_prop_no_request_before_snapshot() {
    let db = Store::open_models(None, vec![moments_spec()], std::sync::Arc::new(crate::runtime::SystemClock)).map(std::sync::Arc::new).unwrap();
    let transport = Arc::new(MockTransport::new());
    let life = start(&db, &transport).await;
    life.register_operations(vec![("Op".into(), crate::owners::calls::CallKind::Query)], Vec::new(), false);
    let waiting = life.run(1, "Op".into(), "{}".into());
    assert_eq!(answer(life.set_online(false, crate::ffi::next_stamp())).await, Reply::Online { closed: false });
    assert_eq!(answer(waiting).await, Reply::Call(Err(CallError::Offline)), "the call waited for the first value and the value was false");
    assert!(transport.calls().is_empty(), "no request left before the network said it is online");
    assert_eq!(answer(life.set_online(true, crate::ffi::next_stamp())).await, Reply::Online { closed: false });
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
    let db = Store::open_models(None, vec![moments_spec()], std::sync::Arc::new(crate::runtime::SystemClock)).map(std::sync::Arc::new).unwrap();
    let transport = Arc::new(MockTransport::new());
    let life = start(&db, &transport).await;
    life.barrier();
    assert_eq!(answer(life.set_online(false, crate::ffi::next_stamp())).await, Reply::Online { closed: false });
    let heard: Arc<Mutex<Vec<(Peer, NetworkEvent)>>> = Arc::default();
    for owner in [Peer::Rounds, Peer::Session, Peer::Transfer, Peer::Windows, Peer::Calls] {
        let heard = heard.clone();
        life.on_network(owner, move |event| heard.lock().unwrap().push((owner, event)));
    }
    assert_eq!(answer(life.background()).await, Reply::Online { closed: false });
    assert_eq!(answer(life.foreground()).await, Reply::Online { closed: false });
    assert_eq!(answer(life.set_power_saving(true)).await, Reply::Online { closed: false });
    assert_eq!(answer(life.set_power_saving(true)).await, Reply::Online { closed: false });
    assert_eq!(answer(life.background()).await, Reply::Online { closed: false });
    assert_eq!(answer(life.foreground()).await, Reply::Online { closed: false });
    assert_eq!(
        *heard.lock().unwrap(),
        vec![
            (Peer::Rounds, NetworkEvent::Foreground(1)),
            (Peer::Session, NetworkEvent::Foreground(1)),
            (Peer::Windows, NetworkEvent::Foreground(1)),
            (Peer::Transfer, NetworkEvent::PowerSaving { saving: true, epoch: None }),
            (Peer::Windows, NetworkEvent::PowerSaving { saving: true, epoch: None }),
            (Peer::Rounds, NetworkEvent::Foreground(2)),
            (Peer::Session, NetworkEvent::Foreground(2)),
            (Peer::Windows, NetworkEvent::Foreground(2)),
        ],
        "each change goes out once, to the owners the table names"
    );
    heard.lock().unwrap().clear();
    life.credential_committed(IDENTITY);
    crate::test_wait::settled(&life, "the second epoch was published", || life.published().epoch == 2).await;
    let epoch = life.published().epoch;
    crate::test_wait::settled(&life, "the snapshot of the new epoch reached Calls", || heard.lock().unwrap().iter().any(|(owner, event)| *owner == Peer::Calls && matches!(event, NetworkEvent::Online { epoch: Some(_), .. }))).await;
    let heard = heard.lock().unwrap().clone();
    assert!(heard.contains(&(Peer::Calls, NetworkEvent::Online { online: false, epoch: Some(epoch) })));
    closed(&life).await;
}

/// net_prop_snapshot_after_create: a snapshot addressed to an epoch that is already past reaches no receiver.
#[tokio::test(flavor = "multi_thread")]
async fn a_snapshot_of_a_past_epoch_reaches_no_receiver() {
    use crate::owners::contract::NetworkEvent;
    use crate::owners::lifecycle::Peer;
    use crate::owners::network::NetworkInput;
    use std::sync::Mutex;
    let db = Store::open_models(None, vec![moments_spec()], std::sync::Arc::new(crate::runtime::SystemClock)).map(std::sync::Arc::new).unwrap();
    let transport = Arc::new(MockTransport::new());
    let life = start(&db, &transport).await;
    let heard: Arc<Mutex<Vec<NetworkEvent>>> = Arc::default();
    let sink = heard.clone();
    life.on_network(Peer::Transfer, move |event| sink.lock().unwrap().push(event));
    let epoch = life.published().epoch;
    life.wiring.to_network(NetworkInput::SnapshotRequest { owner: Peer::Transfer, epoch: epoch - 1 });
    life.wiring.to_network(NetworkInput::SnapshotRequest { owner: Peer::Transfer, epoch });
    assert_eq!(answer(life.set_online(true, crate::ffi::next_stamp())).await, Reply::Online { closed: false });
    crate::test_wait::settled(&life, "the snapshot of the current epoch arrived", || heard.lock().unwrap().iter().any(|event| matches!(event, NetworkEvent::Online { epoch: Some(heard), .. } if *heard == epoch))).await;
    assert!(!heard.lock().unwrap().iter().any(|event| matches!(event, NetworkEvent::Online { epoch: Some(heard), .. } if *heard != epoch)), "the snapshot of the past epoch was dropped");
    closed(&life).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn the_rows_of_a_batch_land_through_the_common_path() {
    use crate::owners::contract::BatchAuthor;
    use crate::owners::store::{StoreBatch, StoreInput, StoreReply};
    let db = Store::open_models(None, vec![moments_spec()], std::sync::Arc::new(crate::runtime::SystemClock)).map(std::sync::Arc::new).unwrap();
    let transport = Arc::new(MockTransport::new());
    let life = start(&db, &transport).await;
    let (sender, receiver) = std::sync::mpsc::channel();
    life.on_store_reply(BatchAuthor::Window, move |batch, reply| { let _ = sender.send((batch, reply)); });
    let epoch = life.published().epoch;
    let batch = |id, row: &str| StoreBatch { author: BatchAuthor::Window, id, stamp: crate::runtime::EpochStamp::Epoch(epoch), rows: vec![("moments".into(), row.into())], intents: 0, credential: None, ops: Vec::new() };
    life.wiring.to_store(StoreInput::Commit(batch(1, r#"{"id":"m1"}"#)));
    let (id, reply) = crate::test_wait::recv_within(&receiver);
    assert_eq!(id, 1);
    assert!(matches!(reply, StoreReply::Committed { .. }), "{reply:?}");
    assert_eq!(db.row_count("moments").unwrap(), 1);
    life.wiring.to_store(StoreInput::Commit(batch(2, r#"{"name":"no id"}"#)));
    let (id, reply) = crate::test_wait::recv_within(&receiver);
    assert_eq!(id, 2);
    assert!(matches!(reply, StoreReply::Rejected(_)), "a row without an id is refused: {reply:?}");
    assert_eq!(db.row_count("moments").unwrap(), 1);
    closed(&life).await;
}

/// store_commit_ok: every subscriber hears one `Committed` with the revision and the models of the batch.
#[tokio::test(flavor = "multi_thread")]
async fn a_committed_batch_is_broadcast_to_every_subscriber() {
    use crate::owners::contract::{BatchAuthor, Models};
    use crate::owners::store::{StoreBatch, StoreInput, StoreReply};
    let db = Store::open_models(None, vec![moments_spec()], std::sync::Arc::new(crate::runtime::SystemClock)).map(std::sync::Arc::new).unwrap();
    let transport = Arc::new(MockTransport::new());
    let life = start(&db, &transport).await;
    let (replies, reply) = std::sync::mpsc::channel();
    life.on_store_reply(BatchAuthor::Window, move |_, reply| { let _ = replies.send(reply); });
    let (first, heard_first) = std::sync::mpsc::channel();
    let (second, heard_second) = std::sync::mpsc::channel();
    life.on_committed(move |broadcast| { let _ = first.send(broadcast); });
    life.on_committed(move |broadcast| { let _ = second.send(broadcast); });
    let epoch = life.published().epoch;
    let batch = StoreBatch { author: BatchAuthor::Window, id: 1, stamp: crate::runtime::EpochStamp::Epoch(epoch), rows: vec![("moments".into(), r#"{"id":"m1"}"#.into())], intents: 0, credential: None, ops: Vec::new() };
    life.wiring.to_store(StoreInput::Commit(batch));
    let StoreReply::Committed { revision, .. } = crate::test_wait::recv_within(&reply) else { panic!("the batch was refused") };
    for heard in [heard_first, heard_second] {
        let broadcast = crate::test_wait::recv_within(&heard);
        assert_eq!((broadcast.revision, broadcast.models), (revision, Models::Set(["moments".to_string()].into())));
    }
    closed(&life).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unreadable_queue_is_reported_for_outbox_and_open_still_ends() {
    use crate::owners::lifecycle::Peer;
    let transport = Arc::new(MockTransport::new());
    let db = Store::open_models(None, vec![moments_spec()], std::sync::Arc::new(crate::runtime::SystemClock)).map(std::sync::Arc::new).unwrap();
    session_row(&db);
    {
        let inner = db.lock();
        inner.connection.execute_batch("INSERT INTO op (action_key, status, created_at, since) VALUES ('moments:send', 'pending', 1, 1)").unwrap();
    }
    let life = start(&db, &transport).await;
    crate::test_wait::settled(&life, "the failure of the queue was reported", || !life.restore_failures().is_empty()).await;
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
    let mut outbox = OutboxState::new(1, OutboxPeers { store: Address::new(0), session: Some(Address::new(1)), transfer: None });
    let classified = Outbox::step(&mut outbox, OutboxInput::RestoreFailed { error: "disk".into() }, crate::runtime::Time(0));
    assert!(matches!(classified.as_slice(), [Effect::Run(OutboxEffect::RestoreClassified { epoch: 1 })]), "{classified:?}");
    let effects = Outbox::step(&mut outbox, OutboxInput::Retry { model: "moments".into(), row_id: "m1".into(), action: None, reply: 5 }, crate::runtime::Time(0));
    assert!(matches!(effects.as_slice(), [Effect::Run(OutboxEffect::Reply { reply: 5, answer: OutboxAnswer::Err(Fault::Rejected(RejectReason::Storage(error))) })] if error == "disk"), "{effects:?}");
    let mut transfer = TransferState::new(1, 1, TransferPeers { store: Address::new(0), outbox: Address::new(2), rounds: None });
    let classified = Transfers::step(&mut transfer, TransferInput::RestoreFailed { error: "disk".into() }, crate::runtime::Time(0));
    assert!(matches!(classified.as_slice(), [Effect::Run(TransferEffect::RestoreClassified { epoch: 1 })]), "{classified:?}");
    let effects = Transfers::step(&mut transfer, TransferInput::Cancel { key: "k".into(), reply: 6 }, crate::runtime::Time(0));
    assert!(matches!(effects.as_slice(), [Effect::Run(TransferEffect::Reply { reply: 6, answer: TransferAnswer::Err(Fault::Rejected(RejectReason::Storage(error))) })] if error == "disk"), "{effects:?}");
}

/// out_local_write: a database without a session writes a local row through the owner LocalWrites and the row is in the base when the owner answers.
#[tokio::test(flavor = "multi_thread")]
async fn a_database_without_a_session_writes_a_local_row_through_local_writes() {
    use crate::owners::store::{EffectRow, IntentEffects};
    use crate::owners::write::outbox::{LocalWriteKind, OutboxAnswer};
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("db.sqlite");
    let transport = Arc::new(MockTransport::new());
    let db = open(&path);
    let life = start(&db, &transport).await;
    let epoch = life.published().epoch;
    let effects = IntentEffects { rows: vec![EffectRow { model: "moments".into(), row_id: "m1".into(), values: json!({"id": "m1"}).to_string() }], ..Default::default() };
    let reply = answer(life.local_write(epoch, "moments".into(), LocalWriteKind::Model(effects), "m1".into(), String::new())).await;
    assert!(matches!(reply, Reply::Outbox(OutboxAnswer::Written { shown: None, .. })), "{reply:?}");
    assert!(db.find("moments", &crate::identity::RowId::new("m1").unwrap()).unwrap().is_some());
    closed(&life).await;
}

/// lifecycle.md `StartObserving`: `open` starts the platform observer once, and the current value it reports first reaches Network.
#[tokio::test(flavor = "multi_thread")]
async fn life_open_starts_observers() {
    use crate::owners::contract::NetworkEvent;
    use crate::owners::lifecycle::Peer;
    use crate::owners::network::NetworkInput;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;
    struct Counting(Arc<AtomicUsize>);
    impl crate::ffi::PlatformObserver for Counting {
        fn start(&self, sink: Arc<crate::ffi::OnlineSink>) { self.0.fetch_add(1, Ordering::SeqCst); sink.set_online(true); }
        fn stop(&self) {}
    }
    let started = Arc::new(AtomicUsize::new(0));
    let slot = crate::ffi::OpenDatabase::new();
    let config = crate::ffi::Config { path: None };
    let transport = Arc::new(crate::ffi::tests::database_tests::Plain);
    let ports = crate::ffi::OpenPorts { transport: Some(transport), endpoint: Some(crate::ffi::FAKE_ENDPOINT.into()), ..Default::default() };
    let database = slot.open(config, crate::ffi::EMPTY_SPECS.into(), crate::ffi::OpenOptions::default(), ports, Some(Arc::new(Counting(started.clone())))).await.unwrap();
    assert_eq!(started.load(Ordering::SeqCst), 1, "the observer started once before open returned");
    let life = database.life_handle();
    let heard = Arc::new(Mutex::new(Vec::new()));
    let sink = heard.clone();
    life.on_network(Peer::Windows, move |event| sink.lock().unwrap().push(event));
    let epoch = life.published().epoch;
    life.wiring.to_network(NetworkInput::SnapshotRequest { owner: Peer::Windows, epoch });
    crate::test_wait::settled(&database, "the snapshot holds the value the observer reported", || heard.lock().unwrap().contains(&NetworkEvent::Online { online: true, epoch: Some(epoch) })).await;
    assert_eq!(started.load(Ordering::SeqCst), 1);
    database.close_database().await.unwrap();
}

fn reserved_load(life: &Life, epoch: u64, key: crate::owners::read::windows::WindowKey, force: bool) -> Waiter<Reply> {
    let (waiter, id) = life.reserve_load(epoch);
    life.load(epoch, id, key, (crate::owners::read::windows::Goal::First, force), 0, None);
    waiter
}
