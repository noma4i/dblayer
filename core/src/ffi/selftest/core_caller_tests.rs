use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::json;

use super::core_caller_stand::{Collector, Event, Hold, Report, Returned};
use super::nested_storage_tests::wrapper_models;
use super::rig_tests::{bound, Rig, World};
use super::upload_stand::{plain, request, CREATE_MOMENT};
use crate::ffi::transport::OnlineSink;
use crate::ffi::upload_wire::{WireSource, WireUploadBatch, WireUploadItem};
use crate::ffi::{NetworkKind, NetworkPath, NetworkStatus, PlatformObserver, ReadClaim, ReadFetch, ReadRequest, ReadStream};
use crate::test_wait::{in_own_process, settled, within, Flag};

const FEED: &str = "query MomentsFeed($first: Int, $after: String) { feed(first: $first, after: $after) { nodes { id uuid likedByMe media createdAt } pageInfo { hasNextPage endCursor } } }";
const NODE: &str = "query ContainerNode($id: ID!) { node(id: $id) { ... on Container { id title } } }";
const NODES: &str = "query ContainersByIds($ids: [ID!]!) { nodes(ids: $ids) { ... on Container { id title } } }";
const MOMENT: &str = r#"{"__typename":"Moment","id":"m77","uuid":"u","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}"#;

struct Observer(Arc<Flag>);

impl PlatformObserver for Observer {
    fn start(&self, sink: Arc<OnlineSink>) {
        self.0.set();
        sink.set_path(path());
    }

    fn stop(&self) {}
}

fn path() -> NetworkPath { NetworkPath { online: true, kind: NetworkKind::Wifi, network: 1 } }

type Run = Box<dyn FnOnce(&Report) -> String + Send>;

/// Requirement P-BR-09: with both readers, the writer and the owner `Network` held, the core does no work on the thread of the caller.
/// Observable: a synchronous entry returns, the first poll of an `async` entry answers pending, and every outcome arrives after the release.
/// Defect caught: the owner answer, the SQLite read, the pool wait or the writer lock on the thread of the platform (F-1, F-2, F-3, F-10, F-11).
#[test]
fn bridge_prop_core_off_caller() {
    in_own_process(module_path!(), "bridge_prop_core_off_caller", &["table"], |_| crate::test_wait::limited(Duration::from_secs(18), run()));
}

async fn run() {
    let rig = Rig::signed_as(World::new("server", "A", true), false, wrapper_models).await;
    let database = rig.database.clone();
    let defs = json!([{"name":"MomentsFeed","document":FEED},{"name":"ContainerNode","document":NODE},{"name":"ContainersByIds","document":NODES},{"name":CREATE_MOMENT.0,"document":CREATE_MOMENT.1}]).to_string();
    database.register_operations(defs.clone()).await.unwrap();
    let page = json!({"data":{"feed":{"nodes":[{"__typename":"Moment","id":"1","uuid":"1","likedByMe":false,"media":{},"createdAt":"2026-10-08T00:00:00.000Z"}],"pageInfo":{"hasNextPage":true,"endCursor":"after-1"}}}});
    rig.world.answer_with("MomentsFeed", page);
    rig.world.answer_with("ContainerNode", json!({"data":{"node":{"__typename":"Container","id":"k1","title":"x"}}}));
    rig.world.answer_with("ContainersByIds", json!({"data":{"nodes":[{"__typename":"Container","id":"k1","title":"x"}]}}));
    rig.world.answer_with("CreateMoment", json!({"data":{"createMoment":serde_json::from_str::<serde_json::Value>(MOMENT).unwrap()}}));
    let epoch = database.session_epoch();
    bound(database.refresh(epoch, "Moment".into(), Some("feed".into()), "{}".into())).await.unwrap();
    let server_gate = rig.world.hold_next("CreateMoment");
    let temp = bound(database.send(plain("Moment", r#"{"text":"first"}"#, epoch))).await.unwrap().optimistic_value.unwrap().id;
    server_gate.entered().await;
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("clip.mp4");
    std::fs::write(&source, b"video").unwrap();

    let started = Arc::new(Flag::default());
    let stream: Arc<Mutex<Option<Arc<ReadStream>>>> = Arc::default();
    let mut table: Vec<(&'static str, Returned, Run)> = Vec::new();
    let (db, id, slot) = (database.clone(), temp.clone(), stream.clone());
    table.push(("subscribe_values", Returned::Result, Box::new(move |report| {
        *slot.lock().unwrap() = Some(db.subscribe_values(epoch, ReadRequest::Entity { model: "Moment".into(), id, fetch: ReadFetch::Never }));
        report.returned(Returned::Result);
        "stream".into()
    })));
    table.push(("set_path", Returned::Result, Box::new(|report| { OnlineSink::of_status().set_path(path()); report.returned(Returned::Result); "returned".into() })));
    table.push(("set_online", Returned::Result, Box::new(|report| { OnlineSink::of_status().set_online(true); report.returned(Returned::Result); "returned".into() })));
    let seen = started.clone();
    table.push(("network_status_new", Returned::Result, Box::new(move |report| { drop(NetworkStatus::new(Arc::new(Observer(seen)))); report.returned(Returned::Result); "returned".into() })));
    let (db, id) = (database.clone(), temp.clone());
    table.push(("peek_row", Returned::Pending, Box::new(move |report| report.drive(db.peek_row(epoch, "Moment".into(), id)))));
    let db = database.clone();
    table.push(("refresh", Returned::Pending, Box::new(move |report| report.drive(db.refresh(epoch, "Moment".into(), Some("feed".into()), "{}".into())))));
    let db = database.clone();
    table.push(("load_more", Returned::Pending, Box::new(move |report| report.drive(db.load_more(epoch, "Moment".into(), Some("feed".into()), "{}".into())))));
    let db = database.clone();
    table.push(("node", Returned::Pending, Box::new(move |report| report.drive(db.node(epoch, "Container".into(), "node".into(), "k1".into(), true)))));
    let db = database.clone();
    table.push(("nodes", Returned::Pending, Box::new(move |report| report.drive(db.nodes(epoch, "Container".into(), "nodes".into(), vec!["k1".into()])))));
    let db = database.clone();
    table.push(("send", Returned::Pending, Box::new(move |report| report.drive(db.send(plain("Moment", r#"{"text":"second"}"#, epoch))))));
    let (db, file) = (database.clone(), source.clone());
    table.push(("upload", Returned::Pending, Box::new(move |report| report.drive(db.upload(epoch, "Moment".into(), "m404".into(), "upload_clip".into(), request(&file))))));
    let db = database.clone();
    table.push(("db_set_online", Returned::Pending, Box::new(move |report| report.drive(db.set_online(true)))));

    let (db, defs) = (database.clone(), defs.clone());
    table.push(("register_operations", Returned::Pending, Box::new(move |report| report.drive(db.register_operations(defs)))));
    let db = database.clone();
    table.push(("create", Returned::Pending, Box::new(move |report| report.drive(db.create(epoch, "Moment".into(), r#"{"text":"third"}"#.into(), "{}".into())))));
    let (db, file) = (database.clone(), source.clone());
    table.push(("upload_create_batch", Returned::Pending, Box::new(move |report| {
        let item = WireUploadItem { values_json: r#"{"clientKey":"k1"}"#.into(), source: WireSource::File { path: file.to_str().unwrap().into(), name: None, mime: None, prepare: None, max_bytes: None }, variables_json: "{}".into() };
        report.drive(async move { db.upload_create_batch(epoch, "Asset".into(), vec![item], WireUploadBatch { operation: "create_files".into(), variables_json: "{}".into() }).await.map(drop) })
    })));
    let db = database.clone();
    table.push(("pull", Returned::Pending, Box::new(move |report| {
        let stream = db.subscribe_values(epoch, ReadRequest::Entity { model: "Moment".into(), id: "1".into(), fetch: ReadFetch::Never });
        report.drive(async move {
            let ReadClaim::Claimed { reader } = stream.claim_reader().await else { return None };
            stream.pull(reader).await.map(drop)
        })
    })));

    let hold = Hold::take(&database).await;
    let collector = Arc::new(Collector::default());
    let expected: Vec<(&'static str, Returned)> = table.iter().map(|(name, how, _)| (*name, *how)).collect();
    let count = expected.len();
    for (name, _, run) in table {
        let report = Report { name, collector: collector.clone() };
        crate::task::spawn_thread(format!("case-{}", report.name), move || { let outcome = run(&report); report.settled(outcome); }).unwrap();
    }
    let _in_time = collector.arrived("every entry came back while the database and the owner are held", move |found| found.events().iter().filter(|event| matches!(event, Event::Returned(..))).count() == count).await;
    let mut problems = Vec::new();
    for (name, how) in &expected {
        match collector.returned(name) {
            None => problems.push(format!("{name}: did not come back while the database and the owner were held")),
            Some(found) if found != *how => problems.push(format!("{name}: came back as {found:?}, expected {how:?}")),
            Some(_) => {}
        }
    }
    let asynchronous: Vec<&'static str> = expected.iter().filter(|(_, how)| *how == Returned::Pending).map(|(name, _)| *name).collect();
    let executor = crate::ffi::database::executor().unwrap();
    crate::task::spawn_on(executor.handle(), async {}).await.unwrap();
    crate::task::spawn_blocking_on(executor.handle(), None, || ()).await.unwrap();
    let waiting = asynchronous.clone();
    let seen_when_ready: Arc<Mutex<Option<Vec<(&'static str, bool, Option<String>)>>>> = Arc::default();
    let slot = seen_when_ready.clone();
    collector.arrived("every async entry waits after every wake-up while the database and the owner are held", move |found| {
        let events = found.events();
        let seen: Vec<_> = waiting.iter().map(|name| (*name, Collector::parked_in(&events, name), Collector::settled_in(&events, name))).collect();
        let ready = seen.iter().all(|(_, parked, settled)| *parked || settled.is_some());
        if ready { *slot.lock().unwrap() = Some(seen); }
        ready
    }).await;
    let verdicts = seen_when_ready.lock().unwrap().take().unwrap_or_else(|| {
        let events = collector.events();
        asynchronous.iter().map(|name| (*name, Collector::parked_in(&events, name), Collector::settled_in(&events, name))).collect()
    });
    for (name, parked, settled) in verdicts {
        if settled.is_some() && !matches!(name, "register_operations" | "upload_create_batch") { problems.push(format!("{name}: finished while the database and the owner were held")); }
        else if settled.is_none() && !parked { problems.push(format!("{name}: a poll stood while the database and the owner were held")); }
    }

    hold.release();
    let all = collector.arrived("every entry has its outcome after the release", move |found| found.events().iter().filter(|event| matches!(event, Event::Settled(..))).count() == count).await;
    if !all { problems.push("some entries had no outcome after the release".into()); }
    for (name, how) in &expected {
        let Some(outcome) = collector.settled(name) else { continue };
        if *how == Returned::Pending && !matches!(*name, "upload_create_batch" | "pull") && !outcome.starts_with("Ok(") { problems.push(format!("{name}: outcome {outcome}")); }
    }
    settled(&rig, "the owners heard the platform inputs", || true).await;
    started.wait("the observer of the status started").await;
    let held_stream = stream.lock().unwrap().clone().expect("subscribe_values returned a stream");
    match held_stream.claim_reader().await {
        ReadClaim::Claimed { reader } => assert!(within(held_stream.pull(reader)).await.is_some(), "the stream of the temporary row gave no value"),
        ReadClaim::Ended { cause } => problems.push(format!("subscribe_values: the stream ended: {cause:?}")),
    }
    server_gate.release();
    database.close_now();
    assert!(problems.is_empty(), "{problems:#?}");
}
