use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use serde_json::{json, Map, Value};
use crate::db::Db;
use crate::pacing::{Clock, TestClock};
use crate::schema::{Coverage, FieldSpec, MediaSpec, ModelSpec, PageDirection, RelationSpec, RemoteShape, VariableSource};
use crate::transport::{GraphQLRequest, MockTransport, Reply};
use crate::value::ScalarKind;
use super::load::LoadOptions;
use super::rig::{member_ids, params, remote, rig_without_operations, Rig, START_MS};
use super::{locked, OperationDef, Sync, SyncConfig};

fn specs() -> Vec<ModelSpec> {
    let mut model = ModelSpec::new("media", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("text", ScalarKind::Str).optional()]);
    let mut scope = RelationSpec::global("library");
    scope.text_params = vec!["chatId".into()];
    let mut source = remote("Full", RemoteShape::Connection { direction: PageDirection::Forward, coverage: Coverage::Page, max_pages: None, cursor: None }, "5.minutes");
    source.root = Some("node.messages".into());
    source.page_size = Some(2);
    scope.remote = Some(source);
    let bindings = BTreeMap::from([("chatId".into(), VariableSource::Param { name: "chatId".into() })]);
    scope.media = Some(MediaSpec { marker: "Marker".into(), from: "node.mediaLibrary".into(), fields: ["revision", "visualCount", "audioCount", "giftCount"].map(str::to_string).to_vec(), sequence: "latestSequence".into(), tail: "Tail".into(), after: "afterSequence".into(), marker_variables: bindings.clone(), tail_variables: bindings });
    model.relations.push(scope);
    vec![model]
}

#[derive(Default)]
struct Server { size: usize, revision: i64, omit_first: bool, fail: bool, slow: bool, change: bool, bad_cursor: bool, repeat_cursor: bool }

impl Server {
    fn reply(&mut self, request: &GraphQLRequest) -> Reply {
        let variables = &request.variables;
        if request.operation_name == "Marker" {
            let prefix = variables["upToSequence"].as_u64().unwrap() as usize;
            return Reply::data(json!({"node":{"mediaLibrary":{"revision":self.revision.to_string(),"visualCount":self.size.min(prefix) - usize::from(self.omit_first && prefix > 0 && self.size > 0),"audioCount":0,"giftCount":0,"latestSequence":self.size}}}));
        }
        let start = variables["afterSequence"].as_u64().unwrap_or(0) as usize;
        let offset = variables["after"].as_str().and_then(|cursor| cursor.parse::<usize>().ok()).unwrap_or(0);
        if self.fail && offset > 0 { return Reply::graphql_messages(&["second page refused"]); }
        let ids: Vec<_> = ((start + 1)..=self.size).filter(|id| !self.omit_first || *id != 1).collect();
        let nodes: Vec<_> = ids.iter().skip(offset).take(2).map(|id| json!({"id":id.to_string(),"text":"server"})).collect();
        let end = offset + nodes.len();
        let cursor = if self.bad_cursor { Value::Null } else if self.repeat_cursor { json!("0") } else { json!(end.to_string()) };
        let reply = Reply::data(json!({"node":{"messages":{"nodes":nodes,"pageInfo":{"hasNextPage":end < ids.len(),"endCursor":cursor}}}}));
        if self.change { self.revision += 1; }
        if self.slow { reply.after(Duration::from_secs(60)) } else { reply }
    }
}

fn setup(db: Db, server: Arc<Mutex<Server>>) -> Rig {
    let rig = rig_without_operations(db, MockTransport::new(), SyncConfig::default());
    register(&rig.sync);
    rig.transport.respond_with(move |request| Some(locked(&server).reply(request)));
    rig
}

fn register(sync: &Sync) {
    for name in ["Full", "Tail", "Marker"] { sync.register(OperationDef::new(name, "query { node { id } }", "node").paged(2)); }
}

fn arguments(chat: &str) -> Map<String, Value> { params(json!({"chatId":chat})) }
async fn load(rig: &Rig, force: bool) -> Result<(), super::SyncError> { rig.sync.load_all(&rig.sync.inner.db.admit().unwrap(), "media", "library", &arguments("c1"), LoadOptions { force }).await }
fn ids(rig: &Rig) -> Vec<String> { member_ids(rig, "media", "library", &arguments("c1")) }
fn marker(rig: &Rig) -> crate::commit::WindowMarker {
    let key = rig.sync.window_key("media", "library", &arguments("c1")).unwrap();
    rig.db.shared.store.marker(&key, rig.db.generation().value() as i64).unwrap().unwrap()
}

#[test]
fn media_marker_reopen() { crate::test_wait::paused(async {
    let directory = tempfile::tempdir().unwrap();
    let file = directory.path().join("media.sqlite");
    let server = Arc::new(Mutex::new(Server { size: 2, revision: 1, ..Server::default() }));
    {
        let rig = setup(Db::open(&file, specs(), None).unwrap(), server.clone());
        load(&rig, false).await.unwrap();
        assert_eq!(marker(&rig).sequence, 2);
        assert_eq!(ids(&rig), ["1", "2"]);
    }
    let rig = setup(Db::open(&file, specs(), None).unwrap(), server.clone());
    load(&rig, false).await.unwrap();
    assert!(rig.transport.calls().is_empty(), "fresh disk window makes no request");
    load(&rig, true).await.unwrap();
    assert_eq!(rig.transport.calls().iter().map(|call| call.operation_name.as_str()).collect::<Vec<_>>(), ["Marker"]);
    assert_eq!(rig.transport.calls()[0].variables, json!({"chatId":"c1","upToSequence":2}));
    locked(&server).size = 7;
    load(&rig, true).await.unwrap();
    let tails: Vec<_> = rig.transport.calls().into_iter().filter(|call| call.operation_name == "Tail").collect();
    assert_eq!(tails.len(), 3, "tail finishes every cursor page");
    assert!(tails.iter().all(|call| call.variables["afterSequence"] == 2));
    assert_eq!(tails[1].variables["after"], "2");
    assert_eq!(ids(&rig), ["1", "2", "3", "4", "5", "6", "7"]);
    assert_eq!(marker(&rig).sequence, 7);
    locked(&server).omit_first = true;
    let full_before = rig.transport.call_count("Full");
    load(&rig, true).await.unwrap();
    assert_eq!(rig.transport.call_count("Full") - full_before, 3);
    assert_eq!(ids(&rig), ["2", "3", "4", "5", "6", "7"]);
    locked(&server).revision += 1;
    let full_before = rig.transport.call_count("Full");
    load(&rig, true).await.unwrap();
    assert_eq!(rig.transport.call_count("Full") - full_before, 3, "equal counts with a new revision require full coverage");
    rig.db.shared.store.lock().connection.execute("DELETE FROM window_marker", []).unwrap();
    let full_before = rig.transport.call_count("Full");
    load(&rig, true).await.unwrap();
    assert_eq!(rig.transport.call_count("Full") - full_before, 3, "missing marker reloads full coverage");
    let full_before = rig.transport.call_count("Full");
    rig.sync.load_all(&rig.sync.inner.db.admit().unwrap(), "media", "library", &arguments("c2"), LoadOptions::default()).await.unwrap();
    assert_eq!(rig.transport.call_count("Full") - full_before, 3, "parameters isolate markers");
}) }

#[test]
fn media_marker_commit() { crate::test_wait::paused(async {
    for case in ["tail error", "full error", "cancel", "reset", "late row", "transaction", "changed marker", "bad cursor", "repeat cursor"] {
        let server = Arc::new(Mutex::new(Server { size: 2, revision: 1, ..Server::default() }));
        let rig = setup(Db::open_in_memory(specs(), None).unwrap(), server.clone());
        load(&rig, false).await.unwrap();
        let old = marker(&rig);
        let state = rig.sync.relation_state("media", "library", &arguments("c1")).await.unwrap();
        locked(&server).size = 7;
        match case {
            "tail error" | "full error" => {
                locked(&server).fail = true;
                if case == "full error" { locked(&server).revision += 1; }
                assert!(load(&rig, true).await.is_err(), "{case}");
                assert_eq!(ids(&rig), ["1", "2"], "{case}");
                assert_eq!(marker(&rig), old, "{case}");
                assert!(state.borrow().error.is_some(), "{case}: visible snapshot error");
            }
            "cancel" => {
                locked(&server).slow = true;
                assert!(crate::test_wait::timeout(Duration::from_secs(1), load(&rig, true)).await.is_err());
                assert_eq!(rig.transport.call_count("Tail"), 1);
                super::rig::await_state(&state, |state| state.error.is_some()).await;
                let key = rig.sync.window_key("media", "library", &arguments("c1")).unwrap();
                assert_eq!(rig.sync.inner.owner.members_readers(&key), 0);
                assert!(locked(&rig.sync.inner.flights).waiting.is_empty());
                assert_eq!(ids(&rig), ["1", "2"]);
                assert_eq!(marker(&rig), old);
            }
            "reset" => {
                locked(&server).slow = true;
                let (result, ()) = tokio::join!(load(&rig, true), async {
                    rig.transport.await_calls("Tail", 1).await;
                    rig.db.reset(&["media"]).unwrap();
                });
                assert!(result.is_err());
                assert!(ids(&rig).is_empty());
                let key = rig.sync.window_key("media", "library", &arguments("c1")).unwrap();
                assert!(rig.db.shared.store.marker(&key, rig.db.generation().value() as i64).unwrap().is_none());
            }
            "late row" => {
                locked(&server).revision += 1;
                locked(&server).slow = true;
                let (result, ()) = tokio::join!(load(&rig, true), async {
                    rig.transport.await_calls("Full", 2).await;
                    super::rig::put(&rig, "media", json!({"id":"1","text":"new event"}));
                });
                result.unwrap();
                assert_eq!(super::rig::text_of(&rig, "media", "1").as_deref(), Some("new event"));
                assert_eq!(marker(&rig).sequence, 7);
            }
            "transaction" => {
                rig.db.shared.store.lock().connection.execute_batch("CREATE TRIGGER marker_refused BEFORE INSERT ON window_marker BEGIN SELECT RAISE(ABORT, 'marker refused'); END;").unwrap();
                assert!(load(&rig, true).await.is_err());
                assert_eq!(ids(&rig), ["1", "2"]);
                assert_eq!(marker(&rig), old, "row writes roll back with marker failure");
            }
            "changed marker" => {
                locked(&server).change = true;
                load(&rig, true).await.unwrap();
                assert_eq!(marker(&rig), old, "an unstable read cannot confirm coverage");
                let key = rig.sync.window_key("media", "library", &arguments("c1")).unwrap();
                assert!(rig.db.shared.store.window_freshness(&key).unwrap().unwrap().stale_since.is_some());
                assert_eq!(rig.transport.call_count("Tail"), 3, "no immediate retry");
            }
            "bad cursor" | "repeat cursor" => {
                locked(&server).bad_cursor = case == "bad cursor";
                locked(&server).repeat_cursor = case == "repeat cursor";
                assert!(load(&rig, true).await.is_err(), "{case}");
                assert_eq!(ids(&rig), ["1", "2"], "{case}");
                assert_eq!(marker(&rig), old, "{case}");
            }
            _ => unreachable!(),
        }
    }
}) }

struct WorkClock { clock: TestClock, sleeps: AtomicUsize, wakes: AtomicUsize }
impl Clock for WorkClock {
    fn now(&self) -> Duration { self.clock.now() }
    fn sleep(&self, duration: Duration) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
        self.sleeps.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move { self.clock.sleep(duration).await; self.wakes.fetch_add(1, Ordering::SeqCst); })
    }
}

#[test]
fn media_marker_work() { crate::test_wait::paused(async {
    for size in [20, 200, 2000] {
        let server = Arc::new(Mutex::new(Server { size, revision: 1, ..Server::default() }));
        let db = Db::open_in_memory(specs(), None).unwrap();
        let transport = MockTransport::new();
        let clock = Arc::new(WorkClock { clock: crate::test_wait::clock(Duration::from_millis(START_MS)), sleeps: AtomicUsize::new(0), wakes: AtomicUsize::new(0) });
        let sync = Sync::new(db.clone(), Arc::new(transport.clone()), clock.clone(), SyncConfig::default());
        register(&sync);
        let source = server.clone();
        transport.respond_with(move |request| Some(locked(&source).reply(request)));
        let rig = Rig { sync, db, transport, clock: clock.clock.clone() };
        load(&rig, false).await.unwrap();
        locked(&server).size += 1;
        let reads = crate::identity::touches::member_reads();
        load(&rig, true).await.unwrap();
        let work = crate::identity::touches::member_reads() - reads;
        assert!(work <= 4, "{size} existing rows caused {work} member reads for one tail row");
        assert_eq!(rig.transport.call_count("Tail"), 1);
        assert_eq!(marker(&rig).sequence, (size + 1) as i64);
        let before = (rig.transport.calls().len(), clock.sleeps.load(Ordering::SeqCst), clock.wakes.load(Ordering::SeqCst));
        rig.clock.advance(Duration::from_secs(86400));
        crate::test_wait::pass(Duration::from_secs(86400)).await;
        tokio::task::yield_now().await;
        assert_eq!((rig.transport.calls().len(), clock.sleeps.load(Ordering::SeqCst), clock.wakes.load(Ordering::SeqCst)), before, "idle work for {size} rows");
    }
}) }
