use super::*;
use crate::db::fixtures::put;
use crate::test_wait::within;
use serde_json::json;
use tokio::sync::mpsc;

struct Listener(mpsc::UnboundedSender<WireEntitySnapshot>);
impl EntityListener for Listener {
    fn changed(&self, delivery: EntityDelivery) { for snapshot in delivery.snapshots { let _ = self.0.send(snapshot); } }
}
async fn next(receiver: &mut mpsc::UnboundedReceiver<WireEntitySnapshot>, phase: EntityStage) -> WireEntitySnapshot {
    within(async { loop { let snapshot = receiver.recv().await.expect("entity source remains alive"); if snapshot.phase == phase { return snapshot; } } }).await
}
fn request(id: &str, lease: u64) -> EntityRequest { EntityRequest { id: id.into(), lease, fetch: EntityFetch::None } }
fn release(id: &str, lease: u64) -> EntityLease { EntityLease { id: id.into(), lease } }

#[tokio::test]
async fn a_remote_entity_read_without_transport_returns_an_error() {
    let (sample, _) = super::super::load_tests::rig().await;
    let models = serde_json::to_string(&vec![sample.db.spec("Moment").unwrap().clone()]).unwrap();
    let db = Database::start_with(super::super::Config { path: None, data_version: None, cache_rows: 8 }, models, None, Arc::new(crate::pacing::SystemClock), |_, _| Ok((None, None))).unwrap();
    let (sender, _) = mpsc::unbounded_channel();
    let live = db.live_entities("Moment".into(), Arc::new(Listener(sender))).unwrap();
    let result = live.retain(vec![EntityRequest { id: "m1".into(), lease: 1, fetch: EntityFetch::Force }]);
    assert!(matches!(result, Err(DbError::Invalid { detail }) if detail == "entity fetch needs a network session"));
}

#[tokio::test]
async fn held_entities_reload_after_a_data_reset() {
    use crate::schema::{FieldSpec, ModelSpec, NodeRead, RelationSpec, RemoteShape, VariableSource, NODE_PARAM};
    use crate::transport::{MockTransport, Reply};
    use crate::value::ScalarKind;
    let transport = Arc::new(MockTransport::new());
    let mut model = ModelSpec::new("Moment", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("text", ScalarKind::Str).optional()]);
    model.relations.push(super::super::load_tests::relation("feed", "5.minutes"));
    let mut node = RelationSpec::new("node", &[]);
    node.text_params = vec![NODE_PARAM.to_string()];
    node.node = Some(NodeRead::One);
    let mut remote = super::super::load_tests::relation("node", "5.minutes").remote.unwrap();
    remote.operation = "Node".into();
    remote.shape = RemoteShape::Single;
    remote.variables.insert("id".into(), VariableSource::Param { name: NODE_PARAM.into() });
    node.remote = Some(remote);
    model.relations.push(node);
    let wired = transport.clone();
    let db = Database::start_with(super::super::Config { path: None, data_version: None, cache_rows: 8 }, serde_json::to_string(&vec![model]).unwrap(), None, Arc::new(crate::pacing::SystemClock), move |_, _| Ok((Some(wired), None))).unwrap();
    db.register_operations(r#"[{"name":"Feed","document":"query Feed { moments { id text } }","rootPath":"moments"},{"name":"Node","document":"query Node($id: ID!) { node(id: $id) { ... on Moment { id text } } }","rootPath":"node"}]"#.into()).unwrap();
    for operation in ["Feed", "Node"] {
        for text in ["before", "after"] {
            let row = json!({"__typename":"Moment", "id":"m1", "text":text});
            let payload = if operation == "Feed" { json!({"moments":[row]}) } else { json!({"node":row}) };
            transport.enqueue(operation, Reply::data(payload));
        }
    }
    let (sender, mut receiver) = mpsc::unbounded_channel();
    let live = db.live_entities("Moment".into(), Arc::new(Listener(sender))).unwrap();
    live.singleton(r#"{"model":"Moment","relation":"feed","params":{}}"#.into(), 1).unwrap();
    live.retain(vec![EntityRequest { id: "m1".into(), lease: 2, fetch: EntityFetch::Force }]).unwrap();
    crate::test_wait::yield_until("both first reads finish", || {
        let state = locked(&live.state);
        state.singleton.as_ref().is_some_and(|single| single.entry.verdict.settled && single.entry.value.is_some()) && state.entries.get("m1").is_some_and(|entry| entry.verdict.read == EntityRead::Loaded && entry.value.is_some())
    }).await;
    db.db.reset_all().unwrap();
    let generation = db.db.generation().value();
    let mut reset = BTreeMap::new();
    let mut loaded = BTreeMap::new();
    within(async {
        while loaded.len() != 2 {
            let snapshot = within(receiver.recv()).await.unwrap();
            if snapshot.generation != generation { continue; }
            if snapshot.phase == EntityStage::Opening && snapshot.value.is_none() { reset.insert(snapshot.lease, true); }
            if snapshot.value.is_some() && snapshot.read == EntityRead::Loaded { loaded.insert(snapshot.lease, snapshot); }
        }
    }).await;
    assert_eq!(reset.keys().copied().collect::<Vec<_>>(), [1, 2]);
    assert_eq!(loaded.keys().copied().collect::<Vec<_>>(), [1, 2]);
    assert!(loaded.values().all(|snapshot| snapshot.value.as_ref().unwrap().cells.contains(&super::super::wire::Cell::Text { value: "after".into() }) && snapshot.settled));
    assert_eq!(transport.call_count("Feed"), 2);
    assert_eq!(transport.call_count("Node"), 2);
    live.shutdown();
}

#[tokio::test]
async fn held_handles_reload_after_logout_and_login() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use crate::schema::{FieldSpec, ModelSpec, NodeRead, RelationSpec, RemoteShape, VariableSource, NODE_PARAM};
    use crate::transport::{MockTransport, Reply};
    use crate::value::ScalarKind;
    struct WindowListener(std::sync::Mutex<Vec<super::super::LiveChange>>);
    impl super::super::QueryListener for WindowListener {
        fn changed(&self, change: super::super::LiveChange) { self.0.lock().unwrap().push(change); }
    }
    for credential_cleared in [true, false] {
        let transport = Arc::new(MockTransport::new());
        transport.set_session(Some("first"));
        let phase = Arc::new(AtomicUsize::new(0));
        let response_phase = phase.clone();
        transport.respond_with(move |request| {
            let phase = response_phase.load(Ordering::SeqCst);
            assert_ne!(phase, 1, "a held read reached the transport after logout: {}", request.operation_name);
            let row = json!({"__typename":"Moment", "id":if request.operation_name == "Node" { "m2" } else { "m1" }, "text":if phase == 0 { "before" } else { "after" }});
            Some(Reply::data(if request.operation_name == "Node" { json!({"node":row}) } else { json!({"moments":[row]}) }))
        });
        let mut model = ModelSpec::new("Moment", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("text", ScalarKind::Str).optional()]);
        model.relations.push(super::super::load_tests::relation("feed", "5.minutes"));
        for (name, operation) in [("query", "Query"), ("window", "Window")] {
            let mut relation = super::super::load_tests::relation(name, "5.minutes");
            relation.remote.as_mut().unwrap().operation = operation.into();
            model.relations.push(relation);
        }
        let mut node = RelationSpec::new("node", &[]);
        node.text_params = vec![NODE_PARAM.to_string()];
        node.node = Some(NodeRead::One);
        let mut remote = super::super::load_tests::relation("node", "5.minutes").remote.unwrap();
        remote.operation = "Node".into();
        remote.shape = RemoteShape::Single;
        remote.variables.insert("id".into(), VariableSource::Param { name: NODE_PARAM.into() });
        node.remote = Some(remote);
        model.relations.push(node);
        let wired = transport.clone();
        let db = Database::start_with(super::super::Config { path: None, data_version: None, cache_rows: 8 }, serde_json::to_string(&vec![model]).unwrap(), None, Arc::new(crate::pacing::SystemClock), move |_, _| Ok((Some(wired), None))).unwrap();
        db.register_operations(r#"[{"name":"Feed","document":"query Feed { moments { id text } }","rootPath":"moments"},{"name":"Node","document":"query Node($id: ID!) { node(id: $id) { ... on Moment { id text } } }","rootPath":"node"}]"#.into()).unwrap();
        db.register_operations(r#"[{"name":"Query","document":"query Query { moments { id text } }","rootPath":"moments"},{"name":"Window","document":"query Window { moments { id text } }","rootPath":"moments"}]"#.into()).unwrap();
        let (sender, mut receiver) = mpsc::unbounded_channel();
        let entities = db.live_entities("Moment".into(), Arc::new(Listener(sender))).unwrap();
        entities.singleton(r#"{"model":"Moment","relation":"feed","params":{}}"#.into(), 1).unwrap();
        entities.retain(vec![EntityRequest { id: "m2".into(), lease: 2, fetch: EntityFetch::Force }]).unwrap();
        let window_listener = Arc::new(WindowListener(std::sync::Mutex::new(Vec::new())));
        let window = db.live_window(r#"{"model":"Moment","relation":"window","params":{},"order":[{"field":"text","direction":"asc"}]}"#.into(), 10, None, window_listener.clone());
        let query_listener = Arc::new(WindowListener(std::sync::Mutex::new(Vec::new())));
        let query = db.live_query(r#"{"model":"Moment","relation":"query","params":{}}"#.into(), query_listener.clone());
        within(window.wait_for_initial_load()).await.unwrap();
        within(query.wait_for_initial_load()).await.unwrap();
        crate::test_wait::yield_until("initial entity reads", || {
            let state = locked(&entities.state);
            state.singleton.as_ref().is_some_and(|single| single.entry.verdict.settled && single.entry.value.is_some()) && state.entries.get("m2").is_some_and(|entry| entry.verdict.read == EntityRead::Loaded && entry.value.is_some())
        }).await;
        let operations = ["Feed", "Node", "Query", "Window"];
        let calls = operations.map(|operation| transport.call_count(operation));
        phase.store(1, Ordering::SeqCst);
        if credential_cleared { transport.set_session(None); }
        window_listener.0.lock().unwrap().clear();
        query_listener.0.lock().unwrap().clear();
        within(db.logout()).await.unwrap();
        crate::test_wait::yield_until("all held reads wait for the next session", || db.sync().unwrap().suspended_reads() == 4).await;
        assert_eq!(operations.map(|operation| transport.call_count(operation)), calls);
        let generation = db.db.generation().value();
        let mut emptied = BTreeMap::new();
        within(async {
            while emptied.len() != 2 {
                let snapshot = crate::test_wait::serving(receiver.recv()).await.unwrap();
                if snapshot.generation == generation {
                    assert!(snapshot.value.is_none());
                    assert!(!matches!(snapshot.read, EntityRead::Failed { .. }));
                    emptied.insert(snapshot.lease, true);
                }
            }
        }).await;
        for listener in [&window_listener, &query_listener] {
            crate::test_wait::yield_until("the held list publishes the reset", || listener.0.lock().unwrap().iter().any(|change| change.size == 0 && change.rows.is_empty())).await;
            assert!(listener.0.lock().unwrap().iter().all(|change| change.error.is_none()));
        }
        assert_eq!(operations.map(|operation| transport.call_count(operation)), calls);
        phase.store(2, Ordering::SeqCst);
        transport.set_session(Some("second"));
        db.sync().unwrap().session_committed();
        let mut loaded = BTreeMap::new();
        crate::test_wait::within_secs(15, async {
            while loaded.len() != 2 {
                let snapshot = crate::test_wait::serving(receiver.recv()).await.unwrap();
                if snapshot.generation == generation && snapshot.settled && snapshot.read == EntityRead::Loaded && snapshot.value.as_ref().is_some_and(|row| row.cells.contains(&super::super::wire::Cell::Text { value: "after".into() })) {
                    loaded.insert(snapshot.lease, true);
                }
            }
        }).await;
        assert_eq!(loaded.keys().copied().collect::<Vec<_>>(), [1, 2], "credential_cleared={credential_cleared}");
        within(window.wait_for_initial_load()).await.unwrap();
        within(query.wait_for_initial_load()).await.unwrap();
        for listener in [&window_listener, &query_listener] {
            crate::test_wait::yield_until("the held list publishes the new value", || listener.0.lock().unwrap().iter().any(|change| change.rows.iter().any(|row| row.cells.contains(&super::super::wire::Cell::Text { value: "after".into() })))).await;
        }
        within(db.sync().unwrap().resume_settled()).await;
        db.sync().unwrap().session_committed();
        within(db.sync().unwrap().resume_settled()).await;
        assert_eq!(operations.map(|operation| transport.call_count(operation)), calls.map(|count| count + 1));
        window.cancel();
        query.cancel();
        entities.shutdown();
    }
}

#[tokio::test]
async fn testing_reset_empties_rows_and_resets_a_retained_entity() {
    let clock = super::super::TestClock::new(1_000_000);
    let (db, transport, _) = super::super::load_tests::open_rig(Some(clock.clone()), None, None).await;
    db.load("Moment".into(), "feed".into(), "{}".into()).await.unwrap();
    let scope = db.db.spec("Moment").unwrap().selection_key("feed", &serde_json::Map::new()).unwrap();
    let window = crate::store::window_key(scope.as_str(), crate::schema::PageDirection::Forward);
    assert!(db.db.shared.store.window(&window).unwrap().is_some());
    clock.advance(1_000);
    let now = db.clock.now();
    put(&db.db, "Moment", json!({"id":"m1", "text":"before"}));
    db.db.create("Moment", json!({"id":"pending", "text":"pending"}).as_object().unwrap().clone()).unwrap();
    assert_eq!(db.db.outbox().unwrap().len(), 1);
    let (sender, mut receiver) = mpsc::unbounded_channel();
    let live = db.live_entities("Moment".into(), Arc::new(Listener(sender))).unwrap();
    live.retain(vec![request("m1", 1)]).unwrap();
    assert!(next(&mut receiver, EntityStage::Present).await.value.is_some());
    let (log, callback) = crate::db::fixtures::recorder();
    let _subscription = db.db.observe(crate::query::Query::new("Moment"), callback).unwrap();
    db.db.await_revision(db.db.revision_seq().unwrap());
    db.reset_for_testing().await.unwrap();
    let empty = next(&mut receiver, EntityStage::Absent).await;
    db.settle_for_testing().await.unwrap();
    assert!(empty.value.is_none());
    assert!(db.db.find("Moment", &RowId::new("m1").unwrap()).unwrap().is_none());
    assert!(db.db.outbox().unwrap().is_empty());
    assert!(db.db.shared.store.window(&window).unwrap().is_none());
    assert_eq!(db.clock.now(), now);
    assert_eq!(crate::db::fixtures::changes(&log).iter().map(crate::db::fixtures::ids).collect::<Vec<_>>(), vec![vec!["m1", "pending"], vec![]]);
    db.load("Moment".into(), "feed".into(), "{}".into()).await.unwrap();
    assert_eq!(transport.queries.load(std::sync::atomic::Ordering::SeqCst), 2);
    live.shutdown();
}

#[tokio::test]
async fn entity_snapshot_order() {
    let (db, _) = super::super::load_tests::rig().await;
    put(&db.db, "Moment", json!({"id":"m1", "text":"cold"}));
    let old_row = db.db.find("Moment", &RowId::new("m1").unwrap()).unwrap().unwrap();
    let (sender, mut receiver) = mpsc::unbounded_channel();
    let live = db.live_entities("Moment".into(), Arc::new(Listener(sender))).unwrap();
    live.retain(vec![request("m1", 1)]).unwrap();
    let cold = next(&mut receiver, EntityStage::Present).await;
    assert!(cold.settled && !cold.is_stale && cold.value.is_some());
    let mut revisions = vec![cold.revision];
    let old_observer_lease = locked(&live.state).entries["m1"].observer_lease;
    live.release(vec![release("m1", 1)]).unwrap();
    assert_eq!(db.entity_cache.count("Moment", live.now()), 1);
    live.retain(vec![request("m1", 2)]).unwrap();
    let retained = next(&mut receiver, EntityStage::Present).await;
    assert!(retained.is_stale && retained.value == cold.value && retained.lease == 2);
    revisions.push(retained.revision);
    let fresh = next(&mut receiver, EntityStage::Present).await;
    assert!(!fresh.is_stale);
    revisions.push(fresh.revision);
    live.changed(&EntityChange { revision: i64::MAX, epoch: live.epoch(), leases: BTreeMap::from([(old_row.id.clone(), old_observer_lease)]), rows: vec![old_row.clone()], removed: vec![], failure: None });
    assert!(receiver.try_recv().is_err(), "an old acquisition cannot publish to a new lease");
    let current_lease = locked(&live.state).entries["m1"].observer_lease;
    live.changed(&EntityChange { revision: fresh.revision, epoch: live.epoch(), leases: BTreeMap::from([(old_row.id.clone(), current_lease)]), rows: vec![], removed: vec![], failure: Some("read failed".into()) });
    let error = next(&mut receiver, EntityStage::Failed { message: "read failed".into() }).await;
    assert!(matches!(error.phase, EntityStage::Failed { .. }) && error.value == fresh.value && error.settled);
    revisions.push(error.revision);
    let generation = live.generation();
    db.logout().await.unwrap();
    let removed = next(&mut receiver, EntityStage::Absent).await;
    assert!(removed.value.is_none() && removed.generation > generation);
    revisions.push(removed.revision);
    put(&db.db, "Moment", json!({"id":"m1", "text":"new account"}));
    let current = next(&mut receiver, EntityStage::Present).await;
    revisions.push(current.revision);
    assert!(revisions.windows(2).all(|pair| pair[0] <= pair[1]), "{revisions:?}");
    let (first, second) = {
        let mut state = locked(&live.state);
        let mut single = Entry::new(3, true, live.generation(), live.epoch());
        let first = single.verdict.feed;
        single.accept(current.revision, current.value.clone(), Some(first));
        single.verdict = single.machine.reset();
        single.clear_value();
        single.verdict = single.machine.begin_feed();
        let second = single.verdict.feed;
        state.singleton = Some(Singleton { entry: single, query: String::new(), subscription: None });
        (first, second)
    };
    live.single_changed(first, LiveChange::whole(i64::MAX, current.value.clone().into_iter().collect()));
    assert!(receiver.try_recv().is_err());
    live.single_changed(second, LiveChange::whole(current.revision, current.value.into_iter().collect()));
    let singleton = next(&mut receiver, EntityStage::Present).await;
    assert_eq!(singleton.revision, current.revision);
    assert_eq!(singleton.lease, 3);
    live.shutdown();
}

#[tokio::test]
async fn entity_initial_local() {
    let (db, transport) = super::super::load_tests::rig().await;
    put(&db.db, "Moment", json!({"id":"m1", "text":"local"}));
    let (sender, mut receiver) = mpsc::unbounded_channel();
    let live = db.live_entities("Moment".into(), Arc::new(Listener(sender))).unwrap();
    live.retain(vec![request("m1", 1), request("missing", 2)]).unwrap();
    let mut values = BTreeMap::new();
    for _ in 0..2 { let snapshot = within(receiver.recv()).await.unwrap(); assert!(snapshot.settled); values.insert(snapshot.id.clone(), snapshot); }
    assert_eq!(values["m1"].phase, EntityStage::Present);
    assert!(values["m1"].value.is_some());
    assert_eq!(values["missing"].phase, EntityStage::Absent);
    assert!(values["missing"].value.is_none());
    assert_eq!(transport.queries.load(std::sync::atomic::Ordering::SeqCst), 0);
    live.shutdown();
}

#[tokio::test]
async fn entity_retention_budget() {
    let cache = super::super::entity_pool::EntityCache::new();
    let row = || CachedEntity { row: None, revision: 1, generation: 1, epoch: 0 };
    for id in 0..201 { cache.keep("A", &id.to_string(), row(), 0); }
    assert_eq!(cache.count("A", 0), 200);
    assert!(cache.take("A", "0", 0, 1, 0).is_none());
    assert!(cache.take("A", "1", 120_000, 1, 0).is_none());
    assert_eq!(cache.count("A", 120_000), 0);
    cache.keep("A", "generation", row(), 120_001);
    assert!(cache.take("A", "generation", 120_002, 2, 0).is_none());
    let (db, _) = super::super::load_tests::rig().await;
    put(&db.db, "Moment", json!({"id":"large", "text":"bytes"}));
    let stored = db.db.find("Moment", &RowId::new("large").unwrap()).unwrap().unwrap();
    let mut wire = WireModel::of(db.db.spec("Moment").unwrap()).row(&stored);
    wire.estimated_bytes = 5 * 1024 * 1024;
    for model in ["A", "B"] {
        cache.keep(model, "large", CachedEntity { row: Some(wire.clone()), revision: 1, generation: 1, epoch: 0 }, 120_003);
    }
    assert_eq!(cache.count("A", 120_003), 0);
    assert_eq!(cache.count("B", 120_003), 1);
    wire.estimated_bytes = 9 * 1024 * 1024;
    cache.keep("C", "oversized", CachedEntity { row: Some(wire), revision: 1, generation: 1, epoch: 0 }, 120_003);
    assert_eq!(cache.count("C", 120_003), 0);
}

struct IdleClock {
    clock: crate::pacing::ManualClock,
    sleeps: std::sync::atomic::AtomicUsize,
    wakes: std::sync::atomic::AtomicUsize,
}
impl crate::pacing::Clock for IdleClock {
    fn now(&self) -> std::time::Duration { crate::pacing::Clock::now(&self.clock) }
    fn sleep(&self, duration: std::time::Duration) -> std::pin::Pin<Box<dyn std::future::Future<Output=()> + Send + '_>> {
        use std::sync::atomic::Ordering;
        self.sleeps.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move { crate::pacing::Clock::sleep(&self.clock, duration).await; self.wakes.fetch_add(1, Ordering::SeqCst); })
    }
}

#[tokio::test]
async fn entity_idle_work() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    let (sample, _) = super::super::load_tests::rig().await;
    let models = serde_json::to_string(&vec![sample.db.spec("Moment").unwrap().clone()]).unwrap();
    let clock = Arc::new(IdleClock { clock: crate::pacing::ManualClock::new(Duration::from_secs(1000)), sleeps: AtomicUsize::new(0), wakes: AtomicUsize::new(0) });
    let db = Database::start_with(super::super::Config { path: None, data_version: None, cache_rows: 32 }, models, None, clock.clone(), |_, _| Ok((None, None))).unwrap();
    put(&db.db, "Moment", json!({"id":"m1", "text":"idle"}));
    let (sender, mut receiver) = mpsc::unbounded_channel();
    let live = db.live_entities("Moment".into(), Arc::new(Listener(sender))).unwrap();
    live.retain(vec![request("m1", 1)]).unwrap();
    next(&mut receiver, EntityStage::Present).await;
    live.release(vec![release("m1", 1)]).unwrap();
    let before = db.db.counters();
    let sleepers = (clock.sleeps.load(Ordering::SeqCst), clock.wakes.load(Ordering::SeqCst));
    tokio::time::pause();
    clock.clock.advance(Duration::from_secs(130));
    tokio::time::advance(Duration::from_secs(130)).await;
    tokio::task::yield_now().await;
    let after = db.db.counters();
    assert_eq!((after.entity_facts_told, after.rows_loaded, after.observer_calls, after.transport_calls), (before.entity_facts_told, before.rows_loaded, before.observer_calls, before.transport_calls));
    assert_eq!(sleepers, (clock.sleeps.load(Ordering::SeqCst), clock.wakes.load(Ordering::SeqCst)));
    assert!(receiver.try_recv().is_err());
    live.shutdown();
}
