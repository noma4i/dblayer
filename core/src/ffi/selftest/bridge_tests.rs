use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::ThreadId;

use serde_json::json;

use super::rig_tests::{bound, Fault, Rig, Switchable, World};
use crate::ffi::lifecycle_tests::named;
use crate::ffi::{Database, CommandRequest, DbError, EntityFetch, LivePhase, Placement, Replacement, StreamDelivery, StreamEnd, StreamRequest, StreamValue, WireOperationOutcome, WireRow, WireSyncState};
use crate::test_wait::yield_until;

/// What a platform reader holds: the deliveries in order, replayed into the rows its screen shows.
#[derive(Default)]
struct Reader {
    log: Mutex<Vec<StreamDelivery>>,
    threads: Mutex<Vec<ThreadId>>,
    lose_next: AtomicBool,
    changed: tokio::sync::Notify,
}

impl crate::ffi::stream::tape::Reader for Reader {
    fn delivered(&self, delivery: StreamDelivery) {
        self.threads.lock().unwrap().push(std::thread::current().id());
        if self.lose_next.swap(false, Ordering::SeqCst) { return; }
        self.log.lock().unwrap().push(delivery);
        self.changed.notify_one();
    }
}

fn apply_delta(rows: &mut Vec<WireRow>, carried: Vec<WireRow>, indexes: &[u32], removed: &[String], placed: &[Placement], replaced: &[Replacement], size: u32) {
    let by_id: HashMap<String, WireRow> = carried.iter().map(|row| (row.id.clone(), row.clone())).collect();
    for pair in replaced {
        let at = rows.iter().position(|row| row.id == pair.from).expect("a replaced row is held");
        rows[at] = by_id[&pair.to].clone();
    }
    rows.retain(|row| !removed.contains(&row.id));
    let mut taken = HashMap::new();
    for place in placed {
        if let Some(at) = rows.iter().position(|row| row.id == place.id) { taken.insert(place.id.clone(), rows.remove(at)); }
    }
    let mut ordered: Vec<&Placement> = placed.iter().collect();
    ordered.sort_by_key(|place| place.index);
    for place in ordered {
        let row = taken.remove(&place.id).or_else(|| by_id.get(&place.id).cloned()).expect("a placed row is known");
        rows.insert(place.index as usize, row);
    }
    for (row, index) in carried.into_iter().zip(indexes) {
        let index = *index as usize;
        if rows[index].id == row.id && rows[index].cells == row.cells && rows[index].sync == row.sync { continue; }
        rows[index] = row;
    }
    assert_eq!(rows.len(), size as usize, "the delta leaves the size it names");
}

impl Reader {
    fn open() -> Arc<Reader> { Arc::new(Reader::default()) }
    fn deliveries(&self) -> Vec<StreamDelivery> { self.log.lock().unwrap().clone() }
    fn len(&self) -> usize { self.log.lock().unwrap().len() }

    /// The rows after the last snapshot and the deltas that chain from it. A delta whose base is not the held revision is a gap and is counted.
    fn replay(&self) -> (Vec<WireRow>, usize) {
        let (mut rows, mut revision, mut gaps) = (Vec::new(), None, 0);
        for delivery in self.deliveries() {
            match delivery.value {
                StreamValue::Snapshot { revision: held, rows: whole, .. } => { rows = whole; revision = Some(held); }
                StreamValue::Delta { base, revision: next, size, rows: carried, row_indexes, removed, placed, replaced, .. } => {
                    if revision != Some(base) { gaps += 1; continue; }
                    apply_delta(&mut rows, carried, &row_indexes, &removed, &placed, &replaced, size);
                    revision = Some(next);
                }
                _ => {}
            }
        }
        (rows, gaps)
    }

    fn rows(&self) -> Vec<WireRow> { self.replay().0 }
    fn gaps(&self) -> usize { self.replay().1 }
    fn shows(&self, account: &str) -> bool { let rows = self.rows(); !rows.is_empty() && rows.iter().all(|row| named(row, account)) }
    fn ever_showed(&self, account: &str) -> bool {
        self.deliveries().iter().any(|delivery| match &delivery.value {
            StreamValue::Snapshot { rows, .. } | StreamValue::Delta { rows, .. } => rows.iter().any(|row| named(row, account)),
            _ => false,
        })
    }
    fn ended(&self) -> Vec<StreamEnd> {
        self.deliveries().into_iter().filter_map(|delivery| if let StreamValue::Ended { reason } = delivery.value { Some(reason) } else { None }).collect()
    }
    fn snapshots(&self) -> usize { self.deliveries().iter().filter(|delivery| matches!(delivery.value, StreamValue::Snapshot { .. })).count() }
    fn texts(&self) -> Vec<String> {
        self.rows().iter().map(|row| row.cells.iter().filter_map(|cell| if let crate::ffi::Cell::Text { value } = cell { Some(value.clone()) } else { None }).collect::<Vec<_>>().join("|")).collect()
    }
}

/// The network goes away while the held request is at the server: the load ends with the network error at once. A test clock never moves by itself, so a network error the server returns would wait for a retry without end.
async fn network_drops_during(rig: &Rig, held: &super::rig_tests::Gate) {
    held.entered().await;
    rig.database.set_online(false).unwrap();
}

fn profile() -> StreamRequest { StreamRequest::Query { query_json: r#"{"model":"User","relation":"profile","params":{}}"#.into() } }

fn users() -> StreamRequest { StreamRequest::Query { query_json: r#"{"model":"User","relation":"all","params":{}}"#.into() } }

#[tokio::test(flavor = "multi_thread")]
async fn cancelling_one_load_interest_keeps_the_other_call_alive() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let held = rig.world.hold_next("Users");
    let first = rig.database.begin_load(epoch, "User".into(), "all".into(), "{}".into()).unwrap();
    held.entered_for("the shared load").await;
    let second = rig.database.begin_load(epoch, "User".into(), "all".into(), "{}".into()).unwrap();
    first.cancel();
    held.release();
    bound(second.result()).await.unwrap();
    assert_eq!(rig.world.count("Users"), 1);
    let rows = bound(rig.database.read_query(epoch, r#"{"model":"User","relation":"all","params":{}}"#.into())).await.unwrap();
    assert_eq!(ids_of(rows), ["user"]);
    rig.database.close_now();
}

#[derive(Default)]
struct CancelledLoadTransport {
    started: tokio::sync::Notify,
    cancelled: Arc<tokio::sync::Notify>,
}

struct CancelledLoad(Arc<tokio::sync::Notify>);

impl Drop for CancelledLoad {
    fn drop(&mut self) { self.0.notify_one(); }
}

#[async_trait::async_trait]
impl crate::ffi::Transport for CancelledLoadTransport {
    async fn open_cable(&self, _: Arc<crate::ffi::CableLease>) -> Result<(), DbError> { Err(DbError::Network { detail: "no cable".into() }) }
    async fn cable_send(&self, _: String, _: String) -> Result<(), DbError> { unreachable!() }
    async fn cable_next(&self, _: String) -> Option<crate::ffi::EncryptedSocketEvent> { None }
    async fn cable_close(&self, _: String) {}
    async fn query(&self, _: crate::ffi::TransportRequest) -> Result<String, DbError> {
        let _cancelled = CancelledLoad(self.cancelled.clone());
        self.started.notify_one();
        std::future::pending().await
    }
    async fn mutate(&self, _: crate::ffi::TransportRequest) -> Result<String, DbError> { unreachable!() }
    async fn subscribe(&self, _: crate::ffi::TransportRequest, _: Arc<crate::ffi::SubscriptionSink>) -> Result<(), DbError> { unreachable!() }
    fn unsubscribe(&self, _: String) {}
    async fn put(&self, _: String, _: HashMap<String, String>, _: crate::transport::PutFile, _: crate::transport::TransferIdentity, _: Arc<crate::ffi::ByteProgress>) -> Result<crate::ffi::PutReply, DbError> { unreachable!() }
}

#[tokio::test(flavor = "multi_thread")]
async fn cancelling_the_last_load_interest_cancels_the_request() {
    let transport = Arc::new(CancelledLoadTransport::default());
    let database = bound(Database::open(crate::ffi::Config { path: None, data_version: None, cache_rows: 32 }, crate::ffi::lifecycle_tests::models(), Some(transport.clone()), None, None, None, None, None)).await.unwrap();
    database.register_operations(json!([{"name":"Users","document":"query Users { users { id name } }"}]).to_string()).unwrap();
    let request = database.begin_load(database.session_epoch(), "User".into(), "all".into(), "{}".into()).unwrap();
    bound(transport.started.notified()).await;
    request.cancel();
    bound(transport.cancelled.notified()).await;
    database.close_now();
}

#[derive(Default)]
struct MomentTransport { reads: std::sync::atomic::AtomicUsize }

#[async_trait::async_trait]
impl crate::ffi::Transport for MomentTransport {
    async fn open_cable(&self, _: Arc<crate::ffi::CableLease>) -> Result<(), DbError> { Err(DbError::Network { detail: "no cable".into() }) }
    async fn cable_send(&self, _: String, _: String) -> Result<(), DbError> { Err(DbError::Network { detail: "no cable".into() }) }
    async fn cable_next(&self, _: String) -> Option<crate::ffi::EncryptedSocketEvent> { None }
    async fn cable_close(&self, _: String) {}
    async fn query(&self, request: crate::ffi::TransportRequest) -> Result<String, DbError> {
        if request.operation_name != "MomentsFeed" { return Err(DbError::Invalid { detail: format!("unexpected query {}", request.operation_name) }); }
        let id = if self.reads.fetch_add(1, Ordering::SeqCst) == 0 { "1" } else { "2" };
        Ok(json!({"data":{"moments":[{"id":id,"text":id}]}}).to_string())
    }
    async fn mutate(&self, request: crate::ffi::TransportRequest) -> Result<String, DbError> {
        if request.operation_name == "LikeMoment" { return Ok(json!({"data":{"likeMoment":{"id":"1","text":"1"}}}).to_string()); }
        if request.operation_name == "DeleteMoment" { return Ok(json!({"errors":[{"message":"gone","extensions":{"code":"NOT_FOUND"}}]}).to_string()); }
        Err(DbError::Invalid { detail: format!("unexpected mutation {}", request.operation_name) })
    }
    async fn subscribe(&self, _: crate::ffi::TransportRequest, _: Arc<crate::ffi::SubscriptionSink>) -> Result<(), DbError> { unreachable!() }
    fn unsubscribe(&self, _: String) {}
    async fn put(&self, _: String, _: HashMap<String, String>, _: crate::transport::PutFile, _: crate::transport::TransferIdentity, _: Arc<crate::ffi::ByteProgress>) -> Result<crate::ffi::PutReply, DbError> { unreachable!() }
}

struct PagedMomentTransport {
    reads: std::sync::atomic::AtomicUsize,
    first_page: Arc<super::rig_tests::Gate>,
}

#[async_trait::async_trait]
impl crate::ffi::Transport for PagedMomentTransport {
    async fn open_cable(&self, _: Arc<crate::ffi::CableLease>) -> Result<(), DbError> { Err(DbError::Network { detail: "no cable".into() }) }
    async fn cable_send(&self, _: String, _: String) -> Result<(), DbError> { Err(DbError::Network { detail: "no cable".into() }) }
    async fn cable_next(&self, _: String) -> Option<crate::ffi::EncryptedSocketEvent> { None }
    async fn cable_close(&self, _: String) {}
    async fn query(&self, request: crate::ffi::TransportRequest) -> Result<String, DbError> {
        if request.operation_name != "MomentsFeed" { return Err(DbError::Invalid { detail: format!("unexpected query {}", request.operation_name) }); }
        let index = self.reads.fetch_add(1, Ordering::SeqCst);
        if index == 1 { self.first_page.pass().await; }
        let (id, more, cursor) = match index {
            0 => ("1", false, "seed"),
            1 => ("1", true, "first"),
            2 => ("2", false, "second"),
            3 => ("2", false, "fresh"),
            _ => return Err(DbError::Invalid { detail: "unexpected feed request".into() }),
        };
        Ok(json!({"data":{"feed":{"nodes":[{"id":id,"text":id}],"pageInfo":{"hasNextPage":more,"endCursor":cursor}}}}).to_string())
    }
    async fn mutate(&self, request: crate::ffi::TransportRequest) -> Result<String, DbError> {
        <MomentTransport as crate::ffi::Transport>::mutate(&MomentTransport::default(), request).await
    }
    async fn subscribe(&self, _: crate::ffi::TransportRequest, _: Arc<crate::ffi::SubscriptionSink>) -> Result<(), DbError> { unreachable!() }
    fn unsubscribe(&self, _: String) {}
    async fn put(&self, _: String, _: HashMap<String, String>, _: crate::transport::PutFile, _: crate::transport::TransferIdentity, _: Arc<crate::ffi::ByteProgress>) -> Result<crate::ffi::PutReply, DbError> { unreachable!() }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_mutation_during_the_first_page_keeps_the_whole_chain_stale() {
    use crate::schema::{ActionDeclaration, Coverage, FieldSpec, ModelSpec, PageDirection, RelationSpec, RemoteShape, RootShape};
    use crate::value::ScalarKind;
    let mut model = ModelSpec::new("Moment", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("text", ScalarKind::Str)]);
    let mut feed = RelationSpec::global("feed");
    feed.remote = Some(crate::sync::rig::remote("MomentsFeed", RemoteShape::Connection { direction: PageDirection::Forward, coverage: Coverage::Page, max_pages: None, cursor: None }, "5.minutes"));
    model.relations.push(feed);
    let mut like = ActionDeclaration::new("like", RootShape::Upsert);
    like.mutation = Some("LikeMoment".into());
    like.refetch.push("feed".into());
    model.actions.push(like);
    let transport = Arc::new(PagedMomentTransport { reads: 0.into(), first_page: super::rig_tests::Gate::new() });
    let database = bound(Database::open(crate::ffi::Config { path: None, data_version: None, cache_rows: 32 }, serde_json::to_string(&vec![model]).unwrap(), Some(transport.clone()), None, None, None, None, None)).await.unwrap();
    database.register_operations(json!([
        {"name":"MomentsFeed","document":"query MomentsFeed($first: Int, $after: String) { feed(first: $first, after: $after) { nodes { id text } pageInfo { hasNextPage endCursor } } }"},
        {"name":"LikeMoment","document":"mutation LikeMoment($id: ID!) { likeMoment(id: $id) { id text } }"}
    ]).to_string()).unwrap();
    let epoch = database.session_epoch();
    bound(database.load(epoch, "Moment".into(), "feed".into(), "{}".into())).await.unwrap();
    let loading = { let database = database.clone(); tokio::spawn(async move { database.load_all(epoch, "Moment".into(), "feed".into(), "{}".into(), true).await }) };
    transport.first_page.entered().await;
    let receipt = bound(database.act(epoch, "Moment".into(), "1".into(), "like".into(), "{}".into(), "{}".into())).await.unwrap();
    assert!(matches!(bound(database.outcome(receipt)).await.unwrap(), crate::ffi::WireOperationOutcome::Confirmed { .. }));
    transport.first_page.release();
    bound(loading).await.unwrap().unwrap();
    assert_eq!(transport.reads.load(Ordering::SeqCst), 3);
    bound(database.load(epoch, "Moment".into(), "feed".into(), "{}".into())).await.unwrap();
    let rows = bound(database.read_query(epoch, json!({"model":"Moment","relation":"feed","params":{}}).to_string())).await.unwrap();
    assert_eq!(ids_of(rows), ["2"]);
    assert_eq!(transport.reads.load(Ordering::SeqCst), 4);
    bound(database.load(epoch, "Moment".into(), "feed".into(), "{}".into())).await.unwrap();
    assert_eq!(transport.reads.load(Ordering::SeqCst), 4);
    database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_refetch_owed_without_a_reader_survives_reopen() {
    use crate::schema::{ActionDeclaration, Coverage, FieldSpec, ModelSpec, RelationSpec, RemoteShape, RootShape};
    use crate::value::ScalarKind;
    let mut model = ModelSpec::new("Moment", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("text", ScalarKind::Str)]);
    let mut feed = RelationSpec::global("feed");
    feed.remote = Some(crate::sync::rig::remote("MomentsFeed", RemoteShape::List { coverage: Coverage::Complete }, "5.minutes"));
    model.relations.push(feed);
    let mut like = ActionDeclaration::new("like", RootShape::Upsert);
    like.mutation = Some("LikeMoment".into());
    like.refetch.push("feed".into());
    model.actions.push(like);
    let models = serde_json::to_string(&vec![model]).unwrap();
    let operations = json!([
        {"name":"MomentsFeed","document":"query MomentsFeed { moments { id text } }"},
        {"name":"LikeMoment","document":"mutation LikeMoment($id: ID!) { likeMoment(id: $id) { id text } }"}
    ]).to_string();
    let directory = tempfile::tempdir().unwrap();
    let config = crate::ffi::Config { path: Some(directory.path().join("cache.sqlite").to_string_lossy().into_owned()), data_version: None, cache_rows: 32 };
    let opener = crate::ffi::OpenDatabase::new();
    let transport = Arc::new(MomentTransport::default());
    let first = bound(opener.open_with_clock(config.clone(), models.clone(), Some(transport.clone()), None, crate::ffi::TestClock::new(1_000_000), false, false, None, None, None, None)).await.unwrap();
    first.register_operations(operations.clone()).unwrap();
    let epoch = first.session_epoch();
    bound(first.load(epoch, "Moment".into(), "feed".into(), "{}".into())).await.unwrap();
    let receipt = bound(first.act(epoch, "Moment".into(), "1".into(), "like".into(), "{}".into(), "{}".into())).await.unwrap();
    assert!(matches!(bound(first.outcome(receipt)).await.unwrap(), crate::ffi::WireOperationOutcome::Confirmed { .. }));
    assert_eq!(transport.reads.load(Ordering::SeqCst), 1);
    bound(first.close_database()).await.unwrap();
    let second = bound(opener.open_with_clock(config, models, Some(transport.clone()), None, crate::ffi::TestClock::new(1_000_000), false, false, None, None, None, None)).await.unwrap();
    second.register_operations(operations).unwrap();
    let epoch = second.session_epoch();
    bound(second.load(epoch, "Moment".into(), "feed".into(), "{}".into())).await.unwrap();
    let rows = bound(second.read_query(epoch, json!({"model":"Moment","relation":"feed","params":{}}).to_string())).await.unwrap();
    assert_eq!(ids_of(rows), ["2"]);
    assert_eq!(transport.reads.load(Ordering::SeqCst), 2);
    second.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_missing_destroyed_row_causes_one_reread_of_the_open_list() {
    use crate::schema::{ActionDeclaration, Coverage, FieldSpec, ModelSpec, RelationSpec, RemoteShape, RootShape};
    use crate::value::ScalarKind;
    let mut model = ModelSpec::new("Moment", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("text", ScalarKind::Str)]);
    let mut feed = RelationSpec::global("feed");
    feed.remote = Some(crate::sync::rig::remote("MomentsFeed", RemoteShape::List { coverage: Coverage::Complete }, "5.minutes"));
    model.relations.push(feed);
    let mut destroy = ActionDeclaration::new("destroy", RootShape::Destroy);
    destroy.mutation = Some("DeleteMoment".into());
    model.actions.push(destroy);
    let transport = Arc::new(MomentTransport::default());
    let database = bound(Database::open(crate::ffi::Config { path: None, data_version: None, cache_rows: 32 }, serde_json::to_string(&vec![model]).unwrap(), Some(transport.clone()), None, None, None, None, None)).await.unwrap();
    database.register_operations(json!([
        {"name":"MomentsFeed","document":"query MomentsFeed { moments { id text } }"},
        {"name":"DeleteMoment","document":"mutation DeleteMoment($id: ID!) { deleteMoment(id: $id) { id } }"}
    ]).to_string()).unwrap();
    let epoch = database.session_epoch();
    let reader = Reader::open();
    let stream = database.subscribe_reading(epoch, StreamRequest::Query { query_json: json!({"model":"Moment","relation":"feed","params":{}}).to_string() }, reader.clone());
    bound(database.loaded(stream)).await.unwrap();
    assert_eq!(ids_of(reader.rows()), ["1"]);
    let receipt = bound(database.destroy(epoch, "Moment".into(), "1".into())).await.unwrap();
    assert!(matches!(bound(database.outcome(receipt)).await.unwrap(), crate::ffi::WireOperationOutcome::Confirmed { .. }));
    bound(async {
        loop {
            let changed = reader.changed.notified();
            if ids_of(reader.rows()) == ["2"] { break; }
            changed.await;
        }
    }).await;
    assert_eq!(transport.reads.load(Ordering::SeqCst), 2);
    database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_refresh_after_reopen_and_clock_rollback_keeps_the_new_response() {
    use super::rig_tests::{plain_models, plain_operations};
    let directory = tempfile::tempdir().unwrap();
    let config = crate::ffi::Config { path: Some(directory.path().join("cache.sqlite").to_string_lossy().into_owned()), data_version: None, cache_rows: 32 };
    let opener = crate::ffi::OpenDatabase::new();
    let world_a = World::new("server", "A", true);
    let switch = Switchable::new(Some(world_a.clone()), false);
    let first = bound(opener.open_with_clock(config.clone(), plain_models(), Some(switch.clone()), None, crate::ffi::TestClock::new(1_000_000), false, false, None, None, None, None)).await.unwrap();
    first.register_operations(plain_operations()).unwrap();
    bound(first.refresh(first.session_epoch(), "User".into(), "profile".into(), "{}".into())).await.unwrap();
    let query = r#"{"model":"User","relation":"profile","params":{}}"#.to_string();
    let rows = bound(first.read_query(first.session_epoch(), query.clone())).await.unwrap();
    assert!(rows.iter().any(|row| named(row, "A")));
    bound(first.close_database()).await.unwrap();
    let world_b = World::new("server", "B", true);
    switch.set(Some(world_b.clone()));
    let second = bound(opener.open_with_clock(config, plain_models(), Some(switch), None, crate::ffi::TestClock::new(100), false, false, None, None, None, None)).await.unwrap();
    second.register_operations(plain_operations()).unwrap();
    bound(second.refresh(second.session_epoch(), "User".into(), "profile".into(), "{}".into())).await.unwrap();
    let rows = bound(second.read_query(second.session_epoch(), query)).await.unwrap();
    assert!(rows.iter().any(|row| named(row, "B")));
    assert_eq!((world_a.count("MyProfile"), world_b.count("MyProfile")), (1, 1));
    second.close_now();
}

fn thread() -> StreamRequest {
    StreamRequest::Query { query_json: r#"{"model":"Message","relation":"thread","params":{"chatId":"c1"},"order":[{"field":"at","direction":"asc"}]}"#.into() }
}

pub(super) fn message(epoch: u64, text: &str, at: i64) -> CommandRequest {
    CommandRequest { epoch, model: "Message".into(), values_json: json!({"chatId":"c1","text":text,"at":at}).to_string(), args_json: "{}".into(), upload: None }
}

async fn settle(rig: &Rig) { rig.settle().await; }

#[tokio::test(flavor = "multi_thread")]
async fn a_profile_delivery_is_ready_with_its_rows_before_an_explicit_refresh() {
    let rig = Rig::plain(World::new("server", "A", true), false).await;
    let epoch = rig.database.session_epoch();
    let reader = Reader::open();
    rig.database.subscribe_reading(epoch, profile(), reader.clone());
    bound(async {
        loop {
            let changed = reader.changed.notified();
            if reader.deliveries().iter().any(|delivery| match &delivery.value {
                StreamValue::Snapshot { state, .. } | StreamValue::Delta { state, .. } | StreamValue::State { state } => state.settled,
                _ => false,
            }) { break; }
            changed.await;
        }
    }).await;
    assert!(reader.shows("A"));
    assert_eq!(rig.world.count("MyProfile"), 1);
    bound(rig.database.refresh(epoch, "User".into(), "profile".into(), "{}".into())).await.unwrap();
    assert_eq!(rig.world.count("MyProfile"), 2);
    rig.database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_cold_profile_delivers_a_complete_first_frame_when_empty_or_failed() {
    for failed in [false, true] {
        let world = World::new("server", "A", true);
        let down = failed.then(|| world.hold_then("MyProfile", Fault::Down));
        if !failed { world.empty_profile(); }
        let rig = Rig::plain(world, false).await;
        let reader = Reader::open();
        rig.database.subscribe_reading(rig.database.session_epoch(), profile(), reader.clone());
        if let Some(down) = down { network_drops_during(&rig, &down).await; }
        bound(async {
            loop {
                let changed = reader.changed.notified();
                if reader.deliveries().iter().any(|delivery| match &delivery.value {
                    StreamValue::Snapshot { state, .. } | StreamValue::Delta { state, .. } | StreamValue::State { state } => state.settled,
                    _ => false,
                }) { break; }
                changed.await;
            }
        }).await;
        assert!(matches!(&reader.deliveries()[0].value, StreamValue::Snapshot { rows, .. } if rows.is_empty()), "first delivery is not a complete empty frame");
        let last = reader.deliveries().into_iter().filter_map(|delivery| match delivery.value {
            StreamValue::Snapshot { state, .. } | StreamValue::Delta { state, .. } | StreamValue::State { state } if state.settled => Some(state),
            _ => None,
        }).last().unwrap();
        assert_eq!(last.phase, if failed { LivePhase::Failed } else { LivePhase::Empty });
        assert_eq!(matches!(last.error, Some(DbError::Network { .. })), failed);
        rig.database.close_now();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_cached_entity_remains_loaded_when_its_initial_network_read_fails() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let request = || StreamRequest::Entity { model: "User".into(), id: "user".into(), fetch: EntityFetch::Force };
    let first = rig.database.subscribe_reading(epoch, request(), Reader::open());
    bound(rig.database.loaded(first)).await.unwrap();
    rig.database.cancel_stream(first);
    let down = rig.world.hold_then("UserNode", Fault::Down);
    let reader = Reader::open();
    let stream = rig.database.subscribe_reading(epoch, request(), reader.clone());
    network_drops_during(&rig, &down).await;
    let result = bound(rig.database.loaded(stream)).await;
    assert!(result.is_ok(), "a cached entity should remain loaded: {result:?}");
    assert_eq!(reader.rows().first().map(|row| row.id.as_str()), Some("user"));
    bound(async {
        loop {
            let changed = reader.changed.notified();
            if reader.deliveries().iter().any(|delivery| match &delivery.value {
                StreamValue::Snapshot { state, .. } | StreamValue::Delta { state, .. } | StreamValue::State { state } => state.error.is_some(),
                _ => false,
            }) { break; }
            changed.await;
        }
    }).await;
    assert!(reader.deliveries().iter().any(|delivery| match &delivery.value {
        StreamValue::Snapshot { state, .. } | StreamValue::Delta { state, .. } | StreamValue::State { state } => state.phase == LivePhase::Loaded && matches!(&state.error, Some(DbError::Network { .. })),
        _ => false,
    }));
    rig.database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unknown_association_reports_its_error_after_the_stream_ends() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let reader = Reader::open();
    let request = StreamRequest::Related { query_json: r#"{"model":"User"}"#.into(), name: "missing".into() };
    let stream = rig.database.subscribe_reading(rig.database.session_epoch(), request, reader.clone());
    bound(async {
        loop {
            let changed = reader.changed.notified();
            if !reader.ended().is_empty() { break; }
            changed.await;
        }
    }).await;
    assert_eq!(reader.ended(), vec![StreamEnd::Invalid]);
    let result = bound(rig.database.loaded(stream)).await;
    assert!(matches!(result, Err(DbError::Invalid { detail }) if detail.contains("missing")));
    rig.database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_held_entity_ends_its_load_wait_after_session_reset() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let reader = Reader::open();
    let stream = rig.database.subscribe_reading(rig.database.session_epoch(), StreamRequest::Entity { model: "User".into(), id: "user".into(), fetch: EntityFetch::Force }, reader.clone());
    bound(rig.database.loaded(stream)).await.unwrap();
    bound(rig.database.session_reset()).await.unwrap();
    bound(async {
        loop {
            let changed = reader.changed.notified();
            if !reader.ended().is_empty() { break; }
            changed.await;
        }
    }).await;
    assert_eq!(reader.ended(), vec![StreamEnd::SessionEnded]);
    assert!(matches!(bound(rig.database.loaded(stream)).await, Err(DbError::Interrupted { .. })));
    rig.database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn cancelling_a_reader_before_its_first_load_interrupts_the_wait() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let gate = rig.world.hold_next("UserNode");
    let reader = Reader::open();
    let stream = rig.database.subscribe_reading(rig.database.session_epoch(), StreamRequest::Entity { model: "User".into(), id: "other".into(), fetch: EntityFetch::Force }, reader.clone());
    gate.entered().await;
    rig.database.cancel_stream(stream);
    gate.release();
    assert!(matches!(bound(rig.database.loaded(stream)).await, Err(DbError::Interrupted { .. })));
    assert!(reader.rows().is_empty());
    rig.database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn cancelling_a_delivery_wait_leaves_the_stream_running() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let gate = rig.world.hold_next("UserNode");
    let reader = Reader::open();
    let stream = rig.database.subscribe_reading(rig.database.session_epoch(), StreamRequest::Entity { model: "User".into(), id: "user".into(), fetch: EntityFetch::Force }, reader.clone());
    gate.entered_for("the entity request").await;
    let (started, waiting) = tokio::sync::oneshot::channel();
    let pending = { let reader = reader.clone(); tokio::spawn(async move {
        let _ = started.send(());
        loop {
            let changed = reader.changed.notified();
            if reader.deliveries().iter().any(|delivery| matches!(&delivery.value, StreamValue::Snapshot { state, .. } | StreamValue::Delta { state, .. } | StreamValue::State { state } if state.settled)) { return; }
            changed.await;
        }
    }) };
    bound(waiting).await.unwrap();
    pending.abort();
    assert!(bound(pending).await.unwrap_err().is_cancelled());
    gate.release();
    bound(async {
        loop {
            let changed = reader.changed.notified();
            if !reader.rows().is_empty() && reader.deliveries().iter().any(|delivery| matches!(&delivery.value, StreamValue::Snapshot { state, .. } | StreamValue::Delta { state, .. } | StreamValue::State { state } if state.settled)) { break; }
            changed.await;
        }
    }).await;
    assert_eq!(ids_of(reader.rows()), ["user"]);
    rig.database.cancel_stream(stream);
    rig.database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_mutation_answer_requires_one_reread_after_an_older_load() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let (values, args) = group("friends");
    let creation = bound(rig.database.create(epoch, "Group".into(), values, args)).await.unwrap();
    let crate::ffi::WireOperationOutcome::Confirmed { value: Some(created), .. } = bound(rig.database.outcome(creation)).await.unwrap() else { panic!("the group was not confirmed") };
    rig.world.set_members("JOINED", &["u1", "u2"]);
    let query = json!({"model":"Member", "relation":"group_members", "params":{"groupId":created.id,"order":"JOINED"}}).to_string();
    let reader = Reader::open();
    let stream = rig.database.subscribe_reading(epoch, StreamRequest::Query { query_json: query }, reader.clone());
    bound(rig.database.loaded(stream)).await.unwrap();
    let before = rig.world.count("GroupMembers");
    let older = rig.world.hold_next("GroupMembers");
    let response = rig.world.hold_next("RemoveMember");
    let reread = rig.world.hold_next("GroupMembers");
    let refresh = { let database = rig.database.clone(); tokio::spawn(async move { database.refresh_stream(stream).await }) };
    older.entered().await;
    let action = { let database = rig.database.clone(); let id = created.id.clone(); tokio::spawn(async move { database.act(epoch, "Group".into(), id, "remove_member".into(), "{}".into(), json!({"userIds":["u1"]}).to_string()).await }) };
    response.entered().await;
    rig.world.set_members("JOINED", &["u2", "u3"]);
    response.release();
    let receipt = bound(action).await.unwrap().unwrap();
    assert!(matches!(bound(rig.database.outcome(receipt)).await.unwrap(), crate::ffi::WireOperationOutcome::Confirmed { .. }));
    assert_eq!(rig.world.count("GroupMembers"), before + 1);
    older.release();
    reread.entered().await;
    assert_eq!(rig.world.count("GroupMembers"), before + 2);
    reread.release();
    bound(refresh).await.unwrap().unwrap();
    bound(async {
        loop {
            let changed = reader.changed.notified();
            if reader.rows().iter().map(|row| row.id.as_str()).collect::<Vec<_>>() == ["u2", "u3"] { break; }
            changed.await;
        }
    }).await;
    assert_eq!(rig.world.count("GroupMembers"), before + 2);
    rig.database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_receipt_revision_arrives_with_the_updated_row() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let reader = Reader::open();
    let stream = rig.database.subscribe_reading(epoch, profile(), reader.clone());
    bound(rig.database.loaded(stream)).await.unwrap();
    let receipt = bound(rig.database.update(epoch, "User".into(), "user".into(), json!({"name":"B"}).to_string(), "{}".into())).await.unwrap();
    bound(async {
        loop {
            let changed = reader.changed.notified();
            if reader.deliveries().iter().any(|delivery| match &delivery.value {
                StreamValue::Snapshot { revision, .. } | StreamValue::Delta { revision, .. } => *revision >= receipt.revision,
                _ => false,
            }) { break; }
            changed.await;
        }
    }).await;
    assert!(reader.shows("B"));
    rig.database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_fresh_profile_is_read_again_after_expiry_and_keeps_rows_on_error() {
    let rig = Rig::plain(World::new("server", "A", true), false).await;
    let epoch = rig.database.session_epoch();
    let load = || rig.database.load(epoch, "User".into(), "profile".into(), "{}".into());
    bound(load()).await.unwrap();
    let reader = Reader::open();
    let stream = rig.database.subscribe_reading(epoch, profile(), reader.clone());
    bound(rig.database.loaded(stream)).await.unwrap();
    rig.clock.advance(299_000);
    bound(load()).await.unwrap();
    assert_eq!(rig.world.log().iter().filter(|call| call.operation == "MyProfile").count(), 1);
    rig.clock.advance(2_000);
    bound(load()).await.unwrap();
    assert_eq!(rig.world.log().iter().filter(|call| call.operation == "MyProfile").count(), 2);
    rig.clock.advance(301_000);
    let down = rig.world.hold_then("MyProfile", Fault::Down);
    let (outcome, ()) = tokio::join!(bound(load()), network_drops_during(&rig, &down));
    assert!(outcome.is_ok() || matches!(&outcome, Err(DbError::Network { .. })), "unexpected load outcome: {outcome:?}");
    bound(async {
        loop {
            let changed = reader.changed.notified();
            if reader.deliveries().iter().any(|delivery| match &delivery.value {
                StreamValue::Snapshot { state, .. } | StreamValue::Delta { state, .. } | StreamValue::State { state } => state.error.is_some(),
                _ => false,
            }) { break; }
            changed.await;
        }
    }).await;
    assert_eq!(rig.world.log().iter().filter(|call| call.operation == "MyProfile").count(), 3);
    assert!(reader.shows("A"));
    assert!(reader.deliveries().iter().any(|delivery| match &delivery.value {
        StreamValue::Snapshot { state, .. } | StreamValue::Delta { state, .. } | StreamValue::State { state } => state.phase == LivePhase::Loaded && matches!(&state.error, Some(DbError::Network { .. })),
        _ => false,
    }));
    rig.database.close_now();
}

/// Hears that a stream has values and reads nothing: the reader of a test reads the journal itself.
struct Heard(tokio::sync::mpsc::UnboundedSender<()>);

impl crate::ffi::ValueSink for Heard {
    fn available(&self) { let _ = self.0.send(()); }
}

fn chat_entry(index: usize) -> serde_json::Value {
    json!({"topic":"MESSAGE","action":"UPDATED","recordId":format!("m{index}"),"chat":{"id":format!("c{index}"),"title":"t","revision":1,"lists":["PRIMARY"]}})
}

/// S209: a reader of events that reads nothing while 400 events arrive takes 255 events and one lost range, which add up to the 400 sent, then the end after the database closed, without the events it did not read before the end. Observed: the values `read_stream` returns. Breaks when `Journal::evict` drops an event without keeping its position, or when `Facts::finish` keeps the values of the old session.
#[tokio::test(flavor = "multi_thread")]
async fn a_slow_events_reader_reads_the_kept_events_and_one_exact_lost_range_then_one_end() {
    let (database, transport, _) = chat_events().await;
    let (available, mut heard) = tokio::sync::mpsc::unbounded_channel();
    let stream = database.subscribe(database.session_epoch(), StreamRequest::Events { model: "Chat".into() }, Arc::new(Heard(available)));
    let opened = bound(async {
        loop {
            if let Some(delivery) = database.read_stream(stream) { return delivery; }
            crate::test_wait::within(heard.recv()).await;
        }
    }).await;
    assert!(matches!(&opened.value, StreamValue::State { state } if state.settled && state.error.is_none()), "the first value is the open source: {opened:?}");
    assert_eq!(opened.seq, 1);

    transport.emit(json!({"data":{"recordEvents":{"entries":(0..400).map(chat_entry).collect::<Vec<_>>()}}}).to_string()).unwrap();
    bound(crate::ffi::settled(&database)).await.unwrap();
    let burst: Vec<StreamDelivery> = std::iter::from_fn(|| database.read_stream(stream)).collect();
    let events: Vec<String> = burst.iter().filter_map(|delivery| match &delivery.value { StreamValue::Event { event } => Some(event.id.clone()), _ => None }).collect();
    let lost: Vec<(u64, u64)> = burst.iter().filter_map(|delivery| match delivery.value { StreamValue::Lost { first, last } => Some((first, last)), _ => None }).collect();
    assert_eq!(lost, [(2, 146)], "the positions of the evicted events form one range before the kept ones");
    assert_eq!(events.len() as u64 + lost.iter().map(|(first, last)| last - first + 1).sum::<u64>(), 400, "kept events and lost positions add up to the events sent");
    assert_eq!(events.first().map(String::as_str), Some("c145"), "the first kept event follows the lost range");
    assert_eq!(events.last().map(String::as_str), Some("c399"));

    transport.emit(json!({"data":{"recordEvents":{"entries":(400..405).map(chat_entry).collect::<Vec<_>>()}}}).to_string()).unwrap();
    bound(crate::ffi::settled(&database)).await.unwrap();
    database.close_now();
    let after: Vec<StreamValue> = std::iter::from_fn(|| database.read_stream(stream)).map(|delivery| delivery.value).collect();
    assert!(matches!(after.as_slice(), [StreamValue::Ended { reason: StreamEnd::Closed }]), "after the close only one end follows: {after:?}");
    assert_eq!(database.stream_census().open, 0);
}

/// A database whose chats arrive through a change feed with the chat in its payload.
async fn chat_events() -> (Arc<Database>, Arc<Switchable>, Arc<World>) {
    use crate::schema::{Coverage, FieldSpec, Membership, ModelSpec, NodeRead, RelationSpec, RemoteShape, VariableSource, NODE_PARAM};
    use crate::value::ScalarKind;
    let mut revision = FieldSpec::scalar("serverRevision", ScalarKind::Int);
    revision.from = Some("revision".into());
    let mut chat = ModelSpec::new("Chat", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("title", ScalarKind::Str), revision, FieldSpec::list("lists", ScalarKind::Str)]);
    chat.server_revision = Some("serverRevision".into());
    let mut node = RelationSpec::new("node", &[]);
    node.text_params = vec![NODE_PARAM.into()];
    node.node = Some(NodeRead::One);
    let mut remote = crate::sync::rig::remote("ChatNode", RemoteShape::Single, "5.minutes");
    remote.variables.insert("id".into(), VariableSource::Param { name: NODE_PARAM.into() });
    node.remote = Some(remote);
    let mut list = RelationSpec::new("list", &[]);
    list.text_params.push("list".into());
    list.membership.insert("list".into(), Membership::Contains("lists".into()));
    list.joins_node = true;
    list.joins_events = true;
    let mut listed = crate::sync::rig::remote("ChatList", RemoteShape::List { coverage: Coverage::Complete }, "5.minutes");
    listed.variables.insert("list".into(), VariableSource::Param { name: "list".into() });
    list.remote = Some(listed);
    chat.relations.extend([node, list]);
    chat.changes.push(serde_json::from_value(json!({"area":"MESSAGE","relation":"node","payload":"chat"})).unwrap());
    chat.change_feed = Some(crate::schema::ChangeFeed { subscription: "RecordEvents".into(), root: "recordEvents.entries".into(), area: "topic".into(), kind: "action".into(), id: "recordId".into() });
    let world = World::new("server", "A", true);
    let transport = Switchable::new(Some(world.clone()), false);
    let database = bound(Database::open(crate::ffi::Config { path: None, data_version: None, cache_rows: 32 }, serde_json::to_string(&vec![chat]).unwrap(), Some(transport.clone()), None, None, None, None, None)).await.unwrap();
    database.register_operations(json!([
        {"name":"ChatList","document":"query ChatList($list: String) { chats(list: $list) { id title revision lists } }"},
        {"name":"ChatNode","document":"query ChatNode($id: ID!) { node(id: $id) { ... on Chat { id title revision lists } } }"},
        {"name":"RecordEvents","document":"subscription RecordEvents { recordEvents { entries { topic action recordId chat { id title revision lists } } } }"}
    ]).to_string()).unwrap();
    (database, transport, world)
}

#[tokio::test(flavor = "multi_thread")]
async fn an_event_opened_before_linked_lists_reports_the_committed_payload() {
    let (database, transport, world) = chat_events().await;
    let epoch = database.session_epoch();
    let reader = Reader::open();
    let events = database.subscribe_reading(epoch, StreamRequest::Events { model: "Chat".into() }, reader.clone());
    bound(database.loaded(events)).await.unwrap();
    let params = |list: &str| json!({"list":list}).to_string();
    for list in ["PRIMARY", "SECONDARY"] { bound(database.load(epoch, "Chat".into(), "list".into(), params(list))).await.unwrap(); }
    let before = bound(database.node(epoch, "Chat".into(), "node".into(), "c1".into(), true)).await.unwrap().unwrap();
    assert!(named(&before, "before"));
    let query = |list: &str| json!({"model":"Chat","relation":"list","params":{"list":list}}).to_string();
    assert_eq!(ids_of(bound(database.read_query(epoch, query("SECONDARY"))).await.unwrap()), ["c1"]);
    transport.emit(json!({"data":{"recordEvents":{"entries":[{"topic":"MESSAGE","action":"UPDATED","recordId":"m1","chat":{"id":"c1","title":"after","revision":2,"lists":["PRIMARY"]}}]}}}).to_string()).unwrap();
    bound(async {
        loop {
            let changed = reader.changed.notified();
            if reader.deliveries().iter().any(|delivery| matches!(&delivery.value, StreamValue::Event { .. })) { break; }
            changed.await;
        }
    }).await;
    let event = reader.deliveries().into_iter().find_map(|delivery| match delivery.value { StreamValue::Event { event } => Some(event), _ => None }).unwrap();
    assert_eq!(event.id, "c1");
    assert!(event.before.as_ref().is_some_and(|row| named(row, "before")));
    assert!(event.after.as_ref().is_some_and(|row| named(row, "after")));
    let primary = bound(database.read_query(epoch, query("PRIMARY"))).await.unwrap();
    assert_eq!(ids_of(primary.clone()), ["c1"]);
    assert!(named(&primary[0], "after"));
    assert!(bound(database.read_query(epoch, query("SECONDARY"))).await.unwrap().is_empty());
    let calls = world.log().into_iter().map(|entry| entry.operation).collect::<Vec<_>>();
    assert_eq!(calls, ["subscribe:RecordEvents", "ChatList", "ChatList", "ChatNode"]);
    database.close_now();
}

/// B01: after the login a stream delivers the account as one snapshot in the epoch of the session, in order, and the first value does not come from the calling thread.
#[tokio::test(flavor = "multi_thread")]
async fn a_stream_opened_after_the_login_starts_with_a_snapshot_of_the_account_in_its_epoch() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let (reader, caller) = (Reader::open(), std::thread::current().id());
    let stream = rig.database.subscribe_reading(epoch, profile(), reader.clone());
    let newest = bound(rig.database.loaded(stream)).await.unwrap();
    yield_until("the stream shows A", || reader.shows("A")).await;
    let log = reader.deliveries();
    assert!(log.iter().any(|delivery| delivery.seq == newest), "loaded names {newest}, a sequence number the reader did not receive: {log:?}");
    assert!(matches!(log[0].value, StreamValue::Snapshot { .. }), "the first value is a snapshot: {:?}", log[0]);
    assert!(log.iter().all(|delivery| delivery.epoch == epoch && delivery.stream == stream), "every delivery names the stream and its epoch");
    assert!(log.iter().enumerate().all(|(index, delivery)| delivery.seq == index as u64 + 1), "the sequence counts from 1 with no gap");
    assert!(reader.threads.lock().unwrap().iter().all(|thread| *thread != caller), "a delivery ran on the thread that subscribed");
    rig.database.close_now();
}

/// B02: the sign-out ends the stream with one outcome and no value of the account follows. Nothing stays open in the core.
#[tokio::test(flavor = "multi_thread")]
async fn the_session_reset_ends_every_stream_with_one_outcome_and_no_account_value_follows() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let (profile_log, users_log) = (Reader::open(), Reader::open());
    let streams = [rig.database.subscribe_reading(epoch, profile(), profile_log.clone()), rig.database.subscribe_reading(epoch, users(), users_log.clone())];
    for stream in streams { bound(rig.database.loaded(stream)).await.unwrap(); }
    yield_until("both streams show A", || profile_log.shows("A") && users_log.shows("A")).await;

    bound(rig.database.session_reset()).await.unwrap();
    settle(&rig).await;

    for log in [&profile_log, &users_log] {
        assert_eq!(log.ended(), vec![StreamEnd::SessionEnded], "exactly one outcome");
        assert!(matches!(log.deliveries().last().unwrap().value, StreamValue::Ended { .. }), "nothing follows the outcome");
    }
    assert_eq!(rig.database.stream_census().open, 0);
    assert_eq!(rig.database.subscription_census().subscriptions, 0);
    let marks = (profile_log.len(), users_log.len());
    rig.world.forbid(true);
    settle(&rig).await;
    assert!(rig.world.violations().is_empty(), "a read followed the exit: {:?}", rig.world.violations());
    assert_eq!(marks, (profile_log.len(), users_log.len()));
    rig.database.close_now();
}

/// B03: the account switch A -> B gives new streams in a new epoch. The old projection ends, B never shows A and A never shows B.
#[tokio::test(flavor = "multi_thread")]
async fn an_account_switch_gives_new_streams_and_no_value_of_a_reaches_b() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch_a = rig.database.session_epoch();
    let (first_profile, first_users) = (Reader::open(), Reader::open());
    for (request, log) in [(profile(), &first_profile), (users(), &first_users)] {
        let stream = rig.database.subscribe_reading(epoch_a, request, log.clone());
        bound(rig.database.loaded(stream)).await.unwrap();
    }
    yield_until("A shows", || first_profile.shows("A") && first_users.shows("A")).await;

    bound(rig.database.session_reset()).await.unwrap();
    rig.login_ios("B").await;
    let epoch_b = rig.database.session_epoch();
    assert_ne!(epoch_a, epoch_b, "the switch moved the epoch");
    let (second_profile, second_users) = (Reader::open(), Reader::open());
    for (request, log) in [(profile(), &second_profile), (users(), &second_users)] {
        let stream = rig.database.subscribe_reading(epoch_b, request, log.clone());
        bound(rig.database.loaded(stream)).await.unwrap();
    }
    yield_until("B shows", || second_profile.shows("B") && second_users.shows("B")).await;
    settle(&rig).await;

    assert!(!second_profile.ever_showed("A") && !second_users.ever_showed("A"), "a value of A reached the projections of B");
    assert!(!first_profile.ever_showed("B") && !first_users.ever_showed("B"), "a value of B reached the projections of A");
    assert!(second_profile.deliveries().iter().all(|delivery| delivery.epoch == epoch_b));
    assert_eq!(first_profile.ended(), vec![StreamEnd::SessionEnded]);
    rig.database.close_now();
}

/// B04: a message sent offline shows at once, goes out once when the network returns, and keeps its position when the server confirms it.
#[tokio::test(flavor = "multi_thread")]
async fn a_message_sent_offline_goes_out_once_and_keeps_its_position() {
    let rig = Rig::encrypted().await;
    rig.world.seed_message("A", "m1", "first", 1);
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let reader = Reader::open();
    let stream = rig.database.subscribe_reading(epoch, thread(), reader.clone());
    bound(rig.database.loaded(stream)).await.unwrap();
    yield_until("the thread shows the first message", || reader.texts().iter().any(|text| text.contains("first"))).await;

    rig.database.set_online(false).unwrap();
    let receipt = bound(rig.database.send(message(epoch, "second", 2))).await.unwrap();
    yield_until("the thread shows the queued message", || reader.rows().len() == 2).await;
    let queued = reader.rows();
    assert_eq!(queued[1].id, receipt.optimistic_value.as_ref().unwrap().id, "the new message sits after the first");
    assert_eq!(queued[1].sync.state, WireSyncState::Pending);
    assert_eq!(receipt.optimistic_value.as_ref().unwrap().sync.state, WireSyncState::Pending);
    assert_eq!(rig.world.count("SendMessage"), 0, "the offline send reached the server");

    rig.database.set_online(true).unwrap();
    let outcome = bound(rig.database.outcome(receipt.clone())).await.unwrap();
    assert!(matches!(outcome, WireOperationOutcome::Confirmed { value: Some(row), .. } if row.id == "m2"));
    yield_until("the thread shows the confirmed message", || reader.rows().get(1).is_some_and(|row| row.id == "m2")).await;
    settle(&rig).await;

    assert_eq!(rig.world.count("SendMessage"), 1, "the message went out once");
    let confirmed = reader.rows();
    assert_eq!(confirmed.len(), 2, "one message, not a temporary row and a confirmed row");
    assert_eq!((confirmed[0].id.as_str(), confirmed[1].id.as_str()), ("m1", "m2"), "the position did not change");
    assert_eq!(confirmed[1].sync.state, WireSyncState::Synced);
    assert_eq!(reader.gaps(), 0);
    rig.database.close_now();
}

/// B05: an answer of the old epoch that arrives late changes nothing: the call ends as interrupted, the new projection never shows it, and a command of the old epoch is refused.
#[tokio::test(flavor = "multi_thread")]
async fn a_late_answer_of_the_old_epoch_is_dropped_and_a_command_of_the_old_epoch_is_refused() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch_a = rig.database.session_epoch();
    let first = Reader::open();
    let stream = rig.database.subscribe_reading(epoch_a, users(), first.clone());
    bound(rig.database.loaded(stream)).await.unwrap();
    yield_until("A shows", || first.shows("A")).await;

    let gate = rig.world.hold_next("Users").as_account("A");
    let late = { let database = rig.database.clone(); tokio::spawn(async move { database.refresh_stream(stream).await }) };
    gate.entered().await;
    bound(rig.database.session_reset()).await.unwrap();
    rig.login_ios("B").await;
    let epoch_b = rig.database.session_epoch();
    let second = Reader::open();
    let next = rig.database.subscribe_reading(epoch_b, users(), second.clone());
    bound(rig.database.loaded(next)).await.unwrap();
    yield_until("B shows", || second.shows("B")).await;
    gate.release();

    let answer = bound(late).await.unwrap();
    assert!(matches!(answer, Err(DbError::Interrupted { .. })), "the late call of the old epoch ended as {answer:?}");
    settle(&rig).await;
    assert!(!second.ever_showed("A"), "the late answer of A reached the projection of B");
    assert!(second.shows("B"));
    let refused = bound(rig.database.send(message(epoch_a, "late", 3))).await;
    assert!(matches!(refused, Err(DbError::Interrupted { .. })), "a command of the old epoch ran: {refused:?}");
    assert_eq!(rig.world.count("SendMessage"), 0);
    rig.database.close_now();
}

/// B06: the reader that cancels ends the stream in the core: no subscription stays and no value is delivered after.
#[tokio::test(flavor = "multi_thread")]
async fn the_reader_that_cancels_ends_the_stream_and_the_core_keeps_nothing() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let reader = Reader::open();
    let stream = rig.database.subscribe_reading(epoch, users(), reader.clone());
    bound(rig.database.loaded(stream)).await.unwrap();
    yield_until("A shows", || reader.shows("A")).await;
    assert_eq!(rig.database.stream_census().open, 1);

    rig.database.cancel_stream(stream);

    assert_eq!(rig.database.stream_census().open, 0);
    assert_eq!(rig.database.subscription_census().subscriptions, 0);
    assert!(reader.ended().is_empty(), "a cancelled stream delivers no outcome");
    let mark = reader.len();
    rig.world.set_account("A2");
    bound(rig.database.refresh(rig.database.session_epoch(), "User".into(), "all".into(), "{}".into())).await.unwrap();
    settle(&rig).await;
    assert_eq!(reader.len(), mark, "a value reached a cancelled reader");
    assert!(matches!(bound(rig.database.refresh_stream(stream)).await, Err(DbError::Interrupted { .. })));
    rig.database.close_now();
}

/// B07, spec F: an idle stream costs no request and no wakeup, a change of one row delivers one row, and the number of subscriptions is the number of readers.
#[tokio::test(flavor = "multi_thread")]
async fn work_counters_idle_zero_one_row_one_decode_and_subscriptions_equal_readers() {
    let rig = Rig::encrypted().await;
    for (at, text) in [(1, "one"), (2, "two"), (3, "three"), (4, "four"), (5, "five")] { rig.world.seed_message("A", &format!("s{at}"), text, at); }
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let (reader, other) = (Reader::open(), Reader::open());
    let stream = rig.database.subscribe_reading(epoch, thread(), reader.clone());
    let second = rig.database.subscribe_reading(epoch, profile(), other.clone());
    bound(rig.database.loaded(stream)).await.unwrap();
    bound(rig.database.loaded(second)).await.unwrap();
    yield_until("the thread holds 5 rows", || reader.rows().len() == 5).await;
    settle(&rig).await;
    assert_eq!(rig.database.stream_census().open, 2);
    assert_eq!(rig.database.subscription_census().subscriptions, 2, "subscriptions are readers");

    let idle = (rig.world.hits(), rig.database.db.counters().observer_calls, reader.len(), other.len());
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;
    assert_eq!(idle, (rig.world.hits(), rig.database.db.counters().observer_calls, reader.len(), other.len()), "an idle stream did work");

    let mut costs = Vec::new();
    for (index, text) in ["six", "seven"].into_iter().enumerate() {
        let (before, mark) = (rig.database.db.counters().rows_delivered, reader.len());
        let receipt = bound(rig.database.send(message(epoch, text, 6 + index as i64))).await.unwrap();
        bound(rig.database.outcome(receipt)).await.unwrap();
        settle(&rig).await;
        let delta_rows: Vec<usize> = reader.deliveries().iter().skip(mark).filter_map(|delivery| if let StreamValue::Delta { rows, .. } = &delivery.value { Some(rows.len()) } else { None }).collect();
        assert!(!delta_rows.is_empty() && delta_rows.iter().all(|rows| *rows == 1), "a change of one row carried {delta_rows:?} rows");
        costs.push((delta_rows.len(), rig.database.db.counters().rows_delivered - before));
    }
    assert_eq!(costs[0], costs[1], "the cost of one change grew with the list: {costs:?}");
    assert!(costs[1].1 <= 2 * costs[1].0, "the core delivered more rows than the deltas carry: {costs:?}");
    rig.database.close_now();
}

/// B15, spec F: a write to one row of a list delivers nothing to the entity stream of another row, and one value to a count stream, however long the list is.
#[tokio::test(flavor = "multi_thread")]
async fn work_counters_an_entity_stream_ignores_other_rows_and_a_count_stream_gets_one_value_per_write() {
    let rig = Rig::encrypted().await;
    for (at, text) in [(1, "one"), (2, "two"), (3, "three"), (4, "four"), (5, "five")] { rig.world.seed_message("A", &format!("s{at}"), text, at); }
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let (entity, count, list) = (Reader::open(), Reader::open(), Reader::open());
    let streams = [
        rig.database.subscribe_reading(epoch, StreamRequest::Entity { model: "Message".into(), id: "s1".into(), fetch: crate::ffi::EntityFetch::None }, entity.clone()),
        rig.database.subscribe_reading(epoch, StreamRequest::Count { query_json: r#"{"model":"Message","relation":"thread","params":{"chatId":"c1"}}"#.into() }, count.clone()),
        rig.database.subscribe_reading(epoch, thread(), list.clone()),
    ];
    for stream in streams { bound(rig.database.loaded(stream)).await.unwrap(); }
    yield_until("the list holds 5 rows", || list.rows().len() == 5).await;
    settle(&rig).await;
    assert_eq!(rig.database.subscription_census().subscriptions, 3, "subscriptions are readers");

    let mut costs = Vec::new();
    for (index, text) in ["six", "seven"].into_iter().enumerate() {
        let (entity_mark, count_mark) = (entity.len(), count.len());
        let receipt = bound(rig.database.send(message(epoch, text, 6 + index as i64))).await.unwrap();
        bound(rig.database.outcome(receipt)).await.unwrap();
        settle(&rig).await;
        let entity_values = entity.deliveries().len() - entity_mark;
        let counts: Vec<i64> = count.deliveries().iter().skip(count_mark).filter_map(|delivery| if let StreamValue::Count { count, .. } = &delivery.value { Some(*count) } else { None }).collect();
        assert_eq!(entity_values, 0, "a write to another row reached the entity stream");
        assert_eq!(counts.last().copied(), Some(6 + index as i64), "the count stream shows {counts:?}");
        costs.push(counts.len());
    }
    assert_eq!(costs[0], costs[1], "the cost of one write to the count stream grew with the list: {costs:?}");
    assert!(costs[1] <= 2, "one write delivered {} count values", costs[1]);
    rig.database.close_now();
}

/// A missed revision gives exactly one new snapshot, never a replay of the history.
#[tokio::test(flavor = "multi_thread")]
async fn a_missed_revision_gives_one_new_snapshot() {
    let rig = Rig::encrypted().await;
    rig.world.seed_message("A", "m1", "first", 1);
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let reader = Reader::open();
    let stream = rig.database.subscribe_reading(epoch, thread(), reader.clone());
    bound(rig.database.loaded(stream)).await.unwrap();
    yield_until("the thread shows the first message", || reader.rows().len() == 1).await;
    let snapshots = reader.snapshots();

    reader.lose_next.store(true, Ordering::SeqCst);
    bound(rig.database.send(message(epoch, "lost", 2))).await.unwrap();
    yield_until("the lost delivery was swallowed", || !reader.lose_next.load(Ordering::SeqCst)).await;
    bound(rig.database.send(message(epoch, "after", 3))).await.unwrap();
    yield_until("the delta after the gap arrived", || reader.gaps() == 1).await;

    rig.database.resync_stream(stream);
    yield_until("one new snapshot arrived", || reader.snapshots() == snapshots + 1).await;
    yield_until("the rows are whole again", || reader.rows().len() == 3).await;
    settle(&rig).await;
    assert_eq!(reader.snapshots(), snapshots + 1, "more than one snapshot answered the gap");
    rig.database.close_now();
}

/// B09 (R1, old line command.rs:46-55): a send that waits for the writer while the session ends writes no row. The commit takes the epoch of the caller, not the generation of the data, which moves later.
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::await_holding_lock)]
async fn a_send_that_waits_for_the_writer_while_the_session_ends_writes_no_row() {
    use crate::lifecycle::{Boundary, End};
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    rig.database.set_online(false).unwrap();
    let store = &rig.database.db.shared.store;
    let pinned = store.lifecycle.writes_entered.load(Ordering::SeqCst);
    let writer = store.lock();
    let sender = { let database = rig.database.clone(); tokio::spawn(async move { database.send(message(epoch, "late", 9)).await }) };
    yield_until("the send reached the writer", || store.lifecycle.writes_entered.load(Ordering::SeqCst) > pinned).await;
    let permit = store.lifecycle.try_begin(Boundary::SessionEnd(End::Local), Some(epoch)).ok().and_then(Result::ok).expect("the epoch of the caller is current");
    drop(writer);
    let sent = bound(sender).await.unwrap();
    assert!(matches!(sent, Err(DbError::Interrupted { .. })), "a send of the ended session ran: {sent:?}");
    yield_until("the admitted write finished", || store.lifecycle.writes_entered.load(Ordering::SeqCst) == pinned).await;
    assert_eq!(store.row_count("Message").unwrap(), 0, "the send wrote its row after its session ended");
    drop(permit);
    rig.database.close_now();
}

/// B10 (R2, old line command.rs:76-85): the outcome waits while the command is queued offline and is owned by the epoch of its receipt: it ends with the session, and a receipt of an old epoch stays ended.
#[tokio::test(flavor = "multi_thread")]
async fn the_outcome_of_an_offline_command_waits_and_ends_with_the_epoch_of_its_receipt() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    rig.database.set_online(false).unwrap();
    let receipt = bound(rig.database.send(message(epoch, "queued", 2))).await.unwrap();
    let outcome = { let database = rig.database.clone(); let receipt = receipt.clone(); tokio::spawn(async move { database.outcome(receipt).await }) };
    for _ in 0..50 { tokio::task::yield_now().await; }
    assert!(!outcome.is_finished(), "the outcome of a queued command returned before the command settled");
    bound(rig.database.session_reset()).await.unwrap();
    assert!(matches!(bound(outcome).await.unwrap().unwrap(), WireOperationOutcome::Interrupted));
    rig.database.set_online(true).unwrap();
    rig.login_ios("B").await;
    assert!(matches!(bound(rig.database.outcome(receipt)).await.unwrap(), WireOperationOutcome::Interrupted));
    assert_eq!(rig.world.count("SendMessage"), 0);
    rig.database.close_now();
}

/// B11 (R3, old line stream/mod.rs refresh_stream): a stream request whose caller left does not outlive its epoch: the transition does not wait for the request.
#[tokio::test(flavor = "multi_thread")]
async fn a_stream_request_whose_caller_left_ends_with_its_epoch_and_holds_nothing() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let reader = Reader::open();
    let stream = rig.database.subscribe_reading(epoch, users(), reader.clone());
    bound(rig.database.loaded(stream)).await.unwrap();
    let gate = rig.world.hold_next("Users").as_account("A");
    let refresh = { let database = rig.database.clone(); tokio::spawn(async move { database.refresh_stream(stream).await }) };
    gate.entered().await;
    refresh.abort();
    let _ = refresh.await;
    bound(rig.database.session_reset()).await.unwrap();
    rig.login_ios("B").await;
    gate.release();
    settle(&rig).await;
    assert!(!rig.database.streams.held().iter().any(|held| held.contains("stream")), "a request of the left caller holds a lease: {:?}", rig.database.streams.held());
    rig.database.close_now();
}

/// B12 (R4, old line stream/open.rs connect): a stream whose source cannot be connected ends with one outcome and leaves nothing in the table.
#[tokio::test(flavor = "multi_thread")]
async fn a_stream_that_cannot_connect_ends_once_and_leaves_the_table() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let requests = [
        StreamRequest::Entity { model: "Nope".into(), id: "x".into(), fetch: crate::ffi::EntityFetch::None },
        StreamRequest::Events { model: "Nope".into() },
        StreamRequest::Count { query_json: r#"{"model":"Nope"}"#.into() },
    ];
    for request in requests {
        let reader = Reader::open();
        let stream = rig.database.subscribe_reading(epoch, request.clone(), reader.clone());
        yield_until("the failed stream ended", || !reader.ended().is_empty()).await;
        assert_eq!(reader.ended(), vec![StreamEnd::Invalid], "{request:?} ended with more or less than one outcome");
        assert_eq!(rig.database.stream_census().open, 0, "{request:?} stayed in the table");
        assert!(bound(rig.database.loaded(stream)).await.is_err(), "{request:?} reported a first load");
    }
    rig.database.close_now();
}

/// A reader whose first delivery after arming waits for a release.
struct Blocking {
    inner: Arc<Reader>,
    armed: AtomicBool,
    finished: AtomicBool,
    entered: Mutex<std::sync::mpsc::Sender<()>>,
    release: Mutex<std::sync::mpsc::Receiver<()>>,
}

impl crate::ffi::stream::tape::Reader for Blocking {
    fn delivered(&self, delivery: StreamDelivery) {
        if self.armed.swap(false, Ordering::SeqCst) {
            self.entered.lock().unwrap().send(()).unwrap();
            let _ = self.release.lock().unwrap().recv_timeout(std::time::Duration::from_secs(20));
            self.inner.delivered(delivery);
            self.finished.store(true, Ordering::SeqCst);
            return;
        }
        self.inner.delivered(delivery);
    }
}

/// B13 (R5, B5; old line stream/owner.rs cancelled): `cancel_stream` returns while a delivery in the reader is still running, and no delivery begins after it returns.
#[tokio::test(flavor = "multi_thread")]
async fn the_cancel_of_a_reader_returns_while_a_delivery_runs_and_none_is_admitted_after_it() {
    let rig = Rig::encrypted().await;
    rig.world.seed_message("A", "m1", "first", 1);
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let (entered, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release) = std::sync::mpsc::channel();
    let blocking = Arc::new(Blocking { inner: Reader::open(), armed: AtomicBool::new(false), finished: AtomicBool::new(false), entered: Mutex::new(entered), release: Mutex::new(release) });
    let stream = rig.database.subscribe_reading(epoch, thread(), blocking.clone());
    bound(rig.database.loaded(stream)).await.unwrap();
    yield_until("the thread shows the first message", || blocking.inner.texts().iter().any(|text| text.contains("first"))).await;
    rig.database.set_online(false).unwrap();
    blocking.armed.store(true, Ordering::SeqCst);
    bound(rig.database.send(message(epoch, "second", 2))).await.unwrap();
    tokio::task::spawn_blocking(move || entered_rx.recv_timeout(std::time::Duration::from_secs(10)).unwrap()).await.unwrap();
    rig.database.cancel_stream(stream);
    assert!(!blocking.finished.load(Ordering::SeqCst), "the cancel waited for the reader to finish a delivery");
    release_tx.send(()).unwrap();
    yield_until("the delivery in progress finished", || blocking.finished.load(Ordering::SeqCst)).await;
    let after_cancel = blocking.inner.len();
    bound(rig.database.send(message(epoch, "third", 3))).await.unwrap();
    rig.settle().await;
    assert_eq!(blocking.inner.len(), after_cancel, "a delivery began after the cancel returned");
    assert!(!blocking.inner.texts().iter().any(|text| text.contains("third")));
    rig.database.close_now();
}

pub(super) fn group(name: &str) -> (String, String) { (json!({"name": name}).to_string(), "{}".into()) }

async fn confirmed_group(rig: &Rig, epoch: u64, values: String, args: String) -> WireRow {
    let receipt = bound(rig.database.create(epoch, "Group".into(), values, args)).await.unwrap();
    match bound(rig.database.outcome(receipt)).await.unwrap() {
        WireOperationOutcome::Confirmed { value: Some(row), .. } => row,
        other => panic!("group create did not confirm: {other:?}"),
    }
}

/// K01 (B1, J02): a send that passed its epoch check and waits before its SQL commit lands before the next sign-in changes the epoch, and its announcement is held until the session start finished: the operation of the account that left is in the cutoff and never goes out for the account that came.
#[tokio::test(flavor = "multi_thread")]
async fn a_send_between_its_epoch_check_and_its_commit_never_goes_out_for_the_next_account() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    rig.database.set_online(false).unwrap();
    let store = &rig.database.db.shared.store;
    let operations_begun = |rig: &Rig| rig.database.db.shared.read(&[], |reader| Ok(reader.connection().query_row("SELECT COALESCE((SELECT seq FROM sqlite_sequence WHERE name = 'op'), 0)", [], |row| row.get::<_, i64>(0))?)).unwrap();
    let begun_before = operations_begun(&rig);
    let (entered, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release) = std::sync::mpsc::channel::<()>();
    let expired = Arc::new(AtomicBool::new(false));
    let release_expired = expired.clone();
    *store.after_epoch_check.lock().unwrap() = Some(Box::new(move || { entered.send(()).unwrap(); if release.recv_timeout(std::time::Duration::from_secs(20)).is_err() { release_expired.store(true, Ordering::SeqCst); } }));
    let (committed, committed_rx) = std::sync::mpsc::channel();
    let (publish_tx, publish_release) = std::sync::mpsc::channel::<()>();
    let (committed, publish_release, held) = (Mutex::new(committed), Mutex::new(publish_release), AtomicBool::new(true));
    let publish_expired = expired.clone();
    *store.after_commit.lock().unwrap() = Some(Box::new(move |result| {
        if result.queue_operations.is_empty() || !held.swap(false, Ordering::SeqCst) { return; }
        committed.lock().unwrap().send(()).unwrap();
        if publish_release.lock().unwrap().recv_timeout(std::time::Duration::from_secs(20)).is_err() { publish_expired.store(true, Ordering::SeqCst); }
    }));
    let sender = { let database = rig.database.clone(); tokio::spawn(async move { database.send(message(epoch, "late", 9)).await }) };
    tokio::task::spawn_blocking(move || entered_rx.recv_timeout(std::time::Duration::from_secs(10)).unwrap()).await.unwrap();
    let waits = *store.lifecycle.begin_waits.0.lock().unwrap();
    assert_eq!(rig.database.session_epoch(), epoch);
    let identity = { let database = rig.database.clone(); tokio::task::spawn_blocking(move || database.db.shared.store.lifecycle.session_from(Some("another-account".into()), database.db.shared.store.lifecycle.admit_passive(crate::lifecycle::Origin::Explicit, "test session", Some(epoch)).unwrap()).is_ok()) };
    yield_until("the session start reached the commit line", || *store.lifecycle.begin_waits.0.lock().unwrap() > waits).await;
    assert_eq!(rig.database.session_epoch(), epoch, "the epoch changed while the commit of the send was held");
    release_tx.send(()).unwrap();
    tokio::task::spawn_blocking(move || committed_rx.recv_timeout(std::time::Duration::from_secs(10)).unwrap()).await.unwrap();
    assert!(bound(identity).await.unwrap(), "the session start of the other identity did not begin");
    publish_tx.send(()).unwrap();
    let sent = bound(sender).await.unwrap();
    assert!(sent.is_ok() || matches!(sent, Err(DbError::Interrupted { .. })), "the send ended with {:?}", sent.as_ref().err());
    assert!(!expired.load(Ordering::SeqCst), "a held step ran out of time and was not released by the test");
    assert_ne!(rig.database.session_epoch(), epoch);
    assert!(operations_begun(&rig) > begun_before, "the send did not commit before the epoch changed");
    settle(&rig).await;
    rig.database.set_online(true).unwrap();
    bound(rig.database.flush()).await.unwrap();
    settle(&rig).await;
    assert_eq!(rig.world.count("SendMessage"), 0, "the message of the account that left went out for the next one");
    assert!(rig.database.db.outbox().unwrap().is_empty(), "an operation of the account that left is still queued");
    rig.database.close_now();
}

/// K02 (B2, old lines ffi/database.rs update, destroy, read_query, read_row): a command or a one-off read that carries the epoch of A is refused after the session ended, also when it waited for the writer while the session ended, and it changes nothing for B.
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::await_holding_lock)]
async fn a_command_and_a_read_of_a_left_account_are_refused_and_change_nothing() {
    use crate::lifecycle::{Boundary, End};
    let rig = Rig::encrypted().await;
    rig.world.seed_message("B", "b1", "hello", 1);
    rig.login_ios("A").await;
    let old = rig.database.session_epoch();
    bound(rig.database.session_reset()).await.unwrap();
    rig.login_ios("B").await;
    let current = rig.database.session_epoch();
    assert_ne!(old, current);
    let users = r#"{"model":"User","relation":"all","params":{}}"#;
    let refused = |result: Result<(), DbError>| matches!(result, Err(DbError::Interrupted { .. }));
    assert!(refused(bound(rig.database.read_query(old, users.into())).await.map(|_| ())), "a read of A returned rows in the epoch of B");
    assert!(refused(bound(rig.database.read_row(old, "User".into(), "user".into())).await.map(|_| ())));
    assert!(refused(bound(rig.database.update(old, "User".into(), "user".into(), json!({"name":"hijacked"}).to_string(), "{}".into())).await.map(|_| ())));
    assert!(refused(bound(rig.database.destroy(old, "User".into(), "user".into())).await.map(|_| ())));
    assert!(refused(bound(rig.database.set_local(old, "User".into(), "user".into(), "name".into(), "\"x\"".into())).await));
    assert!(refused(bound(rig.database.clone().upload_blob(old, "n".into(), "create".into(), crate::ffi::upload_wire::WireUploadRequest { path: "/nowhere".into(), name: None, mime: None, variables_json: "{}".into() })).await.map(|_| ())));
    let seen = bound(rig.database.read_row(current, "User".into(), "user".into())).await.unwrap().expect("the row of B");
    assert!(named(&seen, "B"), "the row of B changed");

    let store = &rig.database.db.shared.store;
    let pinned = store.lifecycle.writes_entered.load(Ordering::SeqCst);
    let writer = store.lock();
    let late = { let database = rig.database.clone(); tokio::spawn(async move { database.update(current, "User".into(), "user".into(), json!({"name":"late"}).to_string(), "{}".into()).await }) };
    yield_until("the update reached the writer", || store.lifecycle.writes_entered.load(Ordering::SeqCst) > pinned).await;
    let permit = store.lifecycle.try_begin(Boundary::SessionEnd(End::Local), Some(current)).ok().and_then(Result::ok).expect("the epoch of the caller is current");
    drop(writer);
    assert!(refused(bound(late).await.unwrap().map(|_| ())), "an update that waited for the writer while the session ended ran");
    drop(permit);
    rig.database.close_now();
}

/// K03 (B3, old lines ffi/command.rs in_epoch with Origin::Explicit and sync/send/flush.rs waiting_reason): a send needs a session, and an action that is not a durable command does not wait for the network: it is refused and rolled back, and it never goes out later.
#[tokio::test(flavor = "multi_thread")]
async fn a_send_needs_a_session_and_a_plain_action_never_waits_for_the_network() {
    let rig = Rig::encrypted().await;
    let epoch = rig.database.session_epoch();
    let sent = bound(rig.database.send(message(epoch, "anonymous", 1))).await;
    assert!(matches!(sent, Err(DbError::NoSession { .. })), "a send without a session was accepted: {sent:?}");
    assert!(rig.database.db.outbox().unwrap().is_empty(), "a send without a session left an operation");
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let (values, args) = group("g");
    let admitted = rig.database.db.admit().unwrap();
    rig.database.db.create_with(&admitted, "Group", serde_json::from_str(&values).unwrap(), serde_json::from_str(&args).unwrap()).unwrap();
    drop(admitted);
    assert_eq!(rig.database.db.outbox().unwrap().len(), 1, "the plain action was queued once");
    rig.database.set_online(false).unwrap();
    bound(rig.database.flush()).await.unwrap();
    assert!(rig.database.db.outbox().unwrap().is_empty(), "a plain action stayed queued while the network was away");
    assert_eq!(rig.database.db.shared.store.row_count("Group").unwrap(), 0, "the row of the refused action stayed");
    rig.database.set_online(true).unwrap();
    bound(rig.database.flush()).await.unwrap();
    settle(&rig).await;
    assert_eq!(rig.world.count("CreateGroup"), 0, "a plain action went out after the network returned");
    let receipt = bound(rig.database.send(message(epoch, "offline", 2))).await.unwrap();
    assert_eq!(receipt.optimistic_value.as_ref().unwrap().sync.state, WireSyncState::Pending);
    rig.database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn server_calls_without_a_transport_report_no_session() {
    let rig = Rig::encrypted().await;
    bound(rig.database.close_database()).await.unwrap();
    let database = rig.open_again(None).await.unwrap();
    let epoch = database.session_epoch();
    let (values, args) = group("friends");
    let create = bound(database.create(epoch, "Group".into(), values, args)).await;
    let load = bound(database.load(epoch, "User".into(), "profile".into(), "{}".into())).await;
    let send = bound(database.send(message(epoch, "hello", 1))).await;
    let act = bound(database.act(epoch, "Group".into(), "g1".into(), "leave".into(), "{}".into(), "{}".into())).await;
    assert!(matches!(create, Err(DbError::NoSession { .. })), "create: {create:?}");
    assert!(matches!(load, Err(DbError::NoSession { .. })), "load: {load:?}");
    assert!(matches!(send, Err(DbError::NoSession { .. })), "send: {send:?}");
    assert!(matches!(act, Err(DbError::NoSession { .. })));
    database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn reset_clears_ephemeral_values_before_the_next_account_reads_the_same_id() {
    use crate::schema::{EphemeralSpec, FieldSpec, ModelSpec};
    use crate::value::ScalarKind;
    let mut user: Vec<ModelSpec> = serde_json::from_str(&crate::ffi::lifecycle_tests::models()).unwrap();
    user[0].ephemeral.push(EphemeralSpec::new(FieldSpec::scalar("typing", ScalarKind::Bool).with_default(json!(false))));
    let world = super::rig_tests::World::new("server", "A", true);
    let transport = super::rig_tests::Switchable::new(Some(world.clone()), false);
    let database = Database::open(crate::ffi::Config { path: None, data_version: None, cache_rows: 8 }, serde_json::to_string(&user).unwrap(), Some(transport), None, None, None, None, None).await.unwrap();
    database.register_operations(r#"[{"name":"MyProfile","document":"query MyProfile { me { id name } }"}]"#.into()).unwrap();
    let epoch = database.session_epoch();
    bound(database.load(epoch, "User".into(), "profile".into(), "{}".into())).await.unwrap();
    bound(database.set_ephemeral(epoch, "User".into(), "user".into(), r#"{"typing":true}"#.into())).await.unwrap();
    let old = bound(database.read_row(epoch, "User".into(), "user".into())).await.unwrap().unwrap();
    assert!(matches!(old.cells.last(), Some(crate::ffi::Cell::Bool { value: true })));

    bound(database.session_reset()).await.unwrap();
    world.set_account("B");
    bound(database.load(database.session_epoch(), "User".into(), "profile".into(), "{}".into())).await.unwrap();
    let current = bound(database.read_row(database.session_epoch(), "User".into(), "user".into())).await.unwrap().unwrap();

    assert!(named(&current, "B"));
    assert!(matches!(current.cells.last(), Some(crate::ffi::Cell::Bool { value: false })));
    database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn an_old_subscription_sink_is_closed_before_the_next_account() {
    let world = super::rig_tests::World::new("server", "A", true);
    let rig = Rig::plain(world.clone(), false).await;
    let epoch = rig.database.session_epoch();
    let stream = rig.database.subscribe_reading(epoch, StreamRequest::Query { query_json: r#"{"model":"Notice","relation":"recent","params":{}}"#.into() }, Reader::open());
    let (token, old_sink) = rig.switch.first_sink().await;

    bound(rig.database.session_reset()).await.unwrap();
    rig.switch.wait_unsubscribed(&token).await;
    world.set_account("B");
    bound(rig.database.load(rig.database.session_epoch(), "User".into(), "profile".into(), "{}".into())).await.unwrap();
    old_sink.event(r#"{"data":{"notices":{"id":"old"}}}"#.into()).unwrap();
    let rows = bound(rig.database.read_query(rig.database.session_epoch(), r#"{"model":"Notice","relation":"recent","params":{}}"#.into())).await.unwrap();

    assert!(rows.is_empty());
    assert!(rig.switch.unsubscribed().contains(&token));
    rig.database.cancel_stream(stream);
    rig.database.close_now();
}

/// K04 (B4, old line ffi/database.rs create): a create the server answers goes through `create` with the epoch of the caller, needs the network, and returns the row the server confirmed.
#[tokio::test(flavor = "multi_thread")]
async fn a_server_create_returns_the_confirmed_row_and_needs_the_network() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let (values, args) = group("friends");
    let row = confirmed_group(&rig, epoch, values.clone(), args.clone()).await;
    assert!(!row.id.starts_with("tmp") && !row.id.contains("temp"), "the created row still has a temporary id: {}", row.id);
    assert!(named(&row, "friends"));
    assert_eq!(rig.world.count("CreateGroup"), 1);
    rig.database.set_online(false).unwrap();
    let offline = bound(rig.database.create(epoch, "Group".into(), values, args)).await;
    assert!(matches!(offline, Err(DbError::Network { .. })), "an offline server create did not fail at once: {offline:?}");
    assert_eq!(rig.world.count("CreateGroup"), 1);
    assert!(rig.database.db.outbox().unwrap().is_empty());
    rig.database.close_now();
}

/// A reader whose last reference decides where the platform object dies.
struct DropProbe { held: Arc<Mutex<Vec<String>>>, dropped: Arc<std::sync::atomic::AtomicUsize> }

impl crate::ffi::stream::tape::Reader for DropProbe {
    fn delivered(&self, _: StreamDelivery) {}
}

impl Drop for DropProbe {
    fn drop(&mut self) {
        self.held.lock().unwrap().extend(crate::db::lock::held_classes().into_iter().map(str::to_string));
        self.dropped.fetch_add(1, Ordering::SeqCst);
    }
}

/// K05 (B5, old line stream/owner.rs cancelled): the reader object is released after every library lock, also when the cancel drops the last reference.
#[tokio::test(flavor = "multi_thread")]
async fn the_cancel_of_a_reader_releases_its_object_outside_every_library_lock() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let held = Arc::new(Mutex::new(Vec::new()));
    let dropped = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let stream = rig.database.subscribe_reading(epoch, users(), Arc::new(DropProbe { held: held.clone(), dropped: dropped.clone() }));
    bound(rig.database.loaded(stream)).await.unwrap();
    rig.database.cancel_stream(stream);
    assert_eq!(rig.database.stream_census().open, 0);
    yield_until("the reader object was released", || dropped.load(Ordering::SeqCst) == 1).await;
    assert_eq!(dropped.load(Ordering::SeqCst), 1, "the reader object was released more than once");
    assert!(held.lock().unwrap().is_empty(), "the reader object was released under {:?}", held.lock().unwrap());
    rig.database.close_now();
}

/// K06 (B6): the row of a durable command shows in the stream of its reader as pending before the server answers, and the server has not been asked.
#[tokio::test(flavor = "multi_thread")]
async fn a_queued_message_shows_as_pending_in_the_thread_before_the_server_answers() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let reader = Reader::open();
    let stream = rig.database.subscribe_reading(epoch, thread(), reader.clone());
    bound(rig.database.loaded(stream)).await.unwrap();
    rig.database.set_online(false).unwrap();
    bound(rig.database.send(message(epoch, "queued", 2))).await.unwrap();
    yield_until("the thread shows the queued message", || reader.rows().iter().any(|row| row.sync.state == WireSyncState::Pending)).await;
    assert_eq!(rig.world.count("SendMessage"), 0, "the message was shown only after the server answered");
    assert_eq!(reader.texts().len(), 1);
    rig.database.close_now();
}

/// K07 (B9, old lines ffi/stream/open.rs retain_query and db/observe.rs query_count): a stream that ended leaves no observer and no work behind: a write after it reads nothing and no retained observer is counted.
#[tokio::test(flavor = "multi_thread")]
async fn a_cancelled_list_stream_leaves_no_observer_and_a_later_write_does_no_reading() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let reader = Reader::open();
    let stream = rig.database.subscribe_reading(epoch, thread(), reader.clone());
    bound(rig.database.loaded(stream)).await.unwrap();
    rig.database.cancel_stream(stream);
    let census = rig.database.subscription_census();
    assert_eq!((census.subscriptions, census.observed_queries, census.retained_observers), (0, 0, 0), "a left list stream kept an observer");
    let reads = rig.database.db.counters().observation_reads;
    rig.database.set_online(false).unwrap();
    bound(rig.database.send(message(epoch, "after", 3))).await.unwrap();
    settle(&rig).await;
    assert_eq!(rig.database.db.counters().observation_reads, reads, "a write after the last reader left woke an observer");
    rig.database.close_now();
}


/// K09 (spec B, C; old line ffi/stream/open.rs connect_json with `force` used once at open): a list stream opened with refresh on connect loads again once when the network returns, without a call of the app. A stream without the flag asks nothing.
#[tokio::test(flavor = "multi_thread")]
async fn a_stream_that_refreshes_on_connect_loads_again_once_when_the_network_returns() {
    let rig = Rig::encrypted().await;
    rig.world.seed_message("A", "m1", "first", 1);
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let list = Reader::open();
    let stream = rig.database.subscribe_reading(epoch, StreamRequest::Query { query_json: r#"{"model":"Message","relation":"thread","params":{"chatId":"c1"},"refreshOnConnect":true}"#.into() }, list.clone());
    bound(rig.database.loaded(stream)).await.unwrap();
    settle(&rig).await;
    let before = rig.world.count("Thread");
    rig.database.set_online(false).unwrap();
    rig.database.set_online(true).unwrap();
    yield_until("the list loaded again", || rig.world.count("Thread") > before).await;
    settle(&rig).await;
    assert_eq!(rig.world.count("Thread"), before + 1, "the stream loaded more or less than once on reconnect");
    rig.database.cancel_stream(stream);
    rig.database.set_online(false).unwrap();
    rig.database.set_online(true).unwrap();
    for _ in 0..50 { tokio::task::yield_now().await; }
    settle(&rig).await;
    assert_eq!(rig.world.count("Thread"), before + 1, "a cancelled stream loaded on reconnect");
    rig.database.close_now();
}

/// K10 (offline finding A; old line sync/send/pause.rs retry_due): a durable command whose request failed on the network goes out at once when the network returns, without a step of the clock.
#[tokio::test(flavor = "multi_thread")]
async fn a_durable_command_that_failed_on_the_network_goes_out_at_once_when_the_network_returns() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    rig.world.fail_next("SendMessage", super::rig_tests::Fault::Down);
    let receipt = bound(rig.database.send(message(epoch, "queued", 2))).await.unwrap();
    yield_until("the first attempt failed on the network", || rig.database.db.outbox().unwrap().first().is_some_and(|entry| entry.operation.attempts >= 1 && !entry.operation.transport_started)).await;
    assert_eq!(rig.world.count("SendMessage"), 0, "the failed attempt reached the server");
    rig.database.set_online(false).unwrap();
    rig.database.set_online(true).unwrap();
    let outcome = bound(rig.database.outcome(receipt)).await.unwrap();
    assert!(matches!(outcome, WireOperationOutcome::Confirmed { .. }), "the command did not go out after the network returned: {outcome:?}");
    assert_eq!(rig.world.count("SendMessage"), 1);
    rig.database.close_now();
}

/// K11 (offline finding B; old lines ffi/database.rs refresh and load_more, ffi/stream/mod.rs refresh_stream): a call that always goes to the server fails at once with `Network` while the network is away, and the entity stream shows the error in its state.
#[tokio::test(flavor = "multi_thread")]
async fn a_refresh_and_a_load_more_fail_at_once_offline_and_the_entity_stream_shows_the_error() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let entity = Reader::open();
    let stream = rig.database.subscribe_reading(epoch, StreamRequest::Entity { model: "User".into(), id: "user".into(), fetch: crate::ffi::EntityFetch::Force }, entity.clone());
    bound(rig.database.loaded(stream)).await.unwrap();
    let hits = rig.world.log().len();
    rig.database.set_online(false).unwrap();
    let refreshed = bound(rig.database.refresh_stream(stream)).await;
    assert!(matches!(refreshed, Err(DbError::Network { .. })), "refresh of a stream offline: {refreshed:?}");
    let refreshed = bound(rig.database.refresh(rig.database.session_epoch(), "User".into(), "all".into(), "{}".into())).await;
    assert!(matches!(refreshed, Err(DbError::Network { .. })), "refresh offline: {refreshed:?}");
    let more = bound(rig.database.load_more(rig.database.session_epoch(), "User".into(), "all".into(), "{}".into())).await;
    assert!(matches!(more, Err(DbError::Network { .. })), "load more offline: {more:?}");
    yield_until("the entity stream shows the error", || entity.deliveries().iter().any(|delivery| match &delivery.value {
        StreamValue::State { state } | StreamValue::Snapshot { state, .. } => state.error.is_some(),
        _ => false,
    })).await;
    assert_eq!(rig.world.log().len(), hits, "a call offline reached the server");
    rig.database.close_now();
}

/// K12 (consumer finding forward; old line ffi/database.rs act returned the receiver row): a forward into the same chat and into another chat leaves the source row unchanged, upserts the answer row with its own id in the target chat, and the call returns the answer row. The answer has the shape of the mutation of the app: `messageForward.message` with the id `forwarded-1`.
#[tokio::test(flavor = "multi_thread")]
async fn a_forward_leaves_the_source_unchanged_upserts_the_answer_row_and_returns_it() {
    for (case, target) in [("same chat", "c1"), ("other chat", "c2")] {
        let rig = Rig::encrypted().await;
        rig.world.seed_message("A", "m1", "hello", 1);
        rig.login_ios("A").await;
        let epoch = rig.database.session_epoch();
        let reader = Reader::open();
        let stream = rig.database.subscribe_reading(epoch, thread(), reader.clone());
        bound(rig.database.loaded(stream)).await.unwrap();
        yield_until("the thread shows the source", || reader.texts().iter().any(|text| text.contains("hello"))).await;
        let before = bound(rig.database.read_row(epoch, "Message".into(), "m1".into())).await.unwrap().expect("the source row");
        let acted = bound(rig.database.act(epoch, "Message".into(), "m1".into(), "forward".into(), "{}".into(), json!({"targetChatId": target}).to_string())).await.unwrap();
        let outcome = bound(rig.database.outcome(acted)).await.unwrap();
        assert!(matches!(outcome, WireOperationOutcome::Confirmed { value: Some(row), .. } if row.id == "forwarded-1"), "{case}: the confirmed row differs");
        let after = bound(rig.database.read_row(epoch, "Message".into(), "m1".into())).await.unwrap().expect("the source row");
        assert_eq!(after.cells, before.cells, "{case}: the answer was written onto the source row");
        let forwarded = bound(rig.database.read_row(epoch, "Message".into(), "forwarded-1".into())).await.unwrap().expect("the answer row");
        assert!(forwarded.cells.contains(&crate::ffi::Cell::Text { value: target.into() }), "{case}: the answer row is not in the target chat");
        if target == "c1" {
            yield_until("the thread shows the source and the answer", || reader.rows().len() == 2).await;
        }
        rig.database.close_now();
    }
}

fn groups() -> StreamRequest { StreamRequest::Query { query_json: r#"{"model":"Group","relation":"all","params":{}}"#.into() } }

pub(super) fn member_list(rig: &Rig, group: &str, order: &str) -> Vec<String> {
    let spec = rig.database.db.spec("Member").unwrap();
    let params = json!({"groupId": group, "order": order}).as_object().unwrap().clone();
    let key = spec.selection_key("group_members", &params).unwrap();
    rig.database.db.shared.store.scope_members("Member", key.as_str()).unwrap().into_iter().map(|(id, _)| id).collect()
}

/// The members u1 u2 u3 of the group in both lists (JOINED: u1 u2 u3, OWNER_FIRST: u2 u1 u3) and the row u4 that is in neither list.
pub(super) fn seed_members(rig: &Rig, group: &str) {
    use crate::commit::{CommitEnvelope, EntityOp, ScopeOp, ScopeRef, WriteOrigin};
    use crate::identity::{ModelKey, RowId};
    let db = &rig.database.db;
    let spec = db.spec("Member").unwrap();
    let mut envelope = CommitEnvelope::new(WriteOrigin::Event, db.generation());
    for id in ["u1", "u2", "u3", "u4"] {
        let row = json!({"id": id, "name": id}).as_object().unwrap().clone();
        envelope.entity_ops.push(EntityOp::Upsert { model: ModelKey::new("Member"), id: RowId::new(id).unwrap(), row });
    }
    for (order, ids) in [("JOINED", ["u1", "u2", "u3"]), ("OWNER_FIRST", ["u2", "u1", "u3"])] {
        let params = json!({"groupId": group, "order": order}).as_object().unwrap().clone();
        let scope = ScopeRef::new(ModelKey::new("Member"), spec.selection_key("group_members", &params).unwrap());
        let mut last: Option<String> = None;
        for id in ids {
            let order_key = crate::value::fractional_between(last.as_deref(), None).unwrap();
            last = Some(order_key.clone());
            envelope.scope_ops.push(ScopeOp::Append { scope: scope.clone(), row: RowId::new(id).unwrap(), order_key });
        }
    }
    db.land(&db.admit().unwrap(), envelope).unwrap();
}

fn ids_of(rows: Vec<WireRow>) -> Vec<String> { rows.into_iter().map(|row| row.id).collect() }

pub(super) fn strings(ids: &[&str]) -> Vec<String> { ids.iter().map(|id| id.to_string()).collect() }

/// K32: a member added or removed ends with the member lists of the server in both orders: the added user appears in the lists the screen reads and the removed one leaves them, without a read by the app.
#[tokio::test(flavor = "multi_thread")]
async fn adding_and_removing_a_member_ends_with_the_member_lists_of_the_server_in_both_orders() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let (values, args) = group("friends");
    let created = confirmed_group(&rig, epoch, values, args).await;
    let orders = [("JOINED", ["u1", "u2", "u3"]), ("OWNER_FIRST", ["u2", "u1", "u3"])];
    for (order, ids) in orders { rig.world.set_members(order, &ids); }
    let mut readers = Vec::new();
    for (order, _) in orders {
        let (reader, query) = (Reader::open(), json!({"model": "Member", "relation": "group_members", "params": {"groupId": created.id, "order": order}}).to_string());
        let stream = rig.database.subscribe_reading(epoch, StreamRequest::Query { query_json: query }, reader.clone());
        bound(rig.database.loaded(stream)).await.unwrap();
        readers.push(reader);
    }
    let shown = |readers: &[Arc<Reader>]| readers.iter().map(|reader| reader.rows().into_iter().map(|row| row.id).collect::<Vec<_>>()).collect::<Vec<_>>();
    assert_eq!(shown(&readers), [strings(&["u1", "u2", "u3"]), strings(&["u2", "u1", "u3"])], "the lists before the add");
    type Case = (&'static str, [&'static [&'static str]; 2]);
    let cases: [Case; 2] = [("add_member", [&["u1", "u2", "u3", "u9"], &["u2", "u1", "u3", "u9"]]), ("remove_member", [&["u1", "u2", "u3"], &["u2", "u1", "u3"]])];
    for (action, ended) in cases {
        for (index, (order, _)) in orders.iter().enumerate() { rig.world.set_members(order, ended[index]); }
        let acted = bound(rig.database.act(epoch, "Group".into(), created.id.clone(), action.into(), "{}".into(), json!({"userIds": ["u9"]}).to_string())).await;
        assert!(acted.is_ok(), "{action}: the call failed: {:?}", acted.as_ref().err());
        yield_until(&format!("{action}: both lists end with the lists of the server"), || shown(&readers) == [strings(ended[0]), strings(ended[1])]).await;
    }
    rig.database.close_now();
}

/// K13 (B6, C05; old lines: no rule for these actions): add member and remove member change both member lists (JOINED and OWNER_FIRST) at once, before the server answers. The answer keeps the change, a refusal restores the ids and the order of both lists.
#[tokio::test(flavor = "multi_thread")]
async fn member_actions_change_both_member_lists_before_the_answer_and_a_refusal_restores_them() {
    let joined = |ids: &[&str]| ids.iter().map(|id| id.to_string()).collect::<Vec<_>>();
    type Case = (&'static str, &'static str, &'static str, [&'static [&'static str]; 2], [&'static [&'static str]; 2]);
    let cases: [Case; 2] = [
        ("add_member", "AddMember", "u4", [&["u1", "u2", "u3", "u4"], &["u2", "u1", "u3", "u4"]], [&["u1", "u2", "u3"], &["u2", "u1", "u3"]]),
        ("remove_member", "RemoveMember", "u2", [&["u1", "u3"], &["u1", "u3"]], [&["u1", "u2", "u3"], &["u2", "u1", "u3"]]),
    ];
    for (action, operation, user, changed, original) in cases {
        for refusal in [false, true] {
            let case = format!("{action} {}", if refusal { "refused" } else { "answered" });
            let rig = Rig::encrypted().await;
            rig.login_ios("A").await;
            let epoch = rig.database.session_epoch();
            let (values, args) = group("friends");
            let created = confirmed_group(&rig, epoch, values, args).await;
            seed_members(&rig, &created.id);
            let gate = if refusal { rig.world.hold_then(operation, Fault::Refuse { code: "FORBIDDEN", confirmed: true }) } else { rig.world.hold_next(operation) };
            let acting = { let database = rig.database.clone(); let id = created.id.clone(); tokio::spawn(async move { database.act(epoch, "Group".into(), id, action.into(), "{}".into(), json!({"userIds": [user]}).to_string()).await }) };
            gate.entered_for(&case).await;
            let receipt = bound(acting).await.unwrap().unwrap();
            assert_eq!((member_list(&rig, &created.id, "JOINED"), member_list(&rig, &created.id, "OWNER_FIRST")), (joined(changed[0]), joined(changed[1])), "{case}: the lists before the answer");
            gate.release();
            let result = bound(rig.database.outcome(receipt)).await.unwrap();
            settle(&rig).await;
            let expected = if refusal { original } else { changed };
            assert_eq!((member_list(&rig, &created.id, "JOINED"), member_list(&rig, &created.id, "OWNER_FIRST")), (joined(expected[0]), joined(expected[1])), "{case}: the lists after the outcome");
            assert_eq!(matches!(result, WireOperationOutcome::Rejected { .. }), refusal, "{case}: the operation returned {result:?}");
            rig.database.close_now();
        }
    }
}

/// K13 (B6, C05): leave removes the group from the list before the answer. The answer keeps it away, a refusal puts it back.
#[tokio::test(flavor = "multi_thread")]
async fn leave_removes_the_group_from_its_list_before_the_answer_and_a_refusal_restores_it() {
    for refusal in [false, true] {
        let case = if refusal { "refused" } else { "answered" };
        let rig = Rig::encrypted().await;
        rig.login_ios("A").await;
        let epoch = rig.database.session_epoch();
        let (values, args) = group("friends");
        let created = confirmed_group(&rig, epoch, values, args).await;
        let reader = Reader::open();
        let stream = rig.database.subscribe_reading(epoch, groups(), reader.clone());
        bound(rig.database.loaded(stream)).await.unwrap();
        yield_until("the list shows the group", || ids_of(reader.rows()) == strings(&[created.id.as_str()])).await;
        let gate = if refusal { rig.world.hold_then("LeaveGroup", Fault::Refuse { code: "FORBIDDEN", confirmed: true }) } else { rig.world.hold_next("LeaveGroup") };
        let acting = { let database = rig.database.clone(); let id = created.id.clone(); tokio::spawn(async move { database.act(epoch, "Group".into(), id, "leave".into(), "{}".into(), "{}".into()).await }) };
        gate.entered_for(case).await;
        yield_until("the group left the list before the answer", || ids_of(reader.rows()).is_empty()).await;
        gate.release();
        let receipt = bound(acting).await.unwrap().unwrap();
        let result = bound(rig.database.outcome(receipt)).await.unwrap();
        settle(&rig).await;
        assert_eq!(matches!(result, WireOperationOutcome::Rejected { .. }), refusal, "{case}: the operation returned {result:?}");
        if refusal {
            yield_until("the refusal put the group back", || ids_of(reader.rows()) == strings(&[created.id.as_str()])).await;
        } else {
            assert!(ids_of(reader.rows()).is_empty(), "{case}: the group came back after the answer");
        }
        rig.database.close_now();
    }
}

/// K13 (B6, C05): forward puts a local copy of the message into the thread of the target chat before the answer. The answer replaces the copy with its own row, a refusal removes the copy.
#[tokio::test(flavor = "multi_thread")]
async fn forward_shows_a_local_copy_in_the_target_thread_and_a_refusal_removes_it() {
    for refusal in [false, true] {
        let case = if refusal { "refused" } else { "answered" };
        let rig = Rig::encrypted().await;
        rig.world.seed_message("A", "m1", "hello", 1);
        rig.login_ios("A").await;
        let epoch = rig.database.session_epoch();
        let reader = Reader::open();
        let stream = rig.database.subscribe_reading(epoch, thread(), reader.clone());
        bound(rig.database.loaded(stream)).await.unwrap();
        yield_until("the thread shows the source", || ids_of(reader.rows()) == strings(&["m1"])).await;
        let gate = if refusal { rig.world.hold_then("MessageForward", Fault::Refuse { code: "FORBIDDEN", confirmed: true }) } else { rig.world.hold_next("MessageForward") };
        let acting = { let database = rig.database.clone(); tokio::spawn(async move { database.act(epoch, "Message".into(), "m1".into(), "forward".into(), "{}".into(), json!({"targetChatId": "c1"}).to_string()).await }) };
        gate.entered_for(case).await;
        yield_until("the thread shows the copy before the answer", || ids_of(reader.rows()).len() == 2).await;
        let during = ids_of(reader.rows());
        assert!(during.contains(&"m1".to_string()), "{case}: the source left the thread: {during:?}");
        let copy = during.iter().find(|id| id.as_str() != "m1").unwrap();
        assert!(crate::identity::RowId::new(copy).unwrap().is_temp(), "{case}: the copy {copy} is not a temporary row");
        gate.release();
        let receipt = bound(acting).await.unwrap().unwrap();
        let result = bound(rig.database.outcome(receipt)).await.unwrap();
        settle(&rig).await;
        assert_eq!(matches!(result, WireOperationOutcome::Rejected { .. }), refusal, "{case}: the operation returned {result:?}");
        let expected = if refusal { strings(&["m1"]) } else { strings(&["m1", "forwarded-1"]) };
        yield_until("the thread shows the outcome", || ids_of(reader.rows()) == expected).await;
        rig.database.close_now();
    }
}

type Call = for<'a> fn(&'a Database, u64) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), DbError>> + 'a>>;

fn case(name: &'static str, call: Call) -> (&'static str, Call) { (name, call) }

/// K17 (C01, old lines ffi/database.rs run, load, refresh, load_all, load_more, node, nodes, retry_all, warm, edge_values, peek_row, cancel_upload and stream/mod.rs peek_query): every entry of the database names the epoch of its caller and is refused after that session ended. Nothing reaches the server.
#[tokio::test(flavor = "multi_thread")]
async fn every_entry_of_a_left_account_is_refused_and_reaches_nothing() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let old = rig.database.session_epoch();
    bound(rig.database.session_reset()).await.unwrap();
    rig.login_ios("B").await;
    let before = rig.world.log().len();
    let calls = vec![
        case("run", |db, old| Box::pin(async move { bound(db.run(old, "MyProfile".into(), "{}".into())).await.map(drop) })),
        case("load", |db, old| Box::pin(async move { bound(db.load(old, "User".into(), "all".into(), "{}".into())).await })),
        case("refresh", |db, old| Box::pin(async move { bound(db.refresh(old, "User".into(), "all".into(), "{}".into())).await })),
        case("refresh_preloads", |db, old| Box::pin(async move { bound(db.refresh_preloads(old)).await })),
        case("load_all", |db, old| Box::pin(async move { bound(db.load_all(old, "User".into(), "all".into(), "{}".into(), true)).await })),
        case("load_more", |db, old| Box::pin(async move { bound(db.load_more(old, "User".into(), "all".into(), "{}".into())).await.map(drop) })),
        case("node", |db, old| Box::pin(async move { bound(db.node(old, "User".into(), "node".into(), "user".into(), true)).await.map(drop) })),
        case("nodes", |db, old| Box::pin(async move { bound(db.nodes(old, "User".into(), "nodes".into(), vec!["user".into()])).await.map(drop) })),
        case("retry_all", |db, old| Box::pin(async move { bound(db.retry_all(old)).await.map(drop) })),
        case("warm", |db, old| Box::pin(async move { bound(db.warm(old, r#"{"model":"User"}"#.into())).await })),
        case("edge_values", |db, old| Box::pin(async move { bound(db.edge_values(old, "User".into(), "all".into(), "{}".into(), vec!["user".into()])).await.map(drop) })),
        case("peek_query", |db, old| Box::pin(async move { bound(db.peek_query(old, r#"{"model":"User"}"#.into())).await.map(drop) })),
        case("peek_row", |db, old| Box::pin(async move { db.peek_row(old, "User".into(), "user".into()).map(drop) })),
        case("cancel_upload", |db, old| Box::pin(async move { bound(db.cancel_upload(old, "User".into(), "user".into(), "upload".into())).await.map(drop) })),
    ];
    for (name, call) in calls {
        let result = call(&rig.database, old).await;
        assert!(matches!(result, Err(DbError::Interrupted { .. })), "{name} of a left account was not refused: {result:?}");
    }
    assert_eq!(rig.world.log().len(), before, "a refused entry reached the server");
    rig.database.close_now();
}

/// K18 (C04, old lines sync/send/flush.rs settle_delivery and online.rs set_online): an action that is not a durable command and whose request never reached the server ends with the network error and rolls back. Nothing replays it when the network returns.
#[tokio::test(flavor = "multi_thread")]
async fn a_plain_action_that_never_reached_the_server_ends_with_the_network_error_and_is_not_replayed() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let (values, args) = group("friends");
    let created = confirmed_group(&rig, epoch, values, args).await;
    rig.world.fail_next("LeaveGroup", Fault::Down);
    let receipt = bound(rig.database.act(epoch, "Group".into(), created.id.clone(), "leave".into(), "{}".into(), "{}".into())).await.unwrap();
    let outcome = bound(rig.database.outcome(receipt)).await.unwrap();
    assert!(matches!(outcome, WireOperationOutcome::Rejected { failure } if failure.kind == "network"));
    let after = bound(rig.database.read_row(epoch, "Group".into(), created.id.clone())).await.unwrap();
    assert!(after.is_some(), "the group stayed removed after the failed request");
    assert!(rig.database.db.outbox().unwrap().is_empty(), "the failed action stayed queued");
    rig.database.set_online(false).unwrap();
    rig.database.set_online(true).unwrap();
    bound(rig.database.flush()).await.unwrap();
    settle(&rig).await;
    assert_eq!(rig.world.log().iter().filter(|entry| entry.operation == "LeaveGroup").count(), 1, "the action went out again after the network returned");
    rig.database.close_now();
}

/// K19 (C04, old lines sync/send/flush.rs refuse_non_durable and online.rs set_online): an action queued while online and still unsent when the network drops ends with the network error at the drop, also when the network returns before any flush.
#[tokio::test(flavor = "multi_thread")]
async fn a_plain_action_unsent_when_the_network_drops_never_goes_out_after_it_returns() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let (values, args) = group("friends");
    let created = confirmed_group(&rig, epoch, values, args).await;
    let sync = rig.database.sync.as_ref().expect("the database has a sync").clone();
    let sender_busy = sync.inner.outbox_gate.lock().await;
    let acting = { let database = rig.database.clone(); let id = created.id.clone(); tokio::spawn(async move { database.act(epoch, "Group".into(), id, "leave".into(), "{}".into(), "{}".into()).await }) };
    yield_until("the action is queued", || !rig.database.db.outbox().unwrap().is_empty()).await;
    rig.database.set_online(false).unwrap();
    rig.database.set_online(true).unwrap();
    drop(sender_busy);
    let receipt = bound(acting).await.unwrap().unwrap();
    assert!(matches!(bound(rig.database.outcome(receipt)).await.unwrap(), WireOperationOutcome::Rejected { failure } if failure.kind == "network"));
    bound(rig.database.flush()).await.unwrap();
    settle(&rig).await;
    assert_eq!(rig.world.log().iter().filter(|entry| entry.operation == "LeaveGroup").count(), 0, "an action queued before the drop went out after the network returned");
    assert!(rig.database.db.outbox().unwrap().is_empty());
    rig.database.close_now();
}

/// K20 (C08, old line sync/send/pause.rs retry_due): after a network return the first retry skips the backoff once. A second failure waits by the normal backoff, and the clock then delivers the command.
#[tokio::test(flavor = "multi_thread")]
async fn a_second_failure_after_a_network_return_waits_by_the_backoff_and_the_clock_delivers() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    rig.world.fail_next("SendMessage", super::rig_tests::Fault::Down);
    rig.world.fail_next("SendMessage", super::rig_tests::Fault::Down);
    let receipt = bound(rig.database.send(message(epoch, "queued", 2))).await.unwrap();
    let attempts = |rig: &Rig| rig.world.log().iter().filter(|entry| entry.operation == "SendMessage").count();
    let failed_and_waiting = |rig: &Rig, count: usize| attempts(rig) == count && rig.database.db.outbox().unwrap().first().is_some_and(|entry| entry.operation.attempts as usize >= count && !entry.operation.transport_started);
    yield_until("the first request failed and the command waits", || failed_and_waiting(&rig, 1)).await;
    rig.database.set_online(false).unwrap();
    rig.database.set_online(true).unwrap();
    yield_until("the retry after the network return failed and the command waits", || failed_and_waiting(&rig, 2)).await;
    settle(&rig).await;
    assert_eq!(attempts(&rig), 2, "the command was tried again before its backoff");
    rig.clock.advance(3_600_000);
    let outcome = bound(rig.database.outcome(receipt)).await.unwrap();
    assert!(matches!(outcome, WireOperationOutcome::Confirmed { .. }), "the clock did not deliver the command: {outcome:?}");
    rig.database.close_now();
}

/// K23 (D01, old lines ffi/command.rs serve_in_epoch and sync/land.rs land_rows): a request that was accepted in the epoch of A, and whose answer waits at the server while another identity starts a session, lands nothing and ends refused. The epoch of A does not change, the generation does not either.
#[tokio::test(flavor = "multi_thread")]
async fn an_accepted_request_whose_answer_lands_after_the_next_account_started_writes_nothing() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    bound(rig.database.refresh(epoch, "User".into(), "profile".into(), "{}".into())).await.unwrap();
    let before = bound(rig.database.read_row(epoch, "User".into(), "user".into())).await.unwrap().expect("the row of A");
    assert!(named(&before, "A"));
    let gate = rig.world.hold_next("MyProfile");
    gate.as_account("late");
    let running = { let database = rig.database.clone(); tokio::spawn(async move { database.refresh(epoch, "User".into(), "profile".into(), "{}".into()).await }) };
    gate.entered_for("the accepted request").await;
    let store = &rig.database.db.shared.store;
    let moved = { let database = rig.database.clone(); tokio::task::spawn_blocking(move || database.db.shared.store.lifecycle.session_from(Some("another-account".into()), database.db.shared.store.lifecycle.admit_passive(crate::lifecycle::Origin::Explicit, "test session", Some(epoch)).unwrap()).is_ok()) };
    assert!(bound(moved).await.unwrap(), "the session start of the other identity did not begin");
    assert_ne!(rig.database.session_epoch(), epoch);
    gate.release();
    let result = bound(running).await.unwrap();
    assert!(matches!(result, Err(DbError::Interrupted { .. })), "the request of the epoch that left ended with {result:?}");
    settle(&rig).await;
    let after = bound(rig.database.read_row(rig.database.session_epoch(), "User".into(), "user".into())).await.unwrap().expect("the row");
    assert!(named(&after, "A") && !named(&after, "late"), "the answer of the epoch that left was written");
    assert_eq!(store.row_count("User").unwrap(), 1);
    rig.database.close_now();
}

/// Fable review 1 (old lines lifecycle/mod.rs finish_transition, sync/mod.rs changed): a message of A queued offline never goes out when another identity takes the session without a session reset, also when a flush of the new epoch runs at the first moment the epoch admits work. The app waiting for its outcome hears Interrupted.
#[tokio::test(flavor = "multi_thread")]
async fn a_queued_message_of_a_never_goes_out_when_another_identity_takes_the_session_without_a_reset() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    rig.database.set_online(false).unwrap();
    let receipt = bound(rig.database.send(message(epoch, "of A", 9))).await.unwrap();
    assert_eq!(rig.world.count("SendMessage"), 0, "the offline send reached the server");
    let waiting = { let database = rig.database.clone(); tokio::spawn(async move { database.outcome(receipt).await }) };
    let (opened, opened_rx) = std::sync::mpsc::channel();
    let (release_tx, release) = std::sync::mpsc::channel::<()>();
    let expired = Arc::new(AtomicBool::new(false));
    let release_expired = expired.clone();
    *rig.database.db.shared.store.lifecycle.after_publish.lock().unwrap() = Some(Box::new(move || { opened.send(()).unwrap(); if release.recv_timeout(std::time::Duration::from_secs(20)).is_err() { release_expired.store(true, Ordering::SeqCst); } }));
    let switched = { let database = rig.database.clone(); tokio::task::spawn_blocking(move || { let lifecycle = &database.db.shared.store.lifecycle; lifecycle.session_from(Some("another-account".into()), lifecycle.admit_passive(crate::lifecycle::Origin::Explicit, "test session", Some(epoch)).unwrap()).is_ok() }) };
    tokio::task::spawn_blocking(move || opened_rx.recv_timeout(std::time::Duration::from_secs(10)).unwrap()).await.unwrap();
    assert_ne!(rig.database.session_epoch(), epoch, "the new epoch was not published");
    rig.database.set_online(true).unwrap();
    bound(rig.database.flush()).await.unwrap();
    release_tx.send(()).unwrap();
    assert!(bound(switched).await.unwrap(), "the session start of the other identity did not finish");
    assert!(!expired.load(Ordering::SeqCst), "the held session start ran out of time and was not released by the test");
    let outcome = bound(waiting).await.unwrap().unwrap();
    assert!(matches!(outcome, WireOperationOutcome::Interrupted), "the outcome of the message of A was {outcome:?}");
    settle(&rig).await;
    bound(rig.database.flush()).await.unwrap();
    settle(&rig).await;
    assert_eq!(rig.world.count("SendMessage"), 0, "the message of the account that left went out for the next one");
    assert!(rig.database.db.outbox().unwrap().is_empty(), "an operation of the account that left is still queued");
    rig.database.close_now();
}

/// Fable review 3 (old lines sync/send/flush.rs land_response): a create the server confirmed keeps its row when the cache of another model clears between the answer and its landing. The epoch stays, only the generation moves.
#[tokio::test(flavor = "multi_thread")]
async fn a_confirmed_create_keeps_its_row_when_another_model_resets_before_it_lands() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let gate = rig.world.hold_next("SendMessage");
    let receipt = bound(rig.database.send(message(epoch, "kept", 9))).await.unwrap();
    gate.entered_for("the send").await;
    let generation = rig.database.db.generation();
    let reset = { let database = rig.database.clone(); tokio::task::spawn_blocking(move || database.db.reset(&["User"])) };
    bound(reset).await.unwrap().unwrap();
    assert_ne!(rig.database.db.generation(), generation, "the reset of another model did not move the generation");
    assert_eq!(rig.database.session_epoch(), epoch, "the reset of another model ended the epoch");
    gate.release();
    let outcome = bound(rig.database.outcome(receipt)).await.unwrap();
    let WireOperationOutcome::Confirmed { value: Some(row), .. } = outcome else { panic!("the confirmed create ended with {outcome:?}") };
    settle(&rig).await;
    let found = rig.database.db.find("Message", &crate::identity::RowId::new(&row.id).unwrap()).unwrap();
    assert!(found.is_some(), "the row {} the server confirmed is gone", row.id);
    assert!(rig.database.db.outbox().unwrap().is_empty(), "the confirmed create is still queued");
    rig.database.close_now();
}

/// K24 (D03, old lines sync/send/flush.rs flush and next_ready): a flush that is already running does not send an action that is not a durable command after the network dropped and returned between two of its sends.
#[tokio::test(flavor = "multi_thread")]
async fn a_running_flush_does_not_send_a_plain_action_after_the_network_dropped_between_two_sends() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let (values, args) = group("friends");
    let created = confirmed_group(&rig, epoch, values, args).await;
    let gate = rig.world.hold_next("SendMessage");
    bound(rig.database.send(message(epoch, "first", 1))).await.unwrap();
    gate.entered_for("the first send").await;
    rig.database.set_online(false).unwrap();
    let acted = bound(rig.database.act(epoch, "Group".into(), created.id.clone(), "leave".into(), "{}".into(), "{}".into())).await;
    assert!(matches!(acted, Err(DbError::Network { .. })));
    rig.database.set_online(true).unwrap();
    gate.release();
    settle(&rig).await;
    assert_eq!(rig.world.log().iter().filter(|entry| entry.operation == "LeaveGroup").count(), 0, "a running flush sent the action after the network dropped");
    rig.database.close_now();
}

/// K25 (D04, old lines db/roll_back.rs restore and db/prior.rs reappearing): two operations change the same member of the same lists and both are refused. The lists end the way they were, in both orders of the two changes, the first refusal arriving while the second still holds its answer.
#[tokio::test(flavor = "multi_thread")]
async fn two_refused_changes_of_one_member_restore_the_original_lists_in_both_orders() {
    type Case = (&'static str, [(&'static str, &'static str); 2], &'static str, [&'static [&'static str]; 2], [&'static [&'static str]; 2]);
    let cases: [Case; 2] = [
        ("remove then add a member", [("remove_member", "RemoveMember"), ("add_member", "AddMember")], "u2", [&["u1", "u3", "u2"], &["u1", "u3", "u2"]], [&["u1", "u2", "u3"], &["u2", "u1", "u3"]]),
        ("add then remove a member", [("add_member", "AddMember"), ("remove_member", "RemoveMember")], "u4", [&["u1", "u2", "u3"], &["u2", "u1", "u3"]], [&["u1", "u2", "u3"], &["u2", "u1", "u3"]]),
    ];
    for (case, [first, second], user, while_second, original) in cases {
        let rig = Rig::encrypted().await;
        rig.login_ios("A").await;
        let epoch = rig.database.session_epoch();
        let (values, args) = group("friends");
        let created = confirmed_group(&rig, epoch, values, args).await;
        seed_members(&rig, &created.id);
        let (first_gate, second_gate) = (rig.world.hold_then(first.1, Fault::Refuse { code: "FORBIDDEN", confirmed: true }), rig.world.hold_then(second.1, Fault::Refuse { code: "FORBIDDEN", confirmed: true }));
        let args = json!({"userIds": [user]}).to_string();
        let start = |action: &'static str| { let database = rig.database.clone(); let (id, args) = (created.id.clone(), args.clone()); tokio::spawn(async move { database.act(epoch, "Group".into(), id, action.into(), "{}".into(), args).await }) };
        let first_call = start(first.0);
        first_gate.entered_for(case).await;
        let second_call = start(second.0);
        yield_until("the second change is queued", || rig.database.db.outbox().unwrap().len() == 2).await;
        first_gate.release();
        second_gate.entered_for(case).await;
        let lists = |rig: &Rig| (member_list(rig, &created.id, "JOINED"), member_list(rig, &created.id, "OWNER_FIRST"));
        let expected = |lists: [&[&str]; 2]| (strings(lists[0]), strings(lists[1]));
        assert_eq!(lists(&rig), expected(while_second), "{case}: the lists while the second change waits for its answer");
        second_gate.release();
        let first = bound(first_call).await.unwrap().unwrap();
        let second = bound(second_call).await.unwrap().unwrap();
        assert!(matches!(bound(rig.database.outcome(first)).await.unwrap(), WireOperationOutcome::Rejected { .. }));
        assert!(matches!(bound(rig.database.outcome(second)).await.unwrap(), WireOperationOutcome::Rejected { .. }));
        settle(&rig).await;
        assert_eq!(lists(&rig), expected(original), "{case}: the lists after both refusals");
        rig.database.close_now();
    }
}

/// K26 (E04 and the outcome of an accepted call, old lines db/local_write.rs join_scope_rows and leave_scope_rows, sync/send/mod.rs settled): the same change of a member made twice, once the first and once the second refused, leaves the member the way the accepted change wants, also while the second still holds its answer. The accepted call returns its success and the refused call its refusal.
#[tokio::test(flavor = "multi_thread")]
async fn a_refused_change_of_a_member_does_not_undo_the_same_change_accepted_and_each_call_gets_its_own_outcome() {
    type Case = (&'static str, (&'static str, &'static str), &'static str, [&'static [&'static str]; 2], bool);
    let cases: [Case; 4] = [
        ("add twice, first refused", ("add_member", "AddMember"), "u4", [&["u1", "u2", "u3", "u4"], &["u2", "u1", "u3", "u4"]], true),
        ("remove twice, first refused", ("remove_member", "RemoveMember"), "u2", [&["u1", "u3"], &["u1", "u3"]], true),
        ("add twice, second refused", ("add_member", "AddMember"), "u4", [&["u1", "u2", "u3", "u4"], &["u2", "u1", "u3", "u4"]], false),
        ("remove twice, second refused", ("remove_member", "RemoveMember"), "u2", [&["u1", "u3"], &["u1", "u3"]], false),
    ];
    for (case, (action, operation), user, expected, first_refused) in cases {
        let rig = Rig::encrypted().await;
        rig.login_ios("A").await;
        let epoch = rig.database.session_epoch();
        let (values, args) = group("friends");
        let created = confirmed_group(&rig, epoch, values, args).await;
        seed_members(&rig, &created.id);
        let held = |refused: bool| if refused { rig.world.hold_then(operation, Fault::Refuse { code: "FORBIDDEN", confirmed: true }) } else { rig.world.hold_next(operation) };
        let first_gate = held(first_refused);
        let second_gate = held(!first_refused);
        let args = json!({"userIds": [user]}).to_string();
        let start = || { let database = rig.database.clone(); let (id, args) = (created.id.clone(), args.clone()); tokio::spawn(async move { database.act(epoch, "Group".into(), id, action.into(), "{}".into(), args).await }) };
        let first_call = start();
        first_gate.entered_for(case).await;
        let second_call = start();
        yield_until("the second change is queued", || rig.database.db.outbox().unwrap().len() == 2).await;
        first_gate.release();
        second_gate.entered_for(case).await;
        let lists = |rig: &Rig| (member_list(rig, &created.id, "JOINED"), member_list(rig, &created.id, "OWNER_FIRST"));
        let wanted = (strings(expected[0]), strings(expected[1]));
        assert_eq!(lists(&rig), wanted, "{case}: the lists while the second change waits for its answer");
        second_gate.release();
        let (first, second) = (bound(first_call).await.unwrap().unwrap(), bound(second_call).await.unwrap().unwrap());
        let (first, second) = (bound(rig.database.outcome(first)).await.unwrap(), bound(rig.database.outcome(second)).await.unwrap());
        assert_eq!((matches!(first, WireOperationOutcome::Rejected { .. }), matches!(second, WireOperationOutcome::Rejected { .. })), (first_refused, !first_refused), "{case}: the outcomes differ");
        settle(&rig).await;
        assert_eq!(lists(&rig), wanted, "{case}: the lists after both answers");
        rig.database.close_now();
    }
}

/// K27 (E02, old lines sync/send/flush.rs flush_pass and refuse_non_durable): a network that dropped and returned right after a flush decided which actions to refuse refuses a plain create and a plain action, and nothing of them goes out.
#[tokio::test(flavor = "multi_thread")]
async fn a_network_that_dropped_and_returned_right_after_the_refusal_decision_refuses_every_plain_action() {
    use std::sync::atomic::{AtomicBool, Ordering};
    for case in ["create", "leave", "add_member"] {
        let rig = Rig::encrypted().await;
        rig.login_ios("A").await;
        let epoch = rig.database.session_epoch();
        let (values, args) = group("friends");
        let created = confirmed_group(&rig, epoch, values, args).await;
        seed_members(&rig, &created.id);
        settle(&rig).await;
        let operation = match case { "create" => "CreateGroup", "leave" => "LeaveGroup", _ => "AddMember" };
        let sent_before = rig.world.count(operation);
        let (database, once) = (rig.database.clone(), AtomicBool::new(false));
        *rig.database.sync.as_ref().unwrap().inner.refusal_probe.lock().unwrap() = Some(Box::new(move || {
            if !once.swap(true, Ordering::SeqCst) {
                database.set_online(false).unwrap();
                database.set_online(true).unwrap();
            }
        }));
        let result = match case {
            "create" => { let (values, args) = group("second"); bound(rig.database.create(epoch, "Group".into(), values, args)).await }
            "leave" => bound(rig.database.act(epoch, "Group".into(), created.id.clone(), "leave".into(), "{}".into(), "{}".into())).await,
            _ => bound(rig.database.act(epoch, "Group".into(), created.id.clone(), "add_member".into(), "{}".into(), json!({"userIds": ["u4"]}).to_string())).await,
        };
        let outcome = bound(rig.database.outcome(result.unwrap())).await.unwrap();
        assert!(matches!(outcome, WireOperationOutcome::Rejected { .. }), "{case}: the action succeeded after the network dropped");
        settle(&rig).await;
        assert_eq!(rig.world.count(operation), sent_before, "{case}: the action went out after the network dropped and returned");
        rig.database.close_now();
    }
}

/// K29 (F02, old lines sync/send/flush.rs start and store/apply.rs attempt_in_epoch): a network that moves after the plan of the start of an action and before its commit refuses the start, and the action does not go out.
#[tokio::test(flavor = "multi_thread")]
async fn a_network_that_moves_between_the_plan_and_the_commit_of_a_start_refuses_the_action() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    let (values, args) = group("friends");
    let created = confirmed_group(&rig, epoch, values, args).await;
    settle(&rig).await;
    let sent_before = rig.world.count("LeaveGroup");
    let (shared, inner) = (rig.database.db.shared.clone(), rig.database.sync.as_ref().unwrap().inner.clone());
    let armed = inner.clone();
    *inner.refusal_probe.lock().unwrap() = Some(Box::new(move || {
        let moving = armed.clone();
        *shared.store.after_epoch_check.lock().unwrap() = Some(Box::new(move || { *moving.network_lost.lock().unwrap() = i64::MAX; }));
    }));
    let receipt = bound(rig.database.act(epoch, "Group".into(), created.id.clone(), "leave".into(), "{}".into(), "{}".into())).await.unwrap();
    assert!(matches!(bound(rig.database.outcome(receipt)).await.unwrap(), WireOperationOutcome::Rejected { failure } if failure.kind == "network"));
    settle(&rig).await;
    assert_eq!(rig.world.count("LeaveGroup"), sent_before, "the action went out although the network moved before its start was committed");
    rig.database.close_now();
}

/// K30 (F01, old lines ffi/database.rs flush and sync/send/flush.rs flush_pass): the public flush of a queued durable command lands its SQL commits in the epoch of its own lease, so a session start waits for the commit that is running and the epoch does not change under it.
#[tokio::test(flavor = "multi_thread")]
async fn a_public_flush_holds_the_commit_line_of_its_epoch_while_it_commits() {
    let rig = Rig::encrypted().await;
    rig.login_ios("A").await;
    let epoch = rig.database.session_epoch();
    rig.database.set_online(false).unwrap();
    bound(rig.database.send(message(epoch, "queued", 1))).await.unwrap();
    let store = &rig.database.db.shared.store;
    let (entered, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release) = std::sync::mpsc::channel::<()>();
    *store.after_epoch_check.lock().unwrap() = Some(Box::new(move || { entered.send(()).unwrap(); let _ = release.recv_timeout(std::time::Duration::from_secs(20)); }));
    rig.database.db.connectivity().set_online(true);
    let flushing = { let database = rig.database.clone(); tokio::spawn(async move { database.flush().await }) };
    tokio::task::spawn_blocking(move || entered_rx.recv_timeout(std::time::Duration::from_secs(10)).unwrap()).await.unwrap();
    let waits = *store.lifecycle.begin_waits.0.lock().unwrap();
    let identity = { let database = rig.database.clone(); tokio::task::spawn_blocking(move || database.db.shared.store.lifecycle.session_from(Some("another-account".into()), database.db.shared.store.lifecycle.admit_passive(crate::lifecycle::Origin::Explicit, "test session", Some(epoch)).unwrap()).is_ok()) };
    yield_until("the session start reached the commit line", || *store.lifecycle.begin_waits.0.lock().unwrap() > waits).await;
    assert_eq!(rig.database.session_epoch(), epoch, "the epoch changed while a commit of the flush was held");
    release_tx.send(()).unwrap();
    let _ = bound(flushing).await.unwrap();
    assert!(bound(identity).await.unwrap(), "the session start of the other identity did not begin");
    settle(&rig).await;
    rig.database.close_now();
}
