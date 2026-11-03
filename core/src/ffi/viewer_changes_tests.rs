use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use super::{CableLease, Config, Database, DbError, LiveChange, QueryListener, SubscriptionSink, Transport, TransportRequest};
use crate::schema::{ChangeFeed, ChangeRead, Coverage, FieldSpec, ModelSpec, RelationSpec, RemoteShape, RemoteSpec};
use crate::value::ScalarKind;
use crate::test_wait::{join_within, within, yield_until};

pub(super) struct Recording {
    queries: AtomicUsize,
    subscribed: Mutex<Vec<String>>,
    subscribed_event: tokio::sync::Notify,
    released: AtomicUsize,
    sinks: Mutex<Vec<Arc<SubscriptionSink>>>,
}

impl Recording {
    fn subscriptions(&self, operation: &str) -> usize {
        self.subscribed.lock().unwrap().iter().filter(|name| *name == operation).count()
    }
}

#[async_trait::async_trait]
impl Transport for Recording {
    async fn open_cable(&self, _: Arc<CableLease>) -> Result<(), DbError> { Err(DbError::Network { detail: "fake socket unavailable".into() }) }
    async fn cable_send(&self, _: String, _: String) -> Result<(), DbError> { unreachable!() }
    async fn cable_next(&self, _: String) -> Option<super::EncryptedSocketEvent> { unreachable!() }
    async fn cable_close(&self, _: String) {}
    async fn query(&self, _: TransportRequest) -> Result<String, DbError> {
        self.queries.fetch_add(1, Ordering::SeqCst);
        Ok(r#"{"data":{"moments":[{"id":"m1","text":"a"}]}}"#.into())
    }
    async fn mutate(&self, _: TransportRequest) -> Result<String, DbError> { unreachable!() }
    async fn subscribe(&self, request: TransportRequest, sink: Arc<SubscriptionSink>) -> Result<(), DbError> {
        self.subscribed.lock().unwrap().push(request.operation_name.clone());
        self.subscribed_event.notify_one();
        self.sinks.lock().unwrap().push(sink.clone());
        sink.opened(request.operation_name)?;
        sink.ready();
        Ok(())
    }
    fn unsubscribe(&self, _: String) { self.released.fetch_add(1, Ordering::SeqCst); }
    async fn put(&self, _: String, _: HashMap<String, String>, _: crate::transport::PutFile, _: String, _: Arc<super::ByteProgress>) -> Result<super::PutReply, DbError> { unreachable!() }
}

struct Silent;
impl QueryListener for Silent {
    fn changed(&self, _: LiveChange) {}
}

fn feed_relation(name: &str, text_param: Option<&str>) -> RelationSpec {
    let mut relation = RelationSpec::global(name);
    if let Some(param) = text_param {
        relation.is_global = false;
        relation.text_params.push(param.into());
    }
    relation.remote = Some(RemoteSpec {
        operation: "Feed".into(), shape: RemoteShape::List { coverage: Coverage::Complete }, stale_time: "5.minutes".into(),
        empty_stale_time: None, selects_page_info: false, page_size: None, variables: Default::default(), root: None, total_from: None,
    });
    relation
}

async fn open_viewer() -> (Arc<Database>, Arc<Recording>) {
    open_viewer_as(false).await
}

async fn open_viewer_as(testing: bool) -> (Arc<Database>, Arc<Recording>) {
    let mut moment = ModelSpec::new("Moment", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("text", ScalarKind::Str).optional()]);
    moment.relations.extend([feed_relation("feed", None), feed_relation("vibeFeed", Some("vibeId"))]);
    moment.changes.push(ChangeRead { area: "MOMENT".into(), relation: "feed".into(), parameter: None, removes_on: vec![], observed: false, payload: None, id_of: None });
    let mut notice = ModelSpec::new("ChangeNotice", vec![FieldSpec::scalar("id", ScalarKind::Id)]);
    notice.storage = crate::schema::Storage::Memory;
    notice.change_feed = Some(ChangeFeed { subscription: "DblayerViewerChanges".into(), root: "viewerChanges.changes".into(), area: "area".into(), kind: "kind".into(), id: "entityId".into() });
    let transport = Arc::new(Recording { queries: AtomicUsize::new(0), subscribed: Mutex::new(Vec::new()), subscribed_event: tokio::sync::Notify::new(), released: AtomicUsize::new(0), sinks: Mutex::new(Vec::new()) });
    let models = serde_json::to_string(&vec![moment, notice]).unwrap();
    let config = Config { path: None, data_version: None, cache_rows: 8 };
    let handle = Some(transport.clone() as Arc<dyn Transport>);
    let database = if testing { Database::open_with_clock(config, models, handle, None, super::TestClock::new(1_000_000), None, None, None, None).await.unwrap() } else { Database::open(config, models, handle, None, None, None, None, None).await.unwrap() };
    database.register_operations(r#"[{"name":"Feed","document":"query Feed { moments { id text } }","rootPath":"moments"},{"name":"DblayerViewerChanges","document":"subscription DblayerViewerChanges { viewerChanges { changes { area } } }","rootPath":"viewerChanges"}]"#.into()).unwrap();
    (database, transport)
}

#[tokio::test]
async fn switching_the_observed_list_from_a_foreign_thread_keeps_the_one_viewer_changes_subscription() {
    let (database, transport) = open_viewer().await;
    let feed = database.live_query(r#"{"model":"Moment","relation":"feed","params":{}}"#.into(), Arc::new(Silent));
    feed.wait_for_initial_load().await.unwrap();
    let live = || database.sync().unwrap().inner.live_status("DblayerViewerChanges|{}");
    within(transport.subscribed_event.notified()).await;
    yield_until("the viewer changes live status settled", || live() == Some((2, true))).await;
    assert_eq!(transport.subscriptions("DblayerViewerChanges"), 1);

    join_within(std::thread::spawn(move || feed.cancel()));
    let vibe = database.live_query(r#"{"model":"Moment","relation":"vibeFeed","params":{"vibeId":"3"}}"#.into(), Arc::new(Silent));
    vibe.wait_for_initial_load().await.unwrap();
    yield_until("the viewer changes live status settled", || live() == Some((2, true))).await;

    assert_eq!(transport.subscriptions("DblayerViewerChanges"), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn held_subscriptions_and_model_events_keep_working_through_a_reset_of_the_database() {
    let (database, transport) = open_viewer_as(true).await;
    let feed = database.live_query(r#"{"model":"Moment","relation":"feed","params":{}}"#.into(), Arc::new(Silent));
    feed.wait_for_initial_load().await.unwrap();
    let events = within(database.open_model_events("Moment".into())).await.unwrap();
    yield_until("the viewer changes subscription is open", || transport.subscriptions("DblayerViewerChanges") == 1).await;
    let queries = transport.queries.load(Ordering::SeqCst);
    within(database.reset_for_testing()).await.unwrap();
    let one_open = || { let opened = transport.subscriptions("DblayerViewerChanges"); opened >= 2 && opened == transport.released.load(Ordering::SeqCst) + 1 };
    yield_until("the subscription opened again and every older one was released", one_open).await;
    within(feed.wait_for_initial_load()).await.unwrap();
    assert_eq!(transport.queries.load(Ordering::SeqCst), queries + 1, "the held list rereads once");
    assert!(one_open(), "a subscription token leaked or was released twice");
    let change = |kind: &str| format!(r#"{{"data":{{"viewerChanges":{{"changes":[{{"area":"MOMENT","kind":"{kind}","entityId":"m1"}}]}}}}}}"#);
    let (old, current) = { let sinks = transport.sinks.lock().unwrap(); (sinks[0].clone(), sinks.last().unwrap().clone()) };
    let _ = old.event(change("LATE"));
    current.event(change("NEXT")).unwrap();
    let item = within(events.clone().next()).await.unwrap().expect("the held model event feed ended with the reset");
    assert!(matches!(&item, super::WireModelEventItem::Event { event } if event.kind == "NEXT" && event.id == "m1"), "the first event after the reset must come from the new connection: {item:?}");
    database.shutdown();
}
