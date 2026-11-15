use std::sync::Arc;

use serde_json::{json, Value};

use super::delivery_reactivity_tests::{feed_database_over, FeedTransport};
use super::flow_fixtures::texts;
use super::rig_tests::{bound, World};
use crate::ffi::tape::Rows;
use crate::ffi::wire::{wire_fields, Cell, WireRow};
use crate::ffi::{Database, ReadRequest, ReadStream, ReadValue};

const LOCAL_RECORDS: &str = r#"{"model":"Record"}"#;
const LOCAL_CONTAINERS: &str = r#"{"model":"Container"}"#;

/// The library fixture models (`fixtures/*.dbl`) without server subscriptions of their own (the change feed is the only channel) and with one more `node` read on `User` and one more event area for each read the tests name (`node_area`: the area of the first `node` read of `Record` exists).
fn deps_models(node_area: bool) -> String {
    let mut models: Vec<Value> = serde_json::from_str(include_str!("wrapper_models.inc")).unwrap();
    let index = |models: &[Value], key: &str| models.iter().position(|model| model["key"] == key).unwrap();
    for model in &mut models {
        model["listens"] = json!([]);
        for relation in model["relations"].as_array_mut().into_iter().flatten() { relation["updates"] = json!([]); }
    }
    let (user, record) = (index(&models, "User"), index(&models, "Record"));
    let mut node = models[record]["relations"].as_array().unwrap().iter().find(|relation| relation["name"] == "node").unwrap().clone();
    node["remote"]["operation"] = json!("UserNode");
    models[user]["relations"].as_array_mut().unwrap().push(node);
    models[user]["changes"] = json!([{"area":"USER","relation":"node"}]);
    let changes = models[record]["changes"].as_array_mut().unwrap();
    changes.push(json!({"area":"RECORD_STATUS","relation":"transcode_status"}));
    if node_area { changes.push(json!({"area":"RECORD_NODE","relation":"node"})); }
    serde_json::to_string(&models).unwrap()
}

fn deps_operations() -> Value {
    json!([
        {"name":"UserNode","document":"query UserNode($id: ID!) { node(id: $id) { ... on User { id name } } }"},
        {"name":"RecordNode","document":"query RecordNode($id: ID!) { node(id: $id) { ... on Record { id containerId authorId regionId text createdAt sequenceNumber reactions { body userId } } } }"},
        {"name":"RecordTranscodeStatus","document":"query RecordTranscodeStatus($id: ID!) { node(id: $id) { ... on Record { id containerId authorId regionId text createdAt sequenceNumber reactions { body userId } } } }"},
        {"name":"ContainerNode","document":"query ContainerNode($id: ID!) { node(id: $id) { ... on Container { id title shelves } } }"},
        {"name":"ContainerThread","document":"query ContainerThread($containerId: ID!, $first: Int, $after: String) { container(id: $containerId) { records(first: $first, after: $after) { nodes { id } pageInfo { hasNextPage endCursor } } } }"},
        {"name":"ContainerShelfQuery","document":"query ContainerShelfQuery { containers { id title shelves } }"},
        {"name":"RecordEvents","document":"subscription RecordEvents { recordEvents { entries { topic action recordId } } }"}
    ])
}

fn user(id: &str, name: &str) -> Value { json!({"data":{"node":{"__typename":"User","id":id,"name":name}}}) }

fn container(id: &str) -> Value { json!({"data":{"node":{"__typename":"Container","id":id,"title":"box","shelves":[]}}}) }

fn record_row(id: &str, container: &str, text: &str, day: i64, reactions: Value) -> Value {
    let created = format!("2026-11-{day:02}T00:00:00.000Z");
    json!({"__typename":"Record","id":id,"containerId":container,"authorId":"a1","regionId":null,"text":text,"createdAt":created,"sequenceNumber":day,"reactions":reactions,"attachment":null,"media":null})
}

fn record(id: &str, container: &str, text: &str, day: i64, reactions: Value) -> Value { json!({"data":{"node":record_row(id, container, text, day, reactions)}}) }

fn thread(rows: Vec<Value>) -> Value { json!({"data":{"container":{"records":{"nodes":rows,"pageInfo":{"hasNextPage":false,"endCursor":null}}}}}) }

const REMOTE_THREAD: &str = r#"{"model":"Record","relation":"thread","params":{"containerId":"k1"}}"#;
const REMOTE_SHELF: &str = r#"{"model":"Container","relation":"shelf","params":{"shelf":"PRIMARY"}}"#;

struct Stand {
    database: Arc<Database>,
    transport: Arc<FeedTransport>,
    world: Arc<World>,
    _feed: Arc<ReadStream>,
}

impl Stand {
    async fn open(node_area: bool) -> Self {
        let world = World::new("server", "A", true);
        let (database, transport) = feed_database_over(None, Arc::default(), world.clone(), deps_models(node_area), deps_operations()).await;
        let feed = database.subscribe_values(database.session_epoch(), ReadRequest::Events { model: "Container".into() });
        assert!(matches!(bound(feed.pull_next()).await, Some(ReadValue::Ready)));
        Stand { database, transport, world, _feed: feed }
    }

    /// The row lands through its `node` read and nobody keeps reading it.
    async fn land(&self, model: &str, id: &str, operation: &str, answer: Value) {
        self.world.answer_with(operation, answer);
        let rows = Arc::new(Rows::default());
        let query = json!({"model":model,"relation":"node","params":{crate::schema::NODE_PARAM: id}}).to_string();
        let reader = self.database.watch_query(&query, rows.clone());
        crate::test_wait::settled(&self.database, "the row lands through node", || rows.len() == 1).await;
        reader.cancel();
    }

    /// The server event of `area` for row `id` goes through the change feed; the caller waits for what the event causes.
    async fn event(&self, area: &str, id: &str) {
        crate::test_wait::signalled(&self.transport.opened, "the change feed is subscribed", || self.transport.token.lock().unwrap().is_some()).await;
        self.transport.clock.advance(1_000);
        let token = self.transport.token.lock().unwrap().clone().unwrap();
        assert!(self.transport.cable.event(&token, json!({"data":{"recordEvents":{"entries":[{"topic":area,"action":"UPDATED","recordId":id}]}}})));
    }

    async fn quiet(&self) { crate::test_wait::settled(&self.database, "quiet", || true).await; }

    fn field(&self, model: &str, row: &WireRow, name: &str) -> Cell {
        let at = wire_fields(self.database.db.spec(model).unwrap()).iter().position(|declared| declared.name == name).unwrap();
        row.cells[at].clone()
    }

    fn shows(&self, rows: &Rows, model: &str, name: &str, wanted: impl Fn(&Cell) -> bool) -> bool {
        rows.0.lock().unwrap().iter().any(|row| wanted(&self.field(model, row, name)))
    }
}

/// P-FEED-43 (1): the event of a row that only a `ref:` of another model's value shows reads that row through its `node` once, the value of the other model gets a frame with the new nested value, and nothing else is asked.
#[tokio::test(flavor = "multi_thread")]
async fn feed_event_rereads_a_row_shown_through_a_ref() {
    for window in [false, true] {
        let stand = Stand::open(true).await;
        stand.land("User", "u1", "UserNode", user("u1", "before")).await;
        let reactions = json!([{"body":"B","userId":"u1","user":{"id":"u1"}}]);
        let shown = Arc::new(Rows::default());
        let _list = if window {
            stand.world.answer_with("ContainerThread", thread(vec![record_row("r1", "k1", "text", 1, reactions)]));
            let list = stand.database.watch_query(REMOTE_THREAD, shown.clone());
            bound(list.wait_for_initial_load()).await.unwrap();
            list
        } else {
            stand.land("Record", "r1", "RecordNode", record("r1", "k1", "text", 1, reactions)).await;
            stand.database.watch_query(LOCAL_RECORDS, shown.clone())
        };
        let named = |name: &'static str| move |cell: &Cell| texts(cell).iter().any(|text| text == name);
        crate::test_wait::settled(&stand.database, "the list shows the reactor", || stand.shows(&shown, "Record", "reactions", named("before"))).await;
        let (users, records) = (stand.world.count("UserNode"), stand.world.count("RecordNode"));
        stand.world.answer_with("UserNode", user("u1", "after"));
        stand.event("USER", "u1").await;
        crate::test_wait::settled(&stand.database, "the event starts one UserNode read", || stand.world.count("UserNode") == users + 1).await;
        crate::test_wait::settled(&stand.database, "the list shows the new reactor", || stand.shows(&shown, "Record", "reactions", named("after"))).await;
        stand.quiet().await;
        assert_eq!((stand.world.count("UserNode"), stand.world.count("RecordNode")), (users + 1, records), "window {window}: after the landing nothing is asked and the record is not read");
        bound(stand.database.close_database()).await.unwrap();
    }
}

/// P-FEED-43 (2): the event of a child of a derived field reads the child through its `node` and the shown parent gets a frame with the recomputed field.
#[tokio::test(flavor = "multi_thread")]
async fn feed_event_of_a_child_refreshes_the_derived_parent() {
    for window in [false, true] {
        let stand = Stand::open(true).await;
        stand.land("Container", "k1", "ContainerNode", container("k1")).await;
        let shown = Arc::new(Rows::default());
        let _list = if window {
            stand.world.answer_with("ContainerShelfQuery", json!({"data":{"containers":[{"__typename":"Container","id":"k1","title":"box","shelves":["PRIMARY"]}]}}));
            let list = stand.database.watch_query(REMOTE_SHELF, shown.clone());
            bound(list.wait_for_initial_load()).await.unwrap();
            list
        } else {
            stand.database.watch_query(LOCAL_CONTAINERS, shown.clone())
        };
        stand.land("Record", "r1", "RecordNode", record("r1", "k1", "text", 1, json!([]))).await;
        let derived = || shown.0.lock().unwrap().first().map(|row| stand.field("Container", row, "lastRecordAt"));
        crate::test_wait::settled(&stand.database, "the parent shows the field of the child", || derived().is_some_and(|cell| cell != Cell::Null)).await;
        let before = derived().unwrap();
        let records = stand.world.count("RecordNode");
        stand.world.answer_with("RecordNode", record("r1", "k1", "text", 2, json!([])));
        stand.event("RECORD_NODE", "r1").await;
        crate::test_wait::settled(&stand.database, "the event starts one RecordNode read of the child", || stand.world.count("RecordNode") == records + 1).await;
        crate::test_wait::settled(&stand.database, "the parent shows the recomputed field", || derived().is_some_and(|cell| cell != before)).await;
        stand.quiet().await;
        assert_eq!(stand.world.count("RecordNode"), records + 1, "window {window}: after the landing nothing is asked");
        bound(stand.database.close_database()).await.unwrap();
    }
}

/// P-FEED-43 (3): the event names the second `node: :one` read of the model, so that read is asked and the first one is not, and the landing of the second ends the debt.
#[tokio::test(flavor = "multi_thread")]
async fn feed_event_is_paid_by_the_read_it_names() {
    let stand = Stand::open(true).await;
    stand.land("Record", "r1", "RecordNode", record("r1", "k1", "before", 1, json!([]))).await;
    let shown = Arc::new(Rows::default());
    let _list = stand.database.watch_query(LOCAL_RECORDS, shown.clone());
    let text = |wanted: &'static str| move |cell: &Cell| *cell == Cell::Text { value: wanted.into() };
    crate::test_wait::settled(&stand.database, "the list shows the record", || stand.shows(&shown, "Record", "text", text("before"))).await;
    let (first, second) = (stand.world.count("RecordNode"), stand.world.count("RecordTranscodeStatus"));
    stand.world.answer_with("RecordTranscodeStatus", record("r1", "k1", "after", 1, json!([])));
    stand.event("RECORD_STATUS", "r1").await;
    crate::test_wait::settled(&stand.database, "the event starts one RecordTranscodeStatus read", || stand.world.count("RecordTranscodeStatus") == second + 1).await;
    assert_eq!(stand.world.count("RecordNode"), first, "the first read is not asked");
    crate::test_wait::settled(&stand.database, "the list shows the new value", || stand.shows(&shown, "Record", "text", text("after"))).await;
    stand.quiet().await;
    assert_eq!((stand.world.count("RecordNode"), stand.world.count("RecordTranscodeStatus")), (first, second + 1), "the landing of the named read ends the debt");
    bound(stand.database.close_database()).await.unwrap();
}
