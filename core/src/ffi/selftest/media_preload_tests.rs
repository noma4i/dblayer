use crate::ffi::tape::StreamDelivery;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use serde_json::{json, Value};

use crate::ffi::{ByteProgress, CableLease, Config, DbError, EncryptedSocketEvent, ReadPhase, OpenDatabase, OpenOptions, PutReply, ReadRequest, ReadState, ReadValue, SubscriptionSink, Transport, TransportRequest};
use crate::schema::{Coverage, FieldSpec, ModelSpec, PageDirection, RelationSpec, RemoteShape, RowRule, RowTest, VariableSource};
use crate::test_wait::within_secs;
use crate::value::ScalarKind;

#[derive(Default)]
struct MediaTransport {
    requests: Mutex<Vec<TransportRequest>>,
    preload: tokio::sync::Notify,
}

#[derive(Default)]
struct MediaSink {
    states: Mutex<Vec<ReadState>>,
    changed: tokio::sync::Notify,
}

impl crate::ffi::tape::Reader for MediaSink {
    fn delivered(&self, delivery: StreamDelivery) {
        if let ReadValue::Snapshot { state, .. } | ReadValue::Delta { state, .. } | ReadValue::State { state, .. } = delivery.value {
            self.states.lock().unwrap().push(state);
            self.changed.notify_one();
        }
    }
}

#[async_trait::async_trait]
impl Transport for MediaTransport {
    async fn open_cable(&self, _: Arc<CableLease>, _url: String) -> Result<(), DbError> { Err(DbError::Network { detail: "no cable".into() }) }
    async fn cable_send(&self, _: String, _: String) -> Result<(), DbError> { unreachable!() }
    async fn cable_next(&self, _: String) -> Option<EncryptedSocketEvent> { None }
    async fn cable_close(&self, _: String) {}
    async fn query(&self, request: TransportRequest) -> Result<String, DbError> {
        self.requests.lock().unwrap().push(request.clone());
        if request.operation_name == "PresenceOnline" {
            self.preload.notify_one();
            return Ok(json!({"data":{"presenceOnline":[]}}).to_string());
        }
        if request.operation_name != "ChatMedia" { return Err(DbError::Invalid { detail: format!("unexpected operation {}", request.operation_name) }); }
        let variables: Value = serde_json::from_str(&request.variables_json).unwrap();
        let chat = variables["chatId"].as_str().ok_or_else(|| DbError::Invalid { detail: "missing chatId".into() })?;
        let gifts = variables["includeGifts"] == true;
        let audio = variables["mediaKinds"][0] == "AUDIO";
        let (id, kind, media_kind) = if chat == "c2" { ("x1", "GIFT", Value::Null) } else if gifts { ("g1", "GIFT", Value::Null) } else if audio { ("a1", "TEXT", json!("AUDIO")) } else { ("v1", "TEXT", json!("IMAGE")) };
        let after = variables.get("after").and_then(Value::as_str);
        if chat == "c2" && after.is_some() { return Err(DbError::Network { detail: "down".into() }); }
        let edges = if after.is_some() { vec![] } else { vec![json!({"cursor":id,"node":{"__typename":"MediaMessage","id":id,"chatId":chat,"kind":kind,"mediaKind":media_kind,"sequenceNumber":1}})] };
        Ok(json!({"data":{"node":{"messages":{"edges":edges,"pageInfo":{"hasNextPage":after.is_none(),"endCursor":if after.is_none() { Some(id) } else { None }}}}}}).to_string())
    }
    async fn mutate(&self, _: TransportRequest) -> Result<String, DbError> { unreachable!() }
    async fn subscribe(&self, _: TransportRequest, _: Arc<SubscriptionSink>) -> Result<(), DbError> { unreachable!() }
    fn unsubscribe(&self, _: String) {}
    async fn put(&self, _: String, _: HashMap<String, String>, _: crate::transport::PutFile, _: crate::transport::TransferIdentity, _: Arc<ByteProgress>) -> Result<PutReply, DbError> { unreachable!() }
}

fn models() -> String {
    let mut presence = ModelSpec::new("Presence", vec![FieldSpec::scalar("id", ScalarKind::Id)]);
    let mut online = RelationSpec::global("online");
    online.remote = Some(crate::schema_fixtures::remote("PresenceOnline", RemoteShape::List { coverage: Coverage::Complete }, "30.seconds"));
    online.preload = Some(Default::default());
    presence.relations.push(online);
    let mut media_kind = FieldSpec::scalar("mediaKind", ScalarKind::Str);
    media_kind.nullable = true;
    let mut media = ModelSpec::new("MediaMessage", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("chatId", ScalarKind::Id), FieldSpec::scalar("kind", ScalarKind::Str), media_kind, FieldSpec::scalar("sequenceNumber", ScalarKind::Int)]);
    media.indexes.push(vec!["chatId".into()]);
    for (name, field, value, kinds, gifts) in [("visual_media", "mediaKind", "IMAGE", Some(json!(["IMAGE", "VIDEO"])), false), ("audio_media", "mediaKind", "AUDIO", Some(json!(["AUDIO"])), false), ("gift_media", "kind", "GIFT", None, true)] {
        let mut relation = RelationSpec::new(name, &["chatId"]);
        let test = if name == "visual_media" { RowTest::In { values: vec![json!("IMAGE"), json!("VIDEO")] } } else { RowTest::Equals { value: json!(value) } };
        relation.row_rules.push(RowRule { field: field.into(), test });
        relation.joins_node = true;
        relation.joins_events = true;
        let mut remote = crate::schema_fixtures::remote("ChatMedia", RemoteShape::Connection { direction: PageDirection::Forward, coverage: Coverage::Page, max_pages: None, cursor: None }, "5.minutes");
        remote.root = Some("node.messages".into());
        remote.page_size = Some(30);
        remote.variables.insert("includeGifts".into(), VariableSource::Constant { value: json!(gifts) });
        if let Some(kinds) = kinds { remote.variables.insert("mediaKinds".into(), VariableSource::Constant { value: kinds }); }
        relation.remote = Some(remote);
        media.relations.push(relation);
    }
    serde_json::to_string(&vec![presence, media]).unwrap()
}

#[derive(Default)]
struct MarkedMediaTransport {
    requests: Mutex<Vec<String>>,
}

#[async_trait::async_trait]
impl Transport for MarkedMediaTransport {
    async fn open_cable(&self, _: Arc<CableLease>, _url: String) -> Result<(), DbError> { Err(DbError::Network { detail: "no cable".into() }) }
    async fn cable_send(&self, _: String, _: String) -> Result<(), DbError> { unreachable!() }
    async fn cable_next(&self, _: String) -> Option<EncryptedSocketEvent> { None }
    async fn cable_close(&self, _: String) {}
    async fn query(&self, request: TransportRequest) -> Result<String, DbError> {
        let count = {
            let mut requests = self.requests.lock().unwrap();
            let count = requests.iter().filter(|name| name.as_str() == request.operation_name.as_str()).count();
            requests.push(request.operation_name.clone());
            count
        };
        match request.operation_name.as_str() {
            "MediaMarker" => {
                let visual = if count < 2 { 0 } else { 1 };
                Ok(json!({"data":{"node":{"mediaLibrary":{"revision":1,"visualCount":visual,"audioCount":0,"giftCount":0,"latestSequence":1}}}}).to_string())
            }
            "ChatMedia" => {
                let id = if count == 0 { "v1" } else { "v2" };
                Ok(json!({"data":{"node":{"messages":{"edges":[{"cursor":id,"node":{"id":id,"chatId":"c1","kind":"TEXT","mediaKind":"IMAGE","sequenceNumber":1}}],"pageInfo":{"hasNextPage":false,"endCursor":id}}}}}).to_string())
            }
            other => Err(DbError::Invalid { detail: format!("unexpected operation {other}") }),
        }
    }
    async fn mutate(&self, request: TransportRequest) -> Result<String, DbError> {
        if request.operation_name != "LikeMoment" { return Err(DbError::Invalid { detail: format!("unexpected operation {}", request.operation_name) }); }
        Ok(json!({"data":{"likeMoment":{"id":"v1","chatId":"c1","kind":"TEXT","mediaKind":"IMAGE","sequenceNumber":1}}}).to_string())
    }
    async fn subscribe(&self, _: TransportRequest, _: Arc<SubscriptionSink>) -> Result<(), DbError> { unreachable!() }
    fn unsubscribe(&self, _: String) {}
    async fn put(&self, _: String, _: HashMap<String, String>, _: crate::transport::PutFile, _: crate::transport::TransferIdentity, _: Arc<ByteProgress>) -> Result<PutReply, DbError> { unreachable!() }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_media_refetch_after_mutation_reads_the_whole_stale_window() {
    use crate::schema::{ActionDeclaration, MediaSpec, RootShape};
    let mut specs: Vec<ModelSpec> = serde_json::from_str(&models()).unwrap();
    let media = specs.iter_mut().find(|model| model.key.as_str() == "MediaMessage").unwrap();
    let mut action = ActionDeclaration::new("like", RootShape::Upsert);
    action.mutation = Some("LikeMoment".into());
    action.variables.insert("momentId".into(), "id".into());
    action.refetch.push("visual_media".into());
    media.actions.push(action);
    let relation = media.relations.iter_mut().find(|relation| relation.name == "visual_media").unwrap();
    let bindings = std::collections::BTreeMap::from([("chatId".into(), VariableSource::Param { name: "chatId".into() })]);
    relation.media = Some(MediaSpec { marker: "MediaMarker".into(), from: "node.mediaLibrary".into(), fields: ["revision", "visualCount", "audioCount", "giftCount"].map(str::to_string).to_vec(), sequence: "latestSequence".into(), tail: "MediaTail".into(), after: "afterSequence".into(), marker_variables: bindings.clone(), tail_variables: bindings });
    let operations = json!([
        {"name":"ChatMedia","document":"query ChatMedia($chatId: ID!, $first: Int, $after: String) { node(id: $chatId) { messages(first: $first, after: $after) { edges { cursor node { id chatId kind mediaKind sequenceNumber } } pageInfo { hasNextPage endCursor } } } }"},
        {"name":"MediaMarker","document":"query MediaMarker($chatId: ID!, $upToSequence: Int!) { node(id: $chatId) { mediaLibrary(upToSequence: $upToSequence) { revision visualCount audioCount giftCount latestSequence } } }"},
        {"name":"MediaTail","document":"query MediaTail($chatId: ID!, $afterSequence: Int!, $first: Int, $after: String) { node(id: $chatId) { messages(afterSequence: $afterSequence, first: $first, after: $after) { edges { cursor node { id chatId kind mediaKind sequenceNumber } } pageInfo { hasNextPage endCursor } } } }"},
        {"name":"LikeMoment","document":"mutation LikeMoment($momentId: ID!) { likeMoment(momentId: $momentId) { id chatId kind mediaKind sequenceNumber } }"}
    ]).to_string();
    let transport = Arc::new(MarkedMediaTransport::default());
    let options = OpenOptions { operations, uploads: None, power_saving: false, low_ram: false };
    let database = within_secs(20, OpenDatabase::new().open(Config { path: None, data_version: None, cache_rows: 32 }, serde_json::to_string(&specs).unwrap(), options, Some(transport.clone()), None, None, None, None)).await.unwrap();
    database.set_online(true).unwrap();
    let epoch = database.session_epoch();
    let params = json!({"chatId":"c1"}).to_string();
    within_secs(20, database.load_all(epoch, "MediaMessage".into(), "visual_media".into(), params.clone(), false)).await.unwrap();
    let receipt = within_secs(20, database.act(epoch, "MediaMessage".into(), "v1".into(), "like".into(), "{}".into(), "{}".into())).await.unwrap();
    assert!(matches!(within_secs(20, database.outcome(receipt)).await.unwrap(), crate::ffi::WireOperationOutcome::Confirmed { .. }));
    within_secs(20, database.load_all(epoch, "MediaMessage".into(), "visual_media".into(), params.clone(), false)).await.unwrap();
    let query = json!({"model":"MediaMessage","relation":"visual_media","params":{"chatId":"c1"}}).to_string();
    let rows = within_secs(20, database.read_query(epoch, query)).await.unwrap();
    assert_eq!(rows.iter().map(|row| row.id.as_str()).collect::<Vec<_>>(), ["v2"]);
    within_secs(20, database.load_all(epoch, "MediaMessage".into(), "visual_media".into(), params, false)).await.unwrap();
    let requests = transport.requests.lock().unwrap();
    assert_eq!(requests.iter().filter(|name| *name == "ChatMedia").count(), 2);
    assert_eq!(requests.iter().filter(|name| *name == "MediaMarker").count(), 5);
    drop(requests);
    within_secs(20, database.close_database()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn preloads_and_three_media_loads_finish_on_empty_pages() {
    let transport = Arc::new(MediaTransport::default());
    let operations = json!([
        {"name":"PresenceOnline","document":"query PresenceOnline { presenceOnline { id } }"},
        {"name":"ChatMedia","document":"query ChatMedia($chatId: ID!, $first: Int, $after: Cursor, $mediaKinds: [String!], $includeGifts: Boolean!) { node(id: $chatId) { ... on Chat { messages(first: $first, after: $after, mediaKinds: $mediaKinds, includeGifts: $includeGifts) { edges { cursor node { id chatId kind mediaKind sequenceNumber } } pageInfo { hasNextPage endCursor } } } } }"}
    ]).to_string();
    let options = OpenOptions { operations, uploads: None, power_saving: false, low_ram: false };
    let opener = OpenDatabase::new();
    let database = within_secs(20, opener.open(Config { path: None, data_version: None, cache_rows: 32 }, models(), options, Some(transport.clone()), None, None, None, None)).await.unwrap();
    database.set_online(true).unwrap();
    within_secs(20, transport.preload.notified()).await;
    let epoch = database.session_epoch();
    for (relation, id) in [("visual_media", "v1"), ("audio_media", "a1"), ("gift_media", "g1")] {
        within_secs(20, database.load_all(epoch, "MediaMessage".into(), relation.into(), json!({"chatId":"c1"}).to_string(), false)).await.unwrap();
        let query = json!({"model":"MediaMessage","relation":relation,"params":{"chatId":"c1"}}).to_string();
        let rows = within_secs(20, database.read_query(epoch, query)).await.unwrap();
        assert_eq!(rows.iter().map(|row| row.id.as_str()).collect::<Vec<_>>(), [id]);
        let sink = Arc::new(MediaSink::default());
        let request = json!({"model":"MediaMessage","relation":relation,"params":{"chatId":"c1"}}).to_string();
        let stream = database.subscribe_reading(epoch, ReadRequest::List { query_json: request }, sink.clone());
        within_secs(20, database.loaded(stream)).await.unwrap();
        within_secs(20, async {
            loop {
                let changed = sink.changed.notified();
                if sink.states.lock().unwrap().iter().any(|state| state.loaded.is_some()) { break; }
                changed.await;
            }
        }).await;
        database.cancel_stream(stream);
    }
    let requests = transport.requests.lock().unwrap().clone();
    let media: Vec<_> = requests.iter().filter(|request| request.operation_name == "ChatMedia").collect();
    assert_eq!(media.len(), 6);
    for (index, id) in ["v1", "a1", "g1"].iter().enumerate() {
        let first: Value = serde_json::from_str(&media[index * 2].variables_json).unwrap();
        let second: Value = serde_json::from_str(&media[index * 2 + 1].variables_json).unwrap();
        assert!(first.get("after").is_none());
        assert_eq!(second["after"], *id);
    }
    assert_eq!(requests.iter().filter(|request| request.operation_name == "PresenceOnline").count(), 1);
    let failure_query = json!({"model":"MediaMessage","relation":"gift_media","params":{"chatId":"c2"}}).to_string();
    let sink = Arc::new(MediaSink::default());
    let stream = database.subscribe_reading(epoch, ReadRequest::List { query_json: failure_query.clone() }, sink.clone());
    within_secs(20, database.loaded(stream)).await.unwrap();
    let outcome = within_secs(20, database.load_all(epoch, "MediaMessage".into(), "gift_media".into(), json!({"chatId":"c2"}).to_string(), false)).await;
    assert!(matches!(outcome, Err(DbError::Network { .. })));
    within_secs(20, async {
        loop {
            let changed = sink.changed.notified();
            if sink.states.lock().unwrap().iter().any(|state| state.error.is_some()) { break; }
            changed.await;
        }
    }).await;
    let states = sink.states.lock().unwrap().clone();
    assert!(states.iter().any(|state| state.phase == ReadPhase::Loaded && state.error.is_some()));
    assert_eq!(within_secs(20, database.read_query(epoch, failure_query)).await.unwrap().iter().map(|row| row.id.as_str()).collect::<Vec<_>>(), ["x1"]);
    database.cancel_stream(stream);
    within_secs(20, database.close_database()).await.unwrap();
}
