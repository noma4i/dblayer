use serde_json::{json, Value};

use super::nested_storage_tests::wrapper_models;
use super::crash_image_support::advancing_by;
use super::rig_tests::{bound, Fault, Rig, World};

const DEFINITIONS: &str = r#"[{"name":"MediaMarker","document":"query MediaMarker($chatId: ID!, $upToSequence: Int!) { node(id: $chatId) { mediaLibrary(upToSequence: $upToSequence) { revision visualCount audioCount giftCount latestSequence } } }"},{"name":"MediaTail","document":"query MediaTail($chatId: ID!, $afterSequence: Int!, $first: Int, $after: String) { node(id: $chatId) { messages(afterSequence: $afterSequence, first: $first, after: $after) { edges { cursor node { id chatId kind mediaKind sequenceNumber } } pageInfo { hasNextPage endCursor } } } }"},{"name":"LikeMoment","document":"mutation LikeMoment($momentId: ID!) { likeMoment(momentId: $momentId) { id chatId kind mediaKind sequenceNumber } }"},{"name":"ChatMedia","document":"query ChatMedia($chatId: ID!, $first: Int, $after: Cursor, $mediaKinds: [String!], $includeGifts: Boolean!) { node(id: $chatId) { ... on Chat { messages(first: $first, after: $after, mediaKinds: $mediaKinds, includeGifts: $includeGifts) { edges { cursor node { id chatId kind mediaKind sequenceNumber } } pageInfo { hasNextPage endCursor } } } } }"}]"#;

fn media_models(models: &mut Value) {
    wrapper_models(models);
    let all = models.as_array_mut().unwrap();
    let like = all.iter().find(|model| model["key"] == "Moment").unwrap()["actions"].as_array().unwrap().iter().find(|action| action["name"] == "like").unwrap().clone();
    let mut like = like;
    like["refetch"] = json!(["visual_media"]);
    like["optimistic"] = json!({});
    let media = all.iter_mut().find(|model| model["key"] == "MediaMessage").unwrap();
    media["actions"] = json!([like]);
    let relation = media["relations"].as_array_mut().unwrap().iter_mut().find(|relation| relation["name"] == "visual_media").unwrap();
    let binding = json!({"chatId": {"from": "param", "name": "chatId"}});
    relation["media"] = json!({"marker": "MediaMarker", "from": "node.mediaLibrary", "fields": ["revision", "visualCount", "audioCount", "giftCount"], "sequence": "latestSequence", "tail": "MediaTail", "after": "afterSequence", "marker_variables": binding, "tail_variables": binding});
}

fn marker(count: i64) -> Fault { Fault::Answer(json!({"data":{"node":{"mediaLibrary":{"revision":1,"visualCount":count,"audioCount":0,"giftCount":0,"latestSequence":1}}}})) }

fn page(id: &str) -> Fault {
    Fault::Answer(json!({"data":{"node":{"messages":{"edges":[{"cursor":id,"node":{"__typename":"MediaMessage","id":id,"chatId":"c1","kind":"TEXT","mediaKind":"IMAGE","sequenceNumber":1}}],"pageInfo":{"hasNextPage":false,"endCursor":id}}}}}))
}

async fn driven(rig: &Rig, epoch: u64, params: &str) {
    let load = { let database = rig.database.clone(); let params = params.to_string(); rig.spawn(async move { database.load_all(epoch, "MediaMessage".into(), Some("visual_media".into()), params, false).await }) };
    let ended = bound(advancing_by(&rig.clock, 1_000, load)).await;
    ended.unwrap().unwrap();
}

fn requests(rig: &Rig, operation: &str) -> usize { rig.world.log().iter().filter(|entry| entry.operation == operation).count() }

#[tokio::test(flavor = "multi_thread")]
async fn a_media_refetch_after_mutation_reads_the_whole_stale_window() {
    let rig = Rig::signed_as(World::new("server", "A", true), false, media_models).await;
    rig.database.register_operations(DEFINITIONS.into()).await.unwrap();
    for operation in ["MediaMarker", "MediaTail", "LikeMoment", "ChatMedia"] { rig.world.answer_with(operation, json!({"data":{}})); }
    for count in [0, 0, 1, 1, 1] { rig.world.fail_next("MediaMarker", marker(count)); }
    rig.world.fail_next("ChatMedia", page("v1"));
    rig.world.fail_next("ChatMedia", page("v2"));
    rig.world.fail_next("LikeMoment", Fault::Answer(json!({"data":{"likeMoment":{"__typename":"MediaMessage","id":"v1","chatId":"c1","kind":"TEXT","mediaKind":"IMAGE","sequenceNumber":1}}})));
    let epoch = rig.database.session_epoch();
    let params = json!({"chatId":"c1"}).to_string();
    let query = json!({"model":"MediaMessage","relation":"visual_media","params":{"chatId":"c1"}}).to_string();
    driven(&rig, epoch, &params).await;
    let ids = |rows: Vec<crate::ffi::wire::WireRow>| rows.into_iter().map(|row| row.id).collect::<Vec<_>>();
    assert_eq!(ids(bound(rig.database.read_query(epoch, crate::ffi::QuerySpec::json(query.clone()))).await.unwrap()), ["v1"]);
    let receipt = bound(rig.database.act(epoch, "MediaMessage".into(), "v1".into(), "like".into(), "{}".into(), "{}".into())).await.unwrap();
    assert!(bound(rig.database.confirmed(receipt, false)).await.is_ok());
    bound(rig.database.load_all(epoch, "MediaMessage".into(), Some("visual_media".into()), params.clone(), false)).await.unwrap();
    assert_eq!(ids(bound(rig.database.read_query(epoch, crate::ffi::QuerySpec::json(query))).await.unwrap()), ["v2"], "the stale window was not read again after the mutation");
    assert_eq!(requests(&rig, "ChatMedia"), 2);
    bound(rig.database.load_all(epoch, "MediaMessage".into(), Some("visual_media".into()), params.clone(), false)).await.unwrap();
    assert_eq!(requests(&rig, "MediaMarker"), 5);
    rig.database.close_now();
}

#[derive(Default)]
struct ChainRows {
    rows: crate::ffi::tape::Rows,
    has_more: std::sync::Mutex<Option<bool>>,
}

impl crate::ffi::tape::Reader for ChainRows {
    fn delivered(&self, delivery: crate::ffi::tape::StreamDelivery) {
        match &delivery.value {
            crate::ffi::ReadValue::Snapshot { state, .. } | crate::ffi::ReadValue::Delta { state, .. } | crate::ffi::ReadValue::State { state, .. } => *self.has_more.lock().unwrap() = Some(state.has_more),
            _ => {}
        }
        crate::ffi::tape::Reader::delivered(&self.rows, delivery);
    }
}

fn chain_marker(revision: &str, count: i64, sequence: i64) -> Fault {
    Fault::Answer(json!({"data":{"node":{"mediaLibrary":{"revision":revision,"visualCount":count,"audioCount":0,"giftCount":0,"latestSequence":sequence}}}}))
}

fn chain_page(numbers: &[i32], more: bool) -> Fault {
    let nodes: Vec<_> = numbers.iter().map(|n| json!({"id":n.to_string(),"text":"media","sequenceNumber":n})).collect();
    Fault::Answer(json!({"data":{"node":{"messages":{"nodes":nodes,"pageInfo":{"hasNextPage":more,"endCursor":numbers.last().unwrap().to_string()}}}}}))
}

async fn chain_rig() -> (Rig, std::sync::Arc<ChainRows>, std::sync::Arc<crate::ffi::tape::Watched>) {
    let rig = Rig::signed_as(World::new("server", "A", true), false, |models| {
        *models = serde_json::from_str::<Value>(include_str!("../../../../fixtures/media-marker.json")).unwrap()["models"].clone();
    }).await;
    let document = include_str!("../../../../fixtures/operations/queries/media.graphql");
    rig.database.register_operations(json!([{"name":"chatMedia", "document":document},{"name":"chatMediaState", "document":document}]).to_string()).await.unwrap();
    rig.world.answer_with("chatMedia", json!({"data":{}}));
    rig.world.answer_with("chatMediaState", json!({"data":{}}));
    rig.world.fail_next("chatMediaState", chain_marker("1", 0, 6));
    rig.world.fail_next("chatMedia", chain_page(&[1, 2], true));
    rig.world.fail_next("chatMedia", chain_page(&[3, 4], true));
    rig.world.fail_next("chatMedia", chain_page(&[5, 6], false));
    rig.world.fail_next("chatMediaState", chain_marker("1", 6, 6));
    let rows = std::sync::Arc::new(ChainRows::default());
    let query = crate::ffi::QuerySpec::json(r#"{"model":"MediaEntry","relation":"media","params":{"chatId":"c1"}}"#);
    let stream = rig.database.watch(crate::ffi::ReadRequest::List { query }, rows.clone());
    crate::test_wait::settled(&rig, "three media pages", || rows.rows.len() == 6).await;
    assert_eq!(*rows.has_more.lock().unwrap(), Some(false));
    assert_eq!(requests(&rig, "chatMedia"), 3);
    (rig, rows, stream)
}

fn chain_ids(rows: &ChainRows) -> Vec<String> { rows.rows.0.lock().unwrap().iter().map(|row| row.id.clone()).collect() }

#[tokio::test(flavor = "multi_thread")]
async fn win_chain_refresh_keeps_tail() {
    let (rig, rows, _stream) = chain_rig().await;
    rig.world.fail_next("chatMediaState", chain_marker("1", 6, 6));
    let epoch = rig.database.session_epoch();
    bound(rig.database.refresh(epoch, "MediaEntry".into(), Some("media".into()), r#"{"chatId":"c1"}"#.into())).await.unwrap();
    crate::test_wait::settled(&rig, "the refresh settled", || chain_ids(&rows).len() == 6).await;
    assert_eq!(chain_ids(&rows), ["1", "2", "3", "4", "5", "6"]);
    assert_eq!(*rows.has_more.lock().unwrap(), Some(false));
    let result = bound(rig.database.load_more(epoch, "MediaEntry".into(), Some("media".into()), r#"{"chatId":"c1"}"#.into())).await.unwrap();
    assert!(matches!(result, crate::ffi::LoadMoreResult::NoMore), "{result:?}");
    assert_eq!(requests(&rig, "chatMedia"), 3);
    let calls: Vec<Value> = rig.world.log().into_iter().filter(|entry| entry.operation == "chatMedia").map(|entry| serde_json::from_str::<Value>(&entry.body).unwrap()["variables"].clone()).collect();
    assert!(calls[0].get("after").is_none_or(Value::is_null), "{calls:?}");
    assert_eq!(calls[1]["after"], "2");
    assert_eq!(calls[2]["after"], "4");
    rig.database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn win_chain_marker_change_resets() {
    let (rig, rows, _stream) = chain_rig().await;
    rig.world.fail_next("chatMediaState", chain_marker("2", 0, 8));
    rig.world.fail_next("chatMedia", chain_page(&[7, 8], false));
    rig.world.fail_next("chatMediaState", chain_marker("2", 2, 8));
    let epoch = rig.database.session_epoch();
    rig.clock.advance(1_000);
    bound(rig.database.refresh(epoch, "MediaEntry".into(), Some("media".into()), r#"{"chatId":"c1"}"#.into())).await.unwrap();
    crate::test_wait::settled(&rig, "the new chain landed", || chain_ids(&rows) == ["7", "8"]).await;
    assert_eq!(chain_ids(&rows), ["7", "8"]);
    assert_eq!(*rows.has_more.lock().unwrap(), Some(false));
    let calls: Vec<Value> = rig.world.log().into_iter().filter(|entry| entry.operation == "chatMedia").map(|entry| serde_json::from_str::<Value>(&entry.body).unwrap()["variables"].clone()).collect();
    assert_eq!(calls.len(), 4, "{calls:?}");
    assert!(calls[3].get("after").is_none_or(Value::is_null), "{calls:?}");
    assert!(calls[3].get("afterSequence").is_none_or(Value::is_null), "{calls:?}");
    rig.database.close_now();
}

#[tokio::test(flavor = "multi_thread")]
async fn win_chain_pages_share_an_author() {
    let rig = Rig::signed_as(World::new("server", "A", true), false, |models| {
        *models = serde_json::from_str::<Value>(include_str!("../../../../fixtures/media-marker.json")).unwrap()["models"].clone();
        let all = models.as_array_mut().unwrap();
        let mut author = all[0].clone();
        author["key"] = json!("MediaAuthor");
        author["relations"] = json!([]);
        let mut fields = author["fields"].as_array().unwrap().clone();
        fields.retain(|field| field["name"] == "id" || field["name"] == "text");
        author["fields"] = json!(fields);
        let mut key = fields.iter().find(|field| field["name"] == "text").unwrap().clone();
        key["name"] = json!("authorId");
        key["indexed"] = json!(true);
        all[0]["fields"].as_array_mut().unwrap().push(key);
        all[0]["associations"] = json!([{"dependent": null, "foreign_key": "authorId", "from": "author", "kind": "belongs_to", "name": "author", "newer": [], "sets": {}, "target": "MediaAuthor", "touches_parent": false}]);
        all.push(author);
    }).await;
    let document = include_str!("../../../../fixtures/operations/queries/media.graphql");
    rig.database.register_operations(json!([{"name":"chatMedia", "document":document},{"name":"chatMediaState", "document":document}]).to_string()).await.unwrap();
    rig.world.answer_with("chatMedia", json!({"data":{}}));
    rig.world.answer_with("chatMediaState", json!({"data":{}}));
    let page = |numbers: &[(i32, &str)], more: bool| {
        let nodes: Vec<_> = numbers.iter().map(|(n, name)| json!({"id":n.to_string(),"text":"media","sequenceNumber":n,"author":{"id":"a1","text":name}})).collect();
        Fault::Answer(json!({"data":{"node":{"messages":{"nodes":nodes,"pageInfo":{"hasNextPage":more,"endCursor":numbers.last().unwrap().0.to_string()}}}}}))
    };
    rig.world.fail_next("chatMediaState", chain_marker("1", 0, 3));
    rig.world.fail_next("chatMedia", page(&[(1, "Ann"), (2, "Ann")], true));
    rig.world.fail_next("chatMedia", page(&[(3, "Ann B")], false));
    rig.world.fail_next("chatMediaState", chain_marker("1", 3, 3));
    let rows = std::sync::Arc::new(ChainRows::default());
    let query = crate::ffi::QuerySpec::json(r#"{"model":"MediaEntry","relation":"media","params":{"chatId":"c1"}}"#);
    let _stream = rig.database.watch(crate::ffi::ReadRequest::List { query }, rows.clone());
    crate::test_wait::settled(&rig, "a chain whose pages share an author lands", || rows.rows.last().is_some_and(|state| state.phase == Some(crate::ffi::ReadPhase::Loaded) || state.error.is_some())).await;
    assert!(rows.rows.last().unwrap().error.is_none(), "{:?}", rows.rows.last().unwrap().error);
    assert_eq!(chain_ids(&rows), ["1", "2", "3"], "every row of both pages lands");
    let epoch = rig.database.session_epoch();
    let read = |query: &str| { let database = rig.database.clone(); let query = crate::ffi::QuerySpec::json(query); async move { bound(database.read_query(epoch, query)).await.unwrap() } };
    let authors = read(r#"{"model":"MediaAuthor"}"#).await;
    assert_eq!(authors.len(), 1, "the shared author is 1 row");
    assert!(authors[0].cells.iter().any(|cell| matches!(cell, crate::ffi::Cell::Text { value } if value == "Ann B")), "the last page of the author wins: {:?}", authors[0].cells);
    rig.database.close_now();
}
