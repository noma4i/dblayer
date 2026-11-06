use serde_json::{json, Value};

use super::nested_storage_tests::wrapper_models;
use super::rig_tests::{bound, Fault, Rig, World};
use crate::ffi::WireOperationOutcome;
use crate::test_wait::yield_until;

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
    let load = { let database = rig.database.clone(); let params = params.to_string(); tokio::spawn(async move { database.load_all(epoch, "MediaMessage".into(), "visual_media".into(), params, false).await }) };
    yield_until("the media load ends", || { rig.clock.advance(1_000); load.is_finished() }).await;
    load.await.unwrap().unwrap();
}

fn requests(rig: &Rig, operation: &str) -> usize { rig.world.log().iter().filter(|entry| entry.operation == operation).count() }

#[tokio::test(flavor = "multi_thread")]
async fn a_media_refetch_after_mutation_reads_the_whole_stale_window() {
    let rig = Rig::signed_as(World::new("server", "A", true), false, media_models).await;
    rig.database.register_operations(DEFINITIONS.into()).unwrap();
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
    assert_eq!(ids(bound(rig.database.read_query(epoch, query.clone())).await.unwrap()), ["v1"]);
    let receipt = bound(rig.database.act(epoch, "MediaMessage".into(), "v1".into(), "like".into(), "{}".into(), "{}".into())).await.unwrap();
    assert!(matches!(bound(rig.database.outcome(receipt)).await.unwrap(), WireOperationOutcome::Confirmed { .. }));
    bound(rig.database.load_all(epoch, "MediaMessage".into(), "visual_media".into(), params.clone(), false)).await.unwrap();
    assert_eq!(ids(bound(rig.database.read_query(epoch, query)).await.unwrap()), ["v2"], "the stale window was not read again after the mutation");
    assert_eq!(requests(&rig, "ChatMedia"), 2);
    bound(rig.database.load_all(epoch, "MediaMessage".into(), "visual_media".into(), params.clone(), false)).await.unwrap();
    assert_eq!(requests(&rig, "MediaMarker"), 5);
    rig.database.close_now();
}
