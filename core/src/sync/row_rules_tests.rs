use std::collections::BTreeMap;

use serde_json::{json, Map, Value};

use super::rig::*;
use super::subscribe::{EventEffect, SubscriptionDef};
use super::{LoadOptions, OperationDef, SyncConfig};
use crate::db::Db;
use crate::schema::{Coverage, FieldSpec, ModelSpec, PageDirection, RelationSpec, RemoteShape, RowRule, RowTest, VariableSource};
use crate::transport::{EventSpec, MockTransport, Reply, Response, SubscriptionEvent};
use crate::value::ScalarKind;

fn media_relation(name: &str, since: bool, rules: Vec<RowRule>, constants: Value) -> RelationSpec {
    let mut relation = RelationSpec::new(name, &["chatId"]);
    relation.joins_events = true;
    relation.row_rules = rules;
    if since {
        relation.text_params = vec!["afterSequence".to_string()];
        relation.scalar_types = BTreeMap::from([("afterSequence".to_string(), ScalarKind::Int)]);
    }
    let mut remote = remote("ChatMedia", RemoteShape::Connection { direction: PageDirection::Forward, coverage: Coverage::Page, max_pages: None, cursor: None }, "5.minutes");
    remote.variables = constants.as_object().unwrap().iter().map(|(name, value)| (name.clone(), VariableSource::Constant { value: value.clone() })).collect();
    relation.remote = Some(remote);
    relation
}

fn in_set(field: &str, values: Value) -> RowRule {
    RowRule { field: field.into(), test: RowTest::In { values: values.as_array().unwrap().clone() } }
}

fn equals(field: &str, value: Value) -> RowRule {
    RowRule { field: field.into(), test: RowTest::Equals { value } }
}

fn after(field: &str) -> RowRule {
    RowRule { field: field.into(), test: RowTest::GreaterThan { parameter: "afterSequence".into() } }
}

fn messages_spec() -> ModelSpec {
    let mut spec = ModelSpec::new(
        "messages",
        vec![
            FieldSpec::scalar("id", ScalarKind::Id),
            FieldSpec::scalar("chatId", ScalarKind::Id).indexed(),
            FieldSpec::enumeration("kind", &["TEXT", "GIFT"]),
            FieldSpec::enumeration("mediaKind", &["AUDIO", "IMAGE", "VIDEO"]).optional(),
            FieldSpec::scalar("sequenceNumber", ScalarKind::Int).optional(),
        ],
    );
    let visual = || in_set("mediaKind", json!(["IMAGE", "VIDEO"]));
    let audio = || equals("mediaKind", json!("AUDIO"));
    let gift = || equals("kind", json!("GIFT"));
    let visual_constants = json!({"mediaKinds": ["IMAGE", "VIDEO"], "includeGifts": false});
    let audio_constants = json!({"mediaKinds": ["AUDIO"], "includeGifts": false});
    let gift_constants = json!({"includeGifts": true});
    spec.relations.extend([
        media_relation("visual_media", false, vec![visual()], visual_constants.clone()),
        media_relation("audio_media", false, vec![audio()], audio_constants.clone()),
        media_relation("gift_media", false, vec![gift()], gift_constants.clone()),
        media_relation("visual_since", true, vec![visual(), after("sequenceNumber")], visual_constants),
        media_relation("audio_since", true, vec![audio(), after("sequenceNumber")], audio_constants),
        media_relation("gifts_since", true, vec![gift(), after("sequenceNumber")], gift_constants),
        media_relation("constants_only", false, Vec::new(), json!({"includeGifts": false})),
    ]);
    spec
}

async fn media_rig() -> (Rig, super::subscribe::Updates) {
    let rig = rig_on(Db::open_in_memory(vec![messages_spec()], None).unwrap(), MockTransport::new(), SyncConfig::default());
    rig.sync.register(OperationDef::new("ChatMedia", "operation ChatMedia { x }", "media").paged(2));
    rig.sync.register(OperationDef::new("MessageChanged", "subscription MessageChanged { x }", "messageChanged"));
    let updates = rig.sync.subscribe(SubscriptionDef::new(EventSpec::new("message_changed"), "MessageChanged", "messages", EventEffect::Upsert)).await.unwrap();
    (rig, updates)
}

async fn load(rig: &Rig, relation: &str, after_sequence: Option<i64>) -> Map<String, Value> {
    let mut query = params(json!({"chatId": "c1"}));
    if let Some(bound) = after_sequence { query.insert("afterSequence".into(), json!(bound)); }
    rig.transport.enqueue("ChatMedia", Reply::data(json!({"media": {"nodes": [], "pageInfo": {"hasNextPage": false, "endCursor": "c"}}})));
    rig.sync.load("messages", relation, &query, LoadOptions::default()).await.unwrap();
    query
}

async fn emit(rig: &Rig, row: Value) {
    rig.transport.emit("MessageChanged", SubscriptionEvent::Next(Response::data(json!({"messageChanged": row}))));
    rig.transport.await_consumed("MessageChanged").await;
}

fn message(id: &str, kind: &str, media_kind: Option<&str>, sequence: Option<i64>) -> Value {
    let mut row = json!({"id": id, "chatId": "c1", "kind": kind});
    if let Some(media_kind) = media_kind { row["mediaKind"] = json!(media_kind); }
    if let Some(sequence) = sequence { row["sequenceNumber"] = json!(sequence); }
    row
}

fn listed(rig: &Rig, relation: &str, query: &Map<String, Value>) -> Vec<String> {
    let mut ids = member_ids(rig, "messages", relation, query);
    ids.sort();
    ids
}

#[tokio::test(start_paused = true)]
async fn an_event_row_joins_only_the_loaded_windows_whose_row_rules_hold() {
    let (rig, _updates) = media_rig().await;
    let visual_media = load(&rig, "visual_media", None).await;
    let audio_media = load(&rig, "audio_media", None).await;
    let gift_media = load(&rig, "gift_media", None).await;
    let visual_since = load(&rig, "visual_since", Some(10)).await;
    let visual_since_far = load(&rig, "visual_since", Some(20)).await;
    let audio_since = load(&rig, "audio_since", Some(10)).await;
    let gifts_since = load(&rig, "gifts_since", Some(10)).await;
    let constants_only = load(&rig, "constants_only", None).await;

    for row in [
        message("image-15", "TEXT", Some("IMAGE"), Some(15)),
        message("video-10", "TEXT", Some("VIDEO"), Some(10)),
        message("audio-12", "TEXT", Some("AUDIO"), Some(12)),
        message("audio-9", "TEXT", Some("AUDIO"), Some(9)),
        message("gift-13", "GIFT", None, Some(13)),
        message("gift-null", "GIFT", None, None),
        message("text-14", "TEXT", None, Some(14)),
    ] {
        emit(&rig, row).await;
    }
    let mut elsewhere = message("image-other-chat", "TEXT", Some("IMAGE"), Some(15));
    elsewhere["chatId"] = json!("c2");
    emit(&rig, elsewhere).await;

    assert_eq!(listed(&rig, "visual_media", &visual_media), ["image-15", "video-10"], "mediaKind in (IMAGE, VIDEO)");
    assert_eq!(listed(&rig, "audio_media", &audio_media), ["audio-12", "audio-9"], "mediaKind = AUDIO");
    assert_eq!(listed(&rig, "gift_media", &gift_media), ["gift-13", "gift-null"], "kind = GIFT");
    assert_eq!(listed(&rig, "visual_since", &visual_since), ["image-15"], "sequenceNumber 15 > 10, 10 is not > 10");
    assert_eq!(listed(&rig, "visual_since", &visual_since_far), Vec::<String>::new(), "15 is not > 20: each loaded afterSequence window decides alone");
    assert_eq!(listed(&rig, "audio_since", &audio_since), ["audio-12"], "9 is not > 10");
    assert_eq!(listed(&rig, "gifts_since", &gifts_since), ["gift-13"], "a null sequenceNumber does not match");
    assert_eq!(listed(&rig, "constants_only", &constants_only), Vec::<String>::new(), "constant variables without a row rule keep automatic insertion off");
}

#[tokio::test(start_paused = true)]
async fn a_member_that_stops_matching_leaves_its_windows() {
    let (rig, _updates) = media_rig().await;
    let visual_media = load(&rig, "visual_media", None).await;
    let visual_since = load(&rig, "visual_since", Some(10)).await;
    emit(&rig, message("m1", "TEXT", Some("IMAGE"), Some(15))).await;
    assert_eq!((listed(&rig, "visual_media", &visual_media), listed(&rig, "visual_since", &visual_since)), (vec!["m1".to_string()], vec!["m1".to_string()]));
    emit(&rig, message("m1", "TEXT", Some("AUDIO"), Some(15))).await;
    assert_eq!((listed(&rig, "visual_media", &visual_media), listed(&rig, "visual_since", &visual_since)), (Vec::<String>::new(), Vec::<String>::new()));
    emit(&rig, message("m1", "TEXT", Some("IMAGE"), Some(8))).await;
    assert_eq!((listed(&rig, "visual_media", &visual_media), listed(&rig, "visual_since", &visual_since)), (vec!["m1".to_string()], Vec::<String>::new()));
}
