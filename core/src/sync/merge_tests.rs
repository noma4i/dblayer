use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use serde_json::{json, Value};

use super::rig::*;
use super::subscribe::{EventEffect, SubscriptionDef};
use super::LoadOptions;
use crate::identity::RowId;
use crate::query::Query;
use crate::transport::{EventSpec, Reply, Response, SubscriptionEvent};

fn row(rig: &Rig, model: &str, id: &str) -> crate::db::Row {
    rig.db.find(model, &RowId::new(id).unwrap()).unwrap().unwrap()
}

fn enqueue_pages(rig: &Rig, pages: Vec<Value>) {
    for node in pages {
        rig.transport.enqueue("MomentsFeed", Reply::data(feed_page(vec![node], false, "c")));
    }
}

async fn load_page(rig: &Rig) {
    rig.sync.load("moments", "feed", &serde_json::Map::new(), LoadOptions { force: true }).await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn a_narrower_response_keeps_the_fields_it_does_not_select_on_every_delivery_path() {
    for path in ["load", "next page", "mutation"] {
        let rig = rig();
        let kept: Vec<(&str, Value)> = match path {
            "load" => {
                enqueue_pages(&rig, vec![json!({"id": "m1", "text": "first", "likedByMe": true, "userId": "u1"}), json!({"id": "m1", "mood": "calm"})]);
                load_page(&rig).await;
                load_page(&rig).await;
                vec![("text", json!("first")), ("likedByMe", json!(true)), ("userId", json!("u1"))]
            }
            "next page" => {
                rig.transport.enqueue("MomentsFeed", Reply::data(feed_page(vec![json!({"id": "m1", "text": "first", "userId": "u1"})], true, "c1")));
                rig.transport.enqueue("MomentsFeed", Reply::data(feed_page(vec![json!({"id": "m1", "mood": "calm"})], false, "c2")));
                rig.sync.load("moments", "feed", &serde_json::Map::new(), LoadOptions::default()).await.unwrap();
                rig.sync.load_more("moments", "feed", &serde_json::Map::new()).await.unwrap();
                vec![("text", json!("first")), ("userId", json!("u1"))]
            }
            _ => {
                put(&rig, "moments", json!({"id": "m1", "text": "old", "userId": null, "likedByMe": true}));
                rig.db.update("moments", &RowId::new("m1").unwrap(), params(json!({"text": "new"}))).unwrap();
                rig.transport.enqueue("UpdateMoment", Reply::data(json!({"updateMoment": {"id": "m1", "mood": "calm"}})));
                rig.sync.flush().await.unwrap();
                vec![("text", json!("new")), ("likedByMe", json!(true))]
            }
        };
        let stored = row(&rig, "moments", "m1");
        for (field, value) in kept {
            assert_eq!(stored.get(field), Some(&value), "{path}: {field}");
        }
        assert_eq!(stored.get("mood"), Some(&json!("calm")), "{path}: mood");
    }
}

#[tokio::test(start_paused = true)]
async fn a_nested_row_keeps_the_fields_its_fragment_does_not_select_from_a_page_and_from_an_event() {
    for source in ["page", "event"] {
        let rig = rig();
        if source == "page" {
            enqueue_pages(&rig, vec![json!({"id": "m1", "author": {"id": "u1", "name": "Ann"}}), json!({"id": "m1", "author": {"id": "u1"}})]);
            load_page(&rig).await;
            load_page(&rig).await;
        } else {
            rig.sync.register(super::OperationDef::new("MomentChanged", "subscription MomentChanged { x }", "momentChanged"));
            put(&rig, "users", json!({"id": "u1", "name": "Ann"}));
            let _subscription = rig.sync.subscribe(SubscriptionDef::new(EventSpec::new("moment_changed"), "MomentChanged", "moments", EventEffect::Upsert)).await.unwrap();
            let event = json!({"momentChanged": {"id": "m1", "text": "t", "author": {"id": "u1"}}});
            rig.transport.emit("MomentChanged", SubscriptionEvent::Next(Response::data(event)));
            rig.transport.await_consumed("MomentChanged").await;
        }
        assert_eq!(row(&rig, "users", "u1").get("name"), Some(&json!("Ann")), "{source}");
    }
}

#[tokio::test(start_paused = true)]
async fn a_field_selected_as_null_becomes_absent_and_the_others_stay() {
    let rig = rig();
    enqueue_pages(&rig, vec![json!({"id": "m1", "text": "first", "userId": "u1"}), json!({"id": "m1", "userId": null})]);
    load_page(&rig).await;
    load_page(&rig).await;
    let stored = row(&rig, "moments", "m1");
    assert_eq!((stored.get("userId"), stored.get("text")), (None, Some(&json!("first"))));
}

#[tokio::test(start_paused = true)]
async fn an_observer_gets_a_diff_of_the_changed_fields_only_and_nothing_for_equal_values() {
    let rig = rig();
    enqueue_pages(&rig, vec![json!({"id": "m1", "text": "a", "mood": "calm"}), json!({"id": "m1", "text": "b"}), json!({"id": "m1", "text": "b"})]);
    load_page(&rig).await;
    let deliveries = Arc::new(AtomicUsize::new(0));
    let counter = deliveries.clone();
    let _observer = rig.db.observe(Query::new("moments"), move |_| {
        counter.fetch_add(1, Ordering::SeqCst);
    }).unwrap();
    rig.db.await_revision(rig.db.revision_seq().unwrap());
    let before = rig.db.shared.store.read(&[], |reader| reader.stored_row("moments", "m1")).unwrap().unwrap();
    load_page(&rig).await;
    rig.db.await_revision(rig.db.revision_seq().unwrap());
    let after = rig.db.shared.store.read(&[], |reader| reader.stored_row("moments", "m1")).unwrap().unwrap();
    assert_eq!(deliveries.load(Ordering::SeqCst), 2);
    assert_eq!(after.field_rev["mood"], before.field_rev["mood"]);
    assert!(after.field_rev["text"] > before.field_rev["text"]);
    load_page(&rig).await;
    rig.db.await_revision(rig.db.revision_seq().unwrap());
    assert_eq!(deliveries.load(Ordering::SeqCst), 2);
}
