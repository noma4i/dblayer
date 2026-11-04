use serde_json::json;

use crate::transport::Reply;

use super::super::rig::*;

fn created(id: &str, text: &str) -> Reply {
    Reply::data(json!({"createMoment": {"id": id, "text": text, "userId": null}}))
}

fn deleted(id: &str) -> Reply {
    Reply::data(json!({"deleteMoment": {"id": id}}))
}

#[tokio::test(start_paused = true)]
async fn destroying_a_row_before_its_create_is_sent_leaves_no_row_after_both_are_sent() {
    let rig = rig();
    let local = rig.db.create(&rig.db.admit().unwrap(), "moments", params(json!({"text": "hello"}))).unwrap();
    rig.db.destroy(&rig.db.admit().unwrap(), "moments", &local.id).unwrap();
    rig.transport.enqueue("CreateMoment", created("m77", "hello"));
    rig.transport.enqueue("DeleteMoment", deleted("m77"));
    crate::sync::rig::app(&rig.sync, rig.sync.flush(&rig.sync.inner.db.admit().unwrap())).await.unwrap();
    assert!(rig.db.outbox().unwrap().is_empty());
    assert_eq!(text_of(&rig, "moments", "m77"), None);
}

#[tokio::test(start_paused = true)]
async fn destroying_a_row_after_an_update_queued_on_its_unsent_create_leaves_no_row() {
    let rig = rig();
    let local = rig.db.create(&rig.db.admit().unwrap(), "moments", params(json!({"text": "hello"}))).unwrap();
    rig.db.update(&rig.db.admit().unwrap(), "moments", &local.id, params(json!({"text": "edited"}))).unwrap();
    rig.db.destroy(&rig.db.admit().unwrap(), "moments", &local.id).unwrap();
    rig.transport.enqueue("CreateMoment", created("m77", "hello"));
    rig.transport.enqueue("UpdateMoment", Reply::data(json!({"updateMoment": {"id": "m77", "text": "edited", "userId": null}})));
    rig.transport.enqueue("DeleteMoment", deleted("m77"));
    crate::sync::rig::app(&rig.sync, rig.sync.flush(&rig.sync.inner.db.admit().unwrap())).await.unwrap();
    assert!(rig.db.outbox().unwrap().is_empty());
    assert_eq!(text_of(&rig, "moments", "m77"), None);
}

#[tokio::test(start_paused = true)]
async fn destroying_a_synced_row_after_an_unsent_update_leaves_no_row_after_both_are_sent() {
    let rig = rig();
    put(&rig, "moments", moment("m1", "old"));
    let id = crate::identity::RowId::new("m1").unwrap();
    rig.db.update(&rig.db.admit().unwrap(), "moments", &id, params(json!({"text": "edited"}))).unwrap();
    rig.db.destroy(&rig.db.admit().unwrap(), "moments", &id).unwrap();
    rig.transport.enqueue("UpdateMoment", Reply::data(json!({"updateMoment": {"id": "m1", "text": "edited", "userId": null}})));
    rig.transport.enqueue("DeleteMoment", deleted("m1"));
    crate::sync::rig::app(&rig.sync, rig.sync.flush(&rig.sync.inner.db.admit().unwrap())).await.unwrap();
    assert!(rig.db.outbox().unwrap().is_empty());
    assert_eq!(text_of(&rig, "moments", "m1"), None);
}

fn feed_with(id: &str, text: &str) -> Reply {
    Reply::data(feed_page(vec![json!({"id": id, "text": text, "userId": null})], false, "c1"))
}

#[tokio::test(start_paused = true)]
async fn inv_event_06_a_load_that_still_lists_a_row_with_a_queued_destroy_does_not_bring_it_back() {
    let rig = rig();
    put(&rig, "moments", moment("m1", "old"));
    let id = crate::identity::RowId::new("m1").unwrap();
    rig.db.destroy(&rig.db.admit().unwrap(), "moments", &id).unwrap();
    rig.transport.enqueue("MomentsFeed", feed_with("m1", "old"));
    rig.sync.load(&rig.sync.inner.db.admit().unwrap(), "moments", "feed", &params(json!({})), crate::sync::LoadOptions { force: true }).await.unwrap();
    assert_eq!(text_of(&rig, "moments", "m1"), None);
    assert!(member_ids(&rig, "moments", "feed", &params(json!({}))).is_empty());
    rig.transport.enqueue("DeleteMoment", deleted("m1"));
    crate::sync::rig::app(&rig.sync, rig.sync.flush(&rig.sync.inner.db.admit().unwrap())).await.unwrap();
    assert_eq!(text_of(&rig, "moments", "m1"), None);
}

#[tokio::test(start_paused = true)]
async fn inv_event_06_an_event_for_a_row_with_a_queued_destroy_does_not_bring_it_back() {
    let rig = rig();
    put(&rig, "moments", moment("m1", "old"));
    let id = crate::identity::RowId::new("m1").unwrap();
    rig.db.destroy(&rig.db.admit().unwrap(), "moments", &id).unwrap();
    put(&rig, "moments", moment("m1", "pushed"));
    assert_eq!(text_of(&rig, "moments", "m1"), None);
}

#[tokio::test(start_paused = true)]
async fn a_refused_destroy_still_shows_the_row_again() {
    let rig = rig();
    put(&rig, "moments", moment("m1", "old"));
    let id = crate::identity::RowId::new("m1").unwrap();
    rig.db.destroy(&rig.db.admit().unwrap(), "moments", &id).unwrap();
    rig.transport.enqueue("DeleteMoment", Reply::graphql_error("no", "FORBIDDEN"));
    crate::sync::rig::app(&rig.sync, rig.sync.flush(&rig.sync.inner.db.admit().unwrap())).await.unwrap();
    assert_eq!(text_of(&rig, "moments", "m1"), Some("old".to_string()));
}

#[tokio::test(start_paused = true)]
async fn a_load_after_the_create_landed_does_not_bring_back_a_row_whose_destroy_is_still_queued() {
    let rig = rig();
    let local = rig.db.create(&rig.db.admit().unwrap(), "moments", params(json!({"text": "hello"}))).unwrap();
    rig.transport.enqueue("CreateMoment", created("m77", "hello").after(std::time::Duration::from_secs(5)));
    rig.transport.enqueue("UpdateMoment", Reply::fail(crate::transport::TransportError::Network("down".to_string())));
    let queue_behind_the_create = async {
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        rig.db.update(&rig.db.admit().unwrap(), "moments", &local.id, params(json!({"text": "edited"}))).unwrap();
        rig.db.destroy(&rig.db.admit().unwrap(), "moments", &local.id).unwrap();
    };
    let (flushed, ()) = tokio::join!(crate::sync::rig::app(&rig.sync, rig.sync.flush(&rig.sync.inner.db.admit().unwrap())), queue_behind_the_create);
    flushed.unwrap();
    assert!(!rig.db.outbox().unwrap().is_empty());
    assert_eq!(text_of(&rig, "moments", "m77"), None);
    rig.transport.enqueue("MomentsFeed", feed_with("m77", "edited"));
    rig.sync.load(&rig.sync.inner.db.admit().unwrap(), "moments", "feed", &params(json!({})), crate::sync::LoadOptions { force: true }).await.unwrap();
    assert_eq!(text_of(&rig, "moments", "m77"), None);
}

#[tokio::test(start_paused = true)]
async fn the_real_row_that_landed_before_the_create_response_goes_away_with_the_queued_destroy() {
    let rig = rig();
    let local = rig.db.create(&rig.db.admit().unwrap(), "moments", params(json!({"text": "hello"}))).unwrap();
    rig.transport.enqueue("CreateMoment", created("m77", "hello").after(std::time::Duration::from_secs(5)));
    rig.transport.enqueue("DeleteMoment", deleted("m77"));
    let destroy_then_land = async {
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        rig.db.destroy(&rig.db.admit().unwrap(), "moments", &local.id).unwrap();
        put(&rig, "moments", moment("m77", "hello"));
    };
    let (flushed, ()) = tokio::join!(crate::sync::rig::app(&rig.sync, rig.sync.flush(&rig.sync.inner.db.admit().unwrap())), destroy_then_land);
    flushed.unwrap();
    assert!(rig.db.outbox().unwrap().is_empty());
    assert_eq!(text_of(&rig, "moments", "m77"), None);
}

#[tokio::test(start_paused = true)]
async fn a_refused_destroy_of_a_row_that_was_created_meanwhile_brings_back_the_real_row_not_the_temp_one() {
    let rig = rig();
    let local = rig.db.create(&rig.db.admit().unwrap(), "moments", params(json!({"text": "hello"}))).unwrap();
    rig.transport.enqueue("CreateMoment", created("m77", "hello").after(std::time::Duration::from_secs(5)));
    rig.transport.enqueue("DeleteMoment", Reply::graphql_error("no", "FORBIDDEN"));
    let destroy = async {
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        rig.db.destroy(&rig.db.admit().unwrap(), "moments", &local.id).unwrap();
    };
    let (flushed, ()) = tokio::join!(crate::sync::rig::app(&rig.sync, rig.sync.flush(&rig.sync.inner.db.admit().unwrap())), destroy);
    flushed.unwrap();
    assert_eq!(rig.db.find("moments", &local.id).unwrap(), None);
    assert_eq!(text_of(&rig, "moments", "m77"), Some("hello".to_string()));
    assert_eq!(rig.db.sync_info("moments", &crate::identity::RowId::new("m77").unwrap()).unwrap().state, crate::syncstate::SyncState::Failed);
}
