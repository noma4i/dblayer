use std::time::Duration;

use serde_json::json;

use crate::commit::{CommitEnvelope, EntityOp, WriteOrigin};
use crate::db::Db;
use crate::identity::{ModelKey, RowId};
use crate::transport::{MockTransport, Reply};

use super::super::rig::*;
use super::super::SyncConfig;

fn rid(text: &str) -> RowId {
    RowId::new(text).unwrap()
}

fn correlating_rig() -> Rig {
    let mut moments = moments_spec();
    moments.actions.iter_mut().find(|action| action.name == "create").unwrap().correlate = vec!["text".to_string(), "userId".to_string()];
    rig_on(Db::open_in_memory(vec![users_spec(), moments, vibes_spec()], None).unwrap(), MockTransport::new(), SyncConfig::default())
}

fn landed(rig: &Rig, id: &str, text: &str, user: serde_json::Value) {
    let row = rig.db.spec("moments").unwrap().patch(&params(json!({"id": id, "text": text, "userId": user})));
    let mut envelope = CommitEnvelope::new(WriteOrigin::Event, rig.db.generation());
    envelope.entity_ops.push(EntityOp::Upsert { model: ModelKey::new("moments"), id: rid(id), row });
    rig.db.land(envelope).unwrap();
}

fn ids(rig: &Rig) -> Vec<String> {
    let mut found: Vec<String> = rig.db.query(&crate::query::Query::new("moments")).unwrap().into_iter().map(|row| row.id.to_string()).collect();
    found.sort();
    found
}

#[tokio::test(start_paused = true)]
async fn a_row_that_lands_before_the_answer_replaces_the_sent_temp_row() {
    let rig = correlating_rig();
    put(&rig, "users", json!({"id": "u1", "name": "Ann"}));
    let temp = rig.db.create("moments", params(json!({"text": "hi", "userId": "u1"}))).unwrap();
    rig.transport.enqueue("CreateMoment", Reply::data(json!({"createMoment": {"id": "m9", "text": "hi", "userId": "u1"}})).after(Duration::from_secs(1)));
    let flush = tokio::spawn({
        let sync = rig.sync.clone();
        async move { sync.flush().await }
    });
    let_tasks_run().await;
    landed(&rig, "m9", "hi", json!("u1"));
    assert_eq!(ids(&rig), ["m9"]);
    assert!(rig.db.find("moments", &temp.id).unwrap().is_none());
    tokio::time::sleep(Duration::from_secs(2)).await;
    flush.await.unwrap().unwrap();
    assert_eq!(ids(&rig), ["m9"]);
    assert!(rig.db.outbox().unwrap().is_empty());
}

#[tokio::test(start_paused = true)]
async fn a_row_with_other_fields_or_a_record_not_sent_yet_keeps_the_temp_row() {
    let rig = correlating_rig();
    let temp = rig.db.create("moments", params(json!({"text": "hi", "userId": "u1"}))).unwrap();
    landed(&rig, "m8", "hi", json!("u1"));
    assert_eq!(ids(&rig).len(), 2);
    rig.transport.enqueue("CreateMoment", Reply::data(json!({"createMoment": {"id": "m9", "text": "hi", "userId": "u1"}})).after(Duration::from_secs(1)));
    let flush = tokio::spawn({
        let sync = rig.sync.clone();
        async move { sync.flush().await }
    });
    let_tasks_run().await;
    landed(&rig, "m7", "other", json!("u1"));
    landed(&rig, "m6", "hi", json!(null));
    assert!(rig.db.find("moments", &temp.id).unwrap().is_some());
    flush.abort();
}
