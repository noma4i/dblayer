use serde_json::json;

use super::super::rig::*;
use super::super::SyncConfig;
use super::fixtures::*;
use super::spec_fixtures::*;
use crate::db::Db;
use crate::query::Query;
use crate::transport::{MockTransport, Reply};

#[tokio::test(start_paused = true)]
async fn inv_id_02_inv_id_03_no_reference_to_the_temp_id_survives_the_confirmation_of_its_create() {
    let rig = rig_on(Db::open_in_memory(specs_with_references(), None).unwrap(), MockTransport::new(), SyncConfig::default());
    let parent = rig.db.create("moments", params(json!({"text": "parent"}))).unwrap();
    let temp = parent.id.to_string();
    rig.db.create("notes", params(json!({"id": "n1", "momentId": temp, "text": "one"}))).unwrap();
    rig.db.update("notes", &rid("n1"), params(json!({"momentId": "m5"}))).unwrap();
    rig.db.create("notes", params(json!({"id": "n2", "momentId": temp, "text": "two"}))).unwrap();
    rig.db.attach("moments", &parent.id, "media", b"bytes", "a.bin", "a/b").unwrap();
    rig.db.set_ephemeral("moments", &parent.id, params(json!({"typing": true}))).unwrap();
    rig.transport.enqueue("CreateMoment", created("m77", "parent"));
    rig.sync.flush().await.unwrap();
    let note = rig.db.find("notes", &rid("n2")).unwrap().unwrap();
    assert_eq!(note.get("momentId"), Some(&json!("m77")));
    assert_eq!(rig.db.read("moments", &rid("m77"), "media").unwrap(), Some(b"bytes".to_vec()));
    assert_eq!(rig.db.find("moments", &rid("m77")).unwrap().unwrap().get("typing"), Some(&json!(true)));
    assert_eq!(mentions(&rig.db, &temp), Vec::<String>::new());
}

#[tokio::test(start_paused = true)]
async fn inv_id_06_a_real_row_that_arrived_before_the_create_answer_leaves_one_row_after_it() {
    let rig = rig();
    let local = rig.db.create("moments", params(json!({"text": "hello"}))).unwrap();
    put(&rig, "moments", moment("m77", "hello"));
    rig.transport.enqueue("CreateMoment", created("m77", "hello"));
    rig.sync.flush().await.unwrap();
    let ids: Vec<String> = rig.db.query(&Query::new("moments")).unwrap().iter().map(|row| row.id.to_string()).collect();
    assert_eq!(ids, ["m77"]);
    assert!(rig.db.find("moments", &local.id).unwrap().is_none());
    assert!(rig.db.outbox().unwrap().is_empty());
}

#[tokio::test(start_paused = true)]
async fn inv_rollback_07_a_refused_update_queued_on_a_temp_row_restores_the_row_the_create_confirmed() {
    let rig = rig();
    let local = rig.db.create("moments", params(json!({"text": "first"}))).unwrap();
    rig.db.update("moments", &local.id, params(json!({"text": "second"}))).unwrap();
    rig.transport.enqueue("CreateMoment", created("m77", "first"));
    rig.transport.enqueue("UpdateMoment", Reply::graphql_error("no", "FORBIDDEN"));
    rig.sync.flush().await.unwrap();
    let ids: Vec<String> = rig.db.query(&Query::new("moments")).unwrap().iter().map(|row| row.id.to_string()).collect();
    assert_eq!((ids, text_of(&rig, "moments", "m77")), (vec!["m77".to_string()], Some("first".to_string())));
    assert!(rig.db.outbox().unwrap().is_empty());
}
