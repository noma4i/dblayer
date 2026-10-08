use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use serde_json::json;

use crate::identity::RowId;
use crate::transport::{Reply, TransportError};

use super::super::rig::*;

fn update_texts(rig: &Rig) -> Vec<String> {
    rig.transport.calls().into_iter().filter(|call| call.operation_name == "UpdateMoment").map(|call| call.variables["text"].as_str().unwrap().to_string()).collect()
}

#[tokio::test(start_paused = true)]
async fn a_create_that_is_retried_during_a_pass_does_not_let_its_later_updates_overtake_the_earlier_ones() {
    let rig = rig();
    put(&rig, "moments", moment("m9", "plain"));
    let local = rig.db.create("moments", params(json!({"text": "hello"}))).unwrap();
    rig.db.update("moments", &local.id, params(json!({"text": "one"}))).unwrap();
    rig.db.update("moments", &RowId::new("m9").unwrap(), params(json!({"text": "x"}))).unwrap();
    rig.db.update("moments", &local.id, params(json!({"text": "two"}))).unwrap();
    let creates = Arc::new(AtomicUsize::new(0));
    let (db, seen) = (rig.db.clone(), creates.clone());
    rig.transport.respond_with(move |request| match request.operation_name.as_str() {
        "CreateMoment" if seen.fetch_add(1, Ordering::SeqCst) == 0 => Some(Reply::fail(TransportError::Interrupted("lost".to_string()))),
        "CreateMoment" => Some(Reply::data(json!({"createMoment": {"id": "m77", "text": "hello", "userId": null}}))),
        "UpdateMoment" => {
            if request.variables["id"] == json!("m9") {
                db.retry_all().unwrap();
            }
            Some(Reply::data(json!({"updateMoment": {"id": request.variables["id"], "text": request.variables["text"], "userId": null}})))
        }
        _ => None,
    });
    rig.sync.flush().await.unwrap();
    assert_eq!(update_texts(&rig), ["x"]);
    rig.sync.flush().await.unwrap();
    assert_eq!(update_texts(&rig), ["x", "one", "two"]);
}
