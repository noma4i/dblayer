use serde_json::{json, Value};

use super::super::rig::*;
use super::super::subscribe::{EventEffect, SubscriptionDef};
use super::event_tests::{event, rig_with_events};
use super::fixtures::*;
use crate::sync::LoadOptions;
use crate::transport::{EventSpec, Reply};

const FIELDS: [&str; 3] = ["userId", "text", "likedByMe"];

#[derive(Clone, Copy, Debug)]
enum Path {
    Load,
    Upsert,
    Patch,
    Response,
}

fn base() -> Value {
    json!({"id": "m1", "userId": "u1", "text": "keep", "likedByMe": false})
}

fn replacement(field: &str) -> Value {
    match field {
        "userId" => json!("u2"),
        "text" => json!("new"),
        _ => json!(true),
    }
}

fn answer(modes: &[u8; 3]) -> Value {
    let mut node = json!({"id": "m1"});
    for (field, mode) in FIELDS.iter().zip(modes) {
        match mode {
            1 => node[*field] = replacement(field),
            2 => node[*field] = Value::Null,
            _ => {}
        }
    }
    node
}

fn expected(field: &str, mode: u8) -> Option<Value> {
    match mode {
        0 => base().get(field).cloned(),
        1 => Some(replacement(field)),
        _ => None,
    }
}

async fn land_through(path: Path, node: &Value) -> Rig {
    let rig = rig_with_events();
    put(&rig, "users", json!({"id": "u1", "name": "Ann"}));
    put(&rig, "moments", base());
    match path {
        Path::Load => {
            rig.transport.enqueue("MomentsFeed", Reply::data(feed_page(vec![node.clone()], false, "c1")));
            rig.sync.load("moments", "feed", &params(json!({})), LoadOptions { force: true }).await.unwrap();
        }
        Path::Upsert | Path::Patch => {
            let effect = if matches!(path, Path::Upsert) { EventEffect::Upsert } else { EventEffect::Patch };
            let _subscription = rig.sync.subscribe(SubscriptionDef::new(EventSpec::new("moment_changed"), "MomentChanged", "moments", effect)).await.unwrap();
            rig.transport.emit("MomentChanged", event(node.clone()));
            rig.transport.await_consumed("MomentChanged").await;
        }
        Path::Response => {
            rig.db.as_app().update("moments", &rid("m1"), params(json!({"mood": "calm"}))).unwrap();
            rig.transport.enqueue("UpdateMoment", Reply::data(json!({"updateMoment": node})));
            crate::sync::rig::app(&rig.sync, rig.sync.flush()).await.unwrap();
        }
    }
    rig
}

#[tokio::test(start_paused = true)]
async fn inv_merge_01_inv_merge_02_inv_event_01_inv_event_02_every_path_applies_the_same_subset_of_fields_and_leaves_the_rest() {
    for path in [Path::Load, Path::Upsert, Path::Patch, Path::Response] {
        for case in 1..27u8 {
            let modes = [case % 3, case / 3 % 3, case / 9];
            let rig = land_through(path, &answer(&modes)).await;
            let row = rig.db.find("moments", &rid("m1")).unwrap().unwrap();
            for (field, mode) in FIELDS.iter().zip(modes) {
                let actual = row.get(field).cloned().filter(|value| !value.is_null());
                assert_eq!(actual, expected(field, mode).filter(|value| !value.is_null()), "{path:?} {modes:?} {field}");
            }
            assert_eq!(rig.db.find("users", &rid("u1")).unwrap().unwrap().text("name"), Some("Ann"), "{path:?} {modes:?} touched the user row");
        }
    }
}
