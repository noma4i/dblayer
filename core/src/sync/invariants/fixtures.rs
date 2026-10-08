use serde_json::json;

use crate::sync::rig::Rig;
use crate::transport::Reply;

pub fn created(id: &str, text: &str) -> Reply {
    Reply::data(json!({"createMoment": {"id": id, "text": text, "userId": null}}))
}

pub fn deleted(id: &str) -> Reply {
    Reply::data(json!({"deleteMoment": {"id": id}}))
}

pub fn names(rig: &Rig) -> Vec<String> {
    rig.transport.calls().into_iter().map(|call| call.operation_name).collect()
}
