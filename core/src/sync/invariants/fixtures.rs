use serde_json::json;

use crate::identity::RowId;
use crate::transport::Reply;

pub fn rid(text: &str) -> RowId {
    RowId::new(text).unwrap()
}

pub fn created(id: &str, text: &str) -> Reply {
    Reply::data(json!({"createMoment": {"id": id, "text": text, "userId": null}}))
}

pub fn deleted(id: &str) -> Reply {
    Reply::data(json!({"deleteMoment": {"id": id}}))
}

pub fn updated(id: &str, text: &str) -> Reply {
    Reply::data(json!({"updateMoment": {"id": id, "text": text, "userId": null}}))
}

pub fn start(db: &crate::db::Db, seq: i64) {
    apply(db, crate::commit::OperationTransition::Start { seq });
}

pub fn fail(db: &crate::db::Db, seq: i64, status: crate::operation::OperationStatus) {
    let failure = crate::syncstate::OperationFailure::new(crate::syncstate::FailureKind::Rejected, "no");
    apply(db, crate::commit::OperationTransition::Fail { seq, status, failure });
}

fn apply(db: &crate::db::Db, transition: crate::commit::OperationTransition) {
    let mut envelope = crate::commit::CommitEnvelope::new(crate::commit::WriteOrigin::Response, db.generation());
    envelope.operation_transitions.push(transition);
    db.shared.store.commit(envelope).unwrap();
}

pub fn names(rig: &crate::sync::rig::Rig) -> Vec<String> {
    rig.transport.calls().into_iter().map(|call| call.operation_name).collect()
}
