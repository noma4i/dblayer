use crate::commit::{CommitEnvelope, OperationStatus, OperationTransition, WriteOrigin};
use crate::db::fixtures::rid;
use crate::db::Db;
use crate::syncstate::{FailureKind, OperationFailure, SyncState};

pub fn apply(db: &Db, transition: OperationTransition) {
    let mut envelope = CommitEnvelope::new(WriteOrigin::Response, db.generation());
    envelope.operation_transitions.push(transition);
    db.shared.store.commit(envelope).unwrap();
}

pub fn only_seq(db: &Db) -> i64 {
    db.outbox().unwrap()[0].operation.id.0
}

pub fn start(db: &Db, seq: i64) {
    apply(db, OperationTransition::Start { seq });
}

pub fn fail(db: &Db, seq: i64, status: OperationStatus) {
    apply(db, OperationTransition::Fail { seq, status, failure: OperationFailure::new(FailureKind::Rejected, "no") });
}

pub fn state_of(db: &Db, model: &str, id: &str) -> SyncState {
    db.sync_info(model, &rid(id)).unwrap().state
}

pub fn snapshot(db: &Db) -> (i64, usize, Option<String>) {
    let text = db.find("moments", &rid("m1")).unwrap().and_then(|row| row.text("text").map(str::to_string));
    (db.revision_seq().unwrap(), db.outbox().unwrap().len(), text)
}
