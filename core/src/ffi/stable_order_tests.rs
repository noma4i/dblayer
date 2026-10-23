use std::sync::{Arc, Mutex};

use serde_json::json;

use super::{Config, Database, LiveChange, QueryListener, Transport};
use crate::commit::{CommitEnvelope, EntityOp, ScopeOp, ScopeRef, WriteOrigin};
use crate::identity::{ModelKey, RowId};
use crate::sync::stable_order::rig::messages_spec;

struct Tape(Mutex<Vec<LiveChange>>);

impl QueryListener for Tape {
    fn changed(&self, change: LiveChange) {
        self.0.lock().unwrap().push(change);
    }
}

fn land(db: &Database, scope: Option<&ScopeRef>, id: &str, sequence: serde_json::Value) {
    let row = json!({"id": id, "chatId": "c1", "sequenceNumber": sequence, "createdAt": "2026-01-01T00:00:00.000Z"}).as_object().unwrap().clone();
    let mut envelope = CommitEnvelope::new(WriteOrigin::Event, db.db.generation());
    let row_id = RowId::new(id).unwrap();
    envelope.entity_ops.push(EntityOp::Upsert { model: ModelKey::new("messages"), id: row_id.clone(), row });
    envelope.scope_ops.extend(scope.map(|scope| ScopeOp::Append { scope: scope.clone(), row: row_id, order_key: String::new() }));
    db.db.land(envelope).unwrap();
}

#[tokio::test]
async fn a_live_window_over_a_relation_that_keeps_its_order_follows_the_member_order() {
    let db = Database::open(Config { path: None, data_version: None, cache_rows: 64 }, serde_json::to_string(&vec![messages_spec(true)]).unwrap(), None::<Arc<dyn Transport>>, None, None, None).await.unwrap();
    let query = db.db.remote_relation_query("messages", "thread", json!({"chatId": "c1"}).as_object().unwrap()).unwrap().unwrap();
    let scope = ScopeRef::new(ModelKey::new("messages"), query.scope.unwrap());
    for (id, sequence) in [("m1", json!(1)), ("m2", json!(2)), ("m5", json!(5)), ("m9", json!(null))] {
        land(&db, Some(&scope), id, sequence);
    }
    land(&db, None, "m9", json!(3));
    let tape = Arc::new(Tape(Mutex::default()));
    let _live = db.live_window(r#"{"model":"messages","relation":"thread","params":{"chatId":"c1"}}"#.into(), 10, tape.clone());
    db.db.await_revision(db.db.revision_seq().unwrap());
    let shown: Vec<String> = tape.0.lock().unwrap().last().unwrap().rows.iter().map(|row| row.id.clone()).collect();
    assert_eq!(shown, ["m1", "m2", "m5", "m9"]);
}
