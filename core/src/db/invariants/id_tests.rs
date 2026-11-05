use serde_json::json;

use crate::commit::OperationTransition;
use crate::db::fixtures::*;
use crate::db::{Db, DbError};
use crate::identity::RowId;
use crate::owners::store::{EffectRow, IntentEffects, RowValue, StoreOp};
use crate::query::Query;

use super::fixtures::apply;

fn mentions(db: &Db, needle: &str) -> Vec<String> {
    let store = db.shared.store.lock();
    let connection = &store.connection;
    let tables: Vec<String> = connection
        .prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    let mut found = Vec::new();
    for table in tables.iter().filter(|table| !table.contains("tombstone")) {
        let columns: Vec<String> = connection.prepare(&format!("SELECT name FROM pragma_table_info('{table}')")).unwrap().query_map([], |row| row.get(0)).unwrap().collect::<Result<_, _>>().unwrap();
        for column in columns {
            let hits: i64 = connection.query_row(&format!("SELECT COUNT(*) FROM \"{table}\" WHERE CAST(\"{column}\" AS TEXT) LIKE ?1"), [format!("%{needle}%")], |row| row.get(0)).unwrap();
            if hits > 0 {
                found.push(format!("{table}.{column}"));
            }
        }
    }
    found
}

fn confirm_create(db: &Db, temp: &RowId, real: &str, seq: i64) {
    let row = RowValue { row_id: real.into(), values: json!({"id": real, "userId": "u1", "text": "a"}).to_string() };
    db.write_queue_ops(&[
        StoreOp::Rows { model: "moments".into(), rows: vec![row] },
        StoreOp::Swap { model: "moments".into(), temp: temp.to_string(), real: real.into() },
        StoreOp::RemoveIntent { op_id: u64::try_from(seq).unwrap() },
    ])
    .unwrap();
}

fn with_temp_moment() -> (Db, RowId, i64) {
    let db = booted();
    put(&db, "users", json!({"id": "u1", "name": "Ann"}));
    let moment = db.create(&db.admit().unwrap(), "moments", obj(json!({"userId": "u1", "text": "a"}))).unwrap();
    let seq = db.outbox().unwrap()[0].operation.id.0;
    (db, moment.id.clone(), seq)
}

#[test]
fn inv_id_02_inv_id_03_no_reference_to_the_temp_id_survives_the_confirmation_of_its_create() {
    let (db, temp, seq) = with_temp_moment();
    let vibe = db.create(&db.admit().unwrap(), "vibes", obj(json!({"momentId": temp.as_str(), "kind": "like"}))).unwrap();
    assert!(!mentions(&db, temp.as_str()).is_empty(), "the temp id is stored before the confirmation");
    confirm_create(&db, &temp, "m9", seq);
    assert_eq!(db.find("vibes", &vibe.id).unwrap().unwrap().text("momentId"), Some("m9"));
    assert!(db.find("moments", &temp).unwrap().is_none());
    assert_eq!(mentions(&db, temp.as_str()), Vec::<String>::new());
}

#[test]
fn inv_id_05_an_update_queued_behind_a_create_follows_the_real_id() {
    let (db, temp, seq) = with_temp_moment();
    db.update(&db.admit().unwrap(), "moments", &temp, obj(json!({"text": "second"}))).unwrap();
    confirm_create(&db, &temp, "m9", seq);
    let queue = db.outbox().unwrap();
    assert_eq!(queue.len(), 1);
    assert!(queue[0].operation.row_ids.iter().all(|id| id.as_str() == "m9"), "{:?}", queue[0].operation.row_ids);
}

#[test]
fn inv_id_06_a_real_row_that_arrived_before_the_create_answer_leaves_one_row_after_it() {
    let (db, temp, seq) = with_temp_moment();
    put(&db, "moments", json!({"id": "m9", "userId": "u1", "text": "a"}));
    confirm_create(&db, &temp, "m9", seq);
    assert!(db.find("moments", &temp).unwrap().is_none());
    assert_eq!(db.count(&Query::new("moments")).unwrap(), 1);
}

fn correlating() -> Db {
    let mut messages = messages_spec();
    let mut send = crate::schema::ActionDeclaration::new("send", crate::schema::RootShape::Upsert);
    send.correlate = vec!["body".into()];
    messages.actions.push(send);
    let specs = all_specs().into_iter().map(|spec| if spec.key.as_str() == "messages" { messages.clone() } else { spec }).collect();
    let db = Db::open_in_memory(specs, None).unwrap();
    db.set_clock(std::sync::Arc::new(crate::pacing::ManualClock::new(std::time::Duration::from_millis(1_000_000))));
    db
}

fn sent_message(db: &Db, seq: u64, body: &str) -> RowId {
    let temp = RowId::generate_temp(None);
    let values = json!({"id": temp.to_string(), "chatId": "c1", "body": body}).to_string();
    let effects = IntentEffects { rows: vec![EffectRow { model: "messages".into(), row_id: temp.to_string(), values }], ..Default::default() };
    db.write_queue_ops(&[StoreOp::Intent { op_id: seq, model: "messages".into(), row_id: temp.to_string(), action: "send".into(), payload: "{}".into(), effects }]).unwrap();
    let seq = i64::try_from(seq).unwrap();
    apply(db, OperationTransition::Start { seq });
    apply(db, OperationTransition::Fail { seq, status: crate::operation::OperationStatus::DeliveryUnknown, failure: crate::syncstate::OperationFailure::new(crate::syncstate::FailureKind::Rejected, "no") });
    temp
}

#[test]
fn inv_id_08_one_matching_candidate_is_replaced_by_the_row_of_the_event_and_several_are_not() {
    let db = correlating();
    let temp = sent_message(&db, 1, "hello");
    put(&db, "messages", json!({"id": "mm1", "chatId": "c1", "body": "hello"}));
    assert!(db.find("messages", &temp).unwrap().is_none(), "the temp row of the sent message is replaced");
    assert!(db.find("messages", &rid("mm1")).unwrap().is_some());
    let (first, second) = (sent_message(&db, 2, "dup"), sent_message(&db, 3, "dup"));
    put(&db, "messages", json!({"id": "mm2", "chatId": "c1", "body": "dup"}));
    assert!(db.find("messages", &first).unwrap().is_some() && db.find("messages", &second).unwrap().is_some(), "two candidates match: none is replaced");
    assert!(matches!(db.create(&db.admit().unwrap(), "messages", obj(json!({"id": "mm2", "chatId": "c1", "body": "dup"}))), Err(DbError::AlreadyExists { .. })));
}
