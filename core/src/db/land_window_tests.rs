use super::fixtures::{calls, obj, recorder, rid};
use crate::owners::read::windows::WindowKey;
use crate::owners::store::{RowValue, StoreOp};
use serde_json::{json, Value};

fn page(relation: &str, rows: &[(&str, Value)], replace_all: bool, base_revision: i64) -> StoreOp {
    let key = WindowKey::relation("moments", relation, &obj(json!({})));
    let roots = rows.iter().map(|(id, edge)| RowValue {
        row_id: (*id).into(),
        values: json!({"id": id, "text": "same", "@edge": edge}).to_string(),
    }).collect();
    StoreOp::Window { key: key.name, model: key.model, page: 1, replace_all, cursor: None, roots, related: Vec::new(), total: None, base_revision }
}

fn edges(db: &super::Db, relation: &str, ids: &[&str]) -> Value {
    let ids: Vec<String> = ids.iter().map(|id| (*id).to_string()).collect();
    serde_json::to_value(db.edge_values("moments", relation, &obj(json!({})), &ids).unwrap()).unwrap()
}

fn database() -> super::Db {
    super::Db::open_in_memory(crate::schema_fixtures::specs(), None).unwrap()
}

#[test]
fn store_prop_edge_values() {
    let db = database();
    db.write_queue_ops(&[page("feed", &[("m1", json!({"impressionUuid": "a", "impressionScore": 2})), ("m2", json!({"impressionUuid": "b"}))], true, 0)]).unwrap();
    let before = db.shared.store.read(&["moments"], |reader| reader.stored_row("moments", "m1")).unwrap().unwrap();
    let (log, callback) = recorder();
    let _subscription = db.observe(crate::query::Query::new("moments"), callback).unwrap();
    db.await_revision(db.revision_seq().unwrap());
    let revision = db.revision_seq().unwrap();
    db.write_queue_ops(&[page("foreground_feed", &[("m1", json!({"impressionUuid": "other"}))], true, revision)]).unwrap();
    assert_eq!(edges(&db, "feed", &["m1"])["m1"]["impressionUuid"], "a");
    let revision = db.revision_seq().unwrap();
    db.write_queue_ops(&[page("feed", &[("m1", json!({"impressionUuid": null, "impressionScore": 3}))], true, revision)]).unwrap();
    assert_eq!(edges(&db, "feed", &["m1", "m2"]), json!({"m1": {"impressionUuid": null, "impressionScore": 3}}));
    assert_eq!(edges(&db, "foreground_feed", &["m1"])["m1"]["impressionUuid"], "other");
    db.await_revision(db.revision_seq().unwrap());
    assert_eq!(calls(&log), vec![2]);
    let after = db.shared.store.read(&["moments"], |reader| reader.stored_row("moments", "m1")).unwrap().unwrap();
    assert_eq!(before.row, after.row);
    assert_eq!(before.revision, after.revision);
    assert!(db.find("moments", &rid("m2")).unwrap().is_some());
}

#[test]
fn store_prop_edge_read() {
    let db = database();
    let rows: Vec<_> = (0..200).map(|id| (format!("m{id}"), json!({}))).collect();
    let borrowed: Vec<_> = rows.iter().map(|(id, value)| (id.as_str(), value.clone())).collect();
    db.write_queue_ops(&[page("feed", &borrowed, true, 0)]).unwrap();
    let before = db.counters();
    assert_eq!(edges(&db, "feed", &["m1", "absent"]), json!({"m1": {"impressionUuid": null, "impressionScore": null}}));
    let after = db.counters();
    assert_eq!(after.edge_read_scans - before.edge_read_scans, 0);
    assert!(after.edge_read_steps - before.edge_read_steps < 100);
    db.clear_models(&["moments"]).unwrap();
    assert_eq!(edges(&db, "feed", &["m1"]), json!({}));
}
