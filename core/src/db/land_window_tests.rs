use super::fixtures::{obj, rid};
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
