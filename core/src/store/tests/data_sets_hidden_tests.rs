use std::sync::Arc;

use serde_json::{json, Value};

use crate::owners::contract::BatchAuthor;
use crate::owners::store::{RowValue, StoreOp};
use crate::store::fixtures::{all_specs, put};
use crate::store::Store;

use super::data_sets_tests::*;

fn store_without_newer() -> Arc<Store> {
    let specs = all_specs().into_iter().map(|mut spec| {
        if spec.key.as_str() == "messages" {
            spec.associations.iter_mut().for_each(|association| association.newer.clear());
        }
        spec
    }).collect();
    let db = Arc::new(Store::open_models(None, specs, None, Arc::new(crate::runtime::SystemClock)).unwrap());
    put(&db, "chats", json!({"id": "c1", "title": "t"}));
    db
}

#[test]
fn data_sets_newest_read_is_bounded() {
    let read = |db: Arc<Store>, count: i64| {
        let rows = (1..=count).map(|id| message(&id.to_string(), 5)).collect();
        land_children(&db, rows);
        let before = db.counters().pointer_rows_read;
        let edited = RowValue { row_id: "1".into(), values: json!({"id": "1", "chatId": "c1", "body": "edited", "createdAt": 5}).to_string() };
        land_children(&db, vec![edited]);
        let winner = chat(&db)[0].clone();
        assert_eq!(winner, Value::String(count.to_string()), "{count} children: the newest by the order of ids is the largest number");
        db.counters().pointer_rows_read - before
    };
    for (name, make) in [("newer with equal values", open as fn() -> Arc<Store>), ("no newer", store_without_newer)] {
        let (few, many) = (read(make(), 20), read(make(), 200));
        assert!(few <= 2 && many == few, "{name}: the rows read for one change do not depend on the number of children: {few} for 20, {many} for 200");
    }
}
