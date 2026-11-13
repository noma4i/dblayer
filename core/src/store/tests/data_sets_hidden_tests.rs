use std::sync::Arc;

use serde_json::{json, Value};

use crate::owners::store::RowValue;
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

#[test]
fn data_sets_destroy_ranking_reads_by_index_whatever_the_children() {
    use rusqlite::{params_from_iter, StatementStatus};
    let spec = all_specs().into_iter().find(|spec| spec.key.as_str() == "messages").unwrap();
    let association = spec.associations.iter().find(|association| !association.sets.is_empty()).unwrap().clone();
    let table = crate::store::ddl::table_name("messages");
    let work = |count: i64| {
        let db = open();
        land_children(&db, (1..=count).map(|id| message(&id.to_string(), 5)).collect());
        db.read(&["messages"], |reader| {
            let connection = reader.connection();
            let winner = crate::store::follow_pointer::winner_sql(&table, &association, false);
            let contest = crate::store::follow_pointer::contest_sql(&table, &association);
            let mut steps = 0;
            for (sql, params) in [(&winner, vec!["c1".to_string()]), (&contest, vec![count.to_string(), "0".into(), "5".into(), "5".into()])] {
                let mut statement = connection.prepare(sql).unwrap();
                let _: Option<String> = statement.query_row(params_from_iter(&params), |row| row.get(0)).ok();
                steps += statement.get_status(StatementStatus::VmStep) + statement.get_status(StatementStatus::Sort);
            }
            Ok(steps)
        }).unwrap()
    };
    let (few, many) = (work(3), work(300));
    assert_eq!(few, many, "the work of ranking a destroyed child does not depend on the number of children: {few} for 3, {many} for 300");
}
