use std::sync::{Arc, Mutex};

use super::{Config, Database, LiveChange, QueryListener, Transport};
use crate::schema::{FieldSpec, ModelSpec};
use crate::value::ScalarKind;

struct Tape(Mutex<Vec<LiveChange>>);

impl QueryListener for Tape {
    fn changed(&self, change: LiveChange) {
        self.0.lock().unwrap().push(change);
    }
}

fn with_rows(count: usize) -> (Arc<Database>, Vec<String>) {
    let model = ModelSpec::new("Moment", vec![FieldSpec::scalar("id", ScalarKind::Id), FieldSpec::scalar("text", ScalarKind::Str)]);
    let db = Database::open(Config { path: None, data_version: None, cache_rows: 64 }, serde_json::to_string(&vec![model]).unwrap(), None::<Arc<dyn Transport>>).unwrap();
    let ids = (0..count)
        .map(|n| serde_json::from_str::<serde_json::Value>(&db.create("Moment".into(), format!(r#"{{"text":"t{n:05}"}}"#)).unwrap()).unwrap()["id"].as_str().unwrap().to_string())
        .collect();
    (db, ids)
}

#[test]
fn inv_observe_17_a_live_change_of_one_changed_row_carries_one_row_at_every_list_size() {
    let sizes: Vec<(usize, bool)> = [20, 200, 2000]
        .into_iter()
        .map(|count| {
            let (db, ids) = with_rows(count);
            let tape = Arc::new(Tape(Mutex::default()));
            let _live = db.live_query(r#"{"model":"Moment","order":[{"field":"text","direction":"asc"}]}"#.into(), tape.clone()).unwrap();
            db.db.settle();
            assert_eq!(tape.0.lock().unwrap()[0].rows.len(), count, "the first delivery is the whole list");
            db.update("Moment".into(), ids[count / 2].clone(), format!(r#"{{"text":"t{:05}x"}}"#, count / 2)).unwrap();
            db.db.settle();
            let last = tape.0.lock().unwrap().last().unwrap().clone();
            (last.rows.len(), last.full)
        })
        .collect();
    assert_eq!(sizes, vec![(1, false); 3], "(rows on the wire, full) after 1 update at 20, 200, 2000 rows");
}
