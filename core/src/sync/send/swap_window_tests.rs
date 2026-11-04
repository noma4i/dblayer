use std::sync::{Arc, Mutex};

use serde_json::json;

use crate::query::Query;
use crate::transport::Reply;

use super::super::rig::*;

type Ids = Vec<String>;

fn ids(rows: &[crate::db::Row]) -> Ids {
    rows.iter().map(|row| row.id.to_string()).collect()
}

#[tokio::test(start_paused = true)]
async fn readers_opened_between_the_commit_and_its_announcement_of_a_swap_never_see_an_empty_set() {
    let rig = rig();
    let temp = rig.db.as_app().create("moments", params(json!({"text": "hello"}))).unwrap().id.to_string();
    let query = Query::new("moments");
    rig.db.warm(&query).unwrap();
    let warm_seen: Arc<Mutex<Vec<Ids>>> = Arc::default();
    let first_seen: Arc<Mutex<Vec<Ids>>> = Arc::default();
    let full_seen: Arc<Mutex<Vec<Ids>>> = Arc::default();
    let handles = Arc::new(Mutex::new(Vec::new()));
    let (db, probe_query) = (rig.db.clone(), query.clone());
    let (warm, first, full, held) = (warm_seen.clone(), first_seen.clone(), full_seen.clone(), handles.clone());
    *rig.db.shared.store.after_commit.lock().unwrap() = Some(Box::new(move |result| {
        if result.replaced.is_empty() {
            return;
        }
        warm.lock().unwrap().push(db.peek_query(&probe_query).map(|peeked| ids(&peeked.rows)).unwrap_or_default());
        full.lock().unwrap().push(ids(&db.query(&probe_query).unwrap()));
        let sink = first.clone();
        held.lock().unwrap().push(db.observe(probe_query.clone(), move |change| sink.lock().unwrap().push(ids(&change.rows))).unwrap());
    }));
    rig.transport.enqueue("CreateMoment", Reply::data(json!({"createMoment": {"id": "m77", "text": "hello", "userId": null}})));
    crate::sync::rig::app(&rig.sync, rig.sync.flush()).await.unwrap();
    rig.db.await_revision(rig.db.revision_seq().unwrap());
    let after = vec!["m77".to_string()];
    let before = vec![temp];
    for (name, seen) in [("warm peek", &warm_seen), ("full read", &full_seen), ("observer first value", &first_seen)] {
        let seen = seen.lock().unwrap();
        assert!(!seen.is_empty(), "{name}: nothing was read");
        assert!(seen.iter().all(|rows| *rows == before || *rows == after), "{name}: {seen:?}");
    }
}
