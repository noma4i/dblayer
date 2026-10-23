use std::sync::Arc;
use std::time::Duration;

use serde_json::Map;

use super::load_tests::rig;
use super::{LiveChange, QueryListener};
use crate::sync::LoadPhase;

struct Quiet;

impl QueryListener for Quiet {
    fn changed(&self, _: LiveChange) {}
}

#[tokio::test]
async fn inv_loadm_12_a_query_the_core_rejects_tells_its_subscriber_in_the_snapshot_and_leaves_the_relation_window_alone() {
    let (db, _) = rig(Duration::ZERO).await;
    let sync = db.sync.as_ref().unwrap();
    let state = sync.relation_state("Moment", "feed", &Map::new()).unwrap();
    let query = r#"{"model":"Moment","relation":"feed","params":{},"where":{"type":"equal","field":"nope","value":1}}"#;
    let rejected = db.live_query(query.into(), Arc::new(Quiet));
    let first = rejected.current().await;
    assert_eq!((first.phase, first.error_kind.as_deref(), first.rows.len()), (Some(super::LivePhase::Failed), Some("invalid"), 0));
    let untouched = state.borrow().clone();
    assert_eq!((untouched.phase, untouched.error.map(|error| error.kind)), (LoadPhase::NotLoaded, None));
}

struct Failures(std::sync::Mutex<Vec<Option<String>>>);

impl super::EntityListener for Failures {
    fn changed(&self, delivery: super::EntityDelivery) {
        self.0.lock().unwrap().push(delivery.failure);
    }
}

#[tokio::test]
async fn inv_observe_24_an_entity_read_failure_reaches_the_foreign_listener() {
    let (db, _) = rig(Duration::ZERO).await;
    let listener = Arc::new(Failures(std::sync::Mutex::new(Vec::new())));
    let live = db.live_entities("Moment".into(), listener.clone()).unwrap();
    db.db.shared.store.fail_next_entity_reads(1);
    live.retain(vec!["m1".into()]).unwrap();
    db.db.settle();
    let told = listener.0.lock().unwrap().clone();
    assert_eq!(told.len(), 1);
    assert!(told[0].is_some(), "deliveries {told:?}");
}
