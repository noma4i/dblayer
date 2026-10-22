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
async fn inv_loadm_12_a_query_the_core_rejects_fails_its_relation_window_as_invalid() {
    let (db, _) = rig(Duration::ZERO).await;
    let sync = db.sync.as_ref().unwrap();
    let state = sync.relation_state("Moment", "feed", &Map::new()).unwrap();
    let query = r#"{"model":"Moment","relation":"feed","params":{},"where":{"type":"equal","field":"nope","value":1}}"#;
    assert!(db.live_query(query.into(), Arc::new(Quiet)).is_err());
    let rejected = state.borrow().clone();
    assert_eq!((rejected.phase, rejected.error.map(|error| error.kind)), (LoadPhase::Failed, Some("invalid")));
}
